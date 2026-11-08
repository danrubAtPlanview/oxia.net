// =====================================================================================================================
// PORT NOTE (tests / diagnostics)
//   Upstream : (none)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : An OPT-IN diagnostic (set the environment variable OXIA_Z1_PROFILE=1, run in Release): runs one scenario of
//              EndToEndAllocationTests over FakeWireInvoker with an in-process EventListener on the runtime's GC allocation ticks and
//              prints which TYPES the bytes per operation belong to.  It is how the allocation sources in docs/performance-allocation.md were
//              found; without the variable the test returns immediately (it asserts nothing).
//   Divergences: n/a (diagnostic).
// =====================================================================================================================
using System.Diagnostics.Tracing;
using System.Threading.Channels;
using Oxia.Client.Pooled;
using Xunit;

namespace Oxia.Client.Tests.Alloc;

[Collection("Z1 serial")]
public class AllocationProfileTests(ITestOutputHelper output)
{
    private sealed class TickListener : EventListener
    {
        private readonly Dictionary<string, long> _bytes = new();
        private volatile bool _on;
        private long _total;

        protected override void OnEventSourceCreated(EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime") EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1);
        }

        public void Start() => _on = true;
        public void Stop() => _on = false;

        protected override void OnEventWritten(EventWrittenEventArgs e)
        {
            if (!_on || e.EventName is null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal)) return;
            string type = "?";
            long amount = 0;
            for (int i = 0; i < e.PayloadNames!.Count; i++)
            {
                if (e.PayloadNames[i] == "TypeName") type = (string?)e.Payload![i] ?? "?";
                else if (e.PayloadNames[i] == "AllocationAmount64") amount = (long)Convert.ToUInt64(e.Payload![i]);
            }
            lock (_bytes)
            {
                _bytes[type] = _bytes.GetValueOrDefault(type) + amount;
                _total += amount;
            }
        }

        public string Report(long ops)
        {
            Thread.Sleep(400);
            lock (_bytes)
            {
                var lines = _bytes.OrderByDescending(kv => kv.Value).Where(kv => kv.Value / (double)ops >= 0.5)
                    .Select(kv => $"   {kv.Value / (double)ops,8:F1} B/op  {kv.Key}");
                return $"sampled {_total / (double)ops:F1} B/op\n" + string.Join("\n", lines);
            }
        }
    }

    private static bool Enabled =>
#if DEBUG
        false;
#else
        Environment.GetEnvironmentVariable("OXIA_Z1_PROFILE") == "1";
#endif

    private async Task Profile(string name, Func<int, Task<long>> rounds)
    {
        await rounds(400);
        for (int pause = 0; pause < 4; pause++)
        {
            await rounds(300);
            await Task.Delay(120);
        }

        using var listener = new TickListener();
        GC.Collect();
        listener.Start();
        long ops = await rounds(30_000);
        listener.Stop();
        output.WriteLine($"== {name}: {listener.Report(ops)}");
    }

    [Fact]
    public async Task ProfileGets()
    {
        if (!Enabled) return;
        const int window = 12;
        var wire = new FakeWireClient(window);
        wire.Invoker.GetsNotFound = true;
        var tasks = new ValueTask<GetResult>[window];
        var keys = Enumerable.Range(0, window).Select(i => "/bench/key-" + i).ToArray();
        await Profile("get (not found)", async rounds =>
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int i = 0; i < window; i++) tasks[i] = wire.Client.GetAsync(keys[i]);
                for (int i = 0; i < window; i++) await tasks[i];
            }
            return (long)rounds * window;
        });
        await wire.CloseAsync();
    }

    [Fact]
    public async Task ProfileNotifications()
    {
        if (!Enabled) return;
        var wire = new FakeWireClient(12);
        var notifications = await NotificationsImpl.CreateAsync(wire.Client.Ctx, wire.Client.Options, wire.Provider, wire.ShardManager);
        var reader = notifications.Ch;
        await Profile("notification delivery", async rounds =>
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int n = 0; n < 200; n++) await reader.ReadAsync();
            }
            return (long)rounds * 200;
        });
        await notifications.CloseAsync();
        await wire.CloseAsync();
    }

    [Fact]
    public async Task ProfileDeletes()
    {
        if (!Enabled) return;
        const int window = 12;
        var wire = new FakeWireClient(window, intercepted: true);
        var keys = Enumerable.Range(0, window).Select(i => "/bench/key-" + i).ToArray();
        var tasks = new ValueTask<Exception?>[window];
        async Task<long> Rounds(int rounds)
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int i = 0; i < window; i++) tasks[i] = wire.Client.DeleteAsync(keys[i]);
                for (int i = 0; i < window; i++) await tasks[i];
            }
            return (long)rounds * window;
        }
        await Profile("delete", Rounds);
        await wire.CloseAsync();
    }

    [Fact]
    public async Task ProfileWrites()
    {
        if (!Enabled) return;
        const int window = 12;
        var wire = new FakeWireClient(window, intercepted: true);
        var tasks = new ValueTask<PutResult>[window];
        var value = new byte[100];
        var keys = Enumerable.Range(0, window).Select(i => "/bench/key-" + i).ToArray();
        await Profile("put", async rounds =>
        {
            for (int r = 0; r < rounds; r++)
            {
                for (int i = 0; i < window; i++) tasks[i] = wire.Client.PutAsync(keys[i], value);
                for (int i = 0; i < window; i++) await tasks[i];
            }
            return (long)rounds * window;
        });
        await wire.CloseAsync();
    }
}
