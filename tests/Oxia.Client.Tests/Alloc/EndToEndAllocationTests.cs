// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- allocation budgets of the wave 2 / Z1 work, end to end)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The allocation budget of OUR side of the client, per operation, END TO END: a real AsyncClientImpl (options, batchers,
//              rpc provider, write stream wrapper, OxiaClientRpc, metrics interceptor) over FakeWireInvoker, which runs the real
//              serializers and answers through the real deserializers (so the pooled response graphs travel the real path), and itself
//              allocates almost nothing.  Measured process-wide (GC.GetTotalAllocatedBytes) over 100 000 operations after a warm-up that
//              lets the tiered JIT promote the hot paths; the best of 3 measurements counts (a GC or a tiering step can add noise, never
//              remove allocations).  Release builds only (Debug allocates differently): the tests skip themselves in Debug like the
//              other allocation tests.  The tests run alone ("Z1 serial"): the counter is process-wide.
//              Fewer than 16 operations are in flight at a time so that the batchers' queues (ProcessorCount slots) never fill: the
//              slow path of AddAsync (a full queue) is the batcher's own cost and is not what is measured here.
//   Budgets  : every non-zero budget is justified at its test.  What the budgets do NOT contain, by design: Grpc.Net.Client and the HTTP/2
//              stack (about 2.3 KB per write-stream message, about 9 KB per Read / RangeScan call, shared by all the operations of a
//              batch -- see docs/performance-allocation.md and `dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc`).
//   Divergences: n/a (new tests).
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Pooled;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Alloc;

[Collection("Z1 serial")]
public class EndToEndAllocationTests(ITestOutputHelper output)
{
    private const int Window = 12;
    private const int Operations = 100_000;

    private static readonly byte[] Value = new byte[100];
    private static readonly string[] Keys = Enumerable.Range(0, Window).Select(i => "/bench/key-" + i.ToString("D6")).ToArray();

    private static void RequireOptimizedBuild()
    {
#if DEBUG
        Assert.Skip("allocation budgets only mean something in Release (dotnet test -c Release)");
#endif
    }

    /// <summary>Bytes allocated per operation (process-wide), best of three measurements after a warm-up.</summary>
    private async Task<double> MeasureAsync(string name, Func<Task<int>> round, int operations = Operations)
    {
        // warm-up: pools, connection state, JIT tiers (the tiering call counting starts after a quiet period: pause between the rounds)
        for (int i = 0; i < 400; i++) await round();
        for (int pause = 0; pause < 4; pause++)
        {
            for (int i = 0; i < 300; i++) await round();
            await Task.Delay(120);
        }

        double best = double.MaxValue;
        for (int attempt = 0; attempt < 3; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            long done = 0;
            long before = GC.GetTotalAllocatedBytes(precise: true);
            while (done < operations) done += await round();
            double perOp = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)done;
            best = Math.Min(best, perOp);
            if (best <= 1) break;
        }
        output.WriteLine($"{name}: {best:F2} B/op");
        return best;
    }

    // ----- writes -------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task Put_AllocatesAtMost16BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<PutResult>[Window];

        double perOp = await MeasureAsync("put", async () =>
        {
            for (int i = 0; i < Window; i++) tasks[i] = client.PutAsync(Keys[i], Value);
            for (int i = 0; i < Window; i++)
            {
                var r = await tasks[i];
                if (r.Err is not null) throw r.Err;
            }
            return Window;
        });
        await wire.CloseAsync();

        // Budget 16 B: the 12 puts of a batch share one write-stream message; what is left per batch (about 150 B) is the Task of
        // the fake transport's pending read and the timer-less batch hand-over.  The decoded response (PutResponse + ProtoVersion
        // + lists, about 160 B per op before the response recycling) is pooled: 0.
        Assert.True(perOp <= 16, $"Put allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task Delete_AllocatesAtMost16BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<Exception?>[Window];

        double perOp = await MeasureAsync("delete", async () =>
        {
            for (int i = 0; i < Window; i++) tasks[i] = client.DeleteAsync(Keys[i]);
            for (int i = 0; i < Window; i++)
            {
                var r = await tasks[i];
                if (r is not null) throw r;
            }
            return Window;
        });
        await wire.CloseAsync();

        // Budget 16 B: same as Put (the per-batch Task of the fake transport is the only allocation left)
        Assert.True(perOp <= 16, $"Delete allocated {perOp:F1} B/op");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DeleteRange_AllocatesAtMost16BytesPerOperation(bool withPartitionKey)
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<Exception?>[Window];

        double perOp = await MeasureAsync(withPartitionKey ? "delete-range (partition key, one shard)" : "delete-range (all shards = one)", async () =>
        {
            for (int i = 0; i < Window; i++)
            {
                tasks[i] = withPartitionKey
                    ? client.DeleteRangeAsync("/a", "/b", OxiaOptions.PartitionKey("p"))
                    : client.DeleteRangeAsync("/a", "/b");
            }
            for (int i = 0; i < Window; i++)
            {
                var r = await tasks[i];
                if (r is not null) throw r;
            }
            return Window;
        });
        await wire.CloseAsync();

        // Budget 16 B: before the pooled PooledDeleteRange (wait group channel + closure + one call per shard) this was about 1.4 KB per op
        Assert.True(perOp <= 16, $"DeleteRange allocated {perOp:F1} B/op");
    }

    // ----- reads --------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task GetNotFound_AllocatesAtMost24BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        wire.Invoker.GetsNotFound = true;
        var client = wire.Client;
        var tasks = new ValueTask<GetResult>[Window];

        double perOp = await MeasureAsync("get (not found)", async () =>
        {
            for (int i = 0; i < Window; i++) tasks[i] = client.GetAsync(Keys[i]);
            for (int i = 0; i < Window; i++)
            {
                var r = await tasks[i];
                if (r.Err is null) throw new InvalidOperationException("expected KeyNotFound");
            }
            return Window;
        });
        await wire.CloseAsync();

        // Budget 24 B: a Read is one server-streaming CALL per batch, and the call objects (ServerStreamingClient, the pooled-box state
        // of RecvAsync, the fake's pending task) are about 250 B per call, i.e. about 20 B/op at 12 gets per call.  The response graph
        // (ReadResponse + GetResponse list, about 200 B/op before) is pooled.
        Assert.True(perOp <= 24, $"Get (not found) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task GetFound_AllocatesOnlyTheValueArray()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;
        var tasks = new ValueTask<GetResult>[Window];

        double perOp = await MeasureAsync("get (found, faithful API)", async () =>
        {
            for (int i = 0; i < Window; i++) tasks[i] = client.GetAsync(Keys[i]);
            for (int i = 0; i < Window; i++)
            {
                var r = await tasks[i];
                if (r.Err is not null || r.Value!.Length != 100) throw new InvalidOperationException("unexpected result");
            }
            return Window;
        });
        await wire.CloseAsync();

        // The faithful API hands the user a byte[]: 100 bytes + 24 bytes of array header = 124 B/op is REQUIRED, the rest is the same
        // per-call overhead as the not-found case.
        Assert.True(perOp <= 124 + 24, $"Get (found) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task GetFoundPooled_AllocatesAtMost24BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;
        var tasks = new ValueTask<PooledGetResult>[Window];

        double perOp = await MeasureAsync("get (found, pooled API)", async () =>
        {
            for (int i = 0; i < Window; i++) tasks[i] = client.GetPooledAsync(Keys[i]);
            for (int i = 0; i < Window; i++)
            {
                using var r = await tasks[i];
                if (r.Err is not null || r.Value.Length != 100) throw new InvalidOperationException("unexpected result");
            }
            return Window;
        });
        await wire.CloseAsync();

        // Budget 24 B: only the per-call overhead of the Read stream (see GetNotFound); the value lives in a pooled lease
        Assert.True(perOp <= 24, $"Get (pooled) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task RangeScanPooled_1000Records_AllocatesAtMost16BytesPerRecord()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;

        double perRecord = await MeasureAsync("range scan (1000 records, pooled API)", async () =>
        {
            int n = 0;
            var reader = client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0");
            await foreach (var r in reader.ReadAllAsync())
            {
                using (r)
                {
                    if (r.Err is not null) throw r.Err;
                    if (r.Value.Length != 100 || r.KeyUtf8.Length != 18) throw new InvalidOperationException("unexpected record");
                    n++;
                }
            }
            return n;
        }, operations: 100_000);
        await wire.CloseAsync();

        // Budget 16 B per record: one scan (a reader, a task, a linked token, a few boxes: about 1.5 KB) is spread over 1000 records.  Keys
        // and values are bytes in leases: no string, no array per record.
        Assert.True(perRecord <= 16, $"RangeScanPooled allocated {perRecord:F1} B/record");
    }

    // ----- notifications and sessions -----------------------------------------------------------------------------------

    [Fact]
    public async Task NotificationDelivery_AllocatesOnlyTheUserFacingObjects()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        wire.Invoker.NotificationsPerBatch = 20;
        var notifications = await NotificationsImpl.CreateAsync(wire.Client.Ctx, wire.Client.Options, wire.Provider, wire.ShardManager);
        var reader = notifications.Ch;

        long received = 0;
        double perOp = await MeasureAsync("notification delivery", async () =>
        {
            int n = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            for (; n < 200; n++)
            {
                var notification = await reader.ReadAsync(cts.Token);
                if (notification.Type != NotificationType.KeyModified) throw new InvalidOperationException("unexpected notification");
            }
            received += n;
            return n;
        });
        await notifications.CloseAsync();
        await wire.CloseAsync();

        // Budget 120 B: what the public API itself creates per notification -- the Notification object (48 B) and its key string
        // ("/notifications/key-000001" = 25 chars: 22 + 50 = 72 B) -- plus the channel slot.  The wire objects (NotificationBatch,
        // NotificationEntry, wire Notification: about 200 B per notification before) are pooled, the batch is released after delivery.
        Assert.True(received > 0);
        Assert.True(perOp <= 120, $"Notification delivery allocated {perOp:F1} B/notification");
    }

    [Fact]
    public async Task SessionKeepAlive_AllocatesNothingOnOurSide()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: false);
        var heartbeat = new SessionHeartbeat { Shard = 0, SessionId = 77 };

        double perOp = await MeasureAsync("session keep-alive", async () =>
        {
            for (int i = 0; i < 1000; i++) await wire.Provider.KeepAlive(CancellationToken.None, "fake:1", heartbeat);
            return 1000;
        });
        await wire.CloseAsync();

        // Budget 8 B: serialization (real serializer, reused buffer), the unary call plumbing of OxiaClientRpc and the shared empty
        // response are all allocation free; 8 B is slack for the process-wide counter.  (The real HTTP/2 unary call costs about 3 KB
        // once per keep-alive tick, every few seconds.)
        Assert.True(perOp <= 8, $"KeepAlive allocated {perOp:F1} B/call");
    }
}
