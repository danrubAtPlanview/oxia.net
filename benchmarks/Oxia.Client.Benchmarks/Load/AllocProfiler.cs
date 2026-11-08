// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : `--alloc-profile`: where do the allocated bytes per operation come from?  An in-process EventListener on the runtime's GC
//              keyword at verbose level receives GCAllocationTick events (one per ~100 KB allocated, carrying the type of the object that
//              crossed the threshold), which sampled over millions of operations is a proportional breakdown by type.  Only active during
//              the measured window.  Covers everything in the process (generator, client, gRPC, in-process server if any).
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics.Tracing;

namespace Oxia.Client.Benchmarks.Load;

internal sealed class AllocProfiler : EventListener
{
    private readonly ConcurrentDictionary<string, long> _bytes = new();
    private long _total;
    private volatile bool _active;

    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
        {
            EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1);
        }
    }

    public void Start() => _active = true;

    public void Stop() => _active = false;

    protected override void OnEventWritten(EventWrittenEventArgs e)
    {
        if (!_active || e.EventName is null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.Payload is null) return;

        string? type = null;
        long amount = 100_000;
        for (int i = 0; i < e.PayloadNames!.Count; i++)
        {
            switch (e.PayloadNames[i])
            {
                case "TypeName": type = e.Payload[i] as string; break;
                case "AllocationAmount64": amount = Convert.ToInt64(e.Payload[i]); break;
            }
        }
        _bytes.AddOrUpdate(string.IsNullOrEmpty(type) ? "(unknown)" : type, amount, (_, old) => old + amount);
        Interlocked.Add(ref _total, amount);
    }

    public void Print(TextWriter w, long operations)
    {
        long total = Interlocked.Read(ref _total);
        w.WriteLine($"  allocation profile (sampled, {total / 1024.0 / 1024.0:0.0} MB over {operations:N0} ops = {(double)total / Math.Max(1, operations):0} B/op):");
        foreach (var kv in _bytes.OrderByDescending(k => k.Value).Take(18))
        {
            w.WriteLine($"     {100.0 * kv.Value / Math.Max(1, total),5:0.0}%  {(double)kv.Value / Math.Max(1, operations),7:0.0} B/op  {kv.Key}");
        }
    }
}
