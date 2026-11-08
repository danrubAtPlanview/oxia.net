// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Attributes every allocated byte of a run to the .NET type that allocated it, WITHOUT dotnet-counters / dotnet-trace: an
//              in-process EventListener on the runtime's GC provider (keyword 0x1, verbose) receives GCAllocationTick events (one per
//              ~100 KB allocated; each names the type of the object that crossed the threshold, so the samples are proportional to
//              bytes per type).  Reported per type and per allocating thread kind, normalised per operation.
// =====================================================================================================================
using System.Diagnostics.Tracing;
using System.Text;

namespace Oxia.Client.Benchmarks.Alloc;

public sealed class AllocationTickListener : EventListener
{
    private readonly Dictionary<string, long> _bytesByType = new();
    private readonly object _gate = new();
    private long _total;
    private long _events;
    private volatile bool _enabled;

    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
        {
            EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1);
        }
    }

    public void Start()
    {
        lock (_gate)
        {
            _bytesByType.Clear();
            _total = 0;
            _events = 0;
        }
        _enabled = true;
    }

    public void Stop() => _enabled = false;

    protected override void OnEventWritten(EventWrittenEventArgs e)
    {
        if (!_enabled || e.EventName is null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.Payload is null) return;

        string type = "?";
        long amount = 0;
        for (int i = 0; i < e.PayloadNames!.Count; i++)
        {
            switch (e.PayloadNames[i])
            {
                case "TypeName": type = (string?)e.Payload[i] ?? "?"; break;
                case "AllocationAmount64": amount = (long)Convert.ToUInt64(e.Payload[i]); break;
            }
        }

        lock (_gate)
        {
            _bytesByType[type] = _bytesByType.GetValueOrDefault(type) + amount;
            _total += amount;
            _events++;
        }
    }

    /// <summary>A table of the bytes per operation by type, largest first (types below 0.5 B/op are summed up).</summary>
    public string Report(string title, long operations)
    {
        Thread.Sleep(500); // events are delivered asynchronously
        var sb = new StringBuilder();
        lock (_gate)
        {
            sb.AppendLine($"== {title}: {_total / (double)operations:F1} B/op sampled ({_events} ticks over {operations} ops)");
            double rest = 0;
            foreach (var (type, bytes) in _bytesByType.OrderByDescending(kv => kv.Value))
            {
                double perOp = bytes / (double)operations;
                if (perOp < 0.5)
                {
                    rest += perOp;
                    continue;
                }
                sb.AppendLine($"   {perOp,8:F1} B/op  {type}");
            }
            if (rest > 0) sb.AppendLine($"   {rest,8:F1} B/op  (smaller entries)");
        }
        return sb.ToString();
    }
}
