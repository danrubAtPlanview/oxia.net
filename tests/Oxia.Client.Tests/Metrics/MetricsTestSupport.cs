// =====================================================================================================================
// PORT NOTE (test helper, no Go counterpart)
//   Upstream : oxia/internal/metrics/metrics_test.go  (setup(), counter[N](), histogram(), assertDataPoints(), ...)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go reads the metrics with the OTel SDK's ManualReader (`reader.Collect(&rm)`) and walks
//              `rm.ScopeMetrics[0].Metrics`.  Here a MeterListener subscribed to ONE Meter (the one the Metrics under test
//              created through a private IMeterFactory, so parallel tests cannot interfere) aggregates every measurement
//              per (instrument, tag set): Sum for counters, Sum + Count for histograms -- the data points of the OTel
//              cumulative aggregation.
// =====================================================================================================================
using System.Diagnostics;
using System.Diagnostics.Metrics;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Metrics;

/// <summary>An IMeterFactory that hands out private Meters (as a DI container would).</summary>
internal sealed class TestMeterFactory : IMeterFactory
{
    private readonly List<Meter> _meters = new();

    public Meter Create(MeterOptions options)
    {
        var meter = new Meter(options.Name, options.Version, options.Tags, scope: this);
        _meters.Add(meter);
        return meter;
    }

    public void Dispose()
    {
        foreach (var meter in _meters) meter.Dispose();
        _meters.Clear();
    }
}

/// <summary>A data point: the aggregation of the measurements of one instrument with one tag set.</summary>
internal sealed class DataPoint<N>
{
    public required IReadOnlyDictionary<string, string> Attributes { get; init; }
    public N Value { get; set; } = default!;
    public ulong Count { get; set; }
}

internal sealed class MetricsReader : IDisposable
{
    private sealed record Key(Instrument Instrument, string Tags);

    private readonly MeterListener _listener = new();
    private readonly object _lock = new();
    private readonly Dictionary<Key, (Dictionary<string, string> Tags, double Sum, ulong Count)> _points = new();

    public MetricsReader(Meter meter)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (!ReferenceEquals(instrument.Meter, meter)) return;
            lock (_lock) _published.Add(instrument);
            listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.Start();
    }

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var dict = new Dictionary<string, string>();
        foreach (var tag in tags) dict[tag.Key] = (string)tag.Value!;
        var key = new Key(instrument, string.Join(",", dict.OrderBy(t => t.Key).Select(t => t.Key + "=" + t.Value)));
        lock (_lock)
        {
            _points.TryGetValue(key, out var current);
            _points[key] = (dict, current.Sum + value, current.Count + 1);
        }
    }

    /// <summary>The data points of the counters called <paramref name="name"/> of value type N (empty: "not found").</summary>
    public List<DataPoint<N>> Counter<N>(string name) where N : struct =>
        Points<N>(name, instrument => instrument is Counter<N>);

    /// <summary>The data points of the long histogram called <paramref name="name"/> (Sum, Count; empty: "not found").</summary>
    public List<DataPoint<long>> Histogram(string name) =>
        Points<long>(name, instrument => instrument is Histogram<long>);

    /// <summary>The instruments called <paramref name="name"/> the listener saw published (with or without measurements).</summary>
    public List<Instrument> Published(string name)
    {
        lock (_lock) return _published.Where(i => i.Name == name).ToList();
    }

    private readonly List<Instrument> _published = new();

    private List<DataPoint<N>> Points<N>(string name, Func<Instrument, bool> filter) where N : struct
    {
        lock (_lock)
        {
            var result = new List<DataPoint<N>>();
            foreach (var (key, point) in _points)
            {
                if (key.Instrument.Name != name || !filter(key.Instrument)) continue;
                result.Add(new DataPoint<N>
                {
                    Attributes = point.Tags,
                    Value = (N)Convert.ChangeType(point.Sum, typeof(N)),
                    Count = point.Count,
                });
            }
            return result;
        }
    }

    public void Dispose() => _listener.Dispose();
}
