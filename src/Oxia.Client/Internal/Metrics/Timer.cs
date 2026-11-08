// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/metrics/timer.go  (Timer interface, timerImpl, newTimer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A "timer" metric is the pair Go exposes as two OTel counters with the SAME name: a float64 counter of the
//              accumulated milliseconds (unit "ms") and an int64 counter of the number of measurements (no unit).  Rates and
//              averages are derived by the backend (sum / count).
//   Divergences:
//      * The class is named MetricsTimer (the Go name `Timer` clashes with System.Threading.Timer, imported by default).
//      * Both instruments keep the exact Go name, e.g. `oxia_client_op` twice.  System.Diagnostics.Metrics allows several
//        instruments with the same name in a Meter (they are told apart by value type and unit), and a MeterListener sees
//        both: Counter<double> "ms" and Counter<long> "".  The OpenTelemetry .NET SDK logs a "duplicate instrument name"
//        warning for such a pair but exports both streams, like the OTel Go SDK does (Prometheus names them
//        `<name>_milliseconds_total` and `<name>_total`).
//      * Record takes the tags by `in` TagList: no allocation per measurement.  An instrument nobody listens to makes
//        Counter.Add a few-nanosecond no-op; `Enabled` lets callers skip the clock reads as well.
// =====================================================================================================================
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Oxia.Client.Internal.Metrics;

/// <summary>Go: timerImpl.</summary>
internal sealed class MetricsTimer
{
    private readonly Counter<double> _sum;
    private readonly Counter<long> _count;

    /// <summary>Go: newTimer.</summary>
    public MetricsTimer(Meter meter, string name)
    {
        _sum = MetricsUtils.NewMillisCounter(meter, name);
        _count = MetricsUtils.NewCounter(meter, name, "");
    }

    /// <summary>True if a listener is subscribed to any of the two instruments.</summary>
    public bool Enabled => _sum.Enabled || _count.Enabled;

    /// <summary>Go: Record(ctx, incr, attrs).</summary>
    public void Record(TimeSpan incr, in TagList attrs)
    {
        double millis = (double)incr.Ticks / TimeSpan.TicksPerMillisecond;
        _sum.Add(millis, attrs);
        _count.Add(1, attrs);
    }
}
