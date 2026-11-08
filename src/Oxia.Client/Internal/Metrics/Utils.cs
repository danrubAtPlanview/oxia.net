// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/metrics/utils.go  (newHistogram, newMillisCounter, newCounter, panicOnErr, attrs, result,
//              writeMetrics, readMetrics)     +  common/metric/unit.go (Unit constants)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Instrument factories and the small pure helpers behind the client metrics.
//   Divergences:
//      * OTel -> System.Diagnostics.Metrics.  The OTel SDK validates instrument names and `panicOnErr` turns that error
//        into a panic; System.Diagnostics.Metrics accepts any name, so the OTel rule (must start with a letter, then
//        [A-Za-z0-9_.-/], at most 255 characters) is re-implemented here to keep the same fail-fast behaviour and
//        the same message ("failed to create oxia client metric 1metric: invalid instrument name: 1metric: must start with
//        a letter").  The panic is an InvalidOperationException.
//      * Units are the Go constants of common/metric/unit.go: bytes = "By" (UCUM), milliseconds = "ms", dimensionless
//        = "1"; an empty unit is `null` (no unit) in System.Diagnostics.Metrics.
//      * `attrs(type, err)` returned a freshly allocated attribute set; here it is a TagList (inline storage for <= 8 tags),
//        built on the stack and passed by `in`: no allocation per measurement.
//      * writeMetrics / readMetrics take the request / response objects and return a value tuple (no allocation).
// =====================================================================================================================
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Oxia.Common.Proto;

namespace Oxia.Client.Internal.Metrics;

/// <summary>Go: common/metric Unit.</summary>
internal static class MetricUnit
{
    public const string Dimensionless = "1";
    public const string Bytes = "By";
    public const string Milliseconds = "ms";
}

internal static class MetricsUtils
{
    public const string TypeTag = "type";
    public const string ResultTag = "result";
    public const string Success = "success";
    public const string Failure = "failure";

    /// <summary>Go: newHistogram.</summary>
    public static Histogram<long> NewHistogram(Meter meter, string name, string? unit)
    {
        ValidateInstrumentName(name);
        return meter.CreateHistogram<long>(name, NullIfEmpty(unit));
    }

    /// <summary>Go: newMillisCounter.</summary>
    public static Counter<double> NewMillisCounter(Meter meter, string name)
    {
        ValidateInstrumentName(name);
        return meter.CreateCounter<double>(name, MetricUnit.Milliseconds);
    }

    /// <summary>Go: newCounter.</summary>
    public static Counter<long> NewCounter(Meter meter, string name, string? unit)
    {
        ValidateInstrumentName(name);
        return meter.CreateCounter<long>(name, NullIfEmpty(unit));
    }

    private static string? NullIfEmpty(string? unit) => string.IsNullOrEmpty(unit) ? null : unit;

    /// <summary>
    /// Go: panicOnErr(err, name) around the OTel instrument creation, whose only failure is an invalid name.  Throws
    /// (Go: panics) with the OTel SDK message.
    /// </summary>
    private static void ValidateInstrumentName(string name)
    {
        string? problem = null;
        if (name.Length == 0)
        {
            problem = "is empty";
        }
        else if (name.Length > 255)
        {
            problem = "longer than 255 characters";
        }
        else if (!IsAlpha(name[0]))
        {
            problem = "must start with a letter";
        }
        else
        {
            for (int i = 1; i < name.Length; i++)
            {
                char c = name[i];
                if (!IsAlpha(c) && !(c >= '0' && c <= '9') && c != '_' && c != '.' && c != '-' && c != '/')
                {
                    problem = "must only contain [A-Za-z0-9_.-/]";
                    break;
                }
            }
        }

        if (problem is not null)
        {
            throw new InvalidOperationException(
                $"failed to create oxia client metric {name}: invalid instrument name: {name}: {problem}");
        }
    }

    private static bool IsAlpha(char c) => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');

    /// <summary>Go: attrs(requestType, err) -- the two tags of every measurement.</summary>
    public static TagList Attrs(string requestType, Exception? err) => new()
    {
        { TypeTag, requestType },
        { ResultTag, Result(err) },
    };

    /// <summary>Go: result(err).</summary>
    public static string Result(Exception? err) => err is null ? Success : Failure;

    /// <summary>Go: writeMetrics(request) -- the total size of the values and the number of operations.</summary>
    public static (long ValueSize, long RequestCount) WriteMetrics(WriteRequest request)
    {
        long valueSize = 0;
        var puts = request.Puts;
        for (int i = 0; i < puts.Count; i++)
        {
            valueSize += puts[i].Value.Length;
        }
        long requestCount = puts.Count + request.Deletes.Count + request.DeleteRanges.Count;
        return (valueSize, requestCount);
    }

    /// <summary>Go: readMetrics(response) -- (0, 0) for a failed read, which has no response.</summary>
    public static (long ValueSize, long RequestCount) ReadMetrics(ReadResponse? response)
    {
        if (response is null)
        {
            return (0, 0);
        }

        long valueSize = 0;
        var gets = response.Gets;
        for (int i = 0; i < gets.Count; i++)
        {
            valueSize += gets[i].ValueLength;
        }
        return (valueSize, gets.Count);
    }
}
