// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/metrics/metrics_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 2 (TestMetricsDecorate: 4 operations x 2 results = 8 subtests, TestMetricsCallback: 2 x 2 = 4 subtests)
//              ported: 2 (as [Theory]s with the 12 subtests as rows)   skipped: 0
//   Adaptations: the OTel SDK ManualReader + `reader.Collect` is a MeterListener on the Meter of the Metrics under test
//              (MetricsTestSupport.cs).  The injected `sinceFunc` that read a buffered channel (a second call would block the
//              Go test forever) is a queue that fails the test when it is empty; `time.Now()` execution starts are
//              Stopwatch timestamps; `.Callback(response, err)` on the decorated call is `.Complete(response, err)` (the
//              decorated call's Callback is the same delegate).  Assertions are unchanged.
// =====================================================================================================================
using System.Diagnostics;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Metrics;

public class MetricsTests
{
    private static (OxiaMetrics Metrics, MetricsReader Reader, TestMeterFactory Factory) Setup(Func<long, TimeSpan> sinceFunc)
    {
        var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory, static () => Stopwatch.GetTimestamp(), sinceFunc);
        var reader = new MetricsReader(metrics.Meter);
        return (metrics, reader, factory);
    }

    /// <summary>ch := make(chan time.Duration, n); ch &lt;- 1ms ...; func(time.Time) { return &lt;-ch }</summary>
    private static Func<long, TimeSpan> Since(int count)
    {
        var queue = new Queue<TimeSpan>();
        for (int i = 0; i < count; i++) queue.Enqueue(TimeSpan.FromMilliseconds(1));
        return _ =>
        {
            lock (queue)
            {
                Assert.True(queue.Count > 0, "sinceFunc invoked more times than expected");
                return queue.Dequeue();
            }
        };
    }

    private static void PutFunc(OxiaMetrics metrics, Exception? err) =>
        metrics.DecoratePut(new PutCall { Callback = (_, _) => { }, Value = new byte[] { 0, 1, 2, 3, 4 } })
            .Complete(new PutResponse(), err);

    private static void DeleteFunc(OxiaMetrics metrics, Exception? err) =>
        metrics.DecorateDelete(new DeleteCall { Callback = (_, _) => { } })
            .Complete(new DeleteResponse(), err);

    private static void DeleteRangeFunc(OxiaMetrics metrics, Exception? err) =>
        metrics.DecorateDeleteRange(new DeleteRangeCall { Callback = (_, _) => { } })
            .Complete(new DeleteRangeResponse(), err);

    private static void GetFunc(OxiaMetrics metrics, Exception? err) =>
        metrics.DecorateGet(new GetCall { Callback = (_, _) => { } })
            .Complete(new GetResponse { Value = new byte[] { 0, 1, 2, 3, 4 } }, err);

    public static TheoryData<string, bool, string, bool> DecorateCases() => new()
    {
        { "put", true, "success", false },
        { "put", true, "failure", true },
        { "delete", false, "success", false },
        { "delete", false, "failure", true },
        { "delete_range", false, "success", false },
        { "delete_range", false, "failure", true },
        { "get", true, "success", false },
        { "get", true, "failure", true },
    };

    [Theory]
    [MemberData(nameof(DecorateCases))]
    public void TestMetricsDecorate(string expectedType, bool hasHistogram, string expectedResult, bool fail)
    {
        Exception? err = fail ? OxiaErrors.EndOfStream : null;
        var (metrics, reader, factory) = Setup(Since(1));
        using var _ = factory;
        using var __ = reader;

        switch (expectedType)
        {
            case "put": PutFunc(metrics, err); break;
            case "delete": DeleteFunc(metrics, err); break;
            case "delete_range": DeleteRangeFunc(metrics, err); break;
            case "get": GetFunc(metrics, err); break;
            default: throw new InvalidOperationException(expectedType);
        }

        AssertTimer(reader, "oxia_client_op", expectedType, expectedResult);
        if (hasHistogram)
        {
            AssertWithHistogram(reader, "oxia_client_op_value", 5, expectedType, expectedResult);
        }
        else
        {
            AssertWithoutHistogram(reader, "oxia_client_op_value");
        }
    }

    public static TheoryData<string, string, bool> CallbackCases() => new()
    {
        { "write", "success", false },
        { "write", "failure", true },
        { "read", "success", false },
        { "read", "failure", true },
    };

    [Theory]
    [MemberData(nameof(CallbackCases))]
    public void TestMetricsCallback(string expectedType, string expectedResult, bool fail)
    {
        Exception? err = fail ? OxiaErrors.EndOfStream : null;
        var (metrics, reader, factory) = Setup(Since(2));
        using var _ = factory;
        using var __ = reader;

        if (expectedType == "write")
        {
            var request = new WriteRequest();
            request.Puts.Add(new PutRequest { Value = new byte[] { 0, 1, 2, 3, 4 } });
            metrics.WriteCallback()(Stopwatch.GetTimestamp(), request, new WriteResponse(), err);
        }
        else
        {
            var response = new ReadResponse();
            response.Gets.Add(new GetResponse { Value = new byte[] { 0, 1, 2, 3, 4 } });
            metrics.ReadCallback()(Stopwatch.GetTimestamp(), new ReadRequest(), response, err);
        }

        AssertTimer(reader, "oxia_client_batch_total", expectedType, expectedResult);
        AssertTimer(reader, "oxia_client_batch_exec", expectedType, expectedResult);
        AssertWithHistogram(reader, "oxia_client_batch_value", 5, expectedType, expectedResult);
        AssertWithHistogram(reader, "oxia_client_batch_request", 1, expectedType, expectedResult);
    }

    private static void AssertTimer(MetricsReader reader, string name, string expectedType, string expectedResult)
    {
        var sums = reader.Counter<double>(name);
        AssertDataPoints(sums, 1.0, expectedType, expectedResult);

        var counts = reader.Counter<long>(name);
        AssertDataPoints(counts, 1L, expectedType, expectedResult);
    }

    private static void AssertWithHistogram(MetricsReader reader, string name, long expectedSum, string expectedType, string expectedResult)
    {
        var datapoints = reader.Histogram(name);

        Assert.Single(datapoints);
        var histogram = datapoints[0];
        Assert.Equal(expectedSum, histogram.Value);
        Assert.Equal(1UL, histogram.Count);
        AssertAttributes(histogram.Attributes, expectedType, expectedResult);
    }

    private static void AssertWithoutHistogram(MetricsReader reader, string name)
    {
        var datapoints = reader.Histogram(name);

        Assert.Empty(datapoints);
    }

    private static void AssertDataPoints<N>(List<DataPoint<N>> datapoints, N expectedValue, string expectedType, string expectedResult)
        where N : struct
    {
        Assert.Single(datapoints);
        Assert.Equal(expectedValue, datapoints[0].Value);
        AssertAttributes(datapoints[0].Attributes, expectedType, expectedResult);
    }

    private static void AssertAttributes(IReadOnlyDictionary<string, string> attrs, string expectedType, string expectedResult)
    {
        Assert.Equal(2, attrs.Count);
        AssertAttribute(attrs, "type", expectedType);
        AssertAttribute(attrs, "result", expectedResult);
    }

    private static void AssertAttribute(IReadOnlyDictionary<string, string> attrs, string key, string expected)
    {
        Assert.True(attrs.TryGetValue(key, out var value));
        Assert.Equal(expected, value);
    }
}
