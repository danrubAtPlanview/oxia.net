// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/metrics/utils_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 5 (TestNewHistogramPanicsOnInvalidName, TestNewMillisCounterPanicsOnInvalidName,
//              TestNewCounterPanicsOnInvalidName, TestReadMetrics, TestReadMetricsWithoutResponse)   ported: 5   skipped: 0
//   Adaptations: System.Diagnostics.Metrics does not validate instrument names, so MetricsUtils re-implements the OTel SDK rule
//              and throws (Go: panics) an InvalidOperationException with the same message; assert.PanicsWithError -> Assert.Throws
//              + message equality.  `metric.NewMeterProvider().Meter("test")` -> new Meter("test").
// =====================================================================================================================
using System.Diagnostics.Metrics;
using Oxia.Client.Internal.Metrics;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Metrics;

public class UtilsTests
{
    private const string InvalidNameError = "failed to create oxia client metric 1metric: invalid instrument name: 1metric: " +
                                            "must start with a letter";

    [Fact]
    public void TestNewHistogramPanicsOnInvalidName()
    {
        using var meter = new Meter("test");

        var ex = Assert.Throws<InvalidOperationException>(() => MetricsUtils.NewHistogram(meter, "1metric", ""));
        Assert.Equal(InvalidNameError, ex.Message);
    }

    [Fact]
    public void TestNewMillisCounterPanicsOnInvalidName()
    {
        using var meter = new Meter("test");

        var ex = Assert.Throws<InvalidOperationException>(() => MetricsUtils.NewMillisCounter(meter, "1metric"));
        Assert.Equal(InvalidNameError, ex.Message);
    }

    [Fact]
    public void TestNewCounterPanicsOnInvalidName()
    {
        using var meter = new Meter("test");

        var ex = Assert.Throws<InvalidOperationException>(() => MetricsUtils.NewCounter(meter, "1metric", ""));
        Assert.Equal(InvalidNameError, ex.Message);
    }

    [Fact]
    public void TestReadMetrics()
    {
        var response = new ReadResponse();
        response.Gets.Add(new GetResponse { Value = new byte[] { 0, 1, 2 } });
        response.Gets.Add(new GetResponse { Value = new byte[] { 3 } });

        var (valueSize, requestCount) = MetricsUtils.ReadMetrics(response);
        Assert.Equal(4, valueSize);
        Assert.Equal(2, requestCount);
    }

    // A failed read has no response.
    [Fact]
    public void TestReadMetricsWithoutResponse()
    {
        var (valueSize, requestCount) = MetricsUtils.ReadMetrics(null);
        Assert.Equal(0, valueSize);
        Assert.Equal(0, requestCount);
    }
}
