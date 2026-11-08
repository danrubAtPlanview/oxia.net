// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : none (the Go perf tool uses bmizerany/perks/quantile)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The HDR-style histogram of the load generator (benchmarks/.../Load/LatencyHistogram.cs, compiled into this project):
//              exact for small values, relative error < 0.8 % above, quantiles are monotone and bounded by the max, merge adds,
//              concurrent recording loses nothing, and recording does not allocate.
// =====================================================================================================================
using Oxia.Client.Benchmarks.Load;
using Xunit;

namespace Oxia.Client.Tests.Throughput;

public class LatencyHistogramTests
{
    [Fact]
    public void SmallValues_AreExact()
    {
        var h = new LatencyHistogram();
        for (long v = 0; v < 256; v++) h.Record(v);
        Assert.Equal(256, h.Count);
        Assert.Equal(255, h.Max);
        Assert.Equal(0, h.Quantile(0.0));
        Assert.Equal(127, h.Quantile(0.5));
        Assert.Equal(255, h.Quantile(1.0));
    }

    [Theory]
    [InlineData(1_000L)]
    [InlineData(123_456L)]
    [InlineData(15_600_000L)]
    [InlineData(3_000_000_000L)]
    [InlineData(1L << 44)]
    public void RelativeError_IsBelowOnePercent(long value)
    {
        var h = new LatencyHistogram();
        h.Record(value);
        long reported = h.Quantile(0.5);
        Assert.True(reported <= value, "never above the exact max");
        Assert.True(Math.Abs(reported - value) <= value / 100.0 + 1, $"value {value} reported as {reported}");
    }

    [Fact]
    public void Quantiles_AreMonotone_AndMatchASortedReference()
    {
        var rng = new Random(42);
        var values = new long[200_000];
        var h = new LatencyHistogram();
        for (int i = 0; i < values.Length; i++)
        {
            // log-normal-ish latencies between 20 us and 80 ms, in ns
            values[i] = (long)(20_000 * Math.Exp(rng.NextDouble() * Math.Log(4000)));
            h.Record(values[i]);
        }
        Array.Sort(values);

        long previous = 0;
        foreach (double q in new[] { 0.01, 0.1, 0.5, 0.9, 0.99, 0.999, 1.0 })
        {
            long reported = h.Quantile(q);
            long exact = values[Math.Min(values.Length - 1, (int)Math.Ceiling(q * values.Length) - 1)];
            Assert.True(reported >= previous);
            Assert.True(Math.Abs(reported - exact) <= exact * 0.01 + 1, $"q={q}: exact {exact}, histogram {reported}");
            previous = reported;
        }
        Assert.Equal(values[^1], h.Max);
    }

    [Fact]
    public void Merge_AddsCountsMaxAndMean()
    {
        var a = new LatencyHistogram();
        var b = new LatencyHistogram();
        for (int i = 1; i <= 100; i++) a.Record(i * 1000L);
        for (int i = 1; i <= 100; i++) b.Record(i * 1000L + 500_000);
        a.Merge(b);
        Assert.Equal(200, a.Count);
        Assert.Equal(600_000, a.Max);
        Assert.Equal((50_500.0 + 550_500.0) / 2, a.Mean, 1);
    }

    [Fact]
    public async Task ConcurrentRecording_LosesNothing()
    {
        var h = new LatencyHistogram();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < 100_000; i++) h.RecordConcurrent(1000 + i % 5000 + t);
        })));
        Assert.Equal(800_000, h.Count);
        Assert.Equal(1000 + 4999 + 7, h.Max);
    }

    [Fact]
    public void Recording_DoesNotAllocate()
    {
        var h = new LatencyHistogram();
        for (int i = 0; i < 10_000; i++) h.Record(i * 7L); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000_000; i++) h.Record(i * 13L);
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }
}
