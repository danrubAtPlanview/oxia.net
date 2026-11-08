// =====================================================================================================================
// PORT NOTE
//   Upstream : common/time/jitter_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0.
//   Divergences: time.Nanosecond (the smallest Go duration) -> one TimeSpan tick (100ns, the smallest .NET duration).
// =====================================================================================================================
using Oxia.Common.Time;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class JitterTests
{
    [Fact]
    public void TestJitter()
    {
        var nanosecond = TimeSpan.FromTicks(1);
        Assert.Equal(nanosecond, Jitter.Apply(nanosecond, TimeSpan.Zero));
        Assert.Equal(nanosecond, Jitter.Apply(nanosecond, -nanosecond));

        for (var i = 0; i < 100; i++)
        {
            var interval = Jitter.Apply(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1) / 4);
            Assert.True(interval >= TimeSpan.FromSeconds(45), $"{interval} < 45s");
            Assert.True(interval < TimeSpan.FromSeconds(75), $"{interval} >= 75s");
        }
    }

    [Fact]
    public void Jitter_CoversBothSidesOfTheInterval()
    {
        // not part of the Go test: the range is [interval - dev, interval + dev), both halves must be reachable
        var below = false;
        var above = false;
        for (var i = 0; i < 1000 && !(below && above); i++)
        {
            var j = Jitter.Apply(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
            below |= j < TimeSpan.FromSeconds(10);
            above |= j > TimeSpan.FromSeconds(10);
        }

        Assert.True(below);
        Assert.True(above);
    }
}
