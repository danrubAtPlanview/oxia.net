// =====================================================================================================================
// PORT NOTE
//   Upstream : common/time/jitter.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A duration uniformly distributed in [interval - maxDeviation, interval + maxDeviation); the interval itself
//              when maxDeviation is not positive.
//   Divergences: time.Duration (ns) -> TimeSpan (ticks); math/rand/v2 `rand.N` -> Random.Shared.NextInt64.
// =====================================================================================================================
namespace Oxia.Common.Time;

/// <summary>Go: time.Jitter(interval, maxDeviation).</summary>
public static class Jitter
{
    /// <summary>
    /// Returns a duration uniformly distributed in [interval-maxDeviation, interval+maxDeviation).  If
    /// <paramref name="maxDeviation"/> is not positive, it returns <paramref name="interval"/> unchanged.
    /// </summary>
    public static TimeSpan Apply(TimeSpan interval, TimeSpan maxDeviation)
    {
        if (maxDeviation <= TimeSpan.Zero)
        {
            return interval;
        }

        return TimeSpan.FromTicks(unchecked(interval.Ticks - maxDeviation.Ticks + Random.Shared.NextInt64(checked(2 * maxDeviation.Ticks))));
    }
}
