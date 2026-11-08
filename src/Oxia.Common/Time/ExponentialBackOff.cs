// =====================================================================================================================
// PORT NOTE
//   Upstream : github.com/cenkalti/backoff/v4 v4.3.0  exponential.go  (ExponentialBackOff, getRandomValueFromInterval)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246 (the Go module pins cenkalti/backoff/v4 v4.3.0)
//   Intent   : Exponential backoff with randomisation:  next = currentInterval * (1 +- RandomizationFactor), then
//              currentInterval *= Multiplier capped at MaxInterval (with cenkalti's overflow guard); returns `Stop` once
//              elapsed + next exceeds MaxElapsedTime (MaxElapsedTime == 0 never stops).
//   Divergences:
//      * time.Duration (ns) -> TimeSpan; the arithmetic is done on TimeSpan ticks (100ns) with the same double formulas, so
//        the randomised interval differs from Go's by the rounding of 1 tick instead of 1 ns ("+1" in the range width).
//      * `Clock` (interface with Now()) -> `TimeProvider` (GetTimestamp / GetElapsedTime: monotonic like Go's time.Now()).
//      * rand.Float64() -> Random.Shared.NextDouble() ([0,1) too).  `GetRandomValueFromInterval` is exposed (internal) so
//        the randomisation formula can be tested with a fixed random.
//      * A fresh object is Reset lazily on the first NextBackOff/GetElapsedTime (cenkalti's NewExponentialBackOff() calls
//        Reset(); a struct literal does not, and then yields 0 forever -- an object initializer cannot call Reset() last).
//      * Not safe for concurrent use (same as cenkalti); wrap in ConcurrentBackOff.
// =====================================================================================================================
namespace Oxia.Common.Time;

/// <summary>cenkalti ExponentialBackOff (same field names/semantics; MaxElapsedTime == 0 means never stop).</summary>
public sealed class ExponentialBackOff : IBackOff
{
    public TimeSpan InitialInterval = BackOffConstants.DefaultInitialInterval;
    public double RandomizationFactor = BackOffConstants.DefaultRandomizationFactor;
    public double Multiplier = BackOffConstants.DefaultMultiplier;
    public TimeSpan MaxInterval = BackOffConstants.DefaultMaxInterval;
    /// <summary>After MaxElapsedTime the backoff returns <see cref="Stop"/>.  It never stops if MaxElapsedTime == 0.</summary>
    public TimeSpan MaxElapsedTime = BackOffConstants.DefaultMaxElapsedTime;
    /// <summary>The value returned to signal "give up" (cenkalti's <c>Stop</c> field).</summary>
    public TimeSpan Stop = BackOffConstants.Stop;
    /// <summary>cenkalti's <c>Clock</c>.</summary>
    public TimeProvider TimeProvider = TimeProvider.System;

    private TimeSpan _currentInterval;
    private long _startTimestamp;
    private bool _started;

    /// <summary>The interval the next call to <see cref="NextBackOff"/> will randomise around.</summary>
    public TimeSpan CurrentInterval
    {
        get
        {
            EnsureStarted();
            return _currentInterval;
        }
    }

    /// <summary>Resets the interval back to the initial retry interval and restarts the elapsed-time clock.</summary>
    public void Reset()
    {
        _currentInterval = InitialInterval;
        _startTimestamp = TimeProvider.GetTimestamp();
        _started = true;
    }

    /// <summary>
    /// Calculates the next backoff interval using the formula
    /// <c>Randomized interval = RetryInterval * (1 +- RandomizationFactor)</c>.
    /// </summary>
    public TimeSpan NextBackOff()
    {
        EnsureStarted();

        // Make sure we have not gone over the maximum elapsed time.
        var elapsed = GetElapsedTime();
        var next = GetRandomValueFromInterval(RandomizationFactor, Random.Shared.NextDouble(), _currentInterval);
        IncrementCurrentInterval();
        if (MaxElapsedTime != TimeSpan.Zero && unchecked(elapsed + next) > MaxElapsedTime)
        {
            return Stop;
        }

        return next;
    }

    /// <summary>Time since the instance was created or <see cref="Reset"/> was last called.</summary>
    public TimeSpan GetElapsedTime()
    {
        EnsureStarted();
        return TimeProvider.GetElapsedTime(_startTimestamp);
    }

    private void EnsureStarted()
    {
        if (!_started) Reset();
    }

    /// <summary>Increments the current interval by multiplying it with the multiplier.</summary>
    private void IncrementCurrentInterval()
    {
        // Check for overflow, if overflow is detected set the current interval to the max interval.
        if ((double)_currentInterval.Ticks >= (double)MaxInterval.Ticks / Multiplier)
        {
            _currentInterval = MaxInterval;
        }
        else
        {
            _currentInterval = TimeSpan.FromTicks(DoubleToInt64((double)_currentInterval.Ticks * Multiplier));
        }
    }

    /// <summary>
    /// Returns a random value from the interval
    /// <c>[currentInterval - randomizationFactor * currentInterval, currentInterval + randomizationFactor * currentInterval]</c>.
    /// </summary>
    internal static TimeSpan GetRandomValueFromInterval(double randomizationFactor, double random, TimeSpan currentInterval)
    {
        if (randomizationFactor == 0)
        {
            return currentInterval; // make sure no randomness is used when randomizationFactor is 0.
        }

        double delta = randomizationFactor * (double)currentInterval.Ticks;
        double minInterval = (double)currentInterval.Ticks - delta;
        double maxInterval = (double)currentInterval.Ticks + delta;

        // Get a random value from the range [minInterval, maxInterval].
        // The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
        // we want a 33% chance for selecting either 1, 2 or 3.
        return TimeSpan.FromTicks(DoubleToInt64(minInterval + (random * (maxInterval - minInterval + 1))));
    }

    /// <summary>Go's float64 -> int64 conversion truncates toward zero; .NET 9+ saturates instead of being undefined.</summary>
    private static long DoubleToInt64(double value) => (long)value;
}
