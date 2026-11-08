// =====================================================================================================================
// PORT NOTE
//   Upstream : (none in the oxia repo) -- tests of github.com/cenkalti/backoff/v4 v4.3.0 exponential.go / context.go behaviour that
//              the Go client relies on, re-written from the upstream library's own test intent (TestBackOff,
//              TestGetRandomizedInterval, TestMaxElapsedTime, TestBackOffOverflow, TestCustomStop, context tests).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using Oxia.Common.Time;
using Xunit;

namespace Oxia.Client.Tests.Common;

/// <summary>A clock the test advances by hand; timestamps are TimeSpan ticks.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    public long Ticks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Ticks;

    public void Advance(TimeSpan by) => Ticks += by.Ticks;
}

public class ExponentialBackOffTests
{
    private static ExponentialBackOff New(ManualTimeProvider? clock = null)
    {
        var exp = new ExponentialBackOff();
        if (clock is not null) exp.TimeProvider = clock;
        return exp;
    }

    [Fact]
    public void Defaults_MatchCenkalti()
    {
        var exp = new ExponentialBackOff();
        Assert.Equal(TimeSpan.FromMilliseconds(500), exp.InitialInterval);
        Assert.Equal(0.5, exp.RandomizationFactor);
        Assert.Equal(1.5, exp.Multiplier);
        Assert.Equal(TimeSpan.FromSeconds(60), exp.MaxInterval);
        Assert.Equal(TimeSpan.FromMinutes(15), exp.MaxElapsedTime);
        Assert.Equal(TimeSpan.FromTicks(-1), exp.Stop);
        Assert.Equal(BackOffConstants.Stop, exp.Stop);
    }

    [Fact]
    public void TestBackOff()
    {
        var initial = TimeSpan.FromMilliseconds(500);
        var randomization = 0.1;
        var max = TimeSpan.FromSeconds(5);

        var exp = New();
        exp.InitialInterval = initial;
        exp.RandomizationFactor = randomization;
        exp.Multiplier = 2.0;
        exp.MaxInterval = max;
        exp.MaxElapsedTime = TimeSpan.FromMinutes(15);
        exp.Reset();

        long[] expectedMillis = [500, 1000, 2000, 4000, 5000, 5000, 5000, 5000, 5000, 5000];
        foreach (var ms in expectedMillis)
        {
            var expected = TimeSpan.FromMilliseconds(ms);
            Assert.Equal(expected, exp.CurrentInterval);

            // Assert that the next backoff falls in the expected range.
            var min = expected - TimeSpan.FromTicks((long)(randomization * expected.Ticks));
            var maxInterval = expected + TimeSpan.FromTicks((long)(randomization * expected.Ticks));
            var actual = exp.NextBackOff();
            Assert.True(min <= actual && actual <= maxInterval, $"{actual} not within [{min}, {maxInterval}]");
        }
    }

    [Fact]
    public void TestGetRandomValueFromInterval()
    {
        // The interval is 2 ticks, so the range is [1, 3]
        // 33% chance of being 1.
        Assert.Equal(TimeSpan.FromTicks(1), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0, TimeSpan.FromTicks(2)));
        Assert.Equal(TimeSpan.FromTicks(1), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0.33, TimeSpan.FromTicks(2)));
        // 33% chance of being 2.
        Assert.Equal(TimeSpan.FromTicks(2), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0.34, TimeSpan.FromTicks(2)));
        Assert.Equal(TimeSpan.FromTicks(2), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0.66, TimeSpan.FromTicks(2)));
        // 33% chance of being 3.
        Assert.Equal(TimeSpan.FromTicks(3), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0.67, TimeSpan.FromTicks(2)));
        Assert.Equal(TimeSpan.FromTicks(3), ExponentialBackOff.GetRandomValueFromInterval(0.5, 0.99, TimeSpan.FromTicks(2)));
    }

    [Fact]
    public void ZeroRandomizationFactor_NeverRandomises()
    {
        Assert.Equal(TimeSpan.FromSeconds(3), ExponentialBackOff.GetRandomValueFromInterval(0, 0.999, TimeSpan.FromSeconds(3)));

        var exp = New();
        exp.InitialInterval = TimeSpan.FromMilliseconds(100);
        exp.RandomizationFactor = 0;
        exp.Multiplier = 2;
        exp.MaxInterval = TimeSpan.FromMilliseconds(500);
        exp.MaxElapsedTime = TimeSpan.Zero;
        exp.Reset();

        long[] expected = [100, 200, 400, 500, 500];
        foreach (var ms in expected)
        {
            Assert.Equal(TimeSpan.FromMilliseconds(ms), exp.NextBackOff());
        }
    }

    [Fact]
    public void NextBackOff_StaysWithinTheRandomisationRange()
    {
        var exp = New();
        exp.InitialInterval = TimeSpan.FromSeconds(1);
        exp.RandomizationFactor = 0.5;
        exp.Multiplier = 1; // constant interval
        exp.MaxElapsedTime = TimeSpan.Zero;
        exp.Reset();

        for (var i = 0; i < 2000; i++)
        {
            var next = exp.NextBackOff();
            Assert.InRange(next, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(1500));
        }
    }

    [Fact]
    public void TestMaxElapsedTime()
    {
        var clock = new ManualTimeProvider();
        var exp = New(clock);
        exp.Reset();

        clock.Advance(TimeSpan.FromSeconds(10000));
        Assert.Equal(BackOffConstants.Stop, exp.NextBackOff());
    }

    [Fact]
    public void MaxElapsedTime_IncludesTheNextInterval()
    {
        // v4.3.0: Stop when elapsed + next > MaxElapsedTime
        var clock = new ManualTimeProvider();
        var exp = New(clock);
        exp.InitialInterval = TimeSpan.FromSeconds(10);
        exp.RandomizationFactor = 0;
        exp.MaxElapsedTime = TimeSpan.FromSeconds(25);
        exp.Reset();

        Assert.Equal(TimeSpan.FromSeconds(10), exp.NextBackOff()); // elapsed 0 + 10 <= 25
        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(15), exp.NextBackOff()); // elapsed 10 + 15 <= 25
        clock.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(BackOffConstants.Stop, exp.NextBackOff()); // elapsed 25 + 22.5 > 25
    }

    [Fact]
    public void MaxElapsedTimeZero_NeverStops()
    {
        var clock = new ManualTimeProvider();
        var exp = New(clock);
        exp.MaxElapsedTime = TimeSpan.Zero;
        exp.Reset();

        clock.Advance(TimeSpan.FromDays(3650));
        Assert.NotEqual(BackOffConstants.Stop, exp.NextBackOff());
    }

    [Fact]
    public void TestGetElapsedTime()
    {
        var clock = new ManualTimeProvider();
        var exp = New(clock);
        exp.Reset();

        Assert.Equal(TimeSpan.Zero, exp.GetElapsedTime());
        clock.Advance(TimeSpan.FromSeconds(7));
        Assert.Equal(TimeSpan.FromSeconds(7), exp.GetElapsedTime());
        exp.Reset();
        Assert.Equal(TimeSpan.Zero, exp.GetElapsedTime());
    }

    [Fact]
    public void TestBackOffOverflow()
    {
        var exp = New();
        exp.InitialInterval = TimeSpan.FromTicks(long.MaxValue / 2);
        exp.Multiplier = 2.1;
        exp.MaxInterval = TimeSpan.FromTicks(long.MaxValue);
        exp.Reset();

        exp.NextBackOff();
        // Assert that when an overflow is possible, the current interval is set to the max interval.
        Assert.Equal(TimeSpan.FromTicks(long.MaxValue), exp.CurrentInterval);
    }

    [Fact]
    public void TestCustomStop()
    {
        var clock = new ManualTimeProvider();
        var exp = New(clock);
        exp.Stop = TimeSpan.FromMinutes(1);
        exp.MaxElapsedTime = TimeSpan.FromSeconds(1);
        exp.Reset();

        clock.Advance(TimeSpan.FromSeconds(10000));
        Assert.Equal(TimeSpan.FromMinutes(1), exp.NextBackOff());
    }

    [Fact]
    public void Reset_RestartsTheIntervalAndTheClock()
    {
        var exp = New();
        exp.RandomizationFactor = 0;
        exp.InitialInterval = TimeSpan.FromMilliseconds(10);
        exp.Reset();

        exp.NextBackOff();
        exp.NextBackOff();
        Assert.Equal(TimeSpan.FromMilliseconds(22.5), exp.CurrentInterval);
        exp.Reset();
        Assert.Equal(TimeSpan.FromMilliseconds(10), exp.CurrentInterval);
    }

    [Fact]
    public void FreshInstance_IsResetLazily()
    {
        var exp = New();
        exp.InitialInterval = TimeSpan.FromMilliseconds(40);
        exp.RandomizationFactor = 0;
        exp.MaxElapsedTime = TimeSpan.Zero;
        // no explicit Reset(): a struct literal in Go would yield 0 here; the object initializer pattern must work
        Assert.Equal(TimeSpan.FromMilliseconds(40), exp.NextBackOff());
    }

    // ----- BackOffFactory / context ----------------------------------------------------------------------------------
    [Fact]
    public void WithContext_ReturnsStopOnceTheTokenIsCancelled()
    {
        using var cts = new CancellationTokenSource();
        var bo = BackOffFactory.WithContext(new ZeroBackOff(), cts.Token);

        Assert.Equal(cts.Token, bo.Context);
        Assert.Equal(TimeSpan.Zero, bo.NextBackOff());
        cts.Cancel();
        Assert.Equal(BackOffConstants.Stop, bo.NextBackOff());
    }

    [Fact]
    public void WithContext_OnAContextBackOffReplacesTheToken()
    {
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();
        var inner = new CountingBackOff();
        var one = BackOffFactory.WithContext(inner, first.Token);
        var two = BackOffFactory.WithContext(one, second.Token);

        first.Cancel();
        Assert.Equal(second.Token, two.Context);
        Assert.Equal(TimeSpan.Zero, two.NextBackOff()); // the first token no longer matters
        Assert.Equal(1, inner.NextCalls);

        two.Reset();
        Assert.Equal(1, inner.Resets);
    }

    [Fact]
    public void NewBackOff_Is100msNeverStopping()
    {
        using var cts = new CancellationTokenSource();
        var bo = BackOffFactory.NewBackOff(cts.Token);
        Assert.Equal(TimeSpan.FromMilliseconds(100), BackOffFactory.DefaultInitialInterval);
        Assert.Equal(TimeSpan.FromSeconds(10), BackOffFactory.SlowInitialInterval);

        bo.Reset();
        var first = bo.NextBackOff();
        Assert.InRange(first, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(150));

        // grows towards 60s +- 50% and never returns Stop, however long it has been running
        TimeSpan last = default;
        for (var i = 0; i < 100; i++) last = bo.NextBackOff();
        Assert.InRange(last, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(90));
    }

    [Fact]
    public void NewBackOffWithInitialInterval_UsesTheInterval()
    {
        var bo = BackOffFactory.NewBackOffWithInitialInterval(CancellationToken.None, TimeSpan.FromHours(1));
        bo.Reset();
        Assert.InRange(bo.NextBackOff(), TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(90));
    }

    [Fact]
    public async Task ConcurrentBackOff_SerialisesCalls()
    {
        var inner = new CountingBackOff();
        var bo = new ConcurrentBackOff(inner);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 10_000; i++)
            {
                bo.NextBackOff();
                if (i % 100 == 0) bo.Reset();
            }
        })));

        // the unsynchronised counters would lose updates without the mutex
        Assert.Equal(8 * 10_000, inner.NextCalls);
        Assert.Equal(8 * 100, inner.Resets);
    }

    private sealed class CountingBackOff : IBackOff
    {
        public int NextCalls;
        public int Resets;

        public TimeSpan NextBackOff()
        {
            NextCalls++;
            return TimeSpan.Zero;
        }

        public void Reset() => Resets++;
    }
}
