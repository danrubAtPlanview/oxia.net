// =====================================================================================================================
// PORT NOTE
//   Upstream : (none in the oxia repo) -- tests of the cenkalti backoff v4.3.0 retry loop (retry.go) semantics the client relies
//              on: Retry / RetryNotify / RetryNotifyWithTimer, Permanent, Stop handling, ctx cancellation while waiting, the
//              notify callback and the timer substitution, plus the default SystemRetryTimer.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Diagnostics;
using Oxia.Common.Constant;
using Oxia.Common.Time;
using Xunit;

namespace Oxia.Client.Tests.Common;

/// <summary>A backoff that plays a fixed list of delays, then returns Stop; records how it is driven.</summary>
internal sealed class ScriptedBackOff : IBackOffContext
{
    private readonly TimeSpan[] _delays;
    private readonly List<string> _log;
    private int _index;

    public ScriptedBackOff(CancellationToken context, List<string>? log, params TimeSpan[] delays)
    {
        Context = context;
        _delays = delays;
        _log = log ?? [];
    }

    public int Resets { get; private set; }
    public int NextCalls { get; private set; }
    public CancellationToken Context { get; }

    public TimeSpan NextBackOff()
    {
        NextCalls++;
        _log.Add("next");
        return _index < _delays.Length ? _delays[_index++] : BackOffConstants.Stop;
    }

    public void Reset()
    {
        Resets++;
        _index = 0;
        _log.Add("reset");
    }
}

/// <summary>A timer whose wait completes when the test says so (or immediately), observing the token it is given.</summary>
internal sealed class FakeRetryTimer : IRetryTimer
{
    private readonly List<string>? _log;
    private TaskCompletionSource _fire = NewSource();

    public FakeRetryTimer(List<string>? log = null, bool fireImmediately = false)
    {
        _log = log;
        FireImmediately = fireImmediately;
    }

    public bool FireImmediately { get; set; }
    public List<TimeSpan> Starts { get; } = [];
    public int Stops { get; private set; }
    public CancellationToken LastWaitToken { get; private set; }

    public void Start(TimeSpan duration)
    {
        _log?.Add("start");
        Starts.Add(duration);
        _fire = NewSource();
        if (FireImmediately) _fire.TrySetResult();
    }

    public void Stop()
    {
        _log?.Add("stop");
        Stops++;
    }

    public void Fire() => _fire.TrySetResult();

    public async ValueTask WaitAsync(CancellationToken ct)
    {
        _log?.Add("wait");
        LastWaitToken = ct;
        using var registration = ct.Register(static s => ((TaskCompletionSource)s!).TrySetCanceled(), _fire);
        await _fire.Task.ConfigureAwait(false);
    }

    private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public class RetryTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Success_OnTheFirstAttempt_ReturnsTheValueWithoutWaiting()
    {
        var log = new List<string>();
        var bo = new ScriptedBackOff(CancellationToken.None, log, TimeSpan.FromHours(1));
        var timer = new FakeRetryTimer(log);
        var notified = 0;
        var calls = 0;

        var result = await Retry.RetryNotifyWithTimerAsync<int>(() =>
        {
            calls++;
            return ValueTask.FromResult(42);
        }, bo, (_, _) => notified++, timer);

        Assert.Equal(42, result);
        Assert.Equal(1, calls);
        Assert.Equal(0, notified);
        Assert.Equal(1, bo.Resets);
        Assert.Equal(0, bo.NextCalls);
        Assert.Empty(timer.Starts);
        Assert.Equal(1, timer.Stops); // defer t.Stop()
        Assert.Equal(["reset", "stop"], log);
    }

    [Fact]
    public async Task RetriesUntilSuccess_NotifyGetsTheErrorAndTheDelay_BeforeTheTimerStarts()
    {
        var log = new List<string>();
        var bo = new ScriptedBackOff(CancellationToken.None, log, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(7));
        var timer = new FakeRetryTimer(log, fireImmediately: true);
        var errors = new[] { new InvalidOperationException("first"), new InvalidOperationException("second") };
        var notifications = new List<(Exception Err, TimeSpan Delay)>();
        var attempt = 0;

        var result = await Retry.RetryNotifyWithTimerAsync<string>(() =>
        {
            if (attempt < errors.Length) throw errors[attempt++];
            return ValueTask.FromResult("done");
        }, bo, (e, d) =>
        {
            log.Add("notify");
            notifications.Add((e, d));
        }, timer);

        Assert.Equal("done", result);
        Assert.Equal(1, bo.Resets); // Reset once, not between the attempts
        Assert.Equal(2, notifications.Count);
        Assert.Same(errors[0], notifications[0].Err);
        Assert.Equal(TimeSpan.FromMilliseconds(5), notifications[0].Delay);
        Assert.Same(errors[1], notifications[1].Err);
        Assert.Equal(TimeSpan.FromMilliseconds(7), notifications[1].Delay);
        Assert.Equal([TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(7)], timer.Starts);
        Assert.Equal(1, timer.Stops);
        Assert.Equal(["reset", "next", "notify", "start", "wait", "next", "notify", "start", "wait", "stop"], log);
    }

    [Fact]
    public async Task NonGenericOverloads_Work()
    {
        var calls = 0;
        await Retry.RetryAsync(() =>
        {
            if (++calls < 3) throw new Exception("again");
            return ValueTask.CompletedTask;
        }, new ZeroBackOff());
        Assert.Equal(3, calls);

        calls = 0;
        var notified = 0;
        await Retry.RetryNotifyAsync(async () =>
        {
            await Task.Yield();
            if (++calls < 2) throw new Exception("again");
        }, new ZeroBackOff(), (_, _) => notified++);
        Assert.Equal(2, calls);
        Assert.Equal(1, notified);

        Assert.Equal(7, await Retry.RetryAsync(() => ValueTask.FromResult(7), new ZeroBackOff()));
    }

    [Fact]
    public async Task WhenTheBackOffStops_TheLastErrorIsRethrown()
    {
        var bo = new ScriptedBackOff(CancellationToken.None, null, TimeSpan.Zero); // one retry, then Stop
        var timer = new FakeRetryTimer(fireImmediately: true);
        var attempt = 0;
        var errors = new[] { new InvalidOperationException("one"), new InvalidOperationException("two") };
        var notified = 0;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Retry.RetryNotifyWithTimerAsync<int>(() => throw errors[attempt++], bo, (_, _) => notified++, timer));

        Assert.Same(errors[1], thrown); // the error of the last attempt, not a wrapper
        Assert.Equal(2, attempt);
        Assert.Equal(1, notified); // "if the backoff policy stated to stop retrying, the notify function isn't called"
        Assert.Equal(1, timer.Stops);
    }

    [Fact]
    public async Task WhenTheBackOffStopsBecauseTheContextIsCancelled_OperationCanceledIsThrown()
    {
        using var cts = new CancellationTokenSource();
        var bo = BackOffFactory.WithContext(new ZeroBackOff(), cts.Token);
        var attempts = 0;

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Retry.RetryAsync<int>(() =>
            {
                attempts++;
                cts.Cancel();
                throw new InvalidOperationException("failed");
            }, bo));

        Assert.Equal(cts.Token, thrown.CancellationToken);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TheOperationRunsAtLeastOnce_EvenWithACancelledContext()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var bo = BackOffFactory.WithContext(new ZeroBackOff(), cts.Token);

        Assert.Equal(5, await Retry.RetryAsync(() => ValueTask.FromResult(5), bo));
        Assert.Equal(5, await Retry.RetryAsync(() => ValueTask.FromResult(5), new ZeroBackOff(), cts.Token));

        var attempts = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Retry.RetryAsync<int>(() =>
            {
                attempts++;
                throw new Exception("x");
            }, bo));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task PermanentError_StopsTheLoopAndThrowsTheInnerError()
    {
        var bo = new ScriptedBackOff(CancellationToken.None, null, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
        var timer = new FakeRetryTimer(fireImmediately: true);
        var inner = new InvalidOperationException("permanent cause");
        var attempts = 0;
        var notified = 0;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Retry.RetryNotifyWithTimerAsync<int>(() =>
            {
                if (++attempts == 2) throw new PermanentException(inner);
                throw new Exception("transient");
            }, bo, (_, _) => notified++, timer));

        Assert.Same(inner, thrown); // not the PermanentException wrapper
        Assert.Equal(2, attempts);
        Assert.Equal(1, notified);
        Assert.Single(timer.Starts);
        Assert.Equal(1, timer.Stops);
    }

    [Fact]
    public async Task PermanentError_OnTheFirstAttempt_IsNeverRetried()
    {
        var bo = new ScriptedBackOff(CancellationToken.None, null, TimeSpan.Zero);
        var attempts = 0;

        await Assert.ThrowsAsync<ArgumentException>(async () => await Retry.RetryAsync<int>(() =>
        {
            attempts++;
            throw new PermanentException(new ArgumentException("p"));
        }, bo));

        Assert.Equal(1, attempts);
        Assert.Equal(0, bo.NextCalls);
    }

    [Fact]
    public async Task PermanentError_IsFoundInsideAnotherException()
    {
        var inner = new InvalidOperationException("p");
        var viaInner = new Exception("wrapper", new PermanentException(inner));
        var viaAggregate = new AggregateException(new Exception("a"), new PermanentException(inner));

        foreach (var wrapped in new Exception[] { viaInner, viaAggregate })
        {
            var attempts = 0;
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await Retry.RetryAsync<int>(() =>
                {
                    attempts++;
                    throw wrapped;
                }, new ZeroBackOff()));
            Assert.Same(inner, thrown);
            Assert.Equal(1, attempts);
        }
    }

    [Fact]
    public async Task PermanentError_OfASharedSentinel_ThrowsAFreshExceptionThatStillMatches()
    {
        var thrown = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await Retry.RetryAsync<int>(() => throw new PermanentException(OxiaErrors.ShardNotFound), new ZeroBackOff()));

        Assert.NotSame(OxiaErrors.ShardNotFound, thrown); // never throw the shared instance
        Assert.True(OxiaErrors.Is(thrown, OxiaErrors.ShardNotFound));
        Assert.Equal(OxiaErrors.ShardNotFound.Message, thrown.Message);
    }

    [Fact]
    public async Task CancellingTheBackOffContextWhileWaiting_ThrowsOperationCanceledPromptly()
    {
        using var cts = new CancellationTokenSource();
        var bo = new ScriptedBackOff(cts.Token, null, TimeSpan.FromHours(1));
        var timer = new FakeRetryTimer();

        var run = Retry.RetryNotifyWithTimerAsync<int>(() => throw new Exception("x"), bo, null, timer).AsTask();
        await WaitUntil(() => timer.Starts.Count == 1);
        Assert.False(run.IsCompleted);

        cts.Cancel();
        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Timeout));
        Assert.Equal(cts.Token, thrown.CancellationToken);
        Assert.Equal(1, timer.Stops);
    }

    [Fact]
    public async Task CancellingTheExplicitToken_StopsTheWaitOfABackOffWithoutContext()
    {
        using var cts = new CancellationTokenSource();
        var timer = new FakeRetryTimer();

        var run = Retry.RetryNotifyWithTimerAsync<int>(() => throw new Exception("x"), new ZeroBackOff(), null, timer, cts.Token).AsTask();
        await WaitUntil(() => timer.Starts.Count == 1);
        Assert.Equal(cts.Token, timer.LastWaitToken); // only one token can cancel: no linked source

        cts.Cancel();
        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Timeout));
        Assert.Equal(cts.Token, thrown.CancellationToken);
    }

    [Fact]
    public async Task BothTokens_EitherOneCancelsTheWait()
    {
        foreach (var cancelExplicit in new[] { true, false })
        {
            using var contextSource = new CancellationTokenSource();
            using var explicitSource = new CancellationTokenSource();
            var bo = BackOffFactory.WithContext(new ScriptedBackOff(CancellationToken.None, null, TimeSpan.FromHours(1)), contextSource.Token);
            var timer = new FakeRetryTimer();

            var run = Retry.RetryNotifyWithTimerAsync<int>(() => throw new Exception("x"), bo, null, timer, explicitSource.Token).AsTask();
            await WaitUntil(() => timer.Starts.Count == 1);

            if (cancelExplicit) explicitSource.Cancel();
            else contextSource.Cancel();

            var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Timeout));
            Assert.Equal(cancelExplicit ? explicitSource.Token : contextSource.Token, thrown.CancellationToken);
        }
    }

    [Fact]
    public async Task ExplicitToken_CancelledAfterAFailure_StopsWithoutNotifying()
    {
        using var cts = new CancellationTokenSource();
        var notified = 0;
        var attempts = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Retry.RetryNotifyAsync<int>(() =>
            {
                attempts++;
                cts.Cancel();
                throw new Exception("x");
            }, new ZeroBackOff(), (_, _) => notified++, cts.Token));

        Assert.Equal(1, attempts);
        Assert.Equal(0, notified);
    }

    [Fact]
    public async Task OperationCanceledThrownByTheOperation_IsRetriedLikeAnyOtherError()
    {
        // Go retries a returned context.Canceled until the backoff context itself is done
        var attempts = 0;
        var result = await Retry.RetryAsync<int>(() =>
        {
            if (++attempts < 3) throw new OperationCanceledException();
            return ValueTask.FromResult(attempts);
        }, new ZeroBackOff());
        Assert.Equal(3, result);
    }

    [Fact]
    public async Task TimerSubstitution_ATimerThatFiresEarlyShortCircuitsALongBackOff()
    {
        var bo = new ScriptedBackOff(CancellationToken.None, null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var timer = new FakeRetryTimer(fireImmediately: true);
        var attempts = 0;

        var result = await Retry.RetryNotifyWithTimerAsync<int>(() =>
        {
            if (++attempts < 3) throw new Exception("x");
            return ValueTask.FromResult(attempts);
        }, bo, null, timer).AsTask().WaitAsync(Timeout);

        Assert.Equal(3, result);
        Assert.Equal([TimeSpan.FromHours(1), TimeSpan.FromHours(1)], timer.Starts);
    }

    [Fact]
    public async Task TimerSubstitution_ATimerFiredByTheOwnerWhileWaiting()
    {
        var bo = new ScriptedBackOff(CancellationToken.None, null, TimeSpan.FromHours(1));
        var timer = new FakeRetryTimer();
        var attempts = 0;

        var run = Retry.RetryNotifyWithTimerAsync<int>(() =>
        {
            if (++attempts < 2) throw new Exception("x");
            return ValueTask.FromResult(attempts);
        }, bo, null, timer).AsTask();

        await WaitUntil(() => timer.Starts.Count == 1);
        Assert.False(run.IsCompleted);
        timer.Fire();
        Assert.Equal(2, await run.WaitAsync(Timeout));
    }

    [Fact]
    public async Task TheSuppliedTimerIsStoppedOnEveryExit()
    {
        // success, permanent error, give up, notify throwing
        var timer = new FakeRetryTimer(fireImmediately: true);

        await Retry.RetryNotifyWithTimerAsync<int>(() => ValueTask.FromResult(1), new ZeroBackOff(), null, timer);
        Assert.Equal(1, timer.Stops);

        await Assert.ThrowsAsync<Exception>(async () => await Retry.RetryNotifyWithTimerAsync<int>(
            () => throw new PermanentException(new Exception("p")), new ZeroBackOff(), null, timer));
        Assert.Equal(2, timer.Stops);

        await Assert.ThrowsAsync<Exception>(async () => await Retry.RetryNotifyWithTimerAsync<int>(
            () => throw new Exception("x"), new ScriptedBackOff(CancellationToken.None, null), null, timer));
        Assert.Equal(3, timer.Stops);

        var boom = new InvalidOperationException("notify failed");
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Retry.RetryNotifyWithTimerAsync<int>(
            () => throw new Exception("x"), new ZeroBackOff(), (_, _) => throw boom, timer));
        Assert.Same(boom, thrown);
        Assert.Equal(4, timer.Stops);
    }

    [Fact]
    public async Task DefaultTimer_WaitsForTheBackOffInterval()
    {
        var bo = new ExponentialBackOff
        {
            InitialInterval = TimeSpan.FromMilliseconds(40),
            RandomizationFactor = 0,
            Multiplier = 1,
            MaxElapsedTime = TimeSpan.Zero,
        };
        var attempts = 0;
        var sw = Stopwatch.StartNew();

        await Retry.RetryAsync(() =>
        {
            if (++attempts < 4) throw new Exception("x");
            return ValueTask.CompletedTask;
        }, bo).AsTask().WaitAsync(Timeout);

        Assert.Equal(4, attempts);
        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(3 * 40 - 10), $"3 waits of 40ms took only {sw.Elapsed}");
    }

    [Fact]
    public async Task DefaultTimer_WaitIsInterruptedByCancellation()
    {
        using var cts = new CancellationTokenSource();
        var bo = BackOffFactory.NewBackOffWithInitialInterval(cts.Token, TimeSpan.FromHours(1));
        var waiting = new TaskCompletionSource();

        var run = Retry.RetryNotifyAsync<int>(() => throw new Exception("x"), bo, (_, _) => waiting.TrySetResult()).AsTask();
        await waiting.Task.WaitAsync(Timeout);
        Assert.False(run.IsCompleted);

        cts.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Timeout));
    }

    [Fact]
    public async Task AsynchronousFailuresAreRetriedToo()
    {
        var attempts = 0;
        var result = await Retry.RetryAsync(async () =>
        {
            await Task.Delay(1);
            if (++attempts < 3) throw new Exception("late");
            return attempts;
        }, new ZeroBackOff());
        Assert.Equal(3, result);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(deadline.Elapsed < Timeout, "condition not reached");
            await Task.Delay(1);
        }
    }
}

public class SystemRetryTimerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task FiresAfterTheDuration()
    {
        using var timer = new SystemRetryTimer();
        var sw = Stopwatch.StartNew();
        timer.Start(TimeSpan.FromMilliseconds(50));
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(40), $"fired after {sw.Elapsed}");
    }

    [Fact]
    public async Task IsReusableForManyStartWaitCycles()
    {
        using var timer = new SystemRetryTimer();
        for (var i = 0; i < 200; i++)
        {
            timer.Start(i % 2 == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(1));
            await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
        }
    }

    [Fact]
    public async Task ReusableWithACancellableToken_WithoutLeakingRegistrations()
    {
        using var cts = new CancellationTokenSource();
        using var timer = new SystemRetryTimer();
        for (var i = 0; i < 100; i++)
        {
            timer.Start(TimeSpan.Zero);
            await timer.WaitAsync(cts.Token).AsTask().WaitAsync(Timeout);
        }

        // cancelling afterwards must not touch the timer (the registrations were released)
        cts.Cancel();
        timer.Start(TimeSpan.Zero);
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task NegativeAndHugeDurationsAreAccepted()
    {
        using var timer = new SystemRetryTimer();
        timer.Start(TimeSpan.FromSeconds(-5));
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);

        timer.Start(TimeSpan.FromTicks(-1)); // Go's backoff.Stop value would be a negative duration too
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);

        using var cts = new CancellationTokenSource();
        timer.Start(TimeSpan.MaxValue);
        var wait = timer.WaitAsync(cts.Token).AsTask();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));
    }

    [Fact]
    public async Task CancellationInterruptsTheWait()
    {
        using var cts = new CancellationTokenSource();
        using var timer = new SystemRetryTimer();
        timer.Start(TimeSpan.FromHours(1));
        var wait = timer.WaitAsync(cts.Token).AsTask();
        Assert.False(wait.IsCompleted);

        cts.Cancel();
        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));
        Assert.Equal(cts.Token, thrown.CancellationToken);

        // and the timer can be started again
        timer.Start(TimeSpan.Zero);
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task AnAlreadyCancelledTokenFailsTheWaitImmediately()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        using var timer = new SystemRetryTimer();
        timer.Start(TimeSpan.FromHours(1));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await timer.WaitAsync(cts.Token));
    }

    [Fact]
    public async Task StopFailsAPendingWait_AndIsHarmlessWhenIdleOrFired()
    {
        using var timer = new SystemRetryTimer();
        timer.Stop(); // never started

        timer.Start(TimeSpan.FromHours(1));
        var wait = timer.WaitAsync(CancellationToken.None).AsTask();
        timer.Stop();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));

        timer.Start(TimeSpan.Zero);
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
        timer.Stop(); // already fired
    }

    [Fact]
    public void WaitBeforeStartThrows()
    {
        using var timer = new SystemRetryTimer();
        Assert.Throws<InvalidOperationException>(() => timer.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public void StartAfterDisposeThrows()
    {
        var timer = new SystemRetryTimer();
        timer.Dispose();
        timer.Dispose(); // idempotent
        Assert.Throws<ObjectDisposedException>(() => timer.Start(TimeSpan.Zero));
    }

    [Fact]
    public async Task RestartingBeforeTheWaitRearmsTheTimer()
    {
        using var timer = new SystemRetryTimer();
        timer.Start(TimeSpan.FromHours(1));
        timer.Start(TimeSpan.Zero);
        await timer.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout);
    }
}
