// =====================================================================================================================
// PORT NOTE
//   Upstream : (none) -- extra tests for common/concurrent/future.go and wait_group.go: races between completion and
//              cancellation, the Go "cancelled wait leaves the future untouched" behaviour, reuse, sentinels, allocation.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class FutureExtraTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ACancelledWaitLeavesTheFutureUsable()
    {
        // The Go TestFuture scenario: a wait gives up, then the future is completed and waited on again
        var f = new Future<int>();
        using (var cts = new CancellationTokenSource())
        {
            var wait = f.WaitAsync(cts.Token).AsTask();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));
        }

        Assert.False(f.IsCompleted);
        f.Complete(9);
        Assert.True(f.IsCompleted);
        Assert.Equal(9, await f.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task ACancelledWaitCanBeFollowedByAWaitThatIsCompletedLater()
    {
        var f = new Future<string>();
        using (var cts = new CancellationTokenSource(10))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await f.WaitAsync(cts.Token));
        }

        var second = f.WaitAsync(CancellationToken.None).AsTask();
        Assert.False(second.IsCompleted);
        f.Complete("later");
        Assert.Equal("later", await second.WaitAsync(Timeout));
    }

    [Fact]
    public async Task ACancelledTokenWithNoValueFailsImmediately()
    {
        var f = new Future<int>();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await f.WaitAsync(cts.Token));
        Assert.Equal(cts.Token, thrown.CancellationToken);
    }

    [Fact]
    public async Task ACompletionAfterTheWaitStartedWakesTheWaiter()
    {
        var f = new Future<int>();
        var wait = f.WaitAsync(CancellationToken.None).AsTask();
        Assert.False(wait.IsCompleted);

        f.Complete(3);
        Assert.Equal(3, await wait.WaitAsync(Timeout));
    }

    [Fact]
    public async Task AFailureAfterTheWaitStartedWakesTheWaiterWithTheException()
    {
        var f = new Future<int>();
        var wait = f.WaitAsync(CancellationToken.None).AsTask();

        var boom = new InvalidOperationException("boom");
        f.Fail(boom);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => wait.WaitAsync(Timeout));
        Assert.Same(boom, thrown);
    }

    [Fact]
    public async Task FailingWithASharedSentinelThrowsAFreshExceptionThatMatchesIs()
    {
        // A private OxiaException plays the shared sentinel: the real OxiaErrors.SessionNotFound is process-wide state, and other tests
        // (e.g. SessionsTests.TestClientSessionClose("session-not-found") completes a ValueTask.FromException with it, which stamps
        // its StackTrace when awaited) make "StackTrace is null" depend on the order in which the whole suite runs.
        var sentinel = new OxiaException("oxia: private sentinel of this test");
        var f = new Future<int>();
        f.Fail(sentinel);

        var thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await f.WaitAsync(CancellationToken.None));
        Assert.NotSame(sentinel, thrown);
        Assert.True(OxiaErrors.Is(thrown, sentinel));
        Assert.Null(sentinel.StackTrace); // the shared instance was never thrown
    }

    [Fact]
    public void FailRejectsNull()
    {
        Assert.Throws<ArgumentNullException>(() => new Future<int>().Fail(null!));
    }

    [Fact]
    public async Task ACompletionRacingWithACancellation_ResolvesTheWaiterExactlyOnce()
    {
        // either the value or the cancellation wins; never both, never a hang, never a lost wake-up
        for (var i = 0; i < 2000; i++)
        {
            var f = new Future<int>();
            using var cts = new CancellationTokenSource();
            var wait = f.WaitAsync(cts.Token).AsTask();
            var completer = Task.Run(() => f.Complete(i));
            var canceller = Task.Run(cts.Cancel);

            var err = await Record.ExceptionAsync(() => wait.WaitAsync(Timeout));
            await Task.WhenAll(completer, canceller);
            if (err is null) Assert.Equal(i, await wait);
            else Assert.IsAssignableFrom<OperationCanceledException>(err);
            Assert.True(f.IsCompleted);
        }
    }

    [Fact]
    public async Task ACompletionRacingWithTheStartOfTheWait_IsNeverLost()
    {
        for (var i = 0; i < 5000; i++)
        {
            var f = new Future<int>();
            var start = new ManualResetEventSlim();
            var completer = Task.Run(() =>
            {
                start.Wait();
                f.Complete(i);
            });

            start.Set();
            Assert.Equal(i, await f.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout));
            await completer;
        }
    }

    [Fact]
    public async Task ManyConcurrentCompletions_OneWins_AllWaitersSeeTheSameValue()
    {
        for (var round = 0; round < 200; round++)
        {
            var f = new Future<int>();
            var tasks = Enumerable.Range(0, 8).Select(n => Task.Run(() => f.Complete(n))).ToArray();
            await Task.WhenAll(tasks);

            var first = await f.WaitAsync(CancellationToken.None);
            Assert.InRange(first, 0, 7);
            Assert.Equal(first, await f.WaitAsync(CancellationToken.None)); // the result stays available
        }
    }

    [Fact]
    public async Task ResetMakesTheInstanceReusable()
    {
        var f = new Future<int>();
        for (var i = 0; i < 100; i++)
        {
            var wait = f.WaitAsync(CancellationToken.None).AsTask();
            f.Complete(i);
            f.Complete(-1);
            Assert.Equal(i, await wait.WaitAsync(Timeout));
            f.Reset();
            Assert.False(f.IsCompleted);
        }

        f.Fail(new Exception("x"));
        await Assert.ThrowsAsync<Exception>(async () => await f.WaitAsync(CancellationToken.None));
        f.Reset();
        f.Complete(1);
        Assert.Equal(1, await f.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task ContinuationsRunAsynchronouslyByDefault()
    {
        var f = new Future<int>();
        var completerThread = -1;
        var continuationThread = -1;

        var wait = Task.Run(async () =>
        {
            await f.WaitAsync(CancellationToken.None);
            continuationThread = Environment.CurrentManagedThreadId;
        });
        await Task.Delay(50);

        var completer = new Thread(() =>
        {
            completerThread = Environment.CurrentManagedThreadId;
            f.Complete(1);
        });
        completer.Start();
        completer.Join();
        await wait.WaitAsync(Timeout);

        Assert.NotEqual(completerThread, continuationThread); // not inline in the completing thread
    }

    [Fact]
    public void TheSuccessPathDoesNotAllocate()
    {
        var f = new Future<long>();
        using var cts = new CancellationTokenSource();

        // warm up (JIT, static delegates)
        Cycle(f, cts.Token, 10);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var sink = Cycle(f, cts.Token, 1000);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(sink > 0);
        Assert.Equal(0, allocated);
    }

    // Synchronous on purpose (the ValueTasks are already completed when they are consumed)
    private static long Cycle(Future<long> f, CancellationToken ct, int iterations)
    {
        long sink = 0;
        for (var i = 1; i <= iterations; i++)
        {
            // value first, then wait (ready path)
            f.Reset();
            f.Complete(i);
            sink += f.WaitAsync(ct).GetAwaiter().GetResult();

            // wait first, then value (waiter slot path)
            f.Reset();
            var pending = f.WaitAsync(CancellationToken.None);
            f.Complete(i);
            sink += pending.GetAwaiter().GetResult();
        }

        return sink;
    }
}

public class WaitGroupExtraTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ZeroParties_WaitReturnsImmediately()
    {
        var wg = new WaitGroup(0);
        Assert.Null(await wg.WaitAsync(CancellationToken.None));
        wg.Done(); // harmless
        Assert.Null(await wg.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public void NegativePartiesAreRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new WaitGroup(-1));
    }

    [Fact]
    public async Task DoneBeyondThePartyCountIsIgnored()
    {
        var wg = new WaitGroup(2);
        for (var i = 0; i < 10; i++) wg.Done();
        Assert.Null(await wg.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task FailBeyondThePartyCountIsDropped()
    {
        var wg = new WaitGroup(2);
        wg.Done();
        wg.Done();
        wg.Fail(new Exception("too late")); // Go would block here; the response is dropped
        Assert.Null(await wg.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task TheFirstFailureInResponseOrderIsReturned()
    {
        var wg = new WaitGroup(4);
        var first = new Exception("first");
        wg.Done();
        wg.Fail(first);
        wg.Fail(new Exception("second"));
        wg.Done();
        Assert.Same(first, await wg.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task AFailureWhileWaitingWakesTheWaiterBeforeTheOtherPartiesAreDone()
    {
        var wg = new WaitGroup(3);
        var wait = wg.WaitAsync(CancellationToken.None).AsTask();
        wg.Done();
        Assert.False(wait.IsCompleted);
        var boom = new Exception("boom");
        wg.Fail(boom);
        Assert.Same(boom, await wait.WaitAsync(Timeout));
    }

    [Fact]
    public async Task ACancelledWaitDoesNotSwallowLaterResponses()
    {
        var wg = new WaitGroup(1);
        using (var cts = new CancellationTokenSource())
        {
            var cancelled = wg.WaitAsync(cts.Token).AsTask();
            cts.Cancel();
            Assert.IsAssignableFrom<OperationCanceledException>(await cancelled.WaitAsync(Timeout));
        }

        wg.Done();
        Assert.Null(await wg.WaitAsync(CancellationToken.None));
    }

    [Fact]
    public async Task GoRunsTheFunctionsAndReportsTheFirstFailure()
    {
        var wg = new WaitGroup(3);
        var ran = 0;
        wg.Go(() =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });
        wg.Go(async () =>
        {
            await Task.Yield();
            Interlocked.Increment(ref ran);
        });
        wg.Go(() => Task.CompletedTask);
        Assert.Null(await wg.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout));
        Assert.Equal(2, ran);

        var failing = new WaitGroup(2);
        var boom = new InvalidOperationException("boom");
        failing.Go(() => Task.CompletedTask);
        failing.Go(() => throw boom);
        Assert.Same(boom, await failing.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout));

        var failingAsync = new WaitGroup(1);
        failingAsync.Go(async () =>
        {
            await Task.Yield();
            throw boom;
        });
        Assert.Same(boom, await failingAsync.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public async Task ConcurrentDoneFromManyTasks()
    {
        var wg = new WaitGroup(64);
        for (var i = 0; i < 64; i++) _ = Task.Run(wg.Done);
        Assert.Null(await wg.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Timeout));
    }
}
