// =====================================================================================================================
// PORT NOTE
//   Upstream : common/concurrent/future_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : First-wins Complete/Fail, a ready value beating a done context, and Wait honouring cancellation / timeout.
//   Go tests: 5, ported: 5, skipped: 0.
//   Divergences:
//      * `context.WithTimeout` -> CancellationTokenSource(timeout); the deadline case (`context.DeadlineExceeded`) is an
//        OperationCanceledException here, asserted with OxiaErrors.IsCanceled.
//      * assertFutureNotReady asserts inside a goroutine that is NOT awaited by the Go test (it finishes after the helper's deferred
//        cancel()); the port awaits that wait after cancelling so its assertion is actually observed -- and so that the next wait on
//        the same future starts after the cancelled one ended (a Future serves one wait at a time).
//      * `assert.ErrorIs(err, io.EOF)` -> OxiaErrors.Is(err, OxiaErrors.EndOfStream).
// =====================================================================================================================
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class FutureTests
{
    private static async Task AssertFutureNotReady<T>(Future<T> f)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        var wait = f.WaitAsync(cts.Token).AsTask();
        var first = await Task.WhenAny(wait, Task.Delay(TimeSpan.FromMilliseconds(100)));
        Assert.NotSame(wait, first); // "should have timed out"

        // `defer cancel()` of the helper
        cts.Cancel();
        var err = await Record.ExceptionAsync(() => wait.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(OxiaErrors.IsCanceled(err)); // assert.ErrorIs(err, context.Canceled)
    }

    [Fact]
    public async Task TestFuture()
    {
        var f = new Future<int>();
        await AssertFutureNotReady(f);

        f.Complete(5);
        var res = await f.WaitAsync(CancellationToken.None);
        Assert.Equal(5, res);

        f = new Future<int>();
        f.Fail(OxiaErrors.EndOfStream);
        var err = await Record.ExceptionAsync(async () => await f.WaitAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.EndOfStream));
    }

    [Fact]
    public async Task TestFutureKeepsTheFirstValue()
    {
        var f = new Future<int>();
        f.Complete(5);
        f.Complete(6);
        f.Fail(OxiaErrors.EndOfStream);
        var res = await f.WaitAsync(CancellationToken.None);
        Assert.Equal(5, res);

        f = new Future<int>();
        f.Fail(OxiaErrors.EndOfStream);
        f.Complete(5);
        var err = await Record.ExceptionAsync(async () => await f.WaitAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.EndOfStream));
    }

    [Fact]
    public async Task TestFutureWaitPrefersTheValueOverADoneContext()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        for (var i = 0; i < 100; i++)
        {
            var f = new Future<int>();
            f.Complete(5);
            var res = await f.WaitAsync(cts.Token);
            Assert.Equal(5, res);
        }
    }

    [Fact]
    public async Task TestFuture_Cancel()
    {
        var f = new Future<int>();
        using var cts = new CancellationTokenSource();

        var done = Task.Run(async () =>
        {
            var err = await Record.ExceptionAsync(async () => await f.WaitAsync(cts.Token));
            Assert.True(OxiaErrors.IsCanceled(err)); // assert.ErrorIs(err, context.Canceled); res == 0 is implied by the exception
        });

        cts.Cancel();
        await done.WaitAsync(TimeSpan.FromSeconds(1)); // "shouldn't have timed out"
    }

    [Fact]
    public async Task TestFuture_Timeout()
    {
        var f = new Future<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var done = Task.Run(async () =>
        {
            var err = await Record.ExceptionAsync(async () => await f.WaitAsync(cts.Token));
            Assert.True(OxiaErrors.IsCanceled(err)); // Go: context.DeadlineExceeded
        });

        await done.WaitAsync(TimeSpan.FromSeconds(1)); // "shouldn't have timed out"
    }
}
