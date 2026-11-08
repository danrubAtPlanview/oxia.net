// =====================================================================================================================
// PORT NOTE
//   Upstream : common/concurrent/wait_group_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : WaitGroup returns the first failure, waits for every party, and honours cancellation / timeout.
//   Go tests: 3, ported: 3, skipped: 0.
//   Divergences:
//      * Wait(ctx) error -> `ValueTask<Exception?>` (cancellation is an OperationCanceledException VALUE, not thrown).
//        `context.WithTimeout` -> CancellationTokenSource(timeout); `context.DeadlineExceeded` is an OperationCanceledException here.
//      * assertNotReady asserts inside a goroutine that the Go test does not await; the port awaits the cancelled wait after the
//        helper's `defer cancel()` so that its assertion is observed.
// =====================================================================================================================
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class WaitGroupTests
{
    private static async Task AssertNotReady(WaitGroup wg)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        var wait = wg.WaitAsync(cts.Token).AsTask();
        var first = await Task.WhenAny(wait, Task.Delay(TimeSpan.FromMilliseconds(100)));
        Assert.NotSame(wait, first); // "should have timed out"

        // `defer cancel()` of the helper
        cts.Cancel();
        var err = await wait.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(OxiaErrors.IsCanceled(err)); // assert.ErrorIs(wg.Wait(ctx), context.Canceled)
    }

    [Fact]
    public async Task TestWaitGroup()
    {
        var wg = new WaitGroup(3);
        await AssertNotReady(wg);

        wg = new WaitGroup(1);
        await AssertNotReady(wg);
        wg.Done();
        Assert.Null(await wg.WaitAsync(CancellationToken.None));

        wg = new WaitGroup(3);
        wg.Done();
        wg.Done();
        await AssertNotReady(wg);

        wg = new WaitGroup(3);
        await AssertNotReady(wg);
        wg.Done();
        wg.Done();
        wg.Done();
        Assert.Null(await wg.WaitAsync(CancellationToken.None));

        wg = new WaitGroup(3);
        wg.Fail(OxiaErrors.EndOfStream);
        Assert.True(OxiaErrors.Is(await wg.WaitAsync(CancellationToken.None), OxiaErrors.EndOfStream));

        wg = new WaitGroup(3);
        wg.Done();
        wg.Fail(OxiaErrors.EndOfStream);
        Assert.True(OxiaErrors.Is(await wg.WaitAsync(CancellationToken.None), OxiaErrors.EndOfStream));
    }

    [Fact]
    public async Task TestWaitGroup_Cancel()
    {
        var wg = new WaitGroup(3);
        using var cts = new CancellationTokenSource();

        var done = Task.Run(async () =>
        {
            Assert.True(OxiaErrors.IsCanceled(await wg.WaitAsync(cts.Token))); // assert.ErrorIs(wg.Wait(ctx), context.Canceled)
        });

        cts.Cancel();
        await done.WaitAsync(TimeSpan.FromSeconds(1)); // "shouldn't have timed out"
    }

    [Fact]
    public async Task TestWaitGroup_Timeout()
    {
        var wg = new WaitGroup(3);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var done = Task.Run(async () =>
        {
            Assert.True(OxiaErrors.IsCanceled(await wg.WaitAsync(cts.Token))); // Go: context.DeadlineExceeded
        });

        await done.WaitAsync(TimeSpan.FromSeconds(1)); // "shouldn't have timed out"
    }
}
