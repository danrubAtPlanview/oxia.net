// =====================================================================================================================
// PORT NOTE
//   Upstream : common/time/backoff_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : ConcurrentBackOff keeps the cancellation context of the wrapped backoff, so the retry loop stops waiting as soon
//              as the context is cancelled.
//   Go tests: 2, ported: 2, skipped: 0.
//   Divergences: `backoff.RetryNotify(func() error {return errors.New("failed")}, ...)` -> Retry.RetryNotifyAsync with an
//              operation that throws; `assert.ErrorIs(err, context.Canceled)` -> OxiaErrors.IsCanceled (the loop throws an
//              OperationCanceledException); the Go `select` with a 10s timeout is Task.WaitAsync(10s);
//              `assert.Equal(ctx, bo.Context())` compares the CancellationToken values.
// =====================================================================================================================
using Oxia.Common.Constant;
using Oxia.Common.Time;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class BackoffTests
{
    [Fact]
    public async Task TestConcurrentBackOff_RetryNotifyStopsWaitingOnCancel()
    {
        using var cts = new CancellationTokenSource();
        var bo = new ConcurrentBackOff(BackOffFactory.NewBackOffWithInitialInterval(cts.Token, TimeSpan.FromHours(1)));

        Func<ValueTask> operation = () => throw new Exception("failed");
        var done = Retry.RetryNotifyAsync(operation, bo, (_, _) =>
        {
            // Called right before RetryNotify waits out the backoff interval
            cts.Cancel();
        }).AsTask();

        // RetryNotify must stop waiting as soon as the context is cancelled
        var err = await Record.ExceptionAsync(() => done.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.NotNull(err);
        Assert.IsNotType<TimeoutException>(err);
        Assert.True(OxiaErrors.IsCanceled(err));
    }

    [Fact]
    public void TestConcurrentBackOff_Context()
    {
        using var cts = new CancellationTokenSource();

        Assert.Equal(cts.Token, new ConcurrentBackOff(BackOffFactory.NewBackOff(cts.Token)).Context);
        Assert.Equal(CancellationToken.None, new ConcurrentBackOff(new ZeroBackOff()).Context);
    }
}
