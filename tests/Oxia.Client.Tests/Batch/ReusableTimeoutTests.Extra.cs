// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/internal/batch/{read_batch,write_batch}.go  (`context.WithTimeout(context.Background(), requestTimeout)`)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : the per-batch request timeout is one CancellationTokenSource re-armed for every batch: it fires after the
//              timeout, is reused while it did not fire, and is replaced (never reused cancelled) after it fired; the batches
//              arm it only when the executor does not complete synchronously and pass an already expired token for a zero
//              timeout (Go: a context with a zero timeout is expired).
// =====================================================================================================================
using Oxia.Client.Internal.Batch;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Batch;

public class ReusableTimeoutExtraTests
{
    [Fact]
    public void TheSourceIsReusedWhileItDidNotFire()
    {
        var timeout = new ReusableTimeout();
        var first = timeout.Token;
        timeout.Arm(TimeSpan.FromHours(1));
        Assert.False(first.IsCancellationRequested);
        timeout.Stop();

        var second = timeout.Token;
        Assert.Equal(first, second); // same source
        timeout.Arm(TimeSpan.FromHours(1));
        timeout.Stop();
        Assert.Equal(first, timeout.Token);
    }

    [Fact]
    public async Task AFiredSourceIsReplaced_AndTheNextTokenIsNotCancelled()
    {
        var timeout = new ReusableTimeout();
        var first = timeout.Token;
        timeout.Arm(TimeSpan.FromMilliseconds(10));
        await BatchTestUtil.Eventually(() => first.IsCancellationRequested, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(1));
        timeout.Stop();

        var second = timeout.Token;
        Assert.NotEqual(first, second);
        Assert.False(second.IsCancellationRequested);
    }

    [Fact]
    public void AZeroTimeoutIsAnExpiredContext()
    {
        var timeout = new ReusableTimeout();
        timeout.Arm(TimeSpan.Zero);
        Assert.True(timeout.Token.IsCancellationRequested);
        timeout.Stop();
        Assert.False(timeout.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task TheBatchPassesAnExpiredTokenForAZeroTimeout_AndALiveOneOtherwise()
    {
        var tokens = new List<CancellationToken>();
        var factory = new ReadBatchFactory
        {
            Execute = (ct, request) =>
            {
                tokens.Add(ct);
                return ValueTask.FromResult(new ReadResponse());
            },
            RequestTimeout = TimeSpan.Zero,
        };
        var batch = factory.NewBatch(1);
        batch.Add(new Oxia.Client.Internal.Model.GetCall { Key = "a", Callback = (_, _) => { } });
        await batch.CompleteAsync();
        Assert.True(tokens[0].IsCancellationRequested);

        factory.RequestTimeout = TimeSpan.FromSeconds(30);
        batch = factory.NewBatch(1);
        batch.Add(new Oxia.Client.Internal.Model.GetCall { Key = "a", Callback = (_, _) => { } });
        await batch.CompleteAsync();
        Assert.False(tokens[1].CanBeCanceled && tokens[1].IsCancellationRequested);
    }

    [Fact]
    public async Task TheRequestTimesOutWhenTheExecutorIsSlow()
    {
        var factory = new WriteBatchFactory
        {
            Execute = async (ct, shard, prepare) =>
            {
                await Task.Delay(Timeout.Infinite, ct); // honours the token only
                return new WriteResponse();
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromMilliseconds(50),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(1);
        Exception? error = null;
        batch.Add(new Oxia.Client.Internal.Model.PutCall { Key = "a", Callback = (_, e) => error = e });

        await batch.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.IsAssignableFrom<OperationCanceledException>(error is { InnerException: { } inner } && inner is OperationCanceledException ? inner : error);
        // the executor never called prepare: the request was not sent
        Assert.True(Oxia.Client.Internal.Model.CallContext.IsNotSent(error));
    }
}
