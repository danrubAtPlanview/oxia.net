// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : oxia/internal/write_stream.go, oxia/internal/rpc_provider.go (ShardMapTimer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The contract of the stream wrapper beyond the three Go tests: sends are serialised, responses complete the oldest
//              pending request (FIFO) even when a caller gave up, the stream is marked failed BEFORE the pending requests fail, an
//              end of stream is a retryable EOF, a response without a request fails the stream, a send cut short by the request
//              token kills the stream, and the loop disposes the stream when it ends.  ShardMapTimer: fires on the delay or when the
//              map changes, reusable across attempts, honours the token, Stop interrupts a wait.
// =====================================================================================================================
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Internal;

public class WriteStreamExtraTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;

    private sealed class Harness : IDisposable
    {
        public CancellationTokenSource StreamCts { get; } = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        public FakeWriteStream Stream { get; }
        public StreamWrapper Wrapper { get; }

        public Harness(bool autoRespond = false, Action<FakeWriteStream>? configure = null)
        {
            Stream = new FakeWriteStream(StreamCts.Token, autoRespond);
            configure?.Invoke(Stream);
            Wrapper = new StreamWrapper(0, "leader:1", Stream, StreamCts);
        }

        public void Dispose() => Wrapper.Cancel();
    }

    private static WriteRequest Request() => new();

    private static WriteResponse ResponseWith(int puts)
    {
        var response = new WriteResponse();
        for (int i = 0; i < puts; i++) response.Puts.Add(new PutResponse());
        return response;
    }

    private static async Task UntilAsync(Func<bool> condition, string what)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestCt);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            try { await Task.Delay(5, cts.Token); }
            catch (OperationCanceledException) { Assert.Fail($"timed out waiting for: {what}"); }
        }
    }

    [Fact]
    public async Task Responses_CompleteTheRequestsInTheOrderTheyWereSent()
    {
        using var h = new Harness();
        var tasks = new List<Task<WriteResponse>>();
        for (int i = 1; i <= 3; i++)
        {
            tasks.Add(h.Wrapper.Send(TestCt, static () => Request()).AsTask());
            await UntilAsync(() => h.Stream.Sent == tasks.Count, "the request was sent");
        }

        var responses = new[] { ResponseWith(1), ResponseWith(2), ResponseWith(3) };
        foreach (var response in responses) h.Stream.Respond(response);

        for (int i = 0; i < 3; i++) Assert.Same(responses[i], await tasks[i]);
    }

    [Fact]
    public async Task ARequestThatGaveUpStillConsumesItsResponse()
    {
        using var h = new Harness();
        using var cts = new CancellationTokenSource();
        var abandoned = h.Wrapper.Send(cts.Token, static () => Request()).AsTask();
        await UntilAsync(() => h.Stream.Sent == 1, "the first request was sent");
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);
        Assert.False(h.Wrapper.Failed); // the stream is fine: only the caller gave up

        var next = h.Wrapper.Send(TestCt, static () => Request()).AsTask();
        await UntilAsync(() => h.Stream.Sent == 2, "the second request was sent");
        var late = ResponseWith(1);
        var answer = ResponseWith(2);
        h.Stream.Respond(late);   // belongs to the abandoned request
        h.Stream.Respond(answer); // belongs to the second one

        Assert.Same(answer, await next);
    }

    [Fact]
    public async Task TheStreamIsMarkedFailedBeforeThePendingRequestsFail()
    {
        using var h = new Harness();
        var send = h.Wrapper.Send(TestCt, static () => Request()).AsTask();
        await UntilAsync(() => h.Stream.Sent == 1, "the request was sent");

        bool? failedWhenTheRequestObservedTheFailure = null;
        var observed = send.ContinueWith(_ => failedWhenTheRequestObservedTheFailure = h.Wrapper.Failed,
            TaskContinuationOptions.ExecuteSynchronously);
        var failure = new InvalidOperationException("stream broken");
        h.Stream.FailStream(failure);

        var err = await Assert.ThrowsAnyAsync<Exception>(() => send);
        await observed;
        Assert.True(OxiaErrors.Is(err, failure));
        Assert.True(failedWhenTheRequestObservedTheFailure);
    }

    [Fact]
    public async Task AllThePendingRequestsFailWhenTheStreamFails()
    {
        using var h = new Harness();
        var sends = new List<Task<WriteResponse>>();
        for (int i = 0; i < 5; i++)
        {
            sends.Add(h.Wrapper.Send(TestCt, static () => Request()).AsTask());
            await UntilAsync(() => h.Stream.Sent == sends.Count, "the request was sent");
        }

        var failure = new InvalidOperationException("stream broken");
        h.Stream.FailStream(failure);

        foreach (var send in sends)
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(() => send);
            Assert.True(OxiaErrors.Is(err, failure));
        }
    }

    [Fact]
    public async Task AnEndOfStreamFailsThePendingRequestsWithARetryableEof()
    {
        using var h = new Harness();
        var send = h.Wrapper.Send(TestCt, static () => Request()).AsTask();
        await UntilAsync(() => h.Stream.Sent == 1, "the request was sent");

        h.Stream.CompleteStream();

        var err = await Assert.ThrowsAnyAsync<Exception>(() => send);
        Assert.True(OxiaErrors.Is(err, OxiaErrors.EndOfStream));
        Assert.True(OxiaErrors.IsRetryable(err));
        Assert.True(h.Wrapper.Failed);
    }

    [Fact]
    public async Task AResponseWithoutAPendingRequestFailsTheStream()
    {
        using var h = new Harness();
        h.Stream.Respond(ResponseWith(1));

        await UntilAsync(() => h.Wrapper.Failed, "the stream was marked failed");
        await UntilAsync(() => h.Stream.Disposed, "the stream was disposed");
    }

    [Fact]
    public async Task TheSendsAreSerialised()
    {
        using var h = new Harness(autoRespond: true, s => s.SendDelay = TimeSpan.FromMilliseconds(20));
        var sends = Enumerable.Range(0, 8).Select(_ => h.Wrapper.Send(TestCt, static () => Request()).AsTask()).ToArray();

        await Task.WhenAll(sends);

        Assert.Equal(8, h.Stream.Sent);
        Assert.Equal(1, h.Stream.MaxConcurrentSends);
    }

    [Fact]
    public async Task ASendCutShortByTheRequestTokenFailsAndCancelsTheStream()
    {
        using var h = new Harness(configure: s => s.BlockSends = true);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await h.Wrapper.Send(cts.Token, static () => Request()));

        Assert.True(h.Wrapper.Failed);
        await UntilAsync(() => h.StreamCts.IsCancellationRequested || h.Stream.Disposed, "the stream was cancelled");
        await UntilAsync(() => h.Stream.Disposed, "the stream was disposed");
        Assert.Equal(0, h.Stream.Sent);
    }

    [Fact]
    public async Task TheLoopDisposesAndCancelsTheStreamWhenItEnds()
    {
        var h = new Harness();
        h.Stream.CompleteStream();

        await UntilAsync(() => h.Stream.Disposed, "the stream was disposed");
        Assert.True(h.Wrapper.Failed);
        Assert.True(h.Stream.Context.IsCancellationRequested);
        h.Wrapper.Cancel(); // after the end: no exception
    }

    [Fact]
    public async Task ASendOnAnEndedStreamFailsWithARetryableEofInsteadOfWaitingForever()
    {
        using var h = new Harness(autoRespond: true);
        h.Stream.CompleteStream();
        await UntilAsync(() => h.Stream.Disposed, "the stream ended");

        // nobody would complete the future of this request: the wrapper refuses to queue it
        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await h.Wrapper.Send(TestCt, static () => Request()));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.EndOfStream));
        Assert.True(OxiaErrors.IsRetryable(err));
        Assert.Equal(0, h.Stream.Sent);
    }

    [Fact]
    public async Task PrepareIsInvokedOncePerSendAndAfterTheCancellationCheck()
    {
        using var h = new Harness(autoRespond: true);
        int prepared = 0;
        for (int i = 0; i < 3; i++)
        {
            await h.Wrapper.Send(TestCt, () =>
            {
                prepared++;
                return Request();
            });
        }

        Assert.Equal(3, prepared);
        Assert.Equal(3, h.Stream.Sent);
        Assert.Equal(3, h.Stream.Requests.Count);
    }

    [Fact]
    public async Task APrepareThatThrowsLeavesTheStreamUntouched()
    {
        using var h = new Harness(autoRespond: true);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Wrapper.Send(TestCt, static () => throw new InvalidOperationException("nothing to send")));

        Assert.False(h.Wrapper.Failed);
        Assert.Equal(0, h.Stream.Sent);
        Assert.NotNull(await h.Wrapper.Send(TestCt, static () => Request())); // the stream is still usable
    }

    [Fact]
    public async Task ManyConcurrentRequestsKeepTheFifoAlignmentOverTheRingBuffer()
    {
        // more pending requests than the initial ring capacity, with the responses arriving in bursts
        using var h = new Harness(autoRespond: true, s => s.SendDelay = TimeSpan.FromMilliseconds(1));
        var sends = Enumerable.Range(0, 200).Select(_ => h.Wrapper.Send(TestCt, static () => Request()).AsTask()).ToArray();
        var responses = await Task.WhenAll(sends);
        Assert.Equal(200, responses.Length);
        Assert.Equal(200, responses.Distinct().Count());
    }

    // ----- ShardMapTimer --------------------------------------------------------------------------------------------

    [Fact]
    public async Task ShardMapTimer_WithoutAChangedTaskFiresAfterTheDelay()
    {
        var timer = new ShardMapTimer();
        timer.Start(TimeSpan.FromMilliseconds(80));
        var start = System.Diagnostics.Stopwatch.StartNew();
        await timer.WaitAsync(TestCt);
        Assert.True(start.Elapsed >= TimeSpan.FromMilliseconds(60), $"fired after {start.Elapsed}");
        timer.Stop();
    }

    [Fact]
    public async Task ShardMapTimer_FiresRightAwayWhenTheMapAlreadyChanged()
    {
        var changed = Task.CompletedTask;
        var timer = new ShardMapTimer { Changed = changed };
        timer.Start(TimeSpan.FromHours(1));
        var wait = timer.WaitAsync(TestCt);
        Assert.True(wait.IsCompletedSuccessfully);
        await wait;
    }

    [Fact]
    public async Task ShardMapTimer_IsReusableAcrossAttemptsAndCapturesChangedAtStart()
    {
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timer = new ShardMapTimer { Changed = first.Task };

        timer.Start(TimeSpan.FromHours(1));
        timer.Changed = second.Task; // the attempt that follows replaces the task, but the running wait uses the one captured at Start
        var wait = timer.WaitAsync(TestCt).AsTask();
        second.SetResult();
        await Task.Delay(50, TestCt);
        Assert.False(wait.IsCompleted);
        first.SetResult();
        await wait.WaitAsync(TimeSpan.FromSeconds(10), TestCt);

        timer.Start(TimeSpan.FromHours(1)); // second attempt: Changed is `second`, already done
        await timer.WaitAsync(TestCt);
        timer.Stop();
    }

    [Fact]
    public async Task ShardMapTimer_HonoursTheCancellationToken()
    {
        var timer = new ShardMapTimer { Changed = new TaskCompletionSource().Task };
        timer.Start(TimeSpan.FromHours(1));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await timer.WaitAsync(cts.Token));
        timer.Stop();
    }

    [Fact]
    public async Task ShardMapTimer_StopInterruptsAPendingWait()
    {
        var timer = new ShardMapTimer();
        timer.Start(TimeSpan.FromHours(1));
        var wait = timer.WaitAsync(TestCt).AsTask();
        await Task.Delay(30, TestCt);
        timer.Stop();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        timer.Stop(); // idempotent
    }
}
