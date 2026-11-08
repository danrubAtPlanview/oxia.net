// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/write_stream_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 3, ported: 3, skipped: 0
//   Intent   : the stream wrapper never sends a request after its context is done, fails (and is marked failed) when a send
//              fails, and returns the response of the request logging it at debug level.
//   Adaptations: Go's `testWriteStream` is FakeWriteStream; context.Canceled is OperationCanceledException; the Go test swaps the
//                process wide slog default handler to capture the debug line, here the logger is injected into the wrapper
//                (CapturingLogger).  Logic verified against fakes only (needs component B: Future).
// =====================================================================================================================
using Xunit;
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;
using Oxia.Common.Proto;

namespace Oxia.Client.Tests.Internal;

public class WriteStreamTests
{
    // A request whose deadline passed before it could be sent, e.g. while it was waiting for the write stream, must not be sent anymore.
    [Fact]
    public async Task TestStreamWrapper_DoesNotSendAfterTheDeadline()
    {
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var stream = new FakeWriteStream(streamCts.Token);
        var sw = new StreamWrapper(0, "leader", stream, streamCts);

        try
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            bool prepared = false;
            var err = await Record.ExceptionAsync(async () => await sw.Send(cts.Token, () =>
            {
                prepared = true;
                return new WriteRequest();
            }));
            Assert.IsAssignableFrom<OperationCanceledException>(err); // context.Canceled
            Assert.False(prepared);
            Assert.Equal(0, stream.Sent);
        }
        finally
        {
            sw.Cancel(); // the stream context of the Go test ends with t.Context()
        }
    }

    // A stream that failed to send a request is replaced for the next one.
    [Fact]
    public async Task TestStreamWrapperSendFailsTheStream()
    {
        var errSend = new InvalidOperationException("send failed");
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var stream = new FakeWriteStream(streamCts.Token) { SendError = errSend };
        var sw = new StreamWrapper(0, "leader", stream, streamCts);

        try
        {
            var err = await Record.ExceptionAsync(async () => await sw.Send(TestContext.Current.CancellationToken, static () => new WriteRequest()));
            Assert.True(OxiaErrors.Is(err, errSend));
            Assert.True(sw.Failed);
        }
        finally
        {
            sw.Cancel(); // defer cancelStream()
        }
    }

    [Fact]
    public async Task TestStreamWrapperSendReturnsResponseAndLogsItAtDebugLevel()
    {
        var logs = new CapturingLogger(LogLevel.Debug);
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var stream = new FakeWriteStream(streamCts.Token, autoRespond: true);
        var sw = new StreamWrapper(0, "leader", stream, streamCts, logs);

        try
        {
            var response = await sw.Send(TestContext.Current.CancellationToken, static () => new WriteRequest());

            Assert.NotNull(response);
            Assert.Contains("got response", logs.Text);
        }
        finally
        {
            sw.Cancel(); // defer cancelStream()
        }
    }
}
