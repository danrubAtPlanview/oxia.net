// =====================================================================================================================
// R2 review (fidelity) -- StreamWrapper: FIFO alignment of requests and responses under cancellation.
//
// One caller at a time (the batcher of a shard sends one request at a time) issues requests with random deadlines on a stream whose
// server answers in order, each answer after a random delay; some callers give up while the response is outstanding (their future stays
// in the pending queue and its response must be consumed to keep the FIFO aligned).  Every request that completes must have received
// the response of ITS OWN request (the response echoes the id of the request).
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R2_WriteStreamStressTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task EveryCompletedRequestGetsItsOwnResponseEvenWhenOthersGaveUp(int seed)
    {
        var rng = new Random(seed);
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(TestCt);
        var stream = new FakeWriteStream(streamCts.Token);

        // an in-order server with random latency: it echoes the key of the first put as the key of the response
        var inbox = Channel.CreateUnbounded<WriteRequest>();
        stream.OnSend = (_, request) => inbox.Writer.TryWrite(request);
        var server = Task.Run(async () =>
        {
            var srvRng = new Random(seed + 1000);
            await foreach (var request in inbox.Reader.ReadAllAsync(streamCts.Token))
            {
                for (int y = srvRng.Next(0, 4); y > 0; y--) await Task.Yield();
                var response = new WriteResponse();
                response.Puts.Add(new PutResponse { Key = request.Puts[0].Key });
                stream.Respond(response);
            }
        }, TestCt);

        var wrapper = new StreamWrapper(0, "leader:1", stream, streamCts);
        int completed = 0, gaveUp = 0;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        for (int id = 0; id < 20000 && DateTime.UtcNow < deadline; id++)
        {
            string key = id.ToString();
            using var requestCts = CancellationTokenSource.CreateLinkedTokenSource(TestCt);
            try
            {
                var sending = wrapper.Send(requestCts.Token, () =>
                {
                    var request = new WriteRequest { Shard = 0 };
                    request.Puts.Add(new PutRequest { Key = key, Value = "v"u8.ToArray() });
                    return request;
                }).AsTask();
                // a third of the callers give up at a random moment: before, while or just after the response
                if (rng.Next(3) == 0)
                {
                    Thread.SpinWait(rng.Next(0, 300));
                    await requestCts.CancelAsync();
                }

                var response = await sending;
                Assert.Equal(key, response.Puts[0].Key);
                completed++;
            }
            catch (OperationCanceledException)
            {
                gaveUp++;
            }
        }

        Assert.False(wrapper.Failed, "a caller giving up must not fail the stream");
        Assert.True(completed > 100 && gaveUp > 100, $"weak run: {completed} completed, {gaveUp} gave up");

        inbox.Writer.TryComplete();
        wrapper.Cancel();
        try { await server; } catch (OperationCanceledException) { }
    }
}
