// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sequence_updates_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 7 (TestSequenceUpdatesRejectionDoesNotResetBackoff, TestSequenceUpdatesAcceptedStreamResetsBackoff,
//             TestSequenceUpdatesGetSequenceUpdatesFailsOnRequestError, ...FailsWithContextErrorWhenClosed, ...SkipsEmptyKey,
//             ...EndsWhenContextIsDone, ...WithRetries), ported: 7, skipped: 0.
//   Adaptations (the assertions are the Go ones):
//      * getSequenceUpdates() returns an error in Go; GetSequenceUpdatesAsync() throws it: `assert.ErrorIs(err, X)` is
//        `OxiaErrors.Is(thrown, X)`; io.EOF is a null RecvAsync (end of stream) / OxiaErrors.EndOfStream; context.Canceled is
//        OxiaErrors.IsCanceled.
//      * `time.Sleep(delay)` in the fake stream sleeps AT LEAST the delay (Task.Delay can fire a hair early).
//      * `make(chan string)` (unbuffered) is the capacity-1 channel of the implementation.
//      * backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 2) is MaxRetriesBackOff over ZeroBackOff (F1Support).
// =====================================================================================================================
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;
using Xunit;

namespace Oxia.Client.Tests;

public class SequenceUpdatesTests
{
    private sealed class SequenceUpdatesTestShardManager : UnimplementedShardManager
    {
        public override long Get(string key) => 0;
        public override string Leader(long shardId) => "server-1";
    }

    /// <summary>Returns the highest sequence keys in turn, then fails the next Recv() with Err (io.EOF: ends the stream), once the delay has elapsed.</summary>
    private sealed class SequenceUpdatesTestStream : IServerStreamingClient<GetSequenceUpdatesResponse>
    {
        private readonly Queue<string> _keys;
        private readonly TimeSpan _delay;
        private readonly Exception? _err;

        public SequenceUpdatesTestStream(IEnumerable<string>? keys = null, TimeSpan delay = default, Exception? err = null)
        {
            _keys = new Queue<string>(keys ?? Array.Empty<string>());
            _delay = delay;
            _err = err;
        }

        public async ValueTask<GetSequenceUpdatesResponse?> RecvAsync(CancellationToken ct = default)
        {
            if (_keys.Count > 0) return new GetSequenceUpdatesResponse { HighestSequenceKey = _keys.Dequeue() };

            if (_delay > TimeSpan.Zero)
            {
                var sw = Stopwatch.StartNew();
                await Task.Delay(_delay).ConfigureAwait(false);
                while (sw.Elapsed < _delay) await Task.Yield();
            }
            if (ReferenceEquals(_err, OxiaErrors.EndOfStream)) return null;
            throw OxiaErrors.ToThrowable(_err!);
        }

        public void Dispose() { }
    }

    /// <summary>Returns the stream, or fails the requests with Err, if set.</summary>
    private sealed class SequenceUpdatesTestRpcProvider : UnimplementedRpcProvider
    {
        private long _requests;
        public SequenceUpdatesTestStream? Stream { get; set; }
        public Exception? Err { get; set; }
        public long Requests => Interlocked.Read(ref _requests);

        public override ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target,
            GetSequenceUpdatesRequest request)
        {
            Interlocked.Increment(ref _requests);
            if (Err is not null)
                return ValueTask.FromException<IServerStreamingClient<GetSequenceUpdatesResponse>>(OxiaErrors.ToThrowable(Err));
            return ValueTask.FromResult<IServerStreamingClient<GetSequenceUpdatesResponse>>(Stream!);
        }
    }

    private static (SequenceUpdates, ResetCountingBackOff) NewTestSequenceUpdates(SequenceUpdatesTestStream? stream)
    {
        var bo = new ResetCountingBackOff(BackOffFactory.NewBackOff(CancellationToken.None));
        return (new SequenceUpdates
        {
            PrefixKey = "a",
            PartitionKey = "x",
            Ch = Channel.CreateBounded<string>(1),
            ShardManager = new SequenceUpdatesTestShardManager(),
            RpcProvider = new SequenceUpdatesTestRpcProvider { Stream = stream },
            Ctx = CancellationToken.None,
            Backoff = bo,
            Log = NullLogger.Instance,
        }, bo);
    }

    private static async Task<Exception> Fails(SequenceUpdates su)
        => await Assert.ThrowsAnyAsync<Exception>(async () => await su.GetSequenceUpdatesAsync());

    [Fact]
    public async Task TestSequenceUpdatesRejectionDoesNotResetBackoff()
    {
        // The stream is created even when the server rejects the subscription: the rejection is only reported by the first Recv()
        var (su, bo) = NewTestSequenceUpdates(new SequenceUpdatesTestStream(err: OxiaErrors.NodeIsNotLeader));

        Assert.True(OxiaErrors.Is(await Fails(su), OxiaErrors.NodeIsNotLeader));
        Assert.Equal(0, bo.Resets);
    }

    [Fact]
    public async Task TestSequenceUpdatesAcceptedStreamResetsBackoff()
    {
        // An accepted subscription gets no response until a sequence key exists. When it fails (eg. the leader stepped down),
        // it should be retried quickly.
        var (su, bo) = NewTestSequenceUpdates(new SequenceUpdatesTestStream(delay: SequenceUpdates.AcceptedAfter, err: OxiaErrors.EndOfStream));

        Assert.True(OxiaErrors.Is(await Fails(su), OxiaErrors.EndOfStream));
        Assert.Equal(1, bo.Resets);
    }

    [Fact]
    public async Task TestSequenceUpdatesGetSequenceUpdatesFailsOnRequestError()
    {
        var (su, _) = NewTestSequenceUpdates(null);
        su.RpcProvider = new SequenceUpdatesTestRpcProvider { Err = OxiaErrors.NodeIsNotLeader };

        Assert.True(OxiaErrors.Is(await Fails(su), OxiaErrors.NodeIsNotLeader));
    }

    // A request that fails because the subscription was closed reports the close, not the error of the request.
    [Fact]
    public async Task TestSequenceUpdatesGetSequenceUpdatesFailsWithContextErrorWhenClosed()
    {
        var (su, _) = NewTestSequenceUpdates(null);
        su.RpcProvider = new SequenceUpdatesTestRpcProvider { Err = OxiaErrors.NodeIsNotLeader };
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        su.Ctx = cts.Token;

        Assert.True(OxiaErrors.IsCanceled(await Fails(su)));
    }

    // The first response is empty when no sequence key exists yet: it is not an update.
    [Fact]
    public async Task TestSequenceUpdatesGetSequenceUpdatesSkipsEmptyKey()
    {
        var (su, _) = NewTestSequenceUpdates(new SequenceUpdatesTestStream(new[] { "", "a-1", "a-2" }, err: OxiaErrors.EndOfStream));

        var done = Task.Run(async () =>
        {
            try { await su.GetSequenceUpdatesAsync(); return (Exception?)null; }
            catch (Exception e) { return e; }
        });

        Assert.Equal("a-1", await su.Ch.Reader.ReadAsync());
        Assert.Equal("a-2", await su.Ch.Reader.ReadAsync());
        Assert.True(OxiaErrors.Is(await done, OxiaErrors.EndOfStream));
    }

    // An update that nobody receives does not keep the subscription from ending.
    [Fact]
    public async Task TestSequenceUpdatesGetSequenceUpdatesEndsWhenContextIsDone()
    {
        var (su, _) = NewTestSequenceUpdates(new SequenceUpdatesTestStream(new[] { "a-1" }, err: OxiaErrors.EndOfStream));
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        su.Ctx = cts.Token;

        Assert.True(OxiaErrors.IsCanceled(await Fails(su)));
    }

    // The failed subscriptions are retried, until the retries end, and the channel is closed then.
    [Fact]
    public async Task TestSequenceUpdatesGetSequenceUpdatesWithRetries()
    {
        var (su, _) = NewTestSequenceUpdates(null);
        var provider = new SequenceUpdatesTestRpcProvider { Err = OxiaErrors.NodeIsNotLeader };
        su.RpcProvider = provider;
        su.Backoff = new MaxRetriesBackOff(new ZeroBackOff(), 2);

        await su.GetSequenceUpdatesWithRetriesAsync();

        Assert.False(await su.Ch.Reader.WaitToReadAsync());   // _, open := <-su.ch; assert.False(t, open)
        Assert.Equal(3, provider.Requests);
    }
}
