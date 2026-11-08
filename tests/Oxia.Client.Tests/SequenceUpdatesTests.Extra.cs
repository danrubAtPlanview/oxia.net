// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : none.  End-to-end tests of SequenceUpdates.Create (the Go tests exercise the unexported pieces only): delivery of the
//              keys, completion of the channel when the caller's token is done or when the client is closed, and the retry of a
//              failed subscription.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Xunit;

namespace Oxia.Client.Tests;

public class SequenceUpdatesExtraTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private sealed class Stream : IServerStreamingClient<GetSequenceUpdatesResponse>
    {
        public readonly Channel<GetSequenceUpdatesResponse> Items = Channel.CreateUnbounded<GetSequenceUpdatesResponse>();
        public readonly CancellationToken Ctx;
        public Stream(CancellationToken ctx) => Ctx = ctx;
        public async ValueTask<GetSequenceUpdatesResponse?> RecvAsync(CancellationToken ct = default) => await Items.Reader.ReadAsync(Ctx);
        public void Dispose() { }
        public void Send(string key) => Items.Writer.TryWrite(new GetSequenceUpdatesResponse { HighestSequenceKey = key });
    }

    private sealed class Provider : UnimplementedRpcProvider
    {
        public readonly Channel<(Stream Stream, GetSequenceUpdatesRequest Request, string Target)> Streams = Channel.CreateUnbounded<(Stream, GetSequenceUpdatesRequest, string)>();
        private int _failures;
        public int Failures { set => _failures = value; }
        private int _requests;
        public int Requests => Volatile.Read(ref _requests);

        public override ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target, GetSequenceUpdatesRequest request)
        {
            Interlocked.Increment(ref _requests);
            if (Interlocked.Decrement(ref _failures) >= 0)
                return ValueTask.FromException<IServerStreamingClient<GetSequenceUpdatesResponse>>(OxiaErrors.ToThrowable(OxiaErrors.NodeIsNotLeader));
            var stream = new Stream(ct);
            Streams.Writer.TryWrite((stream, request, target));
            return ValueTask.FromResult<IServerStreamingClient<GetSequenceUpdatesResponse>>(stream);
        }
    }

    private sealed class ShardManager : UnimplementedShardManager
    {
        public string? LastPartitionKey;
        public override long Get(string key) { LastPartitionKey = key; return 5; }
        public override string Leader(long shardId) => "leader-of-" + shardId;
    }

    [Fact]
    public async Task Create_SubscribesToTheShardOfThePartitionKey_AndStreamsTheHighestSequenceKeys()
    {
        var provider = new Provider();
        var sm = new ShardManager();
        using var caller = new CancellationTokenSource();
        using var client = new CancellationTokenSource();

        var reader = SequenceUpdates.Create(caller.Token, client.Token, "/seq/", "partition", provider, sm);

        var (stream, request, target) = await Wait.WithTimeout(provider.Streams.Reader.ReadAsync().AsTask(), Ten, "no subscription");
        Assert.Equal("partition", sm.LastPartitionKey);
        Assert.Equal(5, request.Shard);
        Assert.Equal("/seq/", request.Key);
        Assert.Equal("leader-of-5", target);

        stream.Send("");           // first response with no sequence key yet: ignored
        stream.Send("/seq/00001");
        stream.Send("/seq/00002");
        Assert.Equal("/seq/00001", await Wait.WithTimeout(reader.ReadAsync().AsTask(), Ten, "no update"));
        Assert.Equal("/seq/00002", await Wait.WithTimeout(reader.ReadAsync().AsTask(), Ten, "no update"));
    }

    [Fact]
    public async Task Create_CompletesTheChannel_WhenTheCallerTokenIsDone()
    {
        var provider = new Provider();
        using var caller = new CancellationTokenSource();
        using var client = new CancellationTokenSource();
        var reader = SequenceUpdates.Create(caller.Token, client.Token, "/seq/", "p", provider, new ShardManager());
        var (stream, _, _) = await Wait.WithTimeout(provider.Streams.Reader.ReadAsync().AsTask(), Ten, "no subscription");

        caller.Cancel();

        Assert.False(await Wait.WithTimeout(reader.WaitToReadAsync().AsTask(), Ten, "the channel was not completed"));
        Assert.True(stream.Ctx.IsCancellationRequested);
    }

    [Fact]
    public async Task Create_CompletesTheChannel_WhenTheClientIsClosed()
    {
        var provider = new Provider();
        using var caller = new CancellationTokenSource();
        using var client = new CancellationTokenSource();
        var reader = SequenceUpdates.Create(caller.Token, client.Token, "/seq/", "p", provider, new ShardManager());
        await Wait.WithTimeout(provider.Streams.Reader.ReadAsync().AsTask(), Ten, "no subscription");

        client.Cancel();

        Assert.False(await Wait.WithTimeout(reader.WaitToReadAsync().AsTask(), Ten, "the channel was not completed"));
    }

    [Fact]
    public async Task Create_RetriesAFailedSubscription_AfterTheBackoff()
    {
        var provider = new Provider { Failures = 1 };
        using var caller = new CancellationTokenSource();
        using var client = new CancellationTokenSource();

        var reader = SequenceUpdates.Create(caller.Token, client.Token, "/seq/", "p", provider, new ShardManager());

        // The first request is rejected; the retry comes after the 1s initial interval (randomised +-50%)
        var (stream, _, _) = await Wait.WithTimeout(provider.Streams.Reader.ReadAsync().AsTask(), Ten, "no subscription after the failure");
        Assert.Equal(2, provider.Requests);
        stream.Send("/seq/00007");
        Assert.Equal("/seq/00007", await Wait.WithTimeout(reader.ReadAsync().AsTask(), Ten, "no update"));
        caller.Cancel();
    }
}
