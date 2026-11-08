// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/mock.go   (test helpers of the replication / coordination streams)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : In-memory doubles of the server- and client-side gRPC streams of the replication and shard-assignment services: a pair of
//              buffered channels per stream and a cancellation scope; Send / Recv select on the scope and the channel, a closed channel is
//              io.EOF.  They are not used by the client; they are ported with their tests because they are part of common/rpc.
//   Divergences:
//      * Lives in the test project (it is a test double; nothing in src needs it).  The streams do not implement the Go stream interfaces
//        (grpc.ServerStream, OxiaLogReplication_ReplicateClient, ...): the services they stand for have no generated C# classes here.
//      * chan T (buffered N) -> Channel.CreateBounded<T>(N); unbuffered -> capacity 1 (DESIGN.md).  Send / Recv are ValueTask based; a
//        cancelled scope throws OperationCanceledException (ctx.Err()); a closed channel makes Recv return null (io.EOF).
//      * Go panics ("not implemented") -> NotImplementedException.  metadata.MD -> Grpc.Core.Metadata.
//      * context.Context -> CancellationToken (a zero MockBase has CancellationToken.None, Go's context.Background()).
// =====================================================================================================================
using System.Threading.Channels;
using Grpc.Core;
using G = Io.Oxia.Proto.V1;
using R = Replication;

namespace Oxia.Client.Tests.Common;

public class MockBase
{
    protected CancellationTokenSource? Scope;

    public Metadata? Md { get; private set; }

    /// <summary>Go: Context() -- the scope of the stream, or Background when it has none.</summary>
    public CancellationToken Context => Scope?.Token ?? CancellationToken.None;

    public void SendHeader(Metadata md) => throw new NotImplementedException("not implemented");

    public void RecvMsg(object? message) => throw new NotImplementedException("not implemented");

    public void SendMsg(object? message) => throw new NotImplementedException("not implemented");

    public void SetTrailer(Metadata md) => throw new NotImplementedException("not implemented");

    public Metadata Header() => throw new NotImplementedException("not implemented");

    public Metadata Trailer() => throw new NotImplementedException("not implemented");

    public void SetHeader(Metadata md) => Md = md;

    /// <summary>Go: <c>select { case &lt;-ctx.Done(): return ctx.Err(); case ch &lt;- item: }</c>.</summary>
    protected async ValueTask SelectSendAsync<T>(Channel<T> channel, T item)
    {
        var ct = Context;
        ct.ThrowIfCancellationRequested();
        await channel.Writer.WriteAsync(item, ct).ConfigureAwait(false);
    }

    /// <summary>Go: <c>select { case &lt;-ctx.Done(): return nil, ctx.Err(); case item, ok := &lt;-ch: if !ok { io.EOF } }</c>; null = io.EOF.</summary>
    protected async ValueTask<T?> SelectRecvAsync<T>(Channel<T> channel) where T : class
    {
        var ct = Context;
        ct.ThrowIfCancellationRequested();
        try
        {
            return await channel.Reader.ReadAsync(ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }
}

public sealed class MockServerReplicateStream : MockBase
{
    public const int Capacity = 1000;

    public MockServerReplicateStream()
    {
        Scope = new CancellationTokenSource();
    }

    public Channel<R.Append> Requests { get; } = Channel.CreateBounded<R.Append>(Capacity);

    public Channel<R.Ack> Responses { get; } = Channel.CreateBounded<R.Ack>(Capacity);

    public void Cancel() => Scope!.Cancel();

    public ValueTask AddRequestAsync(R.Append request) => Requests.Writer.WriteAsync(request);

    public ValueTask<R.Ack> GetResponseAsync() => Responses.Reader.ReadAsync();

    public ValueTask SendAsync(R.Ack response) => SelectSendAsync(Responses, response);

    public ValueTask<R.Append?> RecvAsync() => SelectRecvAsync(Requests);
}

public sealed class TruncateResps
{
    public R.TruncateResponse? Response { get; init; }

    public Exception? Error { get; init; }
}

public sealed class MockRpcClient
{
    private readonly object _streamsMutex = new();
    private readonly List<MockReplicateStream> _streams = [];

    public MockSendSnapshotClientStream SendSnapshotStream { get; } = new(CancellationToken.None);

    public Channel<R.Append> AppendReqs { get; } = Channel.CreateBounded<R.Append>(1000);

    public Channel<R.Ack> AckResps { get; } = Channel.CreateBounded<R.Ack>(1000);

    public Channel<R.TruncateRequest> TruncateReqs { get; } = Channel.CreateBounded<R.TruncateRequest>(1000);

    public Channel<TruncateResps> TruncateRespsChannel { get; } = Channel.CreateBounded<TruncateResps>(1000);

    public void Close()
    {
        lock (_streamsMutex)
        {
            foreach (var stream in _streams)
            {
                stream.CloseSend();
            }
        }
    }

    public MockReplicateStream GetReplicateStream(CancellationToken ctx, string namespaceName, string shard, long term, long fromOffset)
    {
        var stream = new MockReplicateStream(ctx, AppendReqs, AckResps);
        lock (_streamsMutex)
        {
            _streams.Add(stream);
        }

        return stream;
    }

    public MockSendSnapshotClientStream SendSnapshot(CancellationToken ctx, string namespaceName, string shard, long term, long fromOffset)
        => SendSnapshotStream;

    public async ValueTask<R.TruncateResponse?> TruncateAsync(string node, R.TruncateRequest request)
    {
        await TruncateReqs.Writer.WriteAsync(request).ConfigureAwait(false);

        // Caller needs to provide Response to the channel
        var x = await TruncateRespsChannel.Reader.ReadAsync().ConfigureAwait(false);
        if (x.Error is not null) throw x.Error;
        return x.Response;
    }
}

public sealed class MockReplicateStream : MockBase
{
    private readonly Channel<R.Append>? _send;
    private readonly Channel<R.Ack>? _recv;

    /// <summary>A stream without scope (Go: the zero value <c>&amp;MockReplicateStream{}</c>).</summary>
    public MockReplicateStream()
    {
    }

    public MockReplicateStream(CancellationToken ctx, Channel<R.Append> send, Channel<R.Ack> recv)
    {
        _send = send;
        _recv = recv;
        Scope = CancellationTokenSource.CreateLinkedTokenSource(ctx);
    }

    public ValueTask SendAsync(R.Append request) => SelectSendAsync(_send!, request);

    public ValueTask<R.Ack?> RecvAsync() => SelectRecvAsync(_recv!);

    public void CloseSend() => Scope?.Cancel();
}

public sealed class MockShardAssignmentClientStream : MockBase
{
    public MockShardAssignmentClientStream()
    {
        Scope = new CancellationTokenSource();
    }

    private Channel<G.ShardAssignments> Responses { get; } = Channel.CreateBounded<G.ShardAssignments>(1000);

    public void Cancel() => Scope!.Cancel();

    public ValueTask<G.ShardAssignments> GetResponseAsync() => Responses.Reader.ReadAsync();

    public ValueTask SendAsync(G.ShardAssignments response) => Responses.Writer.WriteAsync(response);
}

public sealed class MockShardAssignmentControllerStream : MockBase
{
    private readonly Channel<G.ShardAssignments> _requests = Channel.CreateBounded<G.ShardAssignments>(1000);
    private readonly Channel<R.CoordinationShardAssignmentsResponse> _responses = Channel.CreateBounded<R.CoordinationShardAssignmentsResponse>(1000);

    public ValueTask<R.CoordinationShardAssignmentsResponse> GetResponseAsync() => _responses.Reader.ReadAsync();

    public ValueTask SendAndCloseAsync(R.CoordinationShardAssignmentsResponse empty) => _responses.Writer.WriteAsync(empty);

    public ValueTask AddRequestAsync(G.ShardAssignments request) => _requests.Writer.WriteAsync(request);

    public ValueTask<G.ShardAssignments> RecvAsync() => _requests.Reader.ReadAsync();
}

public sealed class MockServerSendSnapshotStream : MockBase
{
    public Channel<R.SnapshotChunk> Chunks { get; } = Channel.CreateBounded<R.SnapshotChunk>(1000);

    public Channel<R.SnapshotResponse> Responses { get; } = Channel.CreateBounded<R.SnapshotResponse>(1000);

    public ValueTask AddChunkAsync(R.SnapshotChunk chunk) => Chunks.Writer.WriteAsync(chunk);

    public ValueTask<R.SnapshotResponse> GetResponseAsync() => Responses.Reader.ReadAsync();

    public ValueTask SendAndCloseAsync(R.SnapshotResponse empty) => Responses.Writer.WriteAsync(empty);

    public ValueTask<R.SnapshotChunk> RecvAsync() => Chunks.Reader.ReadAsync();
}

public sealed class MockSendSnapshotClientStream : MockBase
{
    public const int RequestsCapacity = 100;

    public MockSendSnapshotClientStream(CancellationToken ctx)
    {
        Scope = CancellationTokenSource.CreateLinkedTokenSource(ctx);
    }

    public Channel<R.SnapshotChunk> Requests { get; } = Channel.CreateBounded<R.SnapshotChunk>(RequestsCapacity);

    public Channel<R.SnapshotResponse> Response { get; } = Channel.CreateBounded<R.SnapshotResponse>(1);

    public ValueTask SendAsync(R.SnapshotChunk chunk) => SelectSendAsync(Requests, chunk);

    public ValueTask<R.SnapshotResponse> CloseAndRecvAsync()
    {
        Scope!.Cancel();
        Requests.Writer.TryComplete();
        return Response.Reader.ReadAsync();
    }

    public void CloseSend() => Scope!.Cancel();
}
