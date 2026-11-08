// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/sync_client_impl_test.go (leaderServer, startLeaderServer),
//              oxia/sessions_test.go (shardSession, sessionRequestsServer, frozenShardServer -- used by async_client_reroute_test.go)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Mock Oxia servers: services derived from the protoc-generated Io.Oxia.Proto.V1.OxiaClient.OxiaClientBase and
//              hosted by TestGrpcServer (the in-process Kestrel analogue of `grpc.NewServer()` + `net.Listen`).
//   Divergences: Go's `chan struct{}` closed-to-broadcast (elected / release) -> TaskCompletionSource; `chan string` -> Channel.
// =====================================================================================================================
using System.Threading.Channels;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.F2;

/// <summary>
/// leaderServer leads the only shard of the default namespace, once elected.  It reports the keys of the writes it receives,
/// and it answers the write of the key "stuck" only once released.
/// </summary>
internal sealed class LeaderServer : G.OxiaClient.OxiaClientBase, IAsyncDisposable
{
    private readonly TaskCompletionSource _elected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Channel<string> _received = Channel.CreateUnbounded<string>();
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TestGrpcServer? _server;

    public string Address { get; private set; } = "";

    /// <summary>startLeaderServer</summary>
    public static async Task<LeaderServer> StartAsync()
    {
        var s = new LeaderServer();
        s._server = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<LeaderServer>(),
            services => services.AddSingleton(s));
        s.Address = s._server.Address;
        return s;
    }

    public ValueTask DisposeAsync() => _server is null ? ValueTask.CompletedTask : _server.DisposeAsync();

    /// <summary>elect makes the server the leader of the shard in the shard assignments.</summary>
    public void Elect() => _elected.TrySetResult();

    /// <summary>close(leader.release)</summary>
    public void ReleaseStuck() => _release.TrySetResult();

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream,
        ServerCallContext context)
    {
        var done = new TaskCompletionSource();
        await using var registration = context.CancellationToken.Register(() => done.TrySetResult());

        if (!_elected.Task.IsCompleted)
        {
            await responseStream.WriteAsync(Assignments(""));
            await Task.WhenAny(_elected.Task, done.Task);
            if (!_elected.Task.IsCompleted) return;
        }
        await responseStream.WriteAsync(Assignments(Address));
        await done.Task;
    }

    private static G.ShardAssignments Assignments(string leader)
    {
        var namespaceAssignment = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3 };
        namespaceAssignment.Assignments.Add(new G.ShardAssignment
        {
            Shard = 0,
            Leader = leader,
            Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0, MaxHashInclusive = uint.MaxValue },
        });
        var assignments = new G.ShardAssignments();
        assignments.Namespaces[Constants.DefaultNamespace] = namespaceAssignment;
        return assignments;
    }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream,
        ServerCallContext context)
    {
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var request = requestStream.Current;
            var response = new G.WriteResponse();
            foreach (var put in request.Puts)
            {
                await Receive(context.CancellationToken, put.Key);
                response.Puts.Add(new G.PutResponse { Version = new G.Version() });
            }
            foreach (var del in request.Deletes)
            {
                await Receive(context.CancellationToken, del.Key);
                response.Deletes.Add(new G.DeleteResponse());
            }
            foreach (var deleteRange in request.DeleteRanges)
            {
                await Receive(context.CancellationToken, deleteRange.StartInclusive);
                response.DeleteRanges.Add(new G.DeleteRangeResponse());
            }
            await responseStream.WriteAsync(response);
        }
    }

    private async Task Receive(CancellationToken ct, string key)
    {
        _received.Writer.TryWrite(key);
        if (key != "stuck") return;
        await _release.Task.WaitAsync(ct);
    }

    /// <summary>nextReceived: the key of the next write the leader received (fails the test after 10s).</summary>
    public async Task<string> NextReceivedAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            return await _received.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Xunit.Assert.Fail("the leader did not receive a write");
            return "";
        }
    }
}

/// <summary>sessionRequestsServer records the session requests it receives.  The id of a session it creates is 10 + the shard id.</summary>
internal class SessionRequestsServer : G.OxiaClient.OxiaClientBase
{
    private readonly object _lock = new();
    private readonly List<long> _created = [];
    private readonly List<ShardSession> _closed = [];

    public Channel<ShardSession> Heartbeats { get; } = Channel.CreateBounded<ShardSession>(100);

    public List<long> Created
    {
        get { lock (_lock) return [.. _created]; }
    }

    public List<ShardSession> Closed
    {
        get { lock (_lock) return [.. _closed]; }
    }

    public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
    {
        lock (_lock) _created.Add(request.Shard);
        return Task.FromResult(new G.CreateSessionResponse { SessionId = 10 + request.Shard });
    }

    public override async Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
    {
        await Heartbeats.Writer.WriteAsync(new ShardSession(request.Shard, request.SessionId));
        return new G.KeepAliveResponse();
    }

    public override Task<G.CloseSessionResponse> CloseSession(G.CloseSessionRequest request, ServerCallContext context)
    {
        lock (_lock) _closed.Add(new ShardSession(request.Shard, request.SessionId));
        return Task.FromResult(new G.CloseSessionResponse());
    }
}

internal readonly record struct ShardSession(long Shard, long SessionId);

/// <summary>
/// frozenShardServer is a sessionRequestsServer that rejects the creation of the sessions on shard 0 with ErrNodeIsNotLeader,
/// like the parent of a split while the cutover freezes it, and the data servers once it is deleted.
/// </summary>
internal sealed class FrozenShardServer : SessionRequestsServer
{
    private long _rejected;

    public long Rejected => Interlocked.Read(ref _rejected);

    public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
    {
        if (request.Shard == 0)
        {
            Interlocked.Increment(ref _rejected);
            throw OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader)!;
        }
        return base.CreateSession(request, context);
    }
}
