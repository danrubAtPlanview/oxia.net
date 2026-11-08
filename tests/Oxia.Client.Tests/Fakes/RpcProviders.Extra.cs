// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/sessions_test.go (sessionsTestRpcProvider), oxia/notifications_test.go (notificationsTestRpcProvider),
//              oxia/sync_client_impl_test.go (neverCompleteAsyncClient)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Fakes of IRpcProvider (Go embeds a nil internal.RpcProvider: the methods a test does not use panic, here they
//              throw NotImplementedException) and of IAsyncClient.
//   Divergences: `ctx.Err()` returned by the sessions fake: an EXPIRED context in Go is DeadlineExceeded; a token cannot tell, so
//              the fake throws TimeoutException when its token is done (the DESIGN mapping of DeadlineExceeded).
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal;
using Oxia.Common;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;

namespace Oxia.Client.Tests.F2;

internal abstract class NotImplementedRpcProvider : IRpcProvider
{
    public virtual ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) => throw new NotImplementedException();
    public virtual ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
    public virtual ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> c) => throw new NotImplementedException();
    public virtual ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> c) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<ShardAssignments>> GetShardAssignments(CancellationToken ct, string target, ShardAssignmentsRequest request) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target, GetSequenceUpdatesRequest request) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request) => throw new NotImplementedException();
    public virtual ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request) => throw new NotImplementedException();
    public virtual ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request) => throw new NotImplementedException();
    public virtual ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request) => throw new NotImplementedException();
    public virtual ValueTask CloseAsync() => ValueTask.CompletedTask;
}

/// <summary>sessionsTestRpcProvider fails the creation of the sessions with createErr, and their closing with closeErr.  It records the requests to close them.</summary>
internal sealed class SessionsTestRpcProvider : NotImplementedRpcProvider
{
    public Exception? CreateErr { get; init; }
    public Exception? CloseErr { get; init; }
    public List<string> CloseTargets { get; } = [];
    public List<CloseSessionRequest> CloseRequests { get; } = [];

    public override ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
    {
        // Go: `if err := ctx.Err(); err != nil { return nil, err }` -- here the context of the test is an expired deadline
        if (ct.IsCancellationRequested) return ValueTask.FromException<CreateSessionResponse>(new TimeoutException("context deadline exceeded"));
        if (CreateErr is not null) return ValueTask.FromException<CreateSessionResponse>(CreateErr);
        return ValueTask.FromResult(new CreateSessionResponse { SessionId = 1 });
    }

    public override ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request)
    {
        CloseTargets.Add(target);
        CloseRequests.Add(request);
        if (CloseErr is not null) return ValueTask.FromException<CloseSessionResponse>(CloseErr);
        return ValueTask.FromResult(new CloseSessionResponse());
    }
}

/// <summary>notificationsTestRpcProvider</summary>
internal sealed class NotificationsTestRpcProvider : NotImplementedRpcProvider
{
    private int _failures;
    private int _attempts;

    /// <summary>Number of initial GetNotifications calls that will fail.</summary>
    public int Failures
    {
        get => Volatile.Read(ref _failures);
        set => Volatile.Write(ref _failures, value);
    }

    public Exception? Err { get; init; }
    public int Attempts => Volatile.Read(ref _attempts);

    /// <summary>If set, returned instead of a new stream.</summary>
    public IServerStreamingClient<NotificationBatch>? Stream { get; init; }

    public override ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request)
    {
        Interlocked.Increment(ref _attempts);
        if (Interlocked.Decrement(ref _failures) >= 0)
        {
            return ValueTask.FromException<IServerStreamingClient<NotificationBatch>>(Err!);
        }
        if (Stream is not null) return ValueTask.FromResult(Stream);

        var stream = new NotificationsTestStream(ct);
        // The first batch only confirms that the notification cursor is created
        stream.Batches.Writer.TryWrite(new NotificationBatch { Offset = 0 });
        return ValueTask.FromResult<IServerStreamingClient<NotificationBatch>>(stream);
    }
}

internal sealed class NotificationsTestStream : IServerStreamingClient<NotificationBatch>
{
    private readonly CancellationToken _ctx;

    public Channel<NotificationBatch> Batches { get; } = Channel.CreateBounded<NotificationBatch>(1);

    public NotificationsTestStream(CancellationToken ctx) => _ctx = ctx;

    public async ValueTask<NotificationBatch?> RecvAsync(CancellationToken ct = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _ctx);
        return await Batches.Reader.ReadAsync(linked.Token);
    }

    public void Dispose() { }
}

/// <summary>neverCompleteAsyncClient never completes the operations.  GetNotifications returns notifications and notificationsErr.</summary>
internal sealed class NeverCompleteAsyncClient : IAsyncClient
{
    private long _notificationsRequests;

    public INotifications? Notifications { get; init; }
    public Exception? NotificationsErr { get; init; }
    public long NotificationsRequests => Interlocked.Read(ref _notificationsRequests);

    public ValueTask CloseAsync() => ValueTask.CompletedTask;

    public ValueTask<PutResult> PutAsync(string key, ReadOnlyMemory<byte> value, params ReadOnlySpan<PutOption> options)
        => new(new TaskCompletionSource<PutResult>().Task);

    public ValueTask<Exception?> DeleteAsync(string key, params ReadOnlySpan<DeleteOption> options)
        => new(new TaskCompletionSource<Exception?>().Task);

    public ValueTask<Exception?> DeleteRangeAsync(string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options)
        => new(new TaskCompletionSource<Exception?>().Task);

    public ValueTask<GetResult> GetAsync(string key, params ReadOnlySpan<GetOption> options)
        => new(new TaskCompletionSource<GetResult>().Task);

    public ChannelReader<ListResult> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<ListOption> options)
        => throw new NotImplementedException("not implemented");

    public ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<RangeScanOption> options)
        => throw new NotImplementedException("not implemented");

    public ValueTask<INotifications> GetNotificationsAsync()
    {
        Interlocked.Increment(ref _notificationsRequests);
        if (NotificationsErr is not null) return ValueTask.FromException<INotifications>(NotificationsErr);
        return ValueTask.FromResult(Notifications!);
    }

    public ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options)
        => throw new NotImplementedException("not implemented");
}
