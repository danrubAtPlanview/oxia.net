// =====================================================================================================================
// PORT NOTE   *** CONTRACT -- owned by the lead.  The implementing components fill in the classes; these INTERFACES are fixed. ***
//   Upstream : oxia/internal/executor.go (Executor), oxia/internal/rpc_provider.go (RpcProvider interface),
//              oxia/internal/shard_manager.go (ShardManager, ShardsReplacedListener), oxia/internal/shard_strategy.go
//              (ShardStrategy, Shard, HashRange), oxia/batch/batch.go (Batch), oxia/batch/batcher.go (Batcher, Barrier)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The seams between the Go packages: the batchers talk to an Executor; the clients talk to a ShardManager and
//              an RpcProvider.  Tests substitute fakes for these exactly like the Go tests do.
//   Translation notes:
//      * `chan struct{}` "closed when the shard map changes" -> `Task Changed` (a Task that completes when the map changes;
//        a NEW Task is exposed after every change).
//      * Executor callbacks that returned `error` keep returning `Exception?` (no exceptions for flow control).
//      * `prepare func() (*WriteRequest, error)` -> `Func<WriteRequest>`; a thrown exception aborts the attempt.
//      * Batch.Complete() blocks the batcher goroutine while the RPC runs -> `ValueTask CompleteAsync()` awaited by the batcher
//        loop, so the per-shard "one request at a time" ordering guarantee of the Go client is preserved.
//      * Batcher.Add blocks when the queue is full (back-pressure) -> `ValueTask AddAsync(object call)`.
// =====================================================================================================================
using Oxia.Common;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;

namespace Oxia.Client.Internal;

/// <summary>Go: internal.Executor.</summary>
public interface IExecutor
{
    /// <summary>Sends a write request to the leader of the shard. <paramref name="prepare"/> returns the request right before it
    /// is handed to gRPC (it may leave out operations that must not be sent anymore, or throw to abort); every attempt that gets
    /// that far invokes it, and once it returned a request it must keep returning that one.</summary>
    ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare);

    ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request);

    /// <summary>The consumer returns an error to stop the stream (that error is thrown by ExecuteList).</summary>
    ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer);

    ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> rangeScanResponseConsumer);
}

/// <summary>
/// PROTOTYPE (R3): an executor whose stream consumers may AWAIT.  The next message of the gRPC stream is read only after the
/// consumer returned, so a consumer that waits for room in a bounded channel back-pressures the server through HTTP/2 flow
/// control, exactly like Go's consumer that blocks on `ch &lt;- result` inside the goroutine that calls Recv().
/// </summary>
public interface IStreamingExecutor : IExecutor
{
    ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, ValueTask<Exception?>> listResponseConsumer);

    ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, ValueTask<Exception?>> rangeScanResponseConsumer);
}

/// <summary>Go: internal.RpcProvider.</summary>
public interface IRpcProvider : IExecutor, IAsyncCloser
{
    ValueTask<IServerStreamingClient<ShardAssignments>> GetShardAssignments(CancellationToken ct, string target, ShardAssignmentsRequest request);
    ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target, GetSequenceUpdatesRequest request);
    ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request);
    ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request);
    ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request);
    ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request);
}

/// <summary>Go: internal.HashRange.</summary>
public readonly record struct HashRange(uint MinInclusive, uint MaxInclusive);

/// <summary>Go: internal.Shard.</summary>
public readonly record struct Shard(long Id, string Leader, HashRange HashRange);

/// <summary>Go: internal.ShardStrategy.  `Get(key)` returns the predicate selecting the owning shard.</summary>
public interface IShardStrategy
{
    Func<Shard, bool> Get(string key);
}

/// <summary>Go: internal.ShardsReplacedListener -- invoked with removed shard -> successor shard ids while the shard map is locked.</summary>
public delegate void ShardsReplacedListener(IReadOnlyDictionary<long, long[]> replaced);

/// <summary>Go: internal.ShardManager.</summary>
public interface IShardManager : IDisposable
{
    long Get(string key);
    long[] GetAll();
    string Leader(long shardId);
    bool Exists(long shardId);

    /// <summary>Shards that replaced a shard removed from the shard map after a split; null if the shard was never removed.</summary>
    long[]? GetSuccessors(long shardId);

    /// <summary>The shard that took over the key among the ones that replaced a removed shard.</summary>
    bool GetSuccessor(long shardId, string key, out long successorShardId);

    /// <summary>Go: Changed() -- completes when the shard map changes; call again after it completes to wait for the next change.</summary>
    Task Changed();

    /// <summary>The order of the keys in the shards of the namespace (KeySorting.Unknown when the server does not report it).</summary>
    KeySorting KeySorting { get; }
}
