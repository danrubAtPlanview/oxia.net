// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (the `ch := make(chan PutResult, 1)` + `callback` closure of Put / Delete /
//              DeleteRange / Get, and multiShardGetCallback / multiShardDeleteRangeCallback)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go gives every operation a 1-buffered channel plus a callback closure; the callback sends the result and
//              closes the channel.  Here the call object IS the request (PutCall : PutRequest ...), and it is also the
//              completion source of the ValueTask returned to the caller, so a Put/Get/Delete allocates nothing:
//                 * PooledPutCall / PooledDeleteCall / PooledGetCall are classes from a bounded pool.  They override
//                   `Complete(response, error)` (which the batches invoke exactly once) to resolve a
//                   ManualResetValueTaskSourceCore<T>; the returned ValueTask<T> wraps the call, and the call goes back to
//                   the pool when the awaiter consumes the result (GetResult).
//                 * The less frequent calls (single-shard DeleteRange, multi-shard Get) are not pooled but also avoid
//                   TaskCompletionSource: DeleteRangeCompletion, MultiShardGet.
//   Contract with the batches: after calling `Complete` on a call, the batch must not touch the call again (the awaiter
//              may already have recycled it).  `Complete` is idempotent (only the first outcome wins), so a call that is
//              both failed by a shutting-down batcher and answered by the server cannot corrupt the pool.
//   Divergences: continuations run asynchronously (ManualResetValueTaskSourceCore.RunContinuationsAsynchronously): user
//              code never runs on the batcher loop that completes the calls (a Go goroutine woken by a channel send is
//              scheduled, not run inline).  A `Callback` set by a decorator (metrics) on the call is still invoked.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client;

/// <summary>
/// A bounded pool of reusable call objects.  Level 1 is a small per-thread stack: the thread that completes an operation (the
/// awaiter's continuation recycles the call in GetResult) is almost always the one that rents the next call (its loop), so the hot
/// path never touches shared state -- the single lock of the previous array-stack pool was the most contended point of the client
/// with a thousand producers (docs/performance.md).  Level 2 is a lock-free queue, bounded by <c>capacity</c>, that threads spill to
/// and refill from in chunks.  One pool instance per closed generic type (the thread-local cache is a static of the generic type).
/// </summary>
internal sealed class CallPool<T> where T : class
{
    private const int LocalCapacity = 32;
    private const int Chunk = LocalCapacity / 2;

    private sealed class Local
    {
        public readonly T?[] Items = new T?[LocalCapacity];
        public int Count;
    }

    [ThreadStatic]
    private static Local? t_local;

    private readonly ConcurrentQueue<T> _shared = new();
    private readonly int _capacity;
    private int _sharedCount;

    public CallPool(int capacity) => _capacity = capacity;

    public T? TryRent()
    {
        var local = t_local;
        if (local is not null && local.Count > 0)
        {
            var item = local.Items[--local.Count];
            local.Items[local.Count] = null;
            return item;
        }
        return TryRentSlow();
    }

    private T? TryRentSlow()
    {
        if (!_shared.TryDequeue(out var item)) return null;

        // refill the thread cache with a chunk so the next rents stay local
        var local = t_local ??= new Local();
        int taken = 0;
        while (taken < Chunk && _shared.TryDequeue(out var more))
        {
            local.Items[local.Count++] = more;
            taken++;
        }
        Interlocked.Add(ref _sharedCount, -(taken + 1));
        return item;
    }

    public void Return(T item)
    {
        var local = t_local ??= new Local();
        if (local.Count < LocalCapacity)
        {
            local.Items[local.Count++] = item;
            return;
        }
        ReturnSlow(local, item);
    }

    private void ReturnSlow(Local local, T item)
    {
        // spill the older half of the thread cache (and this item) to the shared queue; drop what exceeds the bound
        int room = _capacity - Volatile.Read(ref _sharedCount);
        int spilled = 0;
        if (room > 0)
        {
            int n = Math.Min(Chunk, room - 1);
            for (int i = 0; i < n; i++)
            {
                _shared.Enqueue(local.Items[i]!);
                spilled++;
            }
            _shared.Enqueue(item);
            spilled++;
            Interlocked.Add(ref _sharedCount, spilled);
            // compact the remaining items
            Array.Copy(local.Items, n, local.Items, 0, local.Count - n);
            Array.Clear(local.Items, local.Count - n, n);
            local.Count -= n;
        }
        else
        {
            // the shared pool is full: keep the local cache as it is and let the item go
        }
    }
}

/// <summary>A pooled Put: the PutCall that is the request, and the source of the ValueTask of PutResult returned to the caller.</summary>
internal sealed class PooledPutCall : PutCall, IValueTaskSource<PutResult>
{
    private static readonly CallPool<PooledPutCall> Pool = new(4096);

    private ManualResetValueTaskSourceCore<PutResult> _core = new() { RunContinuationsAsynchronously = true };
    private int _completed;

    // Ephemeral puts: set by AsyncClientImpl, invoked by Sessions.ExecuteWithSessionId (delegates are created once per pooled object)
    internal AsyncClientImpl? Owner;
    private Func<long>? _shardForKey;
    private Action<long, long, Exception?>? _onSession;

    public static PooledPutCall Rent() => Pool.TryRent() ?? new PooledPutCall();

    public ValueTask<PutResult> AsValueTask() => new(this, _core.Version);

    public override void Complete(PutResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        base.Complete(response, error); // a decorator may have wrapped Callback
        PutResult result = error is not null
            ? new PutResult("", default, error)
            : ProtoUtils.ToPutResult(Key, response!);
        _core.SetResult(result);
    }

    /// <summary>Go: the ephemeral branch of Put -- the session (and with it the shard) is only known once it is established.</summary>
    internal void StartEphemeral(AsyncClientImpl owner)
    {
        Owner = owner;
        _shardForKey ??= ShardForKey;
        _onSession ??= OnSession;
        owner.Sessions.ExecuteWithSessionId(_shardForKey, _onSession);
    }

    private long ShardForKey() => Owner!.GetShardForKey(Key, PartitionKey);

    private void OnSession(long shardId, long sessionId, Exception? err)
    {
        if (err is not null)
        {
            Complete(null, err);
            return;
        }
        SessionId = sessionId;
        Owner!.AddToWriteManager(shardId, this);
    }

    PutResult IValueTaskSource<PutResult>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        Recycle();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<PutResult>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<PutResult>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        _core.Reset();
        Reset();
        Owner = null;
        Volatile.Write(ref _completed, 0);
        Pool.Return(this);
    }
}

/// <summary>A pooled Delete: result is the error (null on success), like Go's error channel.</summary>
internal sealed class PooledDeleteCall : DeleteCall, IValueTaskSource<Exception?>
{
    private static readonly CallPool<PooledDeleteCall> Pool = new(4096);

    private ManualResetValueTaskSourceCore<Exception?> _core = new() { RunContinuationsAsynchronously = true };
    private int _completed;

    public static PooledDeleteCall Rent() => Pool.TryRent() ?? new PooledDeleteCall();

    public ValueTask<Exception?> AsValueTask() => new(this, _core.Version);

    public override void Complete(DeleteResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        base.Complete(response, error);
        _core.SetResult(error ?? ProtoUtils.ToDeleteResult(response!));
    }

    Exception? IValueTaskSource<Exception?>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        Recycle();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<Exception?>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<Exception?>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        _core.Reset();
        Reset();
        Volatile.Write(ref _completed, 0);
        Pool.Return(this);
    }
}

/// <summary>A pooled single-shard Get.</summary>
internal sealed class PooledGetCall : GetCall, IValueTaskSource<GetResult>
{
    private static readonly CallPool<PooledGetCall> Pool = new(4096);

    private ManualResetValueTaskSourceCore<GetResult> _core = new() { RunContinuationsAsynchronously = true };
    private int _completed;

    public static PooledGetCall Rent() => Pool.TryRent() ?? new PooledGetCall();

    public ValueTask<GetResult> AsValueTask() => new(this, _core.Version);

    public override void Complete(GetResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        base.Complete(response, error);
        _core.SetResult(ProtoUtils.ToGetResult(response, Key, error));
    }

    GetResult IValueTaskSource<GetResult>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        Recycle();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<GetResult>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<GetResult>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        _core.Reset();
        Reset();
        Volatile.Write(ref _completed, 0);
        Pool.Return(this);
    }
}

/// <summary>A single-shard DeleteRange (with a partition key): the call that is also the completion source of its ValueTask.</summary>
internal sealed class DeleteRangeCompletion : DeleteRangeCall, IValueTaskSource<Exception?>
{
    private ManualResetValueTaskSourceCore<Exception?> _core = new() { RunContinuationsAsynchronously = true };
    private int _completed;

    public ValueTask<Exception?> AsValueTask() => new(this, _core.Version);

    public override void Complete(DeleteRangeResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        base.Complete(response, error);
        _core.SetResult(error ?? ProtoUtils.ToDeleteRangeResult(response!));
    }

    Exception? IValueTaskSource<Exception?>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<Exception?>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<Exception?>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);
}

/// <summary>
/// Go: multiShardGetCallback -- aggregates the per-shard responses of a multi-shard get: the first error wins and ends the
/// get, discarding the remaining responses; otherwise the best response for the comparison type is selected once every
/// shard has responded.  The aggregate result is delivered once (Go: send on the 1-buffered channel, then close it).
/// </summary>
internal sealed class MultiShardGet : IValueTaskSource<GetResult>
{
    private readonly object _lock = new();
    private readonly string _key;
    private readonly KeyComparisonType _comparisonType;
    private readonly KeyOrder _order;
    private int _counter;
    private GetResponse _selected = AsyncClientImpl.KeyNotFoundResponse;
    private ManualResetValueTaskSourceCore<GetResult> _core = new() { RunContinuationsAsynchronously = true };

    public MultiShardGet(string key, KeyComparisonType comparisonType, KeyOrder order, int numShards)
    {
        _key = key;
        _comparisonType = comparisonType;
        _order = order;
        _counter = numShards;
    }

    /// <summary>Number of results delivered to the caller (0 or 1) -- Go's "closed exactly once".</summary>
    internal int Delivered { get; private set; }

    public ValueTask<GetResult> AsValueTask() => new(this, _core.Version);

    /// <summary>Go: the func(*proto.GetResponse, error) returned by multiShardGetCallback.</summary>
    public void Callback(GetResponse? response, Exception? err)
    {
        lock (_lock)
        {
            if (_counter == 0)
            {
                // Response already sent, nothing to do
                return;
            }

            if (err is not null)
            {
                Deliver(ProtoUtils.ToGetResult(null, _key, err));
                _counter = 0;
                return;
            }

            _selected = AsyncClientImpl.SelectResponse(_comparisonType, _order, _selected, response);

            _counter--;
            if (_counter == 0)
            {
                Deliver(ProtoUtils.ToGetResult(_selected, _key, null));
            }
        }
    }

    private void Deliver(GetResult result)
    {
        Delivered++;
        _core.SetResult(result);
    }

    GetResult IValueTaskSource<GetResult>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<GetResult>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<GetResult>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);
}

/// <summary>One of the per-shard gets of a multi-shard get: reports to the aggregate.</summary>
internal sealed class MultiShardGetCall : GetCall
{
    private readonly MultiShardGet _owner;

    public MultiShardGetCall(MultiShardGet owner) => _owner = owner;

    public override void Complete(GetResponse? response, Exception? error)
    {
        base.Complete(response, error);
        _owner.Callback(response, error);
    }
}
