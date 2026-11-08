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
//   Divergences: Wave 2 / Z1: MultiShardGet keeps a Clone() of the selected response (the batch response it came from is recycled once its calls
//              completed); DeleteRangeCompletion is superseded by Pooled/PooledDeleteRange.cs (kept for reference, no longer used).
//              Continuations run asynchronously (ManualResetValueTaskSourceCore.RunContinuationsAsynchronously): user
//              code never runs on the batcher loop that completes the calls (a Go goroutine woken by a channel send is
//              scheduled, not run inline).  A `Callback` set by a decorator (metrics) on the call is still invoked.
// =====================================================================================================================
using System.Threading.Tasks.Sources;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client;

/// <summary>A small bounded LIFO pool of reusable call objects (lock-protected array stack, allocation free).</summary>
internal sealed class CallPool<T> where T : class
{
    private readonly object _gate = new();
    private readonly T?[] _items;
    private int _count;

    public CallPool(int capacity) => _items = new T?[capacity];

    public T? TryRent()
    {
        lock (_gate)
        {
            if (_count == 0) return null;
            var item = _items[--_count];
            _items[_count] = null;
            return item;
        }
    }

    public void Return(T item)
    {
        lock (_gate)
        {
            if (_count < _items.Length) _items[_count++] = item;
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

            var chosen = AsyncClientImpl.SelectResponse(_comparisonType, _order, _selected, response);

            // The response belongs to a batch response that is recycled once its calls completed: keep a copy of the one selected
            _selected = ReferenceEquals(chosen, response) ? response!.Clone() : chosen;

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
