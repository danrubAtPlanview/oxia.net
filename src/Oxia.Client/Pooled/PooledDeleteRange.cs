// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (DeleteRange: the `wg := common.NewWaitGroup(len(shardIds))` + per-shard callback closure,
//              `wg.Wait(ctx)`; and the single-shard DeleteRange with a partition key)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go gives every DeleteRange a wait group (a channel with one slot per shard), a callback closure and one new call per
//              shard.  Here ONE pooled object per operation is
//                * the completion source of the ValueTask returned to the caller,
//                * the wait group: it counts the shards that answered Ok and records the FIRST failure (an error of the call, or a
//                  non-Ok status of a response) -- exactly Go's "first failure wins, otherwise every party is Done" -- and
//                * the owner of one reusable DeleteRangeCall per shard (PooledDeleteRangeShardCall), whose Complete reports to it.
//              Cancellation of the client context ends the wait with the cancellation (Go: `wg.Wait(ctx)` returns ctx.Err()).
//   Lifetime : a failed shard ends the operation early while the other shards' calls are still queued in their batchers, so the object
//              goes back to the pool only when the awaiter consumed the result AND every shard call completed (a reference count); until
//              then a still-queued call is never reused.
//   Divergences: n/a for behaviour; the calls are classes owned by the operation (Go copied a struct per shard).  A rerouted call (a split
//              shard) is cloned by AsyncClientImpl.SendDeleteRangeToSuccessors as before; the clones complete this operation's call once
//              all the successors answered.
// =====================================================================================================================
using System.Threading.Tasks.Sources;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client.Pooled;

/// <summary>The call a <see cref="PooledDeleteRange"/> sends to one shard.</summary>
internal sealed class PooledDeleteRangeShardCall : DeleteRangeCall
{
    internal PooledDeleteRange? Owner;
    private int _completed;

    internal void Arm() => Volatile.Write(ref _completed, 0);

    public override void Complete(DeleteRangeResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        var owner = Owner;
        base.Complete(response, error); // a decorator's Callback, if any
        owner!.OnShardComplete(response, error);
    }
}

/// <summary>One DeleteRange: its ValueTask source, its wait group and its per-shard calls (pooled).</summary>
internal sealed class PooledDeleteRange : IValueTaskSource<Exception?>
{
    private static readonly CallPool<PooledDeleteRange> Pool = new(4096);

    private static readonly Action<object?, CancellationToken> CancelCallback =
        static (state, token) => ((PooledDeleteRange)state!).OnCancelled(token);

    private readonly object _gate = new();
    private readonly List<PooledDeleteRangeShardCall> _calls = new();
    private ManualResetValueTaskSourceCore<Exception?> _core = new() { RunContinuationsAsynchronously = true };
    private CancellationTokenRegistration _registration;
    private int _waiting;      // shards that must still answer Ok
    private int _outstanding;  // shard calls not completed yet + 1 for the awaiter
    private bool _done;

    /// <summary>
    /// Sends the delete range to the given shards (all of them, or the one of a partition key) and returns the pending result.
    /// <paramref name="partitionKey"/> is only used for routing after a split; <paramref name="callContext"/> is shared by the shards'
    /// calls (once one is sent, the others are not dropped anymore).
    /// </summary>
    public static ValueTask<Exception?> Start(AsyncClientImpl client, ReadOnlySpan<long> shardIds, string minKeyInclusive, string maxKeyExclusive,
        string? partitionKey, CallContext? callContext)
    {
        var op = Pool.TryRent() ?? new PooledDeleteRange();
        int n = shardIds.Length;
        lock (op._gate)
        {
            op._waiting = n;
            op._outstanding = n + 1;
            op._done = false;
        }
        var result = new ValueTask<Exception?>(op, op._core.Version);
        if (n == 0)
        {
            // Go: a wait group of zero parties is done at once
            op.Finish(null);
            return result;
        }

        if (client.Ctx.CanBeCanceled) op._registration = client.Ctx.UnsafeRegister(CancelCallback, op);

        for (int i = 0; i < n; i++)
        {
            if (op._calls.Count <= i) op._calls.Add(new PooledDeleteRangeShardCall());
            var call = op._calls[i];
            call.Owner = op;
            call.Arm();
            call.MinKeyInclusive = minKeyInclusive;
            call.MaxKeyExclusive = maxKeyExclusive;
            call.PartitionKey = partitionKey;
            call.CallContext = callContext;
            client.AddPlainWrite(shardIds[i], call);
        }
        return result;
    }

    /// <summary>Go: the per-shard callback -- Done on an Ok response, Fail with the error (or the status) otherwise.</summary>
    internal void OnShardComplete(DeleteRangeResponse? response, Exception? error)
    {
        bool recycle;
        lock (_gate)
        {
            if (!_done)
            {
                if (error is not null) Finish(error);
                else if (response!.Status != Status.Ok) Finish(ProtoUtils.ToError(response.Status)!);
                else if (--_waiting == 0) Finish(null);
            }
            recycle = --_outstanding == 0;
        }
        if (recycle) Recycle();
    }

    private void OnCancelled(CancellationToken token)
    {
        lock (_gate)
        {
            if (!_done) Finish(new OperationCanceledException(token));
        }
    }

    /// <summary>Resolves the ValueTask (callers hold the gate, or own the object).</summary>
    private void Finish(Exception? result)
    {
        _done = true;
        _core.SetResult(result);
    }

    Exception? IValueTaskSource<Exception?>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        bool recycle;
        lock (_gate)
        {
            recycle = --_outstanding == 0;
        }
        if (recycle) Recycle();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<Exception?>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<Exception?>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        _registration.Dispose();
        _registration = default;
        foreach (var call in _calls)
        {
            call.Reset();
            call.Owner = null;
        }
        _core.Reset();
        Pool.Return(this);
    }
}
