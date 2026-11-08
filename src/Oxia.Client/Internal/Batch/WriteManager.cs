// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/write_manager.go  (WriteManager, writeGate: Add, HandOff, ShardsReplaced, Close, gate,
//              newGate; add, retire, predecessorDone, drain)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Keeps a batcher per shard for the WRITES, and keeps the writes in the order they were issued when the shard map
//              changes.  When a shard is removed after a split, the writes still pending on it are handed off to the shards
//              that replaced it as its batches fail with ErrShardNotFound.  The writes issued after the shard map changed go
//              to the new shards directly, so a new shard HOLDS them until every shard it replaced has handed off all its
//              writes (otherwise they could be applied before an older write to the same key).  The protocol, per shard gate:
//                 pending  = number of replaced shards that have not handed off yet; adds wait on `held` while pending > 0;
//                 retire   = the shard left the map: new adds are forwarded to the successors; the adds in flight are waited
//                            for, then (once pending == 0) a Barrier is queued to its batcher: when every batch queued before
//                            the barrier completed -- the failed ones were handed off -- the barrier tells each successor
//                            (predecessorDone) to stop holding.
//   Divergences:
//      * Blocking calls are async: Add -> AddAsync (ValueTask), the `forward` callback may be async (a ValueTask-returning
//        overload of the constructor is provided; the fixed `Action<long, object>` form is adapted to it).
//      * HandOff stays a synchronous void method (fixed signature) but never blocks a thread and keeps the order of the
//        calls: it hands the call to the batcher of the successor right away if the batcher accepts it synchronously;
//        if the queue of that batcher is full, the call (and the next hand-offs and predecessorDone notifications for that
//        shard, which must stay behind it) wait in a small per-shard FIFO that one async task drains.
//      * Go's per-gate sync.RWMutex (`add` holds the read lock while it adds to the batcher so that `retire`, which takes the
//        write lock, waits for the adds in flight) cannot be held across an await: the read side is an in-flight counter
//        (Interlocked, lock-free fast path guarded by a `slow` flag, Dekker-style with `retire`) and the write side is
//        "set retired, then wait for the counter to reach 0" (WaitForAddersAsync).  An add that starts after `retired` was set
//        is forwarded immediately instead of queueing behind the writer; the successors hold it until the drain is done, so
//        the order is the same.
//      * `go removed.retire(holds)`: RetireAsync is started inline (it only sets flags before its first await), so a write
//        routed to the removed shard after ShardsReplaced returned is already forwarded.
//      * The manager-level RWMutex+map is a ConcurrentDictionary (lock-free `gate` lookup), creation / ShardsReplaced / Close
//        under one lock.  `closed chan struct{}` is a volatile flag + a TaskCompletionSource.
//      * Close() is Dispose(): idempotent; the batchers' errors are thrown at the end, after ALL were closed.  A gate created
//        after Close gets a closed batcher (Go: a live batcher that is never closed).
//      * ORDER OF ONE CALLER'S WRITES (review R1-1).  Go's Put blocks inside Add until the write is in the batcher, so one goroutine's
//        consecutive Puts reach it in issue order even while the shard holds its writes.  AddAsync returns at once (the caller may
//        pipeline PutAsync calls without awaiting), so every add that cannot take the lock-free fast path takes a TICKET in the
//        synchronous part of AddAsync (call order == ticket order) and only starts waiting / forwarding / queueing when the ticket
//        before it is done (its write is queued, or forwarded to the successors).  The fast path is open only while no ticket is
//        outstanding (`_slow` is cleared when the chain has drained), so a write issued after a hold was released cannot overtake
//        the writes still waiting for their turn.  The same chain orders the writes forwarded by a retired gate.
//      * Calls added to a gate share the single-object-per-call rule of Model.cs: one call instance goes to one batcher; a
//        delete range for several successors must be cloned (DeleteRangeCall.Clone) by the forwarder.
// =====================================================================================================================
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Oxia.Client.Batch;
using Oxia.Common.Logging;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Internal.Batch;

/// <summary>Go: internal/batch.WriteManager.</summary>
public sealed class WriteManager : IDisposable
{
    private readonly CancellationToken _ct;
    private readonly Func<CancellationToken, long, IBatcher> _batcherFactory;
    private readonly Func<long, object, ValueTask> _forward;
    private readonly ConcurrentDictionary<long, WriteGate> _gates = new();
    private readonly object _lock = new();
    private readonly TaskCompletionSource _closedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile bool _closed;
    internal readonly ILogger Log = OxiaLog.Create("write-manager");

    /// <summary>
    /// Go: NewWriteManager.  <paramref name="forward"/> sends a write that was routed to a shard removed from the shard map to
    /// the shards that replaced it, with Add (it is invoked on the thread that added the write).
    /// </summary>
    public WriteManager(CancellationToken ct, Func<CancellationToken, long, IBatcher> batcherFactory, Action<long, object> forward)
        : this(ct, batcherFactory, (removedShardId, call) =>
        {
            forward(removedShardId, call);
            return default;
        })
    {
    }

    /// <summary>Same, with an awaitable forwarder (it may wait, e.g. for the shard that receives the write to stop holding).</summary>
    public WriteManager(CancellationToken ct, Func<CancellationToken, long, IBatcher> batcherFactory,
        Func<long, object, ValueTask> forward)
    {
        _ct = ct;
        _batcherFactory = batcherFactory;
        _forward = forward;
    }

    internal bool IsClosed => _closed;

    internal Task ClosedTask => _closedSignal.Task;

    internal ValueTask ForwardAsync(long removedShardId, object call) => _forward(removedShardId, call);

    /// <summary>
    /// Go: Add -- adds a write routed to a shard to its batcher.  Completes when the write is queued; it waits while the
    /// shard holds its writes, and it forwards the writes routed to a removed shard.
    /// </summary>
    public ValueTask AddAsync(long shardId, object call) => Gate(shardId).AddAsync(call);

    /// <summary>
    /// Go: HandOff -- adds a write that was pending on a removed shard to the batcher of a shard that replaced it, ahead of
    /// the writes that shard holds.  Never blocks; the hand-offs of a shard are queued in order (see the header).
    /// </summary>
    public void HandOff(long shardId, object call) => Gate(shardId).HandOff(call);

    /// <summary>
    /// Go: ShardsReplaced -- invoked with the shards removed from the shard map and the shards that replaced each of them,
    /// before any write can be routed with the new shard map.
    /// </summary>
    public void ShardsReplaced(IReadOnlyDictionary<long, long[]> replaced)
    {
        List<(WriteGate Removed, WriteGate[] Holds)>? retiring = null;
        lock (_lock)
        {
            if (_closed)
            {
                // The shard manager can still apply an update after the client is closed
                return;
            }

            // A new shard holds its writes until the shards it replaced have handed
            // off theirs. A removed shard that never had a write has nothing to hand off.
            var pending = new Dictionary<long, int>();
            foreach (var (removedShardId, successors) in replaced)
            {
                if (_gates.ContainsKey(removedShardId))
                {
                    foreach (var successorId in successors)
                    {
                        pending[successorId] = pending.GetValueOrDefault(successorId) + 1;
                    }
                }
            }
            foreach (var (successorId, count) in pending)
            {
                if (!_gates.ContainsKey(successorId))
                {
                    _gates[successorId] = NewGate(successorId, count);
                }
            }

            foreach (var (removedShardId, successors) in replaced)
            {
                if (!_gates.TryGetValue(removedShardId, out var removed))
                {
                    // Forward the writes routed to it with the old shard map
                    _gates[removedShardId] = WriteGate.NewRetired(this, removedShardId);
                    continue;
                }
                var holds = new WriteGate[successors.Length];
                for (int i = 0; i < successors.Length; i++)
                {
                    holds[i] = _gates[successors[i]];
                }
                (retiring ??= new List<(WriteGate, WriteGate[])>()).Add((removed, holds));
            }
        }

        if (retiring is not null)
        {
            // Go: `go removed.retire(holds)`.  Retiring waits for the writes being added to the batcher, which may
            // wait for a batch that needs the new shard map to complete; it is started outside the lock.
            foreach (var (removed, holds) in retiring)
            {
                _ = removed.RetireAsync(holds);
            }
        }
    }

    /// <summary>Go: Close.</summary>
    public void Dispose()
    {
        List<Exception>? errors = null;
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }

            foreach (var g in _gates.Values)
            {
                if (g.Batcher is null) continue;
                try
                {
                    g.Batcher.Dispose();
                }
                catch (Exception e)
                {
                    (errors ??= new List<Exception>()).Add(e);
                }
            }
            // The writes held by the gates are released, and fail on the closed batchers
            _closed = true;
            _closedSignal.TrySetResult();
        }

        if (errors is not null)
        {
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
        }
    }

    private WriteGate Gate(long shardId)
    {
        if (_gates.TryGetValue(shardId, out var g))
        {
            return g;
        }

        // Fallback on the lock
        lock (_lock)
        {
            if (!_gates.TryGetValue(shardId, out g))
            {
                g = NewGate(shardId, 0);
                if (_closed)
                {
                    g.Batcher!.Dispose();
                }
                _gates[shardId] = g;
            }
            return g;
        }
    }

    private WriteGate NewGate(long shardId, int pending) =>
        new(this, shardId, _batcherFactory(_ct, shardId), pending);
}

/// <summary>
/// Go: writeGate -- adds the writes routed to a shard to its batcher, unless the shard holds them, or it was removed from
/// the shard map and forwards them.
/// </summary>
internal sealed class WriteGate
{
    /// <summary>Queued behind the pending hand-offs: "a shard replaced by this one has handed off all its writes".</summary>
    private static readonly object PredecessorDoneMarker = new();

    private readonly WriteManager _manager;
    private readonly long _shardId;

    /// <summary>Null if the shard was removed before any write was added to it.</summary>
    internal IBatcher? Batcher { get; }

    // ---- state guarded by _mu (Go: g.mu) ----
    private readonly object _mu = new();

    /// <summary>The number of shards replaced by this one that have not handed off their writes yet: the writes are held until it drops to 0.</summary>
    private int _pending;

    /// <summary>Completed when the writes are no longer held (or the shard is retired).</summary>
    private TaskCompletionSource? _held;

    private bool _retired;

    /// <summary>The shards that replaced this one, holding their writes until this one has handed off its own.</summary>
    private WriteGate[]? _holds;

    // ---- the "read lock" of Go's RWMutex: the adds in flight ----
    private int _adders;

    /// <summary>1 while adds must take the locked path: the writes are held (pending &gt; 0) or the shard is retired.</summary>
    private int _slow;

    private int _waitingForAdders;
    private TaskCompletionSource? _addersDrained;

    // ---- the FIFO of the writes that take the slow path (guarded by _mu) ----
    // One caller's consecutive PutAsync calls must reach the batcher in the order they were issued (Go's Put blocks in Add, so the
    // second one cannot even start before the first is queued).  Every slow-path add takes a ticket SYNCHRONOUSLY, in the order the
    // calls were made, and starts its work only when the ticket before it is done (its write is queued / forwarded).  The fast path
    // is only open while no ticket is outstanding: `_slow` is not reset before the chain has drained.
    private Task _tail = Task.CompletedTask;
    private int _outstanding;
    private bool _clearSlowWhenDrained;

    // ---- ordered hand-offs ----
    private readonly object _handOffLock = new();
    private Queue<object>? _handOffQueue;
    private bool _handOffDraining;

    internal WriteGate(WriteManager manager, long shardId, IBatcher? batcher, int pending)
    {
        _manager = manager;
        _shardId = shardId;
        Batcher = batcher;
        _pending = pending;
        if (pending > 0)
        {
            _held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _slow = 1;
        }
    }

    /// <summary>Go: &amp;writeGate{manager, shardId, retired: true} -- a removed shard that never had a write.</summary>
    internal static WriteGate NewRetired(WriteManager manager, long shardId)
    {
        var gate = new WriteGate(manager, shardId, null, 0)
        {
            _retired = true,
            _slow = 1,
        };
        return gate;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // add
    // -----------------------------------------------------------------------------------------------------------------

    internal ValueTask AddAsync(object call)
    {
        // Fast path: the read lock is an in-flight counter; `_slow` is read AFTER publishing the increment (full fence), and
        // retire sets it BEFORE it reads the counter (full fence), so at least one of them sees the other.
        Interlocked.Increment(ref _adders);
        if (Volatile.Read(ref _slow) == 0)
        {
            ValueTask t;
            try
            {
                t = Batcher!.AddAsync(call);
            }
            catch
            {
                EndAdd();
                throw;
            }
            if (t.IsCompletedSuccessfully)
            {
                t.GetAwaiter().GetResult();
                EndAdd();
                return default;
            }
            return AwaitAddAsync(t);
        }

        EndAdd();
        return AddSlow(call);
    }

    /// <summary>The synchronous part of a slow-path add: takes the next ticket (call order == ticket order).</summary>
    private ValueTask AddSlow(object call)
    {
        Task prev;
        TaskCompletionSource mine = new(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_mu)
        {
            prev = _tail;
            _tail = mine.Task;
            _outstanding++;
        }
        return AddSlowAsync(call, prev, mine);
    }

    private async ValueTask AwaitAddAsync(ValueTask t)
    {
        try
        {
            await t.ConfigureAwait(false);
        }
        finally
        {
            EndAdd();
        }
    }

    private async ValueTask AddSlowAsync(object call, Task prev, TaskCompletionSource mine)
    {
        try
        {
            // the writes issued before this one go first
            if (!prev.IsCompleted) await prev.ConfigureAwait(false);
            await AddSlowCoreAsync(call).ConfigureAwait(false);
        }
        finally
        {
            lock (_mu)
            {
                _outstanding--;
                if (_outstanding == 0 && _clearSlowWhenDrained)
                {
                    _clearSlowWhenDrained = false;
                    Volatile.Write(ref _slow, 0);
                }
            }
            mine.TrySetResult();
        }
    }

    private async ValueTask AddSlowCoreAsync(object call)
    {
        while (true)
        {
            Task? held = null;
            bool forward = false;
            lock (_mu)
            {
                if (_pending > 0 && !_retired)
                {
                    held = _held!.Task;
                }
                else if (_retired)
                {
                    forward = true;
                }
                else
                {
                    Interlocked.Increment(ref _adders); // the read lock
                }
            }

            if (held is not null)
            {
                await Task.WhenAny(held, _manager.ClosedTask).ConfigureAwait(false);
                if (_manager.IsClosed)
                {
                    // The batcher is closed and fails the write
                    await Batcher!.AddAsync(call).ConfigureAwait(false);
                    return;
                }
                continue;
            }

            if (forward)
            {
                await _manager.ForwardAsync(_shardId, call).ConfigureAwait(false);
                return;
            }

            try
            {
                await Batcher!.AddAsync(call).ConfigureAwait(false);
            }
            finally
            {
                EndAdd();
            }
            return;
        }
    }

    private void EndAdd()
    {
        if (Interlocked.Decrement(ref _adders) == 0 && Volatile.Read(ref _waitingForAdders) != 0)
        {
            lock (_mu)
            {
                if (Volatile.Read(ref _adders) == 0)
                {
                    _addersDrained?.TrySetResult();
                }
            }
        }
    }

    /// <summary>Completes when no add is in flight (the Go write lock waits for the read lock holders).</summary>
    private Task WaitForAddersAsync()
    {
        lock (_mu)
        {
            if (_addersDrained is null || _addersDrained.Task.IsCompleted)
            {
                _addersDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            Interlocked.Exchange(ref _waitingForAdders, 1); // full fence: after `_slow` was published, before reading `_adders`
            if (Volatile.Read(ref _adders) == 0)
            {
                return Task.CompletedTask;
            }
            return _addersDrained.Task;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // retire / predecessorDone / drain
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: retire -- invoked once the shard was removed from the shard map, and the shards that replaced it hold their
    /// writes.  The writes added to the batcher until now are handed off to them as its batches fail, and the ones added from
    /// now on are forwarded.
    /// </summary>
    internal Task RetireAsync(WriteGate[] holds)
    {
        bool drain;
        lock (_mu)
        {
            _retired = true;
            _holds = holds;
            Volatile.Write(ref _slow, 1);
            if (_pending > 0)
            {
                // The held writes are forwarded to the shards that replaced this one,
                // which hold them until this one has handed off its writes
                _held!.TrySetResult();
            }
            drain = _pending == 0;
        }

        return drain ? DrainAsync() : Task.CompletedTask;
    }

    /// <summary>Go: predecessorDone -- a shard replaced by this one has handed off all its writes.  Ordered after the pending hand-offs.</summary>
    internal void PredecessorDone() => Submit(PredecessorDoneMarker);

    private void PredecessorDoneCore()
    {
        bool drain;
        lock (_mu)
        {
            if (_pending == 0)
            {
                // Not holding writes for it
                return;
            }
            _pending--;
            if (_pending > 0)
            {
                return;
            }
            drain = _retired;
            if (!drain)
            {
                // The held writes are added after the handed off ones; the fast path reopens once they are all queued
                _held!.TrySetResult();
                if (_outstanding == 0) Volatile.Write(ref _slow, 0);
                else _clearSlowWhenDrained = true;
            }
        }

        if (drain)
        {
            _ = DrainAsync();
        }
    }

    /// <summary>
    /// Go: drain -- hands off the writes still pending on a retired shard, which has received all the writes handed off by the
    /// shards it replaced.  The batcher completes the batches with the writes added before the barrier, which fail and are
    /// handed off, then the shards that replaced this one stop holding their writes.
    /// </summary>
    private async Task DrainAsync()
    {
        try
        {
            // Go: retire/predecessorDone take the write lock, which waits for the adds in flight
            await WaitForAddersAsync().ConfigureAwait(false);
            await Batcher!.AddAsync(new Barrier(OnDrained)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _manager.Log.LogError(e, "Failed to drain the retired shard {Shard}", _shardId);
        }
    }

    private void OnDrained()
    {
        foreach (var successor in _holds!)
        {
            try
            {
                successor.PredecessorDone();
            }
            catch (Exception e)
            {
                _manager.Log.LogError(e, "Failed to notify shard {Shard} that shard {Predecessor} has handed off", successor._shardId, _shardId);
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HandOff: ordered, non blocking
    // -----------------------------------------------------------------------------------------------------------------

    internal void HandOff(object call) => Submit(call);

    /// <summary>
    /// Runs the item now if nothing is pending; otherwise (or as soon as one of them has to wait) queues it behind the pending
    /// ones: the items of one gate are processed one at a time, in submission order.
    /// </summary>
    private void Submit(object item)
    {
        lock (_handOffLock)
        {
            if (_handOffDraining)
            {
                (_handOffQueue ??= new Queue<object>()).Enqueue(item);
                return;
            }
            _handOffDraining = true;
        }

        // Runs synchronously until the first item that cannot complete synchronously
        _ = DrainHandOffsAsync(item);
    }

    private async Task DrainHandOffsAsync(object item)
    {
        while (true)
        {
            try
            {
                await Process(item).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _manager.Log.LogError(e, "Failed to hand off a write to shard {Shard}", _shardId);
            }

            lock (_handOffLock)
            {
                if (_handOffQueue is null || !_handOffQueue.TryDequeue(out item!))
                {
                    _handOffDraining = false;
                    return;
                }
            }
        }
    }

    private ValueTask Process(object item)
    {
        if (ReferenceEquals(item, PredecessorDoneMarker))
        {
            PredecessorDoneCore();
            return default;
        }

        // Batcher == null: the shard was removed before any write was added to it
        return Batcher is null ? _manager.ForwardAsync(_shardId, item) : Batcher.AddAsync(item);
    }
}
