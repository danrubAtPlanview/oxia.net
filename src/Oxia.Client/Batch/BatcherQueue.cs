// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/batch/batcher.go  (`callC chan any` of capacity batcherChannelBufferSize: Add's blocking send, Run's receive,
//              Close's `close(closeC)`) -- no file of its own upstream.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The call queue of ONE batcher: many producers, ONE consumer (the batcher loop), bounded, FIFO.  What Go gets from a
//              buffered channel -- a send blocks while `cap` calls are queued, parked senders are served in order, the receiver parks
//              while the queue is empty -- without a thread ever blocking and without allocating per operation.
//   Divergences from the previous System.Threading.Channels based queue (behaviour is the same; only the cost changed):
//      * Enqueue is lock free: a CAS on the tail reserves a position (the capacity check is `tail - head < capacity`), the item is then
//        published into a power-of-two ring with one Interlocked.Exchange.  The channel took a lock per write; with a thousand producers
//        that lock was the second most contended point of the client.
//      * A producer that finds the queue FULL parks on a pooled IValueTaskSource (ParkedAdd, FIFO list under a lock; no allocation once
//        warm; the channel allocated a BlockedWriteAsyncOperation, an async state machine box and a ValueTask box per parked write).
//        While at least one producer is parked every new add queues behind it (the fast path checks the parked count), so the order of
//        the calls of one producer is the order of its AddAsync calls.  The consumer serves the parked producers as it frees room;
//        Dekker-style fences on both sides (a producer re-checks the room after announcing itself, the consumer re-checks for parked
//        producers after freeing room) make a lost wake-up impossible.
//      * The consumer parks on ONE reusable ManualResetValueTaskSourceCore (`WaitToReadAsync`); producers wake it after publishing
//        only if it announced that it sleeps (a read of a rarely written cache line).  `Tick()` wakes it without an item (the linger
//        timer), instead of writing a sentinel object into the queue.
//      * Close = `Complete()`: after the closed bit is set no add can reserve a position; the parked producers are failed through
//        `failClosed` (the batcher fails their call with ErrShuttingDown) and woken; the consumer drains what was reserved (a producer
//        between its reservation and its publication is waited for, a few instructions) -- the guarantee `<-b.addsDone` gives in Go.
// =====================================================================================================================
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace Oxia.Client.Batch;

internal enum AddStatus
{
    /// <summary>The item is in the queue.</summary>
    Queued,

    /// <summary>The queue is closed; the item was NOT taken.</summary>
    Closed,

    /// <summary>The queue is full: the producer parked, see the ParkedAdd.</summary>
    Parked,
}

/// <summary>A producer parked on a full queue (pooled).  Completes when its item was queued -- or, after Close, once the item was failed.</summary>
internal sealed class ParkedAdd : IValueTaskSource, IThreadPoolWorkItem
{
    /// <summary>Shared by all the batchers: thread-local first, then a bounded lock-free queue (see CallPool).</summary>
    private static readonly CallPool<ParkedAdd> Pool = new(16_384);

    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = false }; // dispatched by Execute

    internal object? Item;
    internal ParkedAdd? Next;

    internal static ParkedAdd Rent() => Pool.TryRent() ?? new ParkedAdd();

    internal ValueTask AsValueTask() => new(this, _core.Version);

    /// <summary>Wakes the producer on the thread pool (the object itself is the work item: nothing is allocated, and the producer's continuation never runs on the consumer's thread).</summary>
    internal void Complete() => ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

    void IThreadPoolWorkItem.Execute() => _core.SetResult(true);

    void IValueTaskSource.GetResult(short token)
    {
        try
        {
            _core.GetResult(token);
        }
        finally
        {
            Item = null;
            Next = null;
            _core.Reset();
            Pool.Return(this);
        }
    }

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}

/// <summary>The wake-up of the (single) consumer; a separate object so that its writes do not share a cache line with the producers' reads.</summary>
internal sealed class ConsumerSignal : IValueTaskSource<bool>, IThreadPoolWorkItem
{
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = false }; // dispatched by Execute

    internal ValueTask<bool> Wait() => new(this, _core.Version);

    /// <summary>EXPERIMENT (benchmarks): queue the wake-up on the local queue of the producer's thread.</summary>
    internal static bool PreferLocal;

    internal void Set() => ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: PreferLocal);

    void IThreadPoolWorkItem.Execute() => _core.SetResult(true);

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            _core.Reset();
        }
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}

internal sealed class BatcherQueue
{
    private const long ClosedBit = 1L << 62;

    // Hot counters, each on its own cache line pair (128 bytes apart) inside one array: the tail is written by every producer, the head by
    // the consumer, the `waiting` / `waiterCount` / `tick` flags are read by every producer and written rarely.
    private const int TailIdx = 16;
    private const int HeadIdx = 32;
    private const int WaitingIdx = 48;
    private const int WaiterCountIdx = 64;
    private const int TickIdx = 80;
    private readonly long[] _c = new long[96 + 16];

    private readonly object?[] _ring;
    private readonly int _mask;
    private readonly int _capacity;
    private readonly Action<object> _failClosed;
    private readonly ConsumerSignal _signal = new();

    private readonly object _parkLock = new();
    private ParkedAdd? _parkHead;
    private ParkedAdd? _parkTail;

    /// <param name="capacity">The number of calls that can be queued before a producer parks.</param>
    /// <param name="failClosed">Fails the call of a parked producer when the queue is closed.</param>
    internal BatcherQueue(int capacity, Action<object> failClosed)
    {
        _capacity = Math.Max(1, capacity);
        int size = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)_capacity);
        _ring = new object?[size];
        _mask = size - 1;
        _failClosed = failClosed;
    }

    internal int Capacity => _capacity;

    /// <summary>Calls queued (reserved, a producer may still be publishing the last ones).</summary>
    internal int Count => (int)((Volatile.Read(ref C(TailIdx)) & ~ClosedBit) - Volatile.Read(ref C(HeadIdx)));

    internal bool IsClosed => (Volatile.Read(ref C(TailIdx)) & ClosedBit) != 0;

    /// <summary>Closed, and every call that was reserved has been consumed.</summary>
    internal bool IsDrained
    {
        get
        {
            long tail = Volatile.Read(ref C(TailIdx));
            return (tail & ClosedBit) != 0 && (tail & ~ClosedBit) == Volatile.Read(ref C(HeadIdx));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref long C(int index) => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_c), index);

    // -----------------------------------------------------------------------------------------------------------------
    // Producers
    // -----------------------------------------------------------------------------------------------------------------

    private enum Reservation
    {
        Reserved,
        Full,
        Closed,
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Reservation TryReserve(out long position)
    {
        ref long tailRef = ref C(TailIdx);
        long tail = Volatile.Read(ref tailRef);
        while (true)
        {
            if ((tail & ClosedBit) != 0)
            {
                position = 0;
                return Reservation.Closed;
            }
            if (tail - Volatile.Read(ref C(HeadIdx)) >= _capacity)
            {
                position = 0;
                return Reservation.Full;
            }
            long seen = Interlocked.CompareExchange(ref tailRef, tail + 1, tail);
            if (seen == tail)
            {
                position = tail;
                return Reservation.Reserved;
            }
            tail = seen;
        }
    }

    /// <summary>Writes the item into the slot of a reserved position and wakes the consumer if it sleeps.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Publish(long position, object item)
    {
        // The slot is free: positions are reserved only within `capacity` (<= ring size) of the head, which moves past a slot after clearing it.
        Interlocked.Exchange(ref _ring[position & _mask], item); // full fence: the read of `waiting` below cannot move before it
        SignalConsumer();
    }

    /// <summary>
    /// Adds an item.  <see cref="AddStatus.Queued"/>: done.  <see cref="AddStatus.Closed"/>: not taken.  <see cref="AddStatus.Parked"/>: the
    /// queue is full -- await <paramref name="parked"/>, the item is queued when it completes (or failed by Close).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal AddStatus TryAdd(object item, out ParkedAdd? parked)
    {
        parked = null;
        if (Volatile.Read(ref C(WaiterCountIdx)) == 0)
        {
            switch (TryReserve(out long position))
            {
                case Reservation.Reserved:
                    Publish(position, item);
                    return AddStatus.Queued;
                case Reservation.Closed:
                    return AddStatus.Closed;
            }
        }
        return AddSlow(item, out parked);
    }

    private AddStatus AddSlow(object item, out ParkedAdd? parked)
    {
        parked = null;
        lock (_parkLock)
        {
            if (_parkHead is null)
            {
                // nobody is ahead of us: the room may have been freed since the failed reservation
                switch (TryReserve(out long position))
                {
                    case Reservation.Reserved:
                        Publish(position, item);
                        return AddStatus.Queued;
                    case Reservation.Closed:
                        return AddStatus.Closed;
                }
            }
            else if (IsClosed)
            {
                return AddStatus.Closed;
            }

            var waiter = ParkedAdd.Rent();
            waiter.Item = item;
            waiter.Next = null;
            if (_parkTail is null) _parkHead = waiter;
            else _parkTail.Next = waiter;
            _parkTail = waiter;
            Interlocked.Increment(ref C(WaiterCountIdx)); // full fence: announce ourselves BEFORE re-reading the head

            // The consumer may have freed room between our failed reservation and the announcement without seeing us: serve now.
            ServeParkedLocked();
            parked = waiter;
            return AddStatus.Parked;
        }
    }

    /// <summary>Moves the parked producers into the queue, in order, while there is room.  Called with the park lock held.</summary>
    private void ServeParkedLocked()
    {
        while (_parkHead is { } waiter)
        {
            if (TryReserve(out long position) != Reservation.Reserved) return; // full (or closed: Complete fails the parked ones)

            _parkHead = waiter.Next;
            if (_parkHead is null) _parkTail = null;
            var item = waiter.Item!;
            Publish(position, item); // before the parked count drops: a newcomer must queue behind this item
            Interlocked.Decrement(ref C(WaiterCountIdx));
            waiter.Complete();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Consumer (single)
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Takes the oldest published item, if any.  A reserved position whose item is not published yet counts as empty.</summary>
    internal bool TryDequeue([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out object? item)
    {
        long head = Volatile.Read(ref C(HeadIdx));
        ref object? slot = ref _ring[head & _mask];
        var value = Volatile.Read(ref slot);
        if (value is null)
        {
            item = null;
            return false;
        }

        Volatile.Write(ref slot, null);
        Interlocked.Exchange(ref C(HeadIdx), head + 1); // full fence: free the room, THEN look for parked producers
        if (Volatile.Read(ref C(WaiterCountIdx)) != 0)
        {
            lock (_parkLock)
            {
                ServeParkedLocked();
            }
        }
        item = value;
        return true;
    }

    private bool HeadReady() => Volatile.Read(ref _ring[Volatile.Read(ref C(HeadIdx)) & _mask]) is not null;

    private bool ConsumeTick() => Volatile.Read(ref C(TickIdx)) != 0 && Interlocked.Exchange(ref C(TickIdx), 0) != 0;

    /// <summary>
    /// Completes with true when there may be something to read -- an item, a tick, or the close -- and with false when the queue is closed and
    /// drained.  The queue is empty when it returns true and nothing is published: the caller re-checks its own conditions (deadlines).
    /// </summary>
    internal ValueTask<bool> WaitToReadAsync()
    {
        ref long waiting = ref C(WaitingIdx);
        if (HeadReady() || ConsumeTick()) return new ValueTask<bool>(true);
        if (IsClosed) return new ValueTask<bool>(WaitForReservedOrDrained());

        Interlocked.Exchange(ref waiting, 1); // announce that we sleep, THEN look once more (producers publish, then look at this flag)
        if (HeadReady() || ConsumeTick() || IsClosed)
        {
            if (Interlocked.Exchange(ref waiting, 0) != 0) return new ValueTask<bool>(true); // nobody signalled
            // a producer took the announcement and completes the signal: consume it, so that the next wait starts clean
        }
        return _signal.Wait();
    }

    /// <summary>Closed: true when a reserved item is (about to be) there, false when everything was consumed.</summary>
    private bool WaitForReservedOrDrained()
    {
        var spin = new SpinWait();
        while (!HeadReady())
        {
            if (IsDrained) return false;
            spin.SpinOnce(); // a producer between its reservation and its publication
        }
        return true;
    }

    /// <summary>Wakes the consumer without an item (the linger timer).</summary>
    internal void Tick()
    {
        Interlocked.Exchange(ref C(TickIdx), 1); // full fence
        SignalConsumer();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SignalConsumer()
    {
        if (Volatile.Read(ref C(WaitingIdx)) != 0 && Interlocked.Exchange(ref C(WaitingIdx), 0) != 0)
        {
            _signal.Set();
        }
    }

    /// <summary>
    /// After the closed bit is visible, waits for the producers that reserved a position to publish, so that the consumer can drain
    /// deterministically: returns the next item, or null when closed and drained.
    /// </summary>
    internal object? DequeueClosed()
    {
        var spin = new SpinWait();
        while (true)
        {
            if (TryDequeue(out var item)) return item;
            if (IsDrained) return null;
            spin.SpinOnce();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Stops the adds, fails the parked producers (through failClosed) and wakes the consumer.  Idempotent.</summary>
    internal void Complete()
    {
        Interlocked.Or(ref C(TailIdx), ClosedBit); // full fence

        ParkedAdd? list;
        lock (_parkLock)
        {
            list = _parkHead;
            _parkHead = _parkTail = null;
            Volatile.Write(ref C(WaiterCountIdx), 0);
        }

        for (var w = list; w is not null;)
        {
            var next = w.Next;
            try
            {
                _failClosed(w.Item!);
            }
            catch (Exception)
            {
                // a failing callback must not strand the other parked producers
            }
            w.Complete();
            w = next;
        }

        SignalConsumer();
    }
}
