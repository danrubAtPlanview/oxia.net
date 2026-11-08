// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/batch/batcher.go  (batcherImpl: Add / enqueue / failCall / Run / Close)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A per-shard queue of calls plus ONE loop that groups the calls into batches and completes ONE batch at a time
//              (it awaits CompleteAsync), which is what keeps the writes of a shard in order.  A batch is completed when
//              it holds `maxRequestsPerBatch` calls, when the next call does not fit (`CanAdd` false), when the linger time
//              since its first call elapsed (`linger == 0` completes every call immediately), or when a Barrier arrives.
//              Close fails the open batch and every queued call with ErrShuttingDown; a call added after Close (or
//              concurrently with it) is failed right away by wrapping it in a throwaway batch; a Barrier added to a closed
//              batcher is completed (Done) right away.  Every call is completed or failed exactly once.
//   Divergences (all behaviour-preserving, chosen for throughput / zero allocation per operation):
//      * The queue is a BatcherQueue (BatcherQueue.cs): a lock-free bounded MPSC ring (capacity = the factory's QueueCapacity; the client
//        sizes it max(4096, MaxRequestsPerBatch), Go: runtime.GOMAXPROCS) with a single consumer, which replaced the bounded
//        System.Threading.Channels channel of the first version.  The fast path of AddAsync is a CAS + an exchange and returns a default
//        ValueTask (no allocation); a full queue parks the producer on a pooled IValueTaskSource, FIFO (back-pressure, like the blocking
//        channel send of Go; new adds queue behind the parked ones, so one producer's calls stay in order).
//      * Go's `addMu` RWMutex + `closeC` + `addsDone` protocol is replaced by the queue's close: Dispose() calls Complete() (which
//        atomically stops new adds, fails the producers parked on a full queue through FailClosed -> their call fails with
//        ErrShuttingDown, and wakes the loop) and only THEN raises the `_closing` flag the loop polls.  When the loop sees the flag (or
//        a closed, drained queue) no call can be enqueued anymore and the drain (DequeueClosed, which waits for a producer between its
//        reservation and its publication) is race free -- the same guarantee `<-b.addsDone` gives in Go.
//      * The loop is started with the ExecutionContext flow suppressed: it lives as long as the batcher and must not inherit (and keep
//        alive) the AsyncLocals / Activity.Current of the caller whose write created the batcher (a Go goroutine inherits nothing).
//      * EXPERIMENT: `ExperimentalMaxInFlight` > 1 (default 1 = Go's one batch at a time) starts the next batch while earlier ones wait
//        for their response.  A MEASUREMENT switch for docs/performance.md ("proposal: pipelined write batches"): it has no ordered retry.
//      * The linger timer is ONE reusable LingerTimer (BatcherTimer.cs) per batcher that is only a WAKE-UP HINT for an idle
//        loop: the deadline of the open batch is a Stopwatch timestamp the loop compares with the clock itself, so a stale
//        or early timer tick is harmless (it re-arms).  The timer is armed lazily -- only when the loop is idle with an open
//        batch and no earlier arming is still pending -- hence a loop that is saturated (batches complete by size) never
//        touches the timer, and nothing is allocated per batch (no Task.Delay, no CancellationTokenSource, no exception).
//        The tick wakes the loop through BatcherQueue.Tick (no item in the queue); an idle loop re-checks the deadline.  The wake-up service waits on a HIGH RESOLUTION timer on Windows
//        (System.Threading.Timer has the 15.6 ms OS tick there: a 5 ms linger became ~15 ms, see BatcherTimer.cs), so a
//        linger is honoured to ~0.5 ms (Go: ns) on every platform.
//      * `select` between callC / timeout / closeC is a sequential check order (closing first, then queue, then deadline);
//        Go's select picks randomly between ready cases, so any order is a legal Go schedule.
//      * Batches may implement IRecyclableBatch: the loop gives them back once they were completed or failed.
//      * An exception thrown by Add/CompleteAsync/Barrier.Done is logged and does not kill the loop (Go: panic = process
//        crash); a batch whose CompleteAsync threw is failed with that exception.
//      * Close() is Dispose(): idempotent (Go: closing twice panics).  It returns nothing (Go: always nil).
//      * ct (Go: ctx, only used for pprof labels) is accepted and ignored: the loop ends on Dispose, like Go.
// =====================================================================================================================
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Oxia.Common.Logging;

namespace Oxia.Client.Batch;

/// <summary>A batch that can be reused once the batcher is done with it (completed or failed).</summary>
internal interface IRecyclableBatch
{
    /// <summary>Invoked exactly once by the batcher after CompleteAsync / Fail returned; the batch must not be touched afterwards.</summary>
    void Recycle();
}

/// <summary>Go: batcherImpl.</summary>
internal sealed class BatcherImpl : IBatcher
{
    private readonly Func<IBatch> _batchFactory;
    private readonly BatcherQueue _queue;
    private readonly long _lingerTicks;
    private readonly int _maxRequestsPerBatch;
    private readonly LingerTimer? _timer;
    private readonly ILogger _log;

    /// <summary>1 once Dispose() completed the channel (set AFTER TryComplete, see the header).</summary>
    private int _closing;

    /// <summary>1 while a timer arming is outstanding (armed and not fired yet).</summary>
    private int _timerPending;

    private int _started;
    private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal BatcherImpl(Func<IBatch> batchFactory, int queueCapacity, TimeSpan linger, int maxRequestsPerBatch, int experimentalMaxInFlight = 1)
    {
        ExperimentalMaxInFlight = Math.Max(1, experimentalMaxInFlight);
        _batchFactory = batchFactory;
        _maxRequestsPerBatch = maxRequestsPerBatch;
        QueueCapacity = Math.Max(1, queueCapacity);
        _log = OxiaLog.Create("batcher");
        _queue = new BatcherQueue(QueueCapacity, FailClosed);

        if (linger > TimeSpan.Zero)
        {
            _lingerTicks = Math.Max(1, (long)(linger.TotalSeconds * Stopwatch.Frequency));
            _timer = new LingerTimer(static state => ((BatcherImpl)state!).OnTimer(), this);
        }
    }

    /// <summary>Go: len(callC) -- the calls waiting in the queue (tests).</summary>
    internal int QueueCount => _queue.Count;

    /// <summary>Go: cap(callC) (tests).</summary>
    internal int QueueCapacity { get; }

    // -----------------------------------------------------------------------------------------------------------------
    // Producers
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: Add.  Completes when the call is queued (or failed because the batcher is closed).</summary>
    public ValueTask AddAsync(object call)
    {
        switch (_queue.TryAdd(call, out var parked))
        {
            case AddStatus.Queued:
                return default;
            case AddStatus.Closed:
                // Closed: no exception on this path
                FailCall(call, ClientErrors.ShuttingDown);
                return default;
            default:
                // The queue is full: the producer is parked, in FIFO order, until there is room (Go: the blocking send).  Close fails
                // its call (FailClosed) and completes the wait.
                return parked!.AsValueTask();
        }
    }

    /// <summary>Close failed a producer parked on the full queue: its call fails like any call added to a closed batcher.</summary>
    private void FailClosed(object call) => FailCall(call, ClientErrors.ShuttingDown);

    /// <summary>Go: failCall.</summary>
    private void FailCall(object call, Exception err)
    {
        if (call is Barrier barrier)
        {
            barrier.Done();
            return;
        }

        var batch = _batchFactory();
        batch.Add(call);
        batch.Fail(err);
        Recycle(batch);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: Close.</summary>
    public void Dispose()
    {
        // Atomically stops new calls and wakes up (fails) the producers parked on a full queue and the idle loop.
        _queue.Complete();
        // Only now the loop may drain: nothing can be enqueued anymore.
        Volatile.Write(ref _closing, 1);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // The loop
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: Run.  Completes once the batcher is closed and its queue drained.  Starts the loop on the first call.</summary>
    public Task RunAsync()
    {
        if (Interlocked.Exchange(ref _started, 1) == 0)
        {
            // The loop lives as long as the batcher: it must not inherit (and keep alive) the ambient context -- AsyncLocals,
            // Activity.Current -- of the caller whose write happened to create the batcher.  A Go goroutine inherits nothing.
            using (ExecutionContext.SuppressFlow())
            {
                _ = LoopAsync();
            }
        }
        return _done.Task;
    }

    private async Task LoopAsync()
    {
        try
        {
            await LoopCoreAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _log.LogError(e, "The batcher loop failed");
        }
        finally
        {
            _timer?.Cancel();
            _done.TrySetResult();
        }
    }

    private async Task LoopCoreAsync()
    {
        IBatch? batch = null;
        long deadline = 0; // Stopwatch timestamp at which the open batch must be completed (linger > 0)
        var queue = _queue;

        while (true)
        {
            if (Volatile.Read(ref _closing) != 0) break;

            if (!queue.TryDequeue(out var call))
            {
                // Idle
                if (batch is not null)
                {
                    long remaining = deadline - Stopwatch.GetTimestamp();
                    if (remaining <= 0)
                    {
                        await CompleteOrPipelineAsync(batch).ConfigureAwait(false);
                        batch = null;
                        continue;
                    }
                    ArmTimer(deadline);
                }

                // A wake-up without a call (the linger timer, or a close) comes back through here: the checks above are the tick handler.
                if (!await queue.WaitToReadAsync().ConfigureAwait(false)) break; // closed and empty
                continue;
            }

            try
            {
                if (call is Barrier barrier)
                {
                    if (batch is not null)
                    {
                        await CompleteOrPipelineAsync(batch).ConfigureAwait(false);
                        batch = null;
                    }
                    if (_inFlight is { Count: > 0 }) await DrainInFlightAsync().ConfigureAwait(false);
                    barrier.Done();
                    continue;
                }

                if (batch is null)
                {
                    batch = NewBatch(out deadline);
                }

                if (!batch.CanAdd(call))
                {
                    await CompleteOrPipelineAsync(batch).ConfigureAwait(false);
                    batch = NewBatch(out deadline);
                }

                batch.Add(call);
                if (batch.Size == _maxRequestsPerBatch || _lingerTicks == 0)
                {
                    await CompleteOrPipelineAsync(batch).ConfigureAwait(false);
                    batch = null;
                }
            }
            catch (Exception e)
            {
                // Go: a panic here crashes the process.  Do not let a bad call kill the loop of the shard.
                _log.LogError(e, "Unexpected error in the batcher loop");
            }
        }

        // Closed.
        if (_inFlight is { Count: > 0 }) await DrainInFlightAsync().ConfigureAwait(false);
        if (batch is not null)
        {
            try
            {
                batch.Fail(ClientErrors.ShuttingDown);
                Recycle(batch);
            }
            catch (Exception e)
            {
                _log.LogError(e, "Unexpected error failing the open batch of a closed batcher");
            }
        }

        // Drain the queue: no call can be added to it anymore
        while (queue.DequeueClosed() is { } call)
        {
            try
            {
                FailCall(call, ClientErrors.ShuttingDown);
            }
            catch (Exception e)
            {
                _log.LogError(e, "Unexpected error failing a call of a closed batcher");
            }
        }
    }

    private IBatch NewBatch(out long deadline)
    {
        var batch = _batchFactory();
        deadline = _lingerTicks > 0 ? Stopwatch.GetTimestamp() + _lingerTicks : 0;
        return batch;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // EXPERIMENT -- pipelining (NOT a feature: see docs/performance.md, "proposal: pipelined write batches")
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// UNSAFE MEASUREMENT SWITCH, default 1 = the contract (one batch in flight per shard).  With K &gt; 1 the loop starts the next batch
    /// while up to K-1 earlier ones wait for their responses.  It keeps the order in which batches are SENT but has no ordered retry: a failed
    /// batch is retried independently of the ones behind it, which can reorder writes to a key.  It exists to quantify the potential of
    /// pipelining (benchmarks only); nothing in the library sets it.
    /// </summary>
    internal int ExperimentalMaxInFlight { get; }

    private Queue<Task>? _inFlight;

    private ValueTask CompleteOrPipelineAsync(IBatch batch) =>
        ExperimentalMaxInFlight <= 1 ? CompleteAsync(batch) : PipelineAsync(batch);

    private async ValueTask PipelineAsync(IBatch batch)
    {
        var inFlight = _inFlight ??= new Queue<Task>();
        while (inFlight.Count > 0 && inFlight.Peek().IsCompleted) _ = inFlight.Dequeue();
        while (inFlight.Count >= ExperimentalMaxInFlight) await inFlight.Dequeue().ConfigureAwait(false);
        inFlight.Enqueue(CompleteAsync(batch).AsTask());
    }

    private async ValueTask DrainInFlightAsync()
    {
        while (_inFlight is { Count: > 0 } inFlight) await inFlight.Dequeue().ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CompleteAsync(IBatch batch)
    {
        try
        {
            await batch.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _log.LogError(e, "Failed to complete a batch");
            try
            {
                batch.Fail(e);
            }
            catch (Exception failure)
            {
                _log.LogError(failure, "Failed to fail a batch");
            }
        }
        Recycle(batch);
    }

    private static void Recycle(IBatch batch)
    {
        if (batch is IRecyclableBatch recyclable) recyclable.Recycle();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Linger timer
    // -----------------------------------------------------------------------------------------------------------------

    private void ArmTimer(long deadline)
    {
        // An earlier arming that is still outstanding fires no later than this one needs (deadlines only grow).
        if (_timer is null || Interlocked.CompareExchange(ref _timerPending, 1, 0) != 0) return;

        _timer.Arm(deadline);
    }

    private void OnTimer()
    {
        Volatile.Write(ref _timerPending, 0);
        _queue.Tick();
    }
}
