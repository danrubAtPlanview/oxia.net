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
//      * The queue is a bounded System.Threading.Channels channel (capacity = BatcherFactory.BatcherChannelBufferSize =
//        Environment.ProcessorCount, Go: runtime.GOMAXPROCS) with SingleReader = true.  The fast path of AddAsync is a
//        synchronous TryWrite and returns a default ValueTask (no allocation); a full queue parks the producer on a
//        FIFO WriteAsync (back-pressure, like the blocking channel send of Go).
//      * Go's `addMu` RWMutex + `closeC` + `addsDone` protocol is replaced by the channel completion: Dispose() calls
//        Writer.TryComplete() (which atomically stops new writes and fails the producers parked on a full queue with
//        ChannelClosedException -> they fail their call) and only THEN raises the `_closing` flag the loop polls.  So when
//        the loop sees the flag (or sees the completed, empty channel) no call can be enqueued anymore and draining the
//        queue is race free -- the same guarantee `<-b.addsDone` gives in Go.
//      * The linger timer is ONE reusable LingerTimer (BatcherTimer.cs) per batcher that is only a WAKE-UP HINT for an idle
//        loop: the deadline of the open batch is a Stopwatch timestamp the loop compares with the clock itself, so a stale
//        or early timer tick is harmless (it re-arms).  The timer is armed lazily -- only when the loop is idle with an open
//        batch and no earlier arming is still pending -- hence a loop that is saturated (batches complete by size) never
//        touches the timer, and nothing is allocated per batch (no Task.Delay, no CancellationTokenSource, no exception).
//        The tick is delivered by a sentinel object written with TryWrite (dropped when the queue is full: then the loop
//        is not idle and checks the deadline on its own).  The wake-up service waits on a HIGH RESOLUTION timer on Windows
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
using System.Threading.Channels;
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
    /// <summary>Written to the queue by the linger timer to wake up an idle loop.</summary>
    private static readonly object TimerTick = new();

    private readonly Func<IBatch> _batchFactory;
    private readonly Channel<object> _callC;
    private readonly ChannelReader<object> _reader;
    private readonly ChannelWriter<object> _writer;
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

    internal BatcherImpl(Func<IBatch> batchFactory, int queueCapacity, TimeSpan linger, int maxRequestsPerBatch)
    {
        _batchFactory = batchFactory;
        _maxRequestsPerBatch = maxRequestsPerBatch;
        QueueCapacity = Math.Max(1, queueCapacity);
        _callC = Channel.CreateBounded<object>(new BoundedChannelOptions(QueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _reader = _callC.Reader;
        _writer = _callC.Writer;
        _log = OxiaLog.Create("batcher");

        if (linger > TimeSpan.Zero)
        {
            _lingerTicks = Math.Max(1, (long)(linger.TotalSeconds * Stopwatch.Frequency));
            _timer = new LingerTimer(static state => ((BatcherImpl)state!).OnTimer(), this);
        }
    }

    /// <summary>Go: len(callC) -- the calls waiting in the queue (tests).</summary>
    internal int QueueCount => _reader.Count;

    /// <summary>Go: cap(callC) (tests).</summary>
    internal int QueueCapacity { get; }

    // -----------------------------------------------------------------------------------------------------------------
    // Producers
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: Add.  Completes when the call is queued (or failed because the batcher is closed).</summary>
    public ValueTask AddAsync(object call)
    {
        if (_writer.TryWrite(call)) return default;

        if (Volatile.Read(ref _closing) != 0)
        {
            // Closed: no exception on this path
            FailCall(call, ClientErrors.ShuttingDown);
            return default;
        }
        return AddSlowAsync(call);
    }

    /// <summary>The queue is full (or being closed): park, in FIFO order, until there is room (Go: the blocking send).</summary>
    private async ValueTask AddSlowAsync(object call)
    {
        try
        {
            await _writer.WriteAsync(call).ConfigureAwait(false);
            return;
        }
        catch (ChannelClosedException)
        {
            // Close wakes up the calls being added to a full queue
        }
        FailCall(call, ClientErrors.ShuttingDown);
    }

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
        _writer.TryComplete();
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
            _ = LoopAsync();
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
        var reader = _reader;

        while (true)
        {
            if (Volatile.Read(ref _closing) != 0) break;

            if (!reader.TryRead(out var call))
            {
                // Idle
                if (batch is not null)
                {
                    long remaining = deadline - Stopwatch.GetTimestamp();
                    if (remaining <= 0)
                    {
                        await CompleteAsync(batch).ConfigureAwait(false);
                        batch = null;
                        continue;
                    }
                    ArmTimer(deadline);
                }

                if (!await reader.WaitToReadAsync().ConfigureAwait(false)) break; // closed and empty
                continue;
            }

            if (ReferenceEquals(call, TimerTick))
            {
                if (batch is not null && Stopwatch.GetTimestamp() >= deadline)
                {
                    await CompleteAsync(batch).ConfigureAwait(false);
                    batch = null;
                }
                continue;
            }

            try
            {
                if (call is Barrier barrier)
                {
                    if (batch is not null)
                    {
                        await CompleteAsync(batch).ConfigureAwait(false);
                        batch = null;
                    }
                    barrier.Done();
                    continue;
                }

                if (batch is null)
                {
                    batch = NewBatch(out deadline);
                }

                if (!batch.CanAdd(call))
                {
                    await CompleteAsync(batch).ConfigureAwait(false);
                    batch = NewBatch(out deadline);
                }

                batch.Add(call);
                if (batch.Size == _maxRequestsPerBatch || _lingerTicks == 0)
                {
                    await CompleteAsync(batch).ConfigureAwait(false);
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
        while (reader.TryRead(out var call))
        {
            if (ReferenceEquals(call, TimerTick)) continue;
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
        // Dropped when the queue is full (the loop is busy then) or closed
        _writer.TryWrite(TimerTick);
    }
}
