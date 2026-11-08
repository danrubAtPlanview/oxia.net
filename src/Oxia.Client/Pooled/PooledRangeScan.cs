// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (RangeScan, rangeScanFromShard, aggregateAndSortRangeScanAcrossShards)
//              oxia/results_heap.go        (the merge of the shards' streams in key order)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The OPT-IN pooled range scan: IAsyncClient.RangeScanPooledAsync returns a ChannelReader<PooledGetResult>.  Every record
//              arrives with its key AND value as bytes in one pooled buffer (the transport parses the scan with RangeScanRequest.PooledValues,
//              so neither a value array nor a key string is allocated per record), and the user disposes each record when done.
//              The semantics are RangeScanAsync's: all the shards are scanned (one if a PartitionKey is given), the records come in key
//              order (secondary key first for an index scan), an error ends the scan (it is the last item), IncludeValue(false) drops the
//              values, EndInclusive is verified against the shard's answer, and a cancelled token completes the reader quietly.
//   Design   : * one unbounded queue per shard, filled by the shard's task through the (synchronous) consumer of ExecuteRangeScan -- exactly
//                what AsyncClientImpl does with its per-shard channel, but a Queue<PooledGetResult> of structs instead of a channel (no
//                per-record segment garbage once the queues are warm);
//              * the reader (this class) merges lazily on TryRead: it can hand out a record only when every shard either has a record
//                queued or has ended, then takes the lowest by KeyOrder.CompareUtf8 -- a k-way merge by linear scan (k = number of shards),
//                not a heap, because k is small and a scan over 1 shard (the usual test) needs no comparison at all;
//              * WaitToReadAsync parks on one reusable ManualResetValueTaskSourceCore signalled by the producers.
//   Divergences: the streams are read eagerly (no back-pressure to gRPC), like RangeScanAsync.  Tasks are Task.Run'd directly (they are
//              not counted in AsyncClientImpl.RunningStreamTasks).  The reader is single-consumer.
// =====================================================================================================================
using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client.Pooled;

/// <summary>The merged, pooled result stream of one range scan (see the file header).</summary>
internal sealed class PooledScanReader : ChannelReader<PooledGetResult>, IValueTaskSource<bool>
{
    /// <summary>The queues of finished scans (their arrays are kept: a scan after warm-up allocates no queue storage).</summary>
    private static readonly BoundedStack<Queue<PooledGetResult>> QueuePool = new(64);

    private readonly object _gate = new();
    private readonly Queue<PooledGetResult>[] _queues;
    private readonly bool[] _ended;
    private readonly KeyOrder _order;
    private readonly CancellationTokenSource _scanCts;

    private ManualResetValueTaskSourceCore<bool> _waiter = new() { RunContinuationsAsynchronously = true };
    private CancellationTokenRegistration _waitRegistration;
    private bool _waiting;
    private bool _finished;
    private int _cancelProducers; // 1 once the scan is over and the producers must be stopped (done outside the lock)
    private int _endedShards;
    private TaskCompletionSource? _completion;

    public PooledScanReader(int shards, KeyOrder order, CancellationTokenSource scanCts)
    {
        _queues = new Queue<PooledGetResult>[shards];
        for (int i = 0; i < shards; i++) _queues[i] = QueuePool.TryPop() ?? new Queue<PooledGetResult>();
        _ended = new bool[shards];
        _order = order;
        _scanCts = scanCts;
        if (shards == 0) _finished = true;
    }

    // ----- producers ----------------------------------------------------------------------------------------------------

    public void Add(int shard, in PooledGetResult item)
    {
        lock (_gate)
        {
            if (_finished)
            {
                // the reader was abandoned (an error ended the scan): nobody will read this record
                item.Dispose();
                return;
            }
            _queues[shard].Enqueue(item);
            Signal();
        }
        StopProducersIfOver();
    }

    public void End(int shard)
    {
        bool last;
        lock (_gate)
        {
            _ended[shard] = true;
            last = ++_endedShards == _queues.Length;
            Signal();
        }
        StopProducersIfOver();
        if (last) _scanCts.Dispose(); // every producer is done
    }

    /// <summary>Cancels the shard scans once the reader is finished (not under the lock: the cancellation runs callbacks).</summary>
    private void StopProducersIfOver()
    {
        if (Volatile.Read(ref _cancelProducers) == 1 && Interlocked.Exchange(ref _cancelProducers, 2) == 1)
        {
            try { _scanCts.Cancel(); } catch (ObjectDisposedException) { }
        }
    }

    // ----- consumer -----------------------------------------------------------------------------------------------------

    public override bool TryRead(out PooledGetResult item)
    {
        bool ready;
        lock (_gate)
        {
            ready = Pick(dequeue: true, out item) == Readiness.Ready;
        }
        StopProducersIfOver();
        return ready;
    }

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            switch (Pick(dequeue: false, out _))
            {
                case Readiness.Ready: return new ValueTask<bool>(true);
                case Readiness.Finished: return new ValueTask<bool>(false);
            }

            if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled<bool>(cancellationToken);
            _waiter.Reset();
            _waiting = true;
            if (cancellationToken.CanBeCanceled)
            {
                _waitRegistration = cancellationToken.UnsafeRegister(static (state, token) => ((PooledScanReader)state!).OnWaitCancelled(token), this);
            }
            return new ValueTask<bool>(this, _waiter.Version);
        }
    }

    public override Task Completion
    {
        get
        {
            lock (_gate)
            {
                return (_completion ??= CreateCompletion()).Task;
            }
        }
    }

    private TaskCompletionSource CreateCompletion()
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (_finished) tcs.TrySetResult();
        return tcs;
    }

    private enum Readiness { Ready, Wait, Finished }

    /// <summary>
    /// The state of the merge, and (when <paramref name="dequeue"/>) the lowest record.  A record is handed out only when every shard has
    /// one queued or has ended -- otherwise a lower record might still arrive.  An error record goes first and finishes the scan.
    /// </summary>
    private Readiness Pick(bool dequeue, out PooledGetResult result)
    {
        result = default;
        if (_finished) return Readiness.Finished;

        int best = -1;
        PooledGetResult bestItem = default;
        for (int i = 0; i < _queues.Length; i++)
        {
            var queue = _queues[i];
            if (!queue.TryPeek(out var head))
            {
                if (!_ended[i]) return Readiness.Wait;
                continue;
            }

            if (head.Err is not null)
            {
                best = i;
                bestItem = head;
                break;
            }

            if (best < 0 || Less(in head, in bestItem))
            {
                best = i;
                bestItem = head;
            }
        }

        if (best < 0)
        {
            Finish();
            return Readiness.Finished;
        }

        if (!dequeue) return Readiness.Ready;

        _queues[best].Dequeue();
        result = bestItem;
        if (result.Err is not null) Finish();
        return Readiness.Ready;
    }

    /// <summary>Go: ResultHeap.Less -- by secondary key when both records have one, then by key.</summary>
    private bool Less(in PooledGetResult a, in PooledGetResult b)
    {
        if (a.SecondaryIndexKey is { } secondaryA && b.SecondaryIndexKey is { } secondaryB)
        {
            int c = _order.Compare(secondaryA, secondaryB);
            if (c != 0) return c < 0;
        }
        return _order.CompareUtf8(a.KeyUtf8, b.KeyUtf8) < 0;
    }

    /// <summary>The scan is over (all shards drained, or an error was handed out): stop the producers, release what nobody will read.</summary>
    private void Finish()
    {
        _finished = true;
        foreach (var queue in _queues)
        {
            while (queue.TryDequeue(out var item)) item.Dispose();
            QueuePool.TryPush(queue); // this reader never touches its queues again (Add / Pick return early once finished)
        }
        _completion?.TrySetResult();
        Interlocked.CompareExchange(ref _cancelProducers, 1, 0);
    }

    private void Signal()
    {
        if (!_waiting) return;
        var readiness = Pick(dequeue: false, out _);
        if (readiness == Readiness.Wait) return;
        _waiting = false;
        _waiter.SetResult(readiness == Readiness.Ready);
    }

    private void OnWaitCancelled(CancellationToken token)
    {
        lock (_gate)
        {
            if (!_waiting) return;
            _waiting = false;
            _waiter.SetException(new OperationCanceledException(token));
        }
    }

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        try
        {
            return _waiter.GetResult(token);
        }
        finally
        {
            _waitRegistration.Dispose();
            _waitRegistration = default;
        }
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _waiter.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _waiter.OnCompleted(continuation, state, token, flags);
}

internal static class PooledRangeScan
{
    /// <summary>Starts the scan of the shards and returns the merged reader.</summary>
    public static ChannelReader<PooledGetResult> Start(AsyncClientImpl client, CancellationToken ct, string minKeyInclusive,
        string maxKeyExclusive, in RangeScanOptions opts)
    {
        long[] shardIds = opts.PartitionKey is not null
            ? [client.GetShardForKey("", opts.PartitionKey)]
            : client.ShardManager.GetAll();

        var scanCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var reader = new PooledScanReader(shardIds.Length, client.GetKeyOrder(), scanCts);
        var scanToken = scanCts.Token;
        var options = opts;
        for (int i = 0; i < shardIds.Length; i++)
        {
            int index = i;
            long shardId = shardIds[i];
            _ = Task.Run(() => ScanShardAsync(client, ct, scanToken, minKeyInclusive, maxKeyExclusive, options, shardId, reader, index));
        }
        return reader;
    }

    /// <summary>Go: rangeScanFromShard.</summary>
    private static async Task ScanShardAsync(AsyncClientImpl client, CancellationToken ct, CancellationToken scanToken, string minKeyInclusive,
        string maxKeyExclusive, RangeScanOptions opts, long shardId, PooledScanReader reader, int index)
    {
        var request = new RangeScanRequest
        {
            Shard = shardId,
            StartInclusive = minKeyInclusive,
            EndExclusive = maxKeyExclusive,
            SecondaryIndexName = opts.SecondaryIndexName,
            IncludeInternalKeys = opts.ShowInternalKeys,
            EndInclusive = opts.EndInclusive,
            PooledValues = true,
        };
        if (!opts.IncludeValue)
        {
            request.IncludeValues = false;
        }

        try
        {
            using var retryCts = CancellationTokenSource.CreateLinkedTokenSource(scanToken);
            retryCts.CancelAfter(client.Options.RequestTimeout);

            bool responded = false;
            Exception? err = null;
            try
            {
                await client.Executor.ExecuteRangeScan(retryCts.Token, request, response =>
                {
                    if (request.EndInclusive && !response.EndInclusive)
                    {
                        return ClientErrors.EndInclusiveUnsupported;
                    }
                    responded = true;
                    var records = response.Records;
                    for (int i = 0; i < records.Count; i++)
                    {
                        reader.Add(index, ToItem(records[i], opts.IncludeValue));
                    }
                    return null;
                }).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                err = e;
            }

            if (err is null && request.EndInclusive && !responded)
            {
                err = ClientErrors.EndInclusiveUnsupported;
            }
            if (err is not null && !scanToken.IsCancellationRequested)
            {
                reader.Add(index, new PooledGetResult(err));
            }
        }
        finally
        {
            reader.End(index);
        }
    }

    private static PooledGetResult ToItem(GetResponse record, bool includeValue)
    {
        var statusError = ProtoUtils.ToError(record.Status);
        return statusError is not null
            ? new PooledGetResult(statusError)
            : PooledGetResult.FromResponse(record, originalKey: null, includeValue); // older servers send the values of a scan anyway
    }
}
