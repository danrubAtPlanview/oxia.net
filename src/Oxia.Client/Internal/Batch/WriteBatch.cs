// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/write_batch.go  (writeBatchFactory, writeBatch, dropCancelledCalls, dropIfCancelled,
//              getByteSize, ErrRequestTooLarge, errCallsDropped)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A batch of Put/Delete/DeleteRange calls for one shard, sent as ONE WriteRequest.
//              * Size limit: CanAdd is false when the key/value bytes of the batch plus the call exceed maxByteSize.
//              * Op index: Add numbers the calls in the order they were added (across the three kinds); the server applies
//                them in that order and the index survives re-routing (InOpIndexOrder).
//              * Calls that were given up on: right before the request is FIRST handed to gRPC, `prepare` drops (fails with a
//                "not sent" error) the calls whose CallContext is done and marks the others as sent; the request is rebuilt
//                without the dropped ones (keeping their op index).  `prepare` runs once: later attempts get the same request.
//                If nothing is left, prepare fails with errCallsDropped and the batch is done.
//              * A failure of a request that never reached `prepare` (e.g. the leader was unreachable before the timeout) is
//                wrapped with NotSent: the server did not apply it and never will.
//              * ErrShardNotFound (the shard was split/merged) is not a failure: the calls go to the rerouter, with their op
//                index, instead (when one is set).
//              * Metrics: calls are decorated on Add, the batch is measured when it completes.
//   Divergences:
//      * `Complete()` is `CompleteAsync()`; the executor's `(response, error)` is a ValueTask<WriteResponse> that throws; the
//        `prepare func() (*WriteRequest, error)` is a Func<WriteRequest> that throws (errCallsDropped) -- allocated once per
//        pooled batch, not per attempt.
//      * POOLED: the batch implements IRecyclableBatch (the batcher gives it back after CompleteAsync/Fail); the three
//        List<>s, the WriteRequest with its lists, the prepare delegate and the timeout source are reused.  The request is
//        built by REFERENCE (a call is its own wire message) and is valid only until the executor's ValueTask completes: the
//        executor must neither keep the request nor `prepare` afterwards.  Batches nobody recycles (the unit tests) behave
//        like Go's.
//      * The request timeout (`context.WithTimeout`) is a reused CancellationTokenSource armed only if the executor did not
//        complete synchronously (see ReusableTimeout).
//      * The rerouter gets COPIES of the three lists (the batch is recycled right after).
//      * getByteSize counts UTF-8 bytes like Go's len(string); the size computed by CanAdd for a call is cached for the Add of
//        the same call that follows (the string is scanned once).
//      * dropCancelledCalls compacts the lists in place (no closures / delegates); `slices.DeleteFunc` semantics.
//      * Metrics: see ReadBatch.  `time.Now()` execution start / batch start are Metrics clock timestamps read only when
//        somebody listens.
//      * `panic("invalid call")` is InvalidOperationException("invalid call").  A response with fewer results than calls
//        fails the calls without a result with an OxiaException (Go: index out of range panic).
//      * ErrRequestTooLarge is declared in ClientErrors.RequestTooLarge (lead's file); it is not used by the batch itself in
//        Go either (it is returned by the async client before calling Add).
// =====================================================================================================================
using System.Text;
using Microsoft.Extensions.Logging;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Internal.Batch;

/// <summary>Go: writeBatchFactory.</summary>
internal sealed class WriteBatchFactory
{
    /// <summary>Go: errCallsDropped -- fails the execution of a batch whose calls were all dropped.</summary>
    internal static readonly OxiaException ErrCallsDropped = new("all the calls were dropped");

    private readonly BatchPool<WriteBatch> _pool = new();
    internal readonly ILogger Log = OxiaLog.Create("batch");

    public string Namespace { get; set; } = "";

    /// <summary>Go: execute -- IExecutor.ExecuteWrite.</summary>
    public Func<CancellationToken, long, Func<WriteRequest>, ValueTask<WriteResponse>>? Execute { get; set; }

    public WriteRerouter? Reroute { get; set; }

    public OxiaMetrics? Metrics { get; set; }

    public TimeSpan RequestTimeout { get; set; }

    public int MaxByteSize { get; set; }

    /// <summary>Go: newBatch(shardId).</summary>
    public WriteBatch NewBatch(long shardId)
    {
        var batch = _pool.TryRent() ?? new WriteBatch(this);
        batch.Init(shardId);
        return batch;
    }

    internal void Return(WriteBatch batch) => _pool.Return(batch);
}

/// <summary>Go: writeBatch.</summary>
internal sealed class WriteBatch : IBatch, IRecyclableBatch
{
    private readonly WriteBatchFactory _factory;
    private readonly List<PutCall> _puts = new();
    private readonly List<DeleteCall> _deletes = new();
    private readonly List<DeleteRangeCall> _deleteRanges = new();
    private readonly WriteRequest _request = new();
    private readonly ReusableTimeout _timeout = new();
    private readonly Func<WriteRequest> _prepare;

    private long _shardId;
    private long _byteSize;
    private bool _prepared;
    private bool _recordMetrics;
    private long _batchStart;

    // The size CanAdd computed for the call that Add is about to receive
    private object? _sizedCall;
    private int _sizedBytes;

    internal WriteBatch(WriteBatchFactory factory)
    {
        _factory = factory;
        _prepare = Prepare;
    }

    internal void Init(long shardId)
    {
        _shardId = shardId;
        _byteSize = 0;
        _prepared = false;
        var metrics = _factory.Metrics;
        _recordMetrics = metrics is not null && metrics.BatchEnabled;
        _batchStart = _recordMetrics ? metrics!.Now() : 0;
    }

    public bool CanAdd(object call)
    {
        int size = ByteSize(call);
        _sizedCall = call;
        _sizedBytes = size;
        return _byteSize + size <= _factory.MaxByteSize;
    }

    public void Add(object call)
    {
        uint opIndex = (uint)Size;
        var metrics = _factory.Metrics;
        switch (call)
        {
            case PutCall put:
                put.OpIndex = opIndex;
                _puts.Add(metrics is null ? put : metrics.DecoratePut(put));
                break;
            case DeleteCall delete:
                delete.OpIndex = opIndex;
                _deletes.Add(metrics is null ? delete : metrics.DecorateDelete(delete));
                break;
            case DeleteRangeCall deleteRange:
                deleteRange.OpIndex = opIndex;
                _deleteRanges.Add(metrics is null ? deleteRange : metrics.DecorateDeleteRange(deleteRange));
                break;
            default:
                throw new InvalidOperationException("invalid call");
        }
        _byteSize += ByteSize(call);
        _sizedCall = null;
    }

    public int Size => _puts.Count + _deletes.Count + _deleteRanges.Count;

    public async ValueTask CompleteAsync()
    {
        if (Size == 0)
        {
            return;
        }
        var metrics = _factory.Metrics;
        long executionStart = _recordMetrics ? metrics!.Now() : 0;
        ToProto();

        var timeout = _factory.RequestTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            _timeout.Arm(timeout); // already expired
        }

        // The calls are dropped or sent when the request is first handed to gRPC
        _prepared = false;
        WriteResponse? response = null;
        Exception? err = null;
        try
        {
            var pending = _factory.Execute!(_timeout.Token, _shardId, _prepare);
            if (!pending.IsCompleted && timeout > TimeSpan.Zero)
            {
                _timeout.Arm(timeout);
            }
            response = await pending.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
        }
        _timeout.Stop();

        if (Size == 0)
        {
            // Every call was dropped, and failed already
            return;
        }
        if (err is not null && OxiaErrors.Is(err, OxiaErrors.ShardNotFound) && _factory.Reroute is { } reroute)
        {
            if (_factory.Log.IsEnabled(LogLevel.Information))
            {
                _factory.Log.LogInformation(
                    "Shard was split/merged, re-routing write batch operations shard={Shard} puts={Puts} deletes={Deletes} delete-ranges={DeleteRanges}",
                    _shardId, _puts.Count, _deletes.Count, _deleteRanges.Count);
            }
            reroute(_shardId, new List<PutCall>(_puts), new List<DeleteCall>(_deletes), new List<DeleteRangeCall>(_deleteRanges));
            return;
        }

        if (_recordMetrics)
        {
            metrics!.RecordWrite(_batchStart, executionStart, _request, err);
        }

        if (err is not null)
        {
            if (!_prepared)
            {
                // The request was never handed to gRPC: the server did not apply
                // it, and never will
                err = CallContext.NotSent(err)!;
            }
            Fail(err);
        }
        else
        {
            Handle(response!);
        }
    }

    /// <summary>
    /// Go: the `prepare` closure.  Invoked by the executor right before the request is first handed to gRPC.
    /// </summary>
    private WriteRequest Prepare()
    {
        if (!_prepared)
        {
            _prepared = true;
            if (DropCancelledCalls())
            {
                if (Size == 0)
                {
                    throw OxiaErrors.ToThrowable(WriteBatchFactory.ErrCallsDropped);
                }
                ToProto();
            }
        }
        return _request;
    }

    /// <summary>
    /// Go: dropCancelledCalls -- fails and removes the calls whose caller gave up on them, right before the request is first
    /// handed to gRPC.  It marks the other calls as sent, and it returns true if any call was dropped.
    /// </summary>
    private bool DropCancelledCalls()
    {
        int size = Size;

        int kept = 0;
        for (int i = 0; i < _puts.Count; i++)
        {
            var put = _puts[i];
            if (DropIfCancelled(put.CallContext, out var err))
            {
                put.Complete(null, err);
            }
            else
            {
                _puts[kept++] = put;
            }
        }
        _puts.RemoveRange(kept, _puts.Count - kept);

        kept = 0;
        for (int i = 0; i < _deletes.Count; i++)
        {
            var delete = _deletes[i];
            if (DropIfCancelled(delete.CallContext, out var err))
            {
                delete.Complete(null, err);
            }
            else
            {
                _deletes[kept++] = delete;
            }
        }
        _deletes.RemoveRange(kept, _deletes.Count - kept);

        kept = 0;
        for (int i = 0; i < _deleteRanges.Count; i++)
        {
            var deleteRange = _deleteRanges[i];
            if (DropIfCancelled(deleteRange.CallContext, out var err))
            {
                deleteRange.Complete(null, err);
            }
            else
            {
                _deleteRanges[kept++] = deleteRange;
            }
        }
        _deleteRanges.RemoveRange(kept, _deleteRanges.Count - kept);

        return Size < size;
    }

    /// <summary>
    /// Go: dropIfCancelled -- marks a call as sent, unless its caller gave up on it: then <paramref name="err"/> is the
    /// failure of the call, and it returns true.
    /// </summary>
    private static bool DropIfCancelled(CallContext? callContext, out Exception? err)
    {
        if (CallContext.MarkSent(callContext))
        {
            err = null;
            return false;
        }
        err = callContext!.Err();
        return true;
    }

    public void Fail(Exception error)
    {
        for (int i = 0; i < _puts.Count; i++)
        {
            _puts[i].Complete(null, error);
        }
        for (int i = 0; i < _deletes.Count; i++)
        {
            _deletes[i].Complete(null, error);
        }
        for (int i = 0; i < _deleteRanges.Count; i++)
        {
            _deleteRanges[i].Complete(null, error);
        }
    }

    private void Handle(WriteResponse response)
    {
        var puts = response.Puts;
        for (int i = 0; i < _puts.Count; i++)
        {
            if (i < puts.Count) _puts[i].Complete(puts[i], null);
            else _puts[i].Complete(null, MissingResult());
        }
        var deletes = response.Deletes;
        for (int i = 0; i < _deletes.Count; i++)
        {
            if (i < deletes.Count) _deletes[i].Complete(deletes[i], null);
            else _deletes[i].Complete(null, MissingResult());
        }
        var deleteRanges = response.DeleteRanges;
        for (int i = 0; i < _deleteRanges.Count; i++)
        {
            if (i < deleteRanges.Count) _deleteRanges[i].Complete(deleteRanges[i], null);
            else _deleteRanges[i].Complete(null, MissingResult());
        }
    }

    private static OxiaException MissingResult() =>
        new("the write response has fewer results than the write request has operations");

    /// <summary>Go: toProto.  (Re)fills the reusable request with the calls themselves.</summary>
    private WriteRequest ToProto()
    {
        var request = _request;
        request.Reset();
        request.Shard = _shardId;
        for (int i = 0; i < _puts.Count; i++)
        {
            request.Puts.Add(_puts[i]);
        }
        for (int i = 0; i < _deletes.Count; i++)
        {
            request.Deletes.Add(_deletes[i]);
        }
        for (int i = 0; i < _deleteRanges.Count; i++)
        {
            request.DeleteRanges.Add(_deleteRanges[i]);
        }
        return request;
    }

    /// <summary>Go: getByteSize.</summary>
    private int ByteSize(object call)
    {
        if (ReferenceEquals(call, _sizedCall))
        {
            return _sizedBytes;
        }

        switch (call)
        {
            case PutCall put:
                return Encoding.UTF8.GetByteCount(put.Key) + put.Value.Length;
            case DeleteCall delete:
                return Encoding.UTF8.GetByteCount(delete.Key);
            case DeleteRangeCall deleteRange:
                return Encoding.UTF8.GetByteCount(deleteRange.MinKeyInclusive) + Encoding.UTF8.GetByteCount(deleteRange.MaxKeyExclusive);
            default:
                throw new InvalidOperationException("invalid call");
        }
    }

    public void Recycle()
    {
        _puts.Clear();
        _deletes.Clear();
        _deleteRanges.Clear();
        _request.Reset();
        _byteSize = 0;
        _prepared = false;
        _recordMetrics = false;
        _sizedCall = null;
        _factory.Return(this);
    }
}
