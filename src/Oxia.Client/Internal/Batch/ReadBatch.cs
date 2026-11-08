// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/read_batch.go  (readBatchFactory, readBatch)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A batch of Get calls for one shard, sent as ONE ReadRequest.  The response holds one result per get, in order.
//              If the shard is gone from the shard map (ErrShardNotFound) and a rerouter is set, the gets are re-routed to
//              the shards that replaced it instead of failing.  Any other error fails every get.
//   Divergences:
//      * `Complete()` is `CompleteAsync()`; Go's `(response, error)` from the executor is a ValueTask<ReadResponse> that
//        throws: the exception is the error (caught once, in CompleteAsync; the failure path may allocate).
//      * The batch is POOLED: it implements IRecyclableBatch and the batcher gives it back after CompleteAsync / Fail; the
//        List<GetCall>, the ReadRequest (and its Gets list) and the request-timeout CancellationTokenSource are reused.
//        A batch created with factory.NewBatch(...) that nobody recycles (the unit tests) behaves exactly like Go's.
//        The request is built by REFERENCE (a GetCall is a GetRequest, see Model.cs) and is only valid until CompleteAsync
//        returns; the executor must not keep it.
//      * The rerouter receives a COPY of the list (the batch is recycled right after): it may keep it.
//      * Metrics: the callback `b.metrics.ReadCallback()` (a closure per batch in Go) is a timestamp field + a direct call to
//        Metrics.RecordRead, and only when somebody listens (Metrics.BatchEnabled); the gets are decorated only then too.
//      * `panic("invalid call")` is InvalidOperationException("invalid call").
//      * A response with fewer results than gets (a protocol violation that makes Go panic with index out of range) fails the
//        gets without a result with an OxiaException instead of crashing.
//      * `slog.Info("Shard was split/merged, ...")` goes through Oxia.Common.Logging.OxiaLog (category oxia.batch).
//      * `namespace` is carried but, like in Go, unused.
// =====================================================================================================================
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Client.Pooled;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Internal.Batch;

/// <summary>Go: readBatchFactory.</summary>
internal sealed class ReadBatchFactory
{
    private readonly BatchPool<ReadBatch> _pool = new();
    internal readonly ILogger Log = OxiaLog.Create("batch");

    public string Namespace { get; set; } = "";

    /// <summary>Go: execute -- IExecutor.ExecuteRead.</summary>
    public Func<CancellationToken, ReadRequest, ValueTask<ReadResponse>>? Execute { get; set; }

    public ReadRerouter? Reroute { get; set; }

    public OxiaMetrics? Metrics { get; set; }

    public TimeSpan RequestTimeout { get; set; }

    /// <summary>Go: newBatch(shardId).</summary>
    public ReadBatch NewBatch(long shardId)
    {
        var batch = _pool.TryRent() ?? new ReadBatch(this);
        batch.Init(shardId);
        return batch;
    }

    internal void Return(ReadBatch batch) => _pool.Return(batch);
}

/// <summary>Go: readBatch.</summary>
internal sealed class ReadBatch : IBatch, IRecyclableBatch
{
    private readonly ReadBatchFactory _factory;
    private readonly List<GetCall> _gets = new();
    private readonly ReadRequest _request = new();
    private readonly ReusableTimeout _timeout = new();
    private long _shardId;
    private bool _recordMetrics;
    private long _batchStart;
    private bool _pooledValues;

    internal ReadBatch(ReadBatchFactory factory) => _factory = factory;

    internal void Init(long shardId)
    {
        _shardId = shardId;
        var metrics = _factory.Metrics;
        _recordMetrics = metrics is not null && metrics.BatchEnabled;
        _batchStart = _recordMetrics ? metrics!.Now() : 0;
    }

    public bool CanAdd(object call) => true;

    public void Add(object call)
    {
        if (call is not GetCall get)
        {
            throw new InvalidOperationException("invalid call");
        }
        var metrics = _factory.Metrics;
        // The opt-in pooled read API: ONE pooled call makes the server's values land in leases for the whole response (checked on the
        // call itself, a metrics decorator is a plain copy)
        if (get is IPooledValueCall) _pooledValues = true;
        _gets.Add(metrics is null ? get : metrics.DecorateGet(get));
    }

    public int Size => _gets.Count;

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask CompleteAsync()
    {
        var metrics = _factory.Metrics;
        long executionStart = _recordMetrics ? metrics!.Now() : 0;
        var request = ToProto();

        var timeout = _factory.RequestTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            _timeout.Arm(timeout); // already expired
        }

        ReadResponse? response = null;
        Exception? err = null;
        try
        {
            var pending = _factory.Execute!(_timeout.Token, request);
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

        if (err is not null && OxiaErrors.Is(err, OxiaErrors.ShardNotFound) && _factory.Reroute is { } reroute)
        {
            if (_factory.Log.IsEnabled(LogLevel.Information))
            {
                _factory.Log.LogInformation("Shard was split/merged, re-routing read batch operations shard={Shard} gets={Gets}",
                    _shardId, _gets.Count);
            }
            reroute(new List<GetCall>(_gets));
            return;
        }

        if (_recordMetrics)
        {
            metrics!.RecordRead(_batchStart, executionStart, response, err);
        }

        if (err is not null)
        {
            Fail(err);
        }
        else
        {
            try
            {
                Handle(response!);
            }
            finally
            {
                // Every get took what it needs (value arrays are the caller's, leases were detached): recycle the response graph
                response!.Release();
            }
        }
    }

    public void Fail(Exception error)
    {
        for (int i = 0; i < _gets.Count; i++)
        {
            _gets[i].Complete(null, error);
        }
    }

    private void Handle(ReadResponse response)
    {
        var results = response.Gets;
        for (int i = 0; i < _gets.Count; i++)
        {
            if (i < results.Count)
            {
                _gets[i].Complete(results[i], null);
            }
            else
            {
                _gets[i].Complete(null, new OxiaException("the read response has fewer results than the read request has gets"));
            }
        }
    }

    /// <summary>Go: toProto.  Fills the reusable request with the calls themselves.</summary>
    private ReadRequest ToProto()
    {
        _request.Reset();
        _request.Shard = _shardId;
        _request.PooledValues = _pooledValues;
        for (int i = 0; i < _gets.Count; i++)
        {
            _request.Gets.Add(_gets[i]);
        }
        return _request;
    }

    public void Recycle()
    {
        _gets.Clear();
        _request.Reset();
        _pooledValues = false;
        _recordMetrics = false;
        _factory.Return(this);
    }
}
