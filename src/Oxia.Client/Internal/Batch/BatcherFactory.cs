// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/batcher_factory.go  (BatcherFactory, NewBatcherFactory, WriteRerouter, ReadRerouter,
//              NewWriteBatcher, NewReadBatcher, newBatcher)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Creates the write and read batchers of the shards of one namespace: wires an Oxia.Client.Batch batcher to
//              the write/read batches, the executor (the RPC layer), the metrics and the rerouters.
//   Divergences:
//      * Go embeds batch.BatcherFactory (Linger, MaxRequestsPerBatch, NewBatcher): this class derives from it.
//      * `shardId *int64` (a pointer shared with the Manager's map key) is a plain `long`.
//      * The rerouters are `delegate void` with the Go signatures; the lists they receive are copies the receiver may keep.
//        A rerouter must not block: it is invoked on the batcher loop of the removed shard (use WriteManager.HandOff, which
//        keeps the order of the calls without blocking).
//      * The Go value-receiver struct literals `writeBatchFactory{...}.newBatch` become one WriteBatchFactory /
//        ReadBatchFactory (with the batch pool) per batcher.
// =====================================================================================================================
using Oxia.Client.Batch;
using Oxia.Client.Internal.Model;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Internal.Batch;

/// <summary>
/// Go: WriteRerouter -- re-adds the calls of a write batch that failed because its shard is no longer in the shard map.
/// </summary>
public delegate void WriteRerouter(long shardId, List<PutCall> puts, List<DeleteCall> deletes, List<DeleteRangeCall> deleteRanges);

/// <summary>Go: ReadRerouter.</summary>
public delegate void ReadRerouter(List<GetCall> gets);

/// <summary>Go: internal/batch.BatcherFactory.</summary>
public sealed class BatcherFactory : Oxia.Client.Batch.BatcherFactory
{
    public BatcherFactory(IExecutor executor, string ns, TimeSpan linger, int maxRequestsPerBatch, OxiaMetrics metrics,
        TimeSpan requestTimeout)
    {
        Namespace = ns;
        Executor = executor;
        Linger = linger;
        MaxRequestsPerBatch = maxRequestsPerBatch;
        Metrics = metrics;
        RequestTimeout = requestTimeout;
    }

    public string Namespace { get; }

    public IExecutor Executor { get; }

    public TimeSpan RequestTimeout { get; }

    public OxiaMetrics Metrics { get; }

    public WriteRerouter? WriteRerouter { get; set; }

    public ReadRerouter? ReadRerouter { get; set; }

    /// <summary>Go: NewWriteBatcher.</summary>
    public IBatcher NewWriteBatcher(CancellationToken ct, long shardId, int maxWriteBatchSize)
    {
        var factory = new WriteBatchFactory
        {
            Namespace = Namespace,
            Execute = Executor.ExecuteWrite,
            Reroute = WriteRerouter,
            Metrics = Metrics,
            RequestTimeout = RequestTimeout,
            MaxByteSize = maxWriteBatchSize,
        };
        return NewBatcher(ct, shardId, "write", () => factory.NewBatch(shardId));
    }

    /// <summary>Go: NewReadBatcher.</summary>
    public IBatcher NewReadBatcher(CancellationToken ct, long shardId)
    {
        var factory = new ReadBatchFactory
        {
            Namespace = Namespace,
            Execute = Executor.ExecuteRead,
            Reroute = ReadRerouter,
            Metrics = Metrics,
            RequestTimeout = RequestTimeout,
        };
        return NewBatcher(ct, shardId, "read", () => factory.NewBatch(shardId));
    }
}
