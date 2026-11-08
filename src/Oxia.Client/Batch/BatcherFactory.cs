// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/batch/batcher_factory.go  (BatcherFactory, batcherChannelBufferSize)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Creates the batcher of a shard and starts its loop.  `Linger` and `MaxRequestsPerBatch` are the batching
//              parameters shared by every batcher the factory creates.
//   Divergences:
//      * `batcherChannelBufferSize = runtime.GOMAXPROCS(-1)` is `BatcherChannelBufferSize = Environment.ProcessorCount`
//        (internal, settable by tests); an instance can override it with `QueueCapacity` (0 = use the static default).
//      * The loop (Go: `go process.DoWithLabels(ctx, labels, batcher.Run)`) is started by calling RunAsync() on the creating
//        thread; it runs until its first await, which is immediately (an empty queue), so no thread-pool hop is needed.
//        The pprof labels (`oxia=batcher-<type>`, `shard=<id>`) have no .NET equivalent and are dropped; `ct` is accepted
//        for signature fidelity and ignored (Go also ignores it apart from the labels).
//      * Go's `Batcher.Close() error` is `IDisposable.Dispose()`.
// =====================================================================================================================
namespace Oxia.Client.Batch;

/// <summary>Go: batch.BatcherFactory.</summary>
public class BatcherFactory
{
    /// <summary>Go: batcherChannelBufferSize (GOMAXPROCS).</summary>
    internal static int BatcherChannelBufferSize { get; set; } = Math.Max(1, Environment.ProcessorCount);

    /// <summary>Go: Linger.  Zero completes every call as soon as it is dequeued.</summary>
    public TimeSpan Linger { get; set; }

    /// <summary>Go: MaxRequestsPerBatch.</summary>
    public int MaxRequestsPerBatch { get; set; }

    /// <summary>Capacity of the queue of each batcher (back-pressure threshold). 0 = <see cref="BatcherChannelBufferSize"/>.</summary>
    public int QueueCapacity { get; set; }

    /// <summary>Go: NewBatcher.  Creates the batcher and starts its loop.</summary>
    public IBatcher NewBatcher(CancellationToken ct, long shard, string batcherType, Func<IBatch> batchFactory)
    {
        var batcher = new BatcherImpl(batchFactory, QueueCapacity > 0 ? QueueCapacity : BatcherChannelBufferSize,
            Linger, MaxRequestsPerBatch);

        // The loop never throws (it logs and continues), so the returned task needs no observation.
        _ = batcher.RunAsync();

        return batcher;
    }
}
