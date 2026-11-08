// =====================================================================================================================
// PORT NOTE   *** CONTRACT -- owned by the lead; BatcherImpl/BatcherFactory (batcher.go, batcher_factory.go) are owned by
//                 the "batching" component and live in Batcher.cs / BatcherFactory.cs next to this file. ***
//   Upstream : oxia/batch/batch.go (Batch), oxia/batch/batcher.go (Batcher, Barrier)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A Batch accumulates calls until the batcher decides to flush; a Batcher queues calls for ONE shard and runs a
//              single loop that completes one batch at a time.  `Barrier` is queued like a call but is not added to a
//              batch: the batcher completes the batch holding the calls added before it, then invokes Done.
// =====================================================================================================================
namespace Oxia.Client.Batch;

/// <summary>Go: batch.Batch.</summary>
public interface IBatch
{
    bool CanAdd(object call);
    void Add(object call);
    int Size { get; }

    /// <summary>Sends the batch and dispatches the results to the calls' callbacks.</summary>
    ValueTask CompleteAsync();

    /// <summary>Fails every call of the batch with the error.</summary>
    void Fail(Exception error);
}

/// <summary>Go: batch.Batcher.  Close() == Dispose().</summary>
public interface IBatcher : IDisposable
{
    /// <summary>Queues a call (blocks asynchronously while the queue is full). After Close the call is failed with ErrShuttingDown.</summary>
    ValueTask AddAsync(object call);

    /// <summary>The batcher loop (Go: Run, started in its own goroutine by the factory). Completes when the batcher is closed and drained.</summary>
    Task RunAsync();
}

/// <summary>Go: batch.Barrier.</summary>
public sealed class Barrier
{
    public Barrier(Action done) => Done = done;
    public Action Done { get; }
}
