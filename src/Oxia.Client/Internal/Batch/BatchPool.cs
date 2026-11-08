// =====================================================================================================================
// PORT NOTE   (no upstream counterpart: Go allocates a new batch with fresh slices for every batch)
//   Upstream : oxia/internal/batch/read_batch.go, write_batch.go (newBatch)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A tiny thread-safe object pool for the batches of one batcher.  A batcher completes ONE batch at a time, so the
//              pool normally holds a single batch (the lock-free fast slot); the bounded queue behind it only absorbs the
//              throwaway batches built by concurrent producers while the batcher is closed.
// =====================================================================================================================
using System.Collections.Concurrent;

namespace Oxia.Client.Internal.Batch;

internal sealed class BatchPool<T> where T : class
{
    private const int MaxRetained = 4;

    private T? _fast;
    private readonly ConcurrentQueue<T> _items = new();
    private int _count;

    /// <summary>Takes a pooled instance, or null if the pool is empty.</summary>
    public T? TryRent()
    {
        var item = _fast;
        if (item is not null && ReferenceEquals(Interlocked.CompareExchange(ref _fast, null, item), item))
        {
            return item;
        }

        if (_items.TryDequeue(out item))
        {
            Interlocked.Decrement(ref _count);
            return item;
        }
        return null;
    }

    /// <summary>Gives an instance back (dropped when the pool is full).</summary>
    public void Return(T item)
    {
        if (_fast is null && Interlocked.CompareExchange(ref _fast, item, null) is null)
        {
            return;
        }

        if (Interlocked.Increment(ref _count) <= MaxRetained)
        {
            _items.Enqueue(item);
        }
        else
        {
            Interlocked.Decrement(ref _count);
        }
    }
}
