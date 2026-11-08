// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/manager.go  (Manager, NewManager, Get, Close)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Keeps one batcher per shard, created on first use (used for the read batchers).  Close closes all of them
//              and returns the combined errors.
//   Divergences:
//      * `sync.RWMutex` + `map[int64]Batcher` -> a ConcurrentDictionary read lock-free on the hot path (`Get`), with the
//        creation serialised under a lock (the factory starts a loop, so it must run once per shard).
//      * `batcherFactory func(context.Context, *int64)` -> Func<CancellationToken, long, IBatcher>.
//      * Close() is Dispose(); the batchers' errors are thrown at the end (AggregateException of >= 2, the error itself for 1)
//        after ALL batchers were closed -- Go returns them (multierr.Append).
//      * Go: `Get` after `Close` creates a new, never closed batcher (a leak: its loop runs forever).  Here a batcher created
//        after Dispose is created closed, so the calls fail with ErrShuttingDown.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;

namespace Oxia.Client.Internal.Batch;

/// <summary>Go: internal/batch.Manager.</summary>
public sealed class Manager : IDisposable
{
    private readonly CancellationToken _ct;
    private readonly Func<CancellationToken, long, IBatcher> _batcherFactory;
    private readonly ConcurrentDictionary<long, IBatcher> _batchers = new();
    private readonly object _lock = new();
    private bool _closed;

    /// <summary>Go: NewManager.</summary>
    public Manager(CancellationToken ct, Func<CancellationToken, long, IBatcher> batcherFactory)
    {
        _ct = ct;
        _batcherFactory = batcherFactory;
    }

    /// <summary>Go: Get.</summary>
    public IBatcher Get(long shardId)
    {
        if (_batchers.TryGetValue(shardId, out var batcher))
        {
            return batcher;
        }

        // Fallback on the lock
        lock (_lock)
        {
            if (!_batchers.TryGetValue(shardId, out batcher))
            {
                batcher = _batcherFactory(_ct, shardId);
                if (_closed)
                {
                    batcher.Dispose();
                }
                _batchers[shardId] = batcher;
            }
            return batcher;
        }
    }

    /// <summary>Go: Close.</summary>
    public void Dispose()
    {
        List<Exception>? errors = null;
        lock (_lock)
        {
            _closed = true;
            foreach (var id in _batchers.Keys)
            {
                if (!_batchers.TryRemove(id, out var batcher)) continue;
                try
                {
                    batcher.Dispose();
                }
                catch (Exception e)
                {
                    (errors ??= new List<Exception>()).Add(e);
                }
            }
        }

        if (errors is not null)
        {
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
        }
    }
}
