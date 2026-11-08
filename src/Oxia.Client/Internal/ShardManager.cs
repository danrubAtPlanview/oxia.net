// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/shard_manager.go  (shardManagerImpl, NewShardManager, receive loop, update, recordSuccessors, overlap)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Keeps the client's view of the shard map of one namespace.  A background loop (one per manager, like the Go
//              goroutine) subscribes to the shard assignments stream of the service address, retries with an exponential
//              backoff, and applies every response with `Update`:
//                * a shard whose id is new deletes every existing shard whose hash range overlaps it (split / merge);
//                * the removed shards are recorded together with the shards that replaced them ("successors") and the
//                  ShardsReplaced listener is invoked BEFORE the new map becomes visible (the Go code invokes it while holding
//                  the write lock, so no operation can be routed with the new map before the listener rerouted the pending
//                  calls of the removed shards);
//                * the `Changed()` broadcast completes and a fresh one is installed.
//              Errors NamespaceNotFound / Unauthenticated / PermissionDenied are permanent only until the first response was
//              applied (they fail CreateAsync right away); afterwards they are retried like any other error.  CreateAsync waits
//              for the first assignments for at most requestTimeout.  Dispose cancels the loop.
//   Divergences:
//      * Reads are lock free.  Go guards a `map[int64]Shard` with an RWMutex and scans it for every Get.  Here the whole
//        state (shards sorted by hash range, id -> shard, successors, the cached GetAll array, the Changed task) is one
//        immutable `ShardMap` snapshot published with Volatile.Write under a writer-only lock; Get is a binary search over
//        the sorted ranges using a hash computed straight from the string (no closure, no predicate, no allocation) as long
//        as the strategy is a ShardStrategy.  Custom strategies (tests) still go through the predicate API.  Should two ranges
//        ever overlap (the server never sends that) the snapshot falls back to a linear scan like Go.
//      * Because readers do not block, "the new shard map is not visible yet while the listener runs" is guaranteed by
//        publishing the snapshot after the listener returned.  Tests observe the writer lock with IsUpdateLockHeld (Go's
//        `assert.False(t, s.TryRLock())`; a Monitor is reentrant, so IsEntered is the faithful equivalent).
//      * GetAll() returns a cached array shared by all callers: callers must not modify it.  The order is by hash range (Go:
//        random map order).
//      * Go `panic("shard not found")` is an InvalidOperationException("shard not found").
//      * (value, error) returns become exceptions on the cold paths (the receive loop; Retry signals failures by throwing);
//        context.DeadlineExceeded of the initial wait is a TimeoutException("context deadline exceeded") so that
//        OxiaErrors.IsDeadlineExceeded matches it.
//      * Go's `Close() error` is Dispose().  The shard assignments stream is disposed when the receive attempt ends.
//      * The logger is injectable (internal constructor) because the Go tests replace `logger` on the struct.
// =====================================================================================================================
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Time;

namespace Oxia.Client.Internal;

/// <summary>Go: shardManagerImpl.</summary>
public sealed class ShardManager : IShardManager
{
    private readonly IShardStrategy _shardStrategy;
    private readonly ShardStrategy? _fastStrategy;
    private readonly IRpcProvider? _rpcProvider;
    private readonly string _serviceAddress;
    private readonly string _namespace;
    private readonly TimeSpan _requestTimeout;
    private readonly ShardsReplacedListener? _onShardsReplaced;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger _logger;
    private readonly WaitGroup _updatedWg = new(1);

    /// <summary>Serialises the writers (Update, key sorting); readers never take it.</summary>
    private readonly object _gate = new();

    private ShardMap _map;
    private TaskCompletionSource _changedSource;
    private int _keySorting;
    private volatile bool _initialized;

    internal ShardManager(IShardStrategy shardStrategy, IRpcProvider? rpcProvider, string serviceAddress, string ns,
        TimeSpan requestTimeout, ShardsReplacedListener? onShardsReplaced, ILogger? logger = null)
    {
        _shardStrategy = shardStrategy;
        _fastStrategy = shardStrategy as ShardStrategy;
        _rpcProvider = rpcProvider;
        _serviceAddress = serviceAddress;
        _namespace = ns;
        _requestTimeout = requestTimeout;
        _onShardsReplaced = onShardsReplaced;
        _logger = logger ?? OxiaLog.Create("shardManager");
        _changedSource = NewChangedSource();
        _map = ShardMap.Create(new Dictionary<long, Shard>(), new Dictionary<long, Shard[]>(), _changedSource.Task);
    }

    /// <summary>Go: NewShardManager -- waits (at most requestTimeout) for the initial shard assignments.</summary>
    public static async ValueTask<IShardManager> CreateAsync(IShardStrategy strategy, IRpcProvider rpc, string serviceAddress,
        string ns, TimeSpan requestTimeout, ShardsReplacedListener? onShardsReplaced)
    {
        var sm = new ShardManager(strategy, rpc, serviceAddress, ns, requestTimeout, onShardsReplaced);
        var err = await sm.StartAsync().ConfigureAwait(false);
        if (err is not null)
        {
            sm.Dispose();
            throw OxiaErrors.Wrap(err, "oxia: failed to retrieve the initial list of shard assignments");
        }

        return sm;
    }

    /// <summary>Go: Close.  Cancels the receive loop and any pending wait; idempotent.</summary>
    public void Dispose() => _cts.Cancel();

    private bool IsClosed => _cts.IsCancellationRequested;

    /// <summary>Go: start().</summary>
    private async ValueTask<Exception?> StartAsync()
    {
        _ = Task.Run(ReceiveWithRecoveryAsync);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        timeout.CancelAfter(_requestTimeout);
        var err = await _updatedWg.WaitAsync(timeout.Token).ConfigureAwait(false);
        if (err is OperationCanceledException && !IsClosed)
        {
            // the request timeout elapsed: Go's context.DeadlineExceeded
            return new TimeoutException("context deadline exceeded", err);
        }

        return err;
    }

    // ----- reads (lock free) ----------------------------------------------------------------------------------------

    public long Get(string key)
    {
        var map = Volatile.Read(ref _map);
        if (_fastStrategy is { } strategy)
        {
            if (map.TryFind(strategy.Hash(key), out long id)) return id;
            throw ShardNotFoundPanic();
        }

        return GetWithPredicate(map, key);
    }

    private long GetWithPredicate(ShardMap map, string key)
    {
        var predicate = _shardStrategy.Get(key);
        foreach (var shard in map.Shards)
        {
            if (predicate(shard)) return shard.Id;
        }

        throw ShardNotFoundPanic();
    }

    private static InvalidOperationException ShardNotFoundPanic() => new("shard not found");

    /// <summary>The cached array of all the shard ids (sorted by hash range).  Shared: callers must not modify it.</summary>
    public long[] GetAll() => Volatile.Read(ref _map).AllIds;

    public string Leader(long shardId) =>
        Volatile.Read(ref _map).ById.TryGetValue(shardId, out var shard) ? shard.Leader : _serviceAddress;

    public bool Exists(long shardId) => Volatile.Read(ref _map).ById.ContainsKey(shardId);

    public long[]? GetSuccessors(long shardId)
    {
        if (!Volatile.Read(ref _map).Successors.TryGetValue(shardId, out var successors)) return null;
        var ids = new long[successors.Length];
        for (int i = 0; i < successors.Length; i++) ids[i] = successors[i].Id;
        return ids;
    }

    public bool GetSuccessor(long shardId, string key, out long successorShardId)
    {
        var map = Volatile.Read(ref _map);
        if (map.Successors.TryGetValue(shardId, out var successors))
        {
            if (_fastStrategy is { } strategy)
            {
                uint code = strategy.Hash(key);
                foreach (var shard in successors)
                {
                    if (shard.HashRange.MinInclusive <= code && code <= shard.HashRange.MaxInclusive)
                    {
                        successorShardId = shard.Id;
                        return true;
                    }
                }
            }
            else
            {
                var predicate = _shardStrategy.Get(key);
                foreach (var shard in successors)
                {
                    if (predicate(shard))
                    {
                        successorShardId = shard.Id;
                        return true;
                    }
                }
            }
        }

        successorShardId = 0;
        return false;
    }

    public Task Changed() => Volatile.Read(ref _map).Changed;

    public KeySorting KeySorting => (KeySorting)Volatile.Read(ref _keySorting);

    /// <summary>True when the calling thread is inside <see cref="Update"/> (Go tests: <c>!s.TryRLock()</c> from the listener).</summary>
    internal bool IsUpdateLockHeld => Monitor.IsEntered(_gate);

    // ----- the receive loop -----------------------------------------------------------------------------------------

    /// <summary>Go: receiveWithRecovery -- never throws; returns when closed or when a permanent failure was reported.</summary>
    internal async Task ReceiveWithRecoveryAsync()
    {
        var backOff = BackOffFactory.NewBackOff(_cts.Token);
        Exception? failure = null;
        try
        {
            await Retry.RetryNotifyAsync(() => ReceiveAttemptAsync(backOff), backOff, OnReceiveRetry, _cts.Token)
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            failure = e;
        }

        // Closing the shard manager also interrupts the wait before a retry, which is not a failure. The retries only
        // throw the cancellation once the token is cancelled, so this check cannot miss a Close that stopped them.
        if (failure is not null && !IsClosed)
        {
            _logger.LogError(failure, "Failed receiving shard assignments");
            _updatedWg.Fail(failure);
        }
    }

    private async ValueTask ReceiveAttemptAsync(IBackOff backOff)
    {
        try
        {
            await ReceiveAsync(backOff).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            if (IsClosed)
            {
                if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug(e, "Closed");
                return;
            }

            // These errors fail CreateAsync right away. Once the initial assignments are received, there is no caller to
            // report them to, and they are retried like the others: they can be transient, e.g. until the authentication
            // token is renewed.
            var code = OxiaErrors.GrpcCode(e);
            if (!_initialized && (OxiaErrors.Is(e, OxiaErrors.NamespaceNotFound) ||
                                  code == StatusCode.Unauthenticated || code == StatusCode.PermissionDenied))
            {
                throw new PermanentException(e);
            }

            throw;
        }
    }

    private void OnReceiveRetry(Exception err, TimeSpan duration)
    {
        if (OxiaErrors.GrpcCode(err) != StatusCode.Cancelled && _logger.IsEnabled(LogLevel.Warning))
        {
            _logger.LogWarning(err, "Failed receiving shard assignments, retrying later (retry-after {RetryAfter})", duration);
        }
    }

    /// <summary>Go: receive -- runs until the stream fails (always throws).</summary>
    internal async Task ReceiveAsync(IBackOff backOff)
    {
        var request = new ShardAssignmentsRequest { Namespace = _namespace };

        using var stream = await _rpcProvider!.GetShardAssignments(_cts.Token, _serviceAddress, request)
            .ConfigureAwait(false);
        while (true)
        {
            var response = await stream.RecvAsync(_cts.Token).ConfigureAwait(false)
                           ?? throw OxiaErrors.ToThrowable(OxiaErrors.EndOfStream);

            if (!response.Namespaces.TryGetValue(_namespace, out var assignments))
            {
                throw new InvalidOperationException("namespace not found in shards assignments");
            }

            var shards = new Shard[assignments.Assignments.Count];
            for (int i = 0; i < shards.Length; i++)
            {
                if (!ShardConvert.TryToShard(assignments.Assignments[i], out shards[i], out var error)) throw error;
            }

            lock (_gate)
            {
                Volatile.Write(ref _keySorting, (int)assignments.KeySorting);
            }

            Update(shards);
            backOff.Reset();
        }
    }

    // ----- updates (writers) ----------------------------------------------------------------------------------------

    /// <summary>Go: update.</summary>
    internal void Update(IReadOnlyList<Shard> updates)
    {
        lock (_gate)
        {
            var current = _map;
            var shards = new Dictionary<long, Shard>(current.ById);

            List<Shard>? removed = null;
            foreach (var update in updates)
            {
                if (!shards.ContainsKey(update.Id))
                {
                    // delete overlaps
                    List<long>? overlapping = null;
                    foreach (var (shardId, existing) in shards)
                    {
                        if (Overlap(update.HashRange, existing.HashRange))
                        {
                            if (_logger.IsEnabled(LogLevel.Information))
                            {
                                _logger.LogInformation("Deleting shard as it overlaps (existing {Existing}, update {Update})",
                                    existing, update);
                            }

                            (overlapping ??= []).Add(shardId);
                            (removed ??= []).Add(existing);
                        }
                    }

                    if (overlapping is not null)
                    {
                        foreach (long shardId in overlapping) shards.Remove(shardId);
                    }
                }

                shards[update.Id] = update;
            }

            var successors = current.Successors;
            if (removed is { Count: > 0 })
            {
                successors = new Dictionary<long, Shard[]>(current.Successors);
                RecordSuccessors(shards, removed, successors);
            }

            var nextChanged = NewChangedSource();
            var next = ShardMap.Create(shards, successors, nextChanged.Task);
            var previousChanged = _changedSource;
            Volatile.Write(ref _map, next);
            _changedSource = nextChanged;
            previousChanged.TrySetResult();
            _initialized = true;
            _updatedWg.Done();
        }
    }

    /// <summary>
    /// Go: recordSuccessors -- records the shards that replaced each removed shard: the operations still pending on a removed
    /// shard are rerouted to them.  Invoked with the writer lock held and before the new map is published.
    /// </summary>
    private void RecordSuccessors(Dictionary<long, Shard> shards, List<Shard> removed, Dictionary<long, Shard[]> successorsOut)
    {
        var replaced = new Dictionary<long, long[]>(removed.Count);
        foreach (var removedShard in removed)
        {
            List<Shard>? successors = null;
            foreach (var shard in shards.Values)
            {
                if (Overlap(shard.HashRange, removedShard.HashRange)) (successors ??= []).Add(shard);
            }

            successors?.Sort(static (a, b) => a.HashRange.MinInclusive != b.HashRange.MinInclusive
                ? a.HashRange.MinInclusive.CompareTo(b.HashRange.MinInclusive)
                : a.Id.CompareTo(b.Id));

            var successorArray = successors is null ? [] : successors.ToArray();
            var ids = new long[successorArray.Length];
            for (int i = 0; i < ids.Length; i++) ids[i] = successorArray[i].Id;

            successorsOut[removedShard.Id] = successorArray;
            replaced[removedShard.Id] = ids;
        }

        _onShardsReplaced?.Invoke(replaced);
    }

    /// <summary>Go: overlap.</summary>
    internal static bool Overlap(HashRange a, HashRange b) => a.MinInclusive <= b.MaxInclusive && a.MaxInclusive >= b.MinInclusive;

    private static TaskCompletionSource NewChangedSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ----- the immutable snapshot -----------------------------------------------------------------------------------

    /// <summary>Everything a reader needs, immutable once published.</summary>
    private sealed class ShardMap
    {
        public readonly Shard[] Shards;
        private readonly uint[] _mins;
        private readonly uint[] _maxs;
        private readonly long[] _ids;
        private readonly bool _disjoint;
        public readonly Dictionary<long, Shard> ById;
        public readonly Dictionary<long, Shard[]> Successors;
        public readonly long[] AllIds;
        public readonly Task Changed;

        private ShardMap(Shard[] shards, Dictionary<long, Shard> byId, Dictionary<long, Shard[]> successors, Task changed)
        {
            Shards = shards;
            ById = byId;
            Successors = successors;
            Changed = changed;
            _mins = new uint[shards.Length];
            _maxs = new uint[shards.Length];
            _ids = new long[shards.Length];
            AllIds = (long[])_ids.Clone(); // handed out by GetAll(): a caller that modified it must not corrupt the lookup arrays
            bool disjoint = true;
            for (int i = 0; i < shards.Length; i++)
            {
                _mins[i] = shards[i].HashRange.MinInclusive;
                _maxs[i] = shards[i].HashRange.MaxInclusive;
                _ids[i] = shards[i].Id;
                if (_mins[i] > _maxs[i] || (i > 0 && _mins[i] <= _maxs[i - 1])) disjoint = false;
            }

            _disjoint = disjoint;
        }

        public static ShardMap Create(Dictionary<long, Shard> byId, Dictionary<long, Shard[]> successors, Task changed)
        {
            var shards = new Shard[byId.Count];
            byId.Values.CopyTo(shards, 0);
            Array.Sort(shards, static (a, b) => a.HashRange.MinInclusive != b.HashRange.MinInclusive
                ? a.HashRange.MinInclusive.CompareTo(b.HashRange.MinInclusive)
                : a.Id.CompareTo(b.Id));
            return new ShardMap(shards, byId, successors, changed);
        }

        /// <summary>The shard whose inclusive hash range contains <paramref name="code"/>.</summary>
        public bool TryFind(uint code, out long id)
        {
            var mins = _mins;
            if (_disjoint)
            {
                // the last range that starts at or before the code is the only candidate
                int lo = 0, hi = mins.Length - 1, candidate = -1;
                while (lo <= hi)
                {
                    int mid = (int)((uint)(lo + hi) >> 1);
                    if (mins[mid] <= code)
                    {
                        candidate = mid;
                        lo = mid + 1;
                    }
                    else
                    {
                        hi = mid - 1;
                    }
                }

                if (candidate >= 0 && code <= _maxs[candidate])
                {
                    id = _ids[candidate];
                    return true;
                }
            }
            else
            {
                // overlapping ranges (never sent by a server): first match, like the linear scan of the Go code
                for (int i = 0; i < mins.Length; i++)
                {
                    if (mins[i] <= code && code <= _maxs[i])
                    {
                        id = _ids[i];
                        return true;
                    }
                }
            }

            id = 0;
            return false;
        }
    }
}
