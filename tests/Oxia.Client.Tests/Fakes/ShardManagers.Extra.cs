// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/async_client_impl_test.go (staticShardManager), oxia/async_client_reroute_test.go (splittableShardManager),
//              oxia/notifications_test.go (notificationsTestShardManager)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Fakes of IShardManager.  Go's `Changed() <-chan struct{}` is `Task Changed()`: a nil channel (never fires) is a
//              Task that never completes; closing the channel and replacing it is completing the TaskCompletionSource and
//              creating a new one.  `Close() error` -> IDisposable.Dispose.
// =====================================================================================================================
using Oxia.Client.Internal;
using Oxia.Common.Proto;
using ShardStrategyImpl = Oxia.Client.Internal.ShardStrategy;

namespace Oxia.Client.Tests.F2;

/// <summary>staticShardManager routes every key to its first shard.</summary>
internal sealed class StaticShardManager : IShardManager
{
    private static readonly Task NeverChanges = new TaskCompletionSource().Task;

    public long[] Shards { get; set; } = [];
    public string LeaderAddress { get; set; } = "";
    public Dictionary<long, long[]> Successors { get; set; } = new();
    public KeySorting Sorting { get; set; }
    public bool Closed { get; private set; }

    public void Dispose() => Closed = true;

    public long Get(string key) => Shards[0];
    public long[] GetAll() => Shards;
    public string Leader(long shardId) => LeaderAddress;
    public bool Exists(long shardId) => true;
    public long[]? GetSuccessors(long shardId) => Successors.TryGetValue(shardId, out var s) ? s : null;

    /// <summary>GetSuccessor routes every key to the first successor.</summary>
    public bool GetSuccessor(long shardId, string key, out long successorShardId)
    {
        if (Successors.TryGetValue(shardId, out var successors) && successors.Length > 0)
        {
            successorShardId = successors[0];
            return true;
        }
        successorShardId = 0;
        return false;
    }

    public Task Changed() => NeverChanges;

    KeySorting IShardManager.KeySorting => Sorting;
}

/// <summary>splittableShardManager is a ShardManager whose shards can be split, to simulate the assignments update a client receives after a shard split.</summary>
internal sealed class SplittableShardManager : IShardManager
{
    private readonly object _lock = new();
    private readonly ShardStrategyImpl _strategy = ShardStrategyImpl.New();
    private List<Shard> _shards;
    private readonly Dictionary<long, Shard[]> _successors = new();
    private readonly string _leader;
    private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ShardsReplacedListener OnShardsReplaced { get; set; } = _ => { };

    /// <summary>newSplittableShardManager returns a splittableShardManager with a single shard, 0, on the whole hash range, led by the given leader.</summary>
    public SplittableShardManager(string leader)
    {
        _shards = [new Shard(0, "", new HashRange(0, uint.MaxValue))];
        _leader = leader;
    }

    public void Dispose() { }

    public long Get(string key)
    {
        lock (_lock)
        {
            var predicate = _strategy.Get(key);
            foreach (var shard in _shards)
            {
                if (predicate(shard)) return shard.Id;
            }
        }
        throw new InvalidOperationException("shard not found");
    }

    public long[] GetAll()
    {
        lock (_lock)
        {
            return _shards.Select(s => s.Id).ToArray();
        }
    }

    public string Leader(long shardId) => _leader;

    public bool Exists(long shardId)
    {
        lock (_lock)
        {
            return _shards.Any(s => s.Id == shardId);
        }
    }

    public long[]? GetSuccessors(long shardId)
    {
        lock (_lock)
        {
            if (!_successors.TryGetValue(shardId, out var shards)) return null;
            return shards.Select(s => s.Id).ToArray();
        }
    }

    public bool GetSuccessor(long shardId, string key, out long successorShardId)
    {
        lock (_lock)
        {
            var predicate = _strategy.Get(key);
            if (_successors.TryGetValue(shardId, out var shards))
            {
                foreach (var shard in shards)
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

    public Task Changed()
    {
        lock (_lock)
        {
            return _changed.Task;
        }
    }

    public KeySorting KeySorting => KeySorting.Unknown;

    /// <summary>split replaces the parent shard with two shards covering half its hash range each.</summary>
    public void Split(long parent, long left, long right)
    {
        lock (_lock)
        {
            for (int i = 0; i < _shards.Count; i++)
            {
                var shard = _shards[i];
                if (shard.Id != parent) continue;

                var r = shard.HashRange;
                uint mid = r.MinInclusive + (r.MaxInclusive - r.MinInclusive) / 2;
                Shard[] children =
                [
                    new Shard(left, "", new HashRange(r.MinInclusive, mid)),
                    new Shard(right, "", new HashRange(mid + 1, r.MaxInclusive)),
                ];
                _shards.RemoveAt(i);
                _shards.AddRange(children);
                _successors[parent] = children;
                OnShardsReplaced(new Dictionary<long, long[]> { [parent] = [left, right] });
                var old = _changed;
                _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                old.TrySetResult();
                return;
            }
        }
    }
}

/// <summary>notificationsTestShardManager: only the members the notifications / sessions code uses.</summary>
internal sealed class NotificationsTestShardManager : IShardManager
{
    private readonly object _lock = new();
    private long[] _shards = [];
    private Dictionary<long, long[]>? _successors;
    private TaskCompletionSource? _changed;

    public long[] Shards
    {
        get { lock (_lock) return (long[])_shards.Clone(); }
        set { lock (_lock) _shards = value; }
    }

    public Dictionary<long, long[]>? SuccessorsMap
    {
        get { lock (_lock) return _successors; }
        set { lock (_lock) _successors = value; }
    }

    public void Dispose() { }

    public long Get(string key) => throw new NotImplementedException();
    public long[] GetAll() => Shards;
    public string Leader(long shardId) => "server-1";

    public bool Exists(long shard)
    {
        lock (_lock) return Array.IndexOf(_shards, shard) >= 0;
    }

    public long[]? GetSuccessors(long shard)
    {
        lock (_lock) return _successors is not null && _successors.TryGetValue(shard, out var s) ? s : null;
    }

    public bool GetSuccessor(long shardId, string key, out long successorShardId) => throw new NotImplementedException();

    public Task Changed()
    {
        lock (_lock)
        {
            _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _changed.Task;
        }
    }

    public KeySorting KeySorting => KeySorting.Unknown;

    /// <summary>split replaces the shard with its successors in the shard map.</summary>
    public void Split(long shard, params long[] successors)
    {
        lock (_lock)
        {
            _shards = _shards.Where(s => s != shard).Concat(successors).ToArray();
            _successors ??= new Dictionary<long, long[]>();
            _successors[shard] = successors;
            _changed?.TrySetResult();
            _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
