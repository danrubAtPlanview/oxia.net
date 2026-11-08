// =====================================================================================================================
// PORT NOTE
//   Upstream : github.com/dgraph-io/ristretto (the `*ristretto.Cache` that oxia/cache.go builds with
//              NumCounters=1_000_000, MaxCost=64MiB, BufferItems=64); API subset used by cache.go:
//              Get, SetWithTTL, Wait, Del, Clear, Close.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A bounded in-memory cache whose capacity is expressed as a total COST (the cache of Oxia records charges the
//              length in bytes of the serialized value; "key not found" markers cost 0 and never expire), with an optional
//              per-entry time-to-live, and the visibility semantics the Oxia cache relies on: after Set returns the entry is
//              readable, Del/Clear remove it for good.
//   Divergences (ristretto has no .NET twin):
//      * Sharded (32) `Dictionary` + intrusive LRU list per shard, one short `lock` per shard: Get on a hit neither
//        allocates nor takes a global lock.  Eviction when the total cost exceeds MaxCost removes the least recently used
//        entry of the shards in round-robin order (an approximate global LRU), never the entry being inserted.
//      * Ristretto buffers Sets and applies them asynchronously, admitting a new item only when its TinyLFU frequency beats
//        the victims' (and may silently drop a Set).  Here Set is applied synchronously and an item that fits (cost <=
//        MaxCost) is always admitted, so `Wait()` is a no-op kept for API parity; there is no frequency sketch.
//      * Ristretto's `NumCounters` (the number of keys whose frequency is tracked) also bounds cost-0 entries in practice;
//        here MaxItems (default = NumCounters = 1_000_000) bounds the number of entries, evicting LRU.
//      * Entries cost-0 / TTL-0 (ristretto: "no expiry") never expire by time.  Expiry is lazy (checked on Get) plus
//        eviction pressure; there is no background sweep.
//      * After Close, Get misses and Set returns false (ristretto: same).
// =====================================================================================================================
using System.Diagnostics.CodeAnalysis;

namespace Oxia.Client.Internal;

/// <summary>A cost-bounded LRU cache with per-entry TTL (see the PORT NOTE for the relation to ristretto).</summary>
internal sealed class CostCache<TValue> where TValue : class
{
    public const int DefaultShardCount = 32;
    public const int DefaultMaxItems = 1_000_000;

    private sealed class Node
    {
        public readonly string Key;
        public TValue Value;
        public long Cost;
        /// <summary>Environment.TickCount64 (ms) after which the entry is expired; 0 = never.</summary>
        public long ExpiresAt;
        public Node? Prev, Next;   // towards the MRU end (Prev) / LRU end (Next)

        public Node(string key, TValue value, long cost, long expiresAt)
        {
            Key = key; Value = value; Cost = cost; ExpiresAt = expiresAt;
        }
    }

    private sealed class Shard
    {
        public readonly object Lock = new();
        public readonly Dictionary<string, Node> Map = new(StringComparer.Ordinal);
        public Node? Head;   // most recently used
        public Node? Tail;   // least recently used

        public void Unlink(Node n)
        {
            if (n.Prev is not null) n.Prev.Next = n.Next; else Head = n.Next;
            if (n.Next is not null) n.Next.Prev = n.Prev; else Tail = n.Prev;
            n.Prev = n.Next = null;
        }

        public void PushFront(Node n)
        {
            n.Prev = null;
            n.Next = Head;
            if (Head is not null) Head.Prev = n;
            Head = n;
            Tail ??= n;
        }

        public void MoveToFront(Node n)
        {
            if (ReferenceEquals(Head, n)) return;
            Unlink(n);
            PushFront(n);
        }
    }

    private readonly Shard[] _shards;
    private readonly long _maxCost;
    private readonly int _maxItems;
    private long _totalCost;
    private int _count;
    private uint _evictCursor;
    private int _closed;

    public CostCache(long maxCost, int maxItems = DefaultMaxItems, int shardCount = DefaultShardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCost);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxItems);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        _maxCost = maxCost;
        _maxItems = maxItems;
        _shards = new Shard[shardCount];
        for (int i = 0; i < _shards.Length; i++) _shards[i] = new Shard();
    }

    /// <summary>Total cost of the entries currently stored.</summary>
    public long UsedCost => Volatile.Read(ref _totalCost);

    /// <summary>Number of entries currently stored (expired entries not yet removed included).</summary>
    public int Count => Volatile.Read(ref _count);

    public long MaxCost => _maxCost;

    private Shard ShardFor(string key) => _shards[(uint)key.GetHashCode() % (uint)_shards.Length];

    /// <summary>Ristretto <c>Get</c>.  Allocation-free.</summary>
    public bool TryGet(string key, [MaybeNullWhen(false)] out TValue value)
    {
        if (Volatile.Read(ref _closed) != 0) { value = null; return false; }
        var shard = ShardFor(key);
        lock (shard.Lock)
        {
            if (shard.Map.TryGetValue(key, out var node))
            {
                if (node.ExpiresAt == 0 || Environment.TickCount64 < node.ExpiresAt)
                {
                    shard.MoveToFront(node);
                    value = node.Value;
                    return true;
                }
                RemoveLocked(shard, node);   // expired
            }
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Ristretto <c>SetWithTTL</c>: stores <paramref name="value"/> under the key with the given cost; a TTL of zero (or less)
    /// means the entry never expires.  Returns false (stored nothing) when the cache is closed or the cost exceeds the
    /// maximum cost.  When it returns true the entry is readable.
    /// </summary>
    public bool Set(string key, TValue value, long cost, TimeSpan ttl)
    {
        if (Volatile.Read(ref _closed) != 0) return false;
        if (cost < 0) cost = 0;
        if (cost > _maxCost) return false;

        long expiresAt = ttl > TimeSpan.Zero ? Environment.TickCount64 + Math.Max(1, (long)ttl.TotalMilliseconds) : 0;
        var shard = ShardFor(key);
        long delta;
        int added = 0;
        lock (shard.Lock)
        {
            if (shard.Map.TryGetValue(key, out var node))
            {
                delta = cost - node.Cost;
                node.Value = value;
                node.Cost = cost;
                node.ExpiresAt = expiresAt;
                shard.MoveToFront(node);
            }
            else
            {
                node = new Node(key, value, cost, expiresAt);
                shard.Map.Add(key, node);
                shard.PushFront(node);
                delta = cost;
                added = 1;
            }
        }
        if (added != 0) Interlocked.Increment(ref _count);
        long total = delta == 0 ? Volatile.Read(ref _totalCost) : Interlocked.Add(ref _totalCost, delta);
        if (total > _maxCost || Volatile.Read(ref _count) > _maxItems) EvictUntilFits(key);

        // A concurrent Close may have raced with the insertion: leave nothing behind.
        if (Volatile.Read(ref _closed) != 0) Clear();
        return true;
    }

    /// <summary>Ristretto <c>Del</c>.</summary>
    public void Del(string key)
    {
        var shard = ShardFor(key);
        lock (shard.Lock)
        {
            if (shard.Map.TryGetValue(key, out var node)) RemoveLocked(shard, node);
        }
    }

    /// <summary>Ristretto <c>Clear</c>: removes every entry.</summary>
    public void Clear()
    {
        foreach (var shard in _shards)
        {
            long cost = 0;
            int n = 0;
            lock (shard.Lock)
            {
                foreach (var node in shard.Map.Values) { cost += node.Cost; n++; }
                shard.Map.Clear();
                shard.Head = shard.Tail = null;
            }
            if (n != 0)
            {
                Interlocked.Add(ref _totalCost, -cost);
                Interlocked.Add(ref _count, -n);
            }
        }
    }

    /// <summary>Ristretto <c>Wait</c>: blocks until the buffered Sets are applied.  Sets are applied synchronously here.</summary>
    public void Wait() { }

    /// <summary>Ristretto <c>Close</c>: drops every entry; later Gets miss and later Sets are rejected.</summary>
    public void Close()
    {
        Interlocked.Exchange(ref _closed, 1);
        Clear();
    }

    // Must be called with the shard lock held.
    private void RemoveLocked(Shard shard, Node node)
    {
        shard.Map.Remove(node.Key);
        shard.Unlink(node);
        Interlocked.Add(ref _totalCost, -node.Cost);
        Interlocked.Decrement(ref _count);
    }

    /// <summary>Evicts least recently used entries, shard after shard, until the cache fits; never evicts <paramref name="protectedKey"/>.</summary>
    private void EvictUntilFits(string protectedKey)
    {
        int emptyRounds = 0;
        while (Volatile.Read(ref _totalCost) > _maxCost || Volatile.Read(ref _count) > _maxItems)
        {
            bool evicted = false;
            uint start = Interlocked.Increment(ref _evictCursor);
            for (int i = 0; i < _shards.Length; i++)
            {
                var shard = _shards[(start + (uint)i) % (uint)_shards.Length];
                lock (shard.Lock)
                {
                    var victim = shard.Tail;
                    if (victim is not null && string.Equals(victim.Key, protectedKey, StringComparison.Ordinal))
                        victim = victim.Prev;
                    if (victim is null) continue;
                    RemoveLocked(shard, victim);
                    evicted = true;
                }
                if (Volatile.Read(ref _totalCost) <= _maxCost && Volatile.Read(ref _count) <= _maxItems) return;
            }
            if (!evicted && ++emptyRounds >= 2) return;   // only the protected entry is left
        }
    }
}
