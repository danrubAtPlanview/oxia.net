// =====================================================================================================================
// PORT NOTE (benchmark)
//   Upstream : oxia/internal/shard_manager.go  (Get: RLock + hash closure + linear scan of a map)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : ShardManager.Get(key) is the hottest read path of the client (every put/get/delete routes its key).  The port
//              keeps an immutable snapshot (shards sorted by hash range) swapped with Volatile.Write and binary-searches it with a
//              hash computed straight from the string -- no lock, no closure, no allocation.
//   Benchmarks: * Get                -- the port: snapshot + binary search (ShardStrategy fast path)
//               * GetViaPredicate    -- the Go algorithm through the predicate API (what a custom strategy gets): one closure
//                                       allocation per call + a linear scan of the shards
//               * HashOnly           -- the cost of the xxh3 hash of the key alone (lower bound of Get)
//               * GetAllCached       -- GetAll() returns a cached array (no allocation)
//              Shard counts 1 / 16 / 256 over the whole uint32 hash space; 1024 keys of ~24 chars (OperationsPerInvoke = 1024).
//   Allocation budget: Get = 0 B/op, GetAll = 0 B/op.
// =====================================================================================================================
using BenchmarkDotNet.Attributes;
using Oxia.Client.Internal;
using Oxia.Common.Hash;

namespace Oxia.Client.Benchmarks.Internal;

[MemoryDiagnoser]
public class ShardManagerBenchmarks
{
    private const int KeyCount = 1024;

    private ShardManager _fast = null!;
    private ShardManager _predicate = null!;
    private string[] _keys = null!;

    [Params(1, 16, 256)]
    public int Shards { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var shards = new Shard[Shards];
        ulong width = (1UL << 32) / (ulong)Shards;
        for (int i = 0; i < Shards; i++)
        {
            uint min = (uint)((ulong)i * width);
            uint max = i == Shards - 1 ? uint.MaxValue : (uint)(((ulong)(i + 1) * width) - 1);
            shards[i] = new Shard(i, $"leader-{i}:6648", new HashRange(min, max));
        }

        _fast = new ShardManager(ShardStrategy.New(), null, "service:6648", "default", TimeSpan.FromSeconds(1), null);
        _fast.Update(shards);
        _predicate = new ShardManager(new PredicateOnlyStrategy(ShardStrategy.New()), null, "service:6648", "default",
            TimeSpan.FromSeconds(1), null);
        _predicate.Update(shards);

        _keys = new string[KeyCount];
        for (int i = 0; i < KeyCount; i++) _keys[i] = $"/benchmark/partition-{i % 17}/key-{i:D8}";
    }

    [Benchmark(OperationsPerInvoke = KeyCount)]
    public long Get()
    {
        long sum = 0;
        var sm = _fast;
        foreach (string key in _keys) sum += sm.Get(key);
        return sum;
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = KeyCount)]
    public long GetViaPredicate()
    {
        long sum = 0;
        var sm = _predicate;
        foreach (string key in _keys) sum += sm.Get(key);
        return sum;
    }

    [Benchmark(OperationsPerInvoke = KeyCount)]
    public uint HashOnly()
    {
        uint sum = 0;
        foreach (string key in _keys) sum += Hasher.Xxh332(key);
        return sum;
    }

    [Benchmark]
    public int GetAllCached() => _fast.GetAll().Length;

    /// <summary>Hides the ShardStrategy type so that the shard manager has to use the predicate API (as for a custom strategy).</summary>
    private sealed class PredicateOnlyStrategy(IShardStrategy inner) : IShardStrategy
    {
        public Func<Shard, bool> Get(string key) => inner.Get(key);
    }
}
