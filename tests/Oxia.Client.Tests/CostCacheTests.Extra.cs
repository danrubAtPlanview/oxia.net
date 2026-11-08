// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : none.  CostCache replaces github.com/dgraph-io/ristretto, whose behaviour the Oxia cache relies on; these tests
//              pin the semantics oxia/cache.go needs from it (see Internal/CostCache.cs): visibility after Set (+Wait), Del,
//              Clear, Close, TTL, cost 0 / no TTL entries, the 64 MiB-style cost bound and the never-evict-the-new-entry rule.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using Oxia.Client.Internal;
using Xunit;

namespace Oxia.Client.Tests;

public class CostCacheTests
{
    private sealed class Value
    {
        public readonly int Id;
        public Value(int id) => Id = id;
    }

    [Fact]
    public void SetThenWaitMakesTheEntryReadable()
    {
        var cache = new CostCache<Value>(1024);
        var v = new Value(1);

        Assert.True(cache.Set("k", v, 10, TimeSpan.FromMinutes(5)));
        cache.Wait();

        Assert.True(cache.TryGet("k", out var got));
        Assert.Same(v, got);
        Assert.Equal(10, cache.UsedCost);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void MissingKeyIsAMiss()
    {
        var cache = new CostCache<Value>(1024);
        Assert.False(cache.TryGet("nope", out var got));
        Assert.Null(got);
    }

    [Fact]
    public void OverwriteReplacesTheValueAndTheCost()
    {
        var cache = new CostCache<Value>(1024);
        cache.Set("k", new Value(1), 10, TimeSpan.Zero);
        var v2 = new Value(2);
        cache.Set("k", v2, 30, TimeSpan.Zero);

        Assert.True(cache.TryGet("k", out var got));
        Assert.Same(v2, got);
        Assert.Equal(30, cache.UsedCost);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void DelRemovesTheEntryAndItsCost()
    {
        var cache = new CostCache<Value>(1024);
        cache.Set("a", new Value(1), 10, TimeSpan.Zero);
        cache.Set("b", new Value(2), 20, TimeSpan.Zero);

        cache.Del("a");
        cache.Del("missing");   // no-op

        Assert.False(cache.TryGet("a", out _));
        Assert.True(cache.TryGet("b", out _));
        Assert.Equal(20, cache.UsedCost);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void ClearRemovesEverything()
    {
        var cache = new CostCache<Value>(1024);
        for (int i = 0; i < 100; i++) cache.Set($"k{i}", new Value(i), 5, TimeSpan.Zero);
        Assert.Equal(500, cache.UsedCost);

        cache.Clear();

        Assert.Equal(0, cache.UsedCost);
        Assert.Equal(0, cache.Count);
        for (int i = 0; i < 100; i++) Assert.False(cache.TryGet($"k{i}", out _));
    }

    [Fact]
    public void CloseDropsTheEntriesAndRejectsLaterSets()
    {
        var cache = new CostCache<Value>(1024);
        cache.Set("k", new Value(1), 10, TimeSpan.Zero);

        cache.Close();

        Assert.False(cache.TryGet("k", out _));
        Assert.False(cache.Set("k", new Value(2), 10, TimeSpan.Zero));
        Assert.False(cache.TryGet("k", out _));
        Assert.Equal(0, cache.UsedCost);
        cache.Close();   // idempotent
    }

    [Fact]
    public async Task EntriesExpireAfterTheirTtl()
    {
        var cache = new CostCache<Value>(1024);
        cache.Set("short", new Value(1), 10, TimeSpan.FromMilliseconds(80));
        cache.Set("forever", new Value(2), 10, TimeSpan.Zero);

        Assert.True(cache.TryGet("short", out _));
        await Task.Delay(250);

        Assert.False(cache.TryGet("short", out _));
        Assert.True(cache.TryGet("forever", out _));
        Assert.Equal(10, cache.UsedCost);   // the expired entry released its cost
    }

    [Fact]
    public void CostZeroEntriesDoNotCountAgainstTheBound()
    {
        var cache = new CostCache<Value>(100);
        // "key not found" markers of the Oxia cache: cost 0, no TTL
        for (int i = 0; i < 50; i++) Assert.True(cache.Set($"absent-{i}", new Value(i), 0, TimeSpan.Zero));

        Assert.Equal(0, cache.UsedCost);
        Assert.Equal(50, cache.Count);
        for (int i = 0; i < 50; i++) Assert.True(cache.TryGet($"absent-{i}", out _));
    }

    [Fact]
    public void AnEntryCostlierThanTheWholeCacheIsRejected()
    {
        var cache = new CostCache<Value>(100);
        cache.Set("small", new Value(1), 10, TimeSpan.Zero);

        Assert.False(cache.Set("huge", new Value(2), 101, TimeSpan.Zero));

        Assert.False(cache.TryGet("huge", out _));
        Assert.True(cache.TryGet("small", out _));
        Assert.True(cache.Set("exact", new Value(3), 100, TimeSpan.Zero));   // exactly the max cost fits (evicting the rest)
        Assert.True(cache.TryGet("exact", out _));
        Assert.True(cache.UsedCost <= 100);
    }

    [Fact]
    public void TheTotalCostStaysWithinTheBoundAndTheNewEntryIsAlwaysReadable()
    {
        var cache = new CostCache<Value>(1000);
        for (int i = 0; i < 5_000; i++)
        {
            var v = new Value(i);
            Assert.True(cache.Set($"key-{i}", v, 7 + i % 13, TimeSpan.Zero));
            Assert.True(cache.TryGet($"key-{i}", out var got), $"the entry just set is not readable (i={i})");   // never evicts itself
            Assert.Same(v, got);
            Assert.True(cache.UsedCost <= 1000, $"cost {cache.UsedCost} over the bound (i={i})");
        }
        Assert.True(cache.Count > 0);
    }

    [Fact]
    public void TheNumberOfEntriesIsBounded()
    {
        var cache = new CostCache<Value>(long.MaxValue / 2, maxItems: 100);
        for (int i = 0; i < 1_000; i++) cache.Set($"key-{i}", new Value(i), 0, TimeSpan.Zero);

        Assert.True(cache.Count <= 100, $"count {cache.Count}");
        Assert.True(cache.TryGet("key-999", out _));
    }

    [Fact]
    public void ARecentlyReadEntryOutlivesTheColdOnesInItsShard()
    {
        // One shard: the eviction order is exactly LRU
        var cache = new CostCache<Value>(100, shardCount: 1);
        for (int i = 0; i < 10; i++) cache.Set($"k{i}", new Value(i), 10, TimeSpan.Zero);

        Assert.True(cache.TryGet("k0", out _));            // k0 becomes the most recently used
        cache.Set("new", new Value(100), 10, TimeSpan.Zero);   // evicts the least recently used: k1

        Assert.True(cache.TryGet("k0", out _));
        Assert.False(cache.TryGet("k1", out _));
        Assert.True(cache.TryGet("k2", out _));
        Assert.True(cache.TryGet("new", out _));
        Assert.Equal(100, cache.UsedCost);
    }

    [Fact]
    public async Task ConcurrentSetGetDelKeepTheAccountingConsistent()
    {
        var cache = new CostCache<Value>(10_000, maxItems: 5_000);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var tasks = new List<Task>();
        for (int t = 0; t < 8; t++)
        {
            int seed = t;
            tasks.Add(Task.Run(() =>
            {
                var rnd = new Random(seed);
                while (!cts.IsCancellationRequested)
                {
                    string key = "k" + rnd.Next(2_000);
                    switch (rnd.Next(5))
                    {
                        case 0:
                        case 1:
                            cache.Set(key, new Value(1), rnd.Next(0, 50), rnd.Next(4) == 0 ? TimeSpan.FromMilliseconds(rnd.Next(1, 20)) : TimeSpan.Zero);
                            break;
                        case 2:
                            cache.Del(key);
                            break;
                        case 3:
                            if (rnd.Next(500) == 0) cache.Clear();
                            break;
                        default:
                            cache.TryGet(key, out _);
                            break;
                    }
                    Assert.True(cache.UsedCost >= 0);
                }
            }));
        }
        await Task.WhenAll(tasks);

        Assert.True(cache.UsedCost <= 10_000, $"cost {cache.UsedCost}");   // concurrent sets may overshoot briefly, never persistently
        for (int i = 0; i < 2_000; i++) cache.Del("k" + i);
        Assert.Equal(0, cache.UsedCost);
        Assert.Equal(0, cache.Count);
    }
}
