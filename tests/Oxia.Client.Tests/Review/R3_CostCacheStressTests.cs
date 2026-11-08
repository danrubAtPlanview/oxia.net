// =====================================================================================================================
// REVIEW R3 -- stress test of Internal/CostCache.cs (the ristretto replacement): concurrent Set / TryGet / Del / Clear / Close
// with short TTLs and eviction pressure must leave the accounting (UsedCost, Count) equal to what is really stored, never
// above MaxCost / MaxItems.  Go reference: ristretto's contract used by oxia/cache.go (Get, SetWithTTL, Wait, Del, Clear, Close).
// (This test PASSES: it is a guard for a component found faithful.)
// =====================================================================================================================
using Oxia.Client.Internal;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R3_CostCacheStressTests
{
    private sealed class Entry
    {
        public readonly long Cost;
        public Entry(long cost) => Cost = cost;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Accounting_StaysConsistent_UnderConcurrentUse(int seed)
    {
        const int keys = 2000;
        const long maxCost = 50_000;
        var cache = new CostCache<Entry>(maxCost, maxItems: 1500);
        var names = new string[keys];
        var costs = new long[keys];
        var rnd0 = new Random(seed);
        for (int i = 0; i < keys; i++) { names[i] = "k" + i; costs[i] = rnd0.Next(0, 200); }

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var workers = new List<Task>();
        for (int t = 0; t < 8; t++)
        {
            int tt = t;
            workers.Add(Task.Run(() =>
            {
                var rnd = new Random(seed * 100 + tt);
                while (!stop.IsCancellationRequested)
                {
                    int k = rnd.Next(keys);
                    switch (rnd.Next(10))
                    {
                        case 0: cache.Del(names[k]); break;
                        case 1 when rnd.Next(200) == 0: cache.Clear(); break;
                        case 2 or 3 or 4:
                            cache.TryGet(names[k], out _);
                            break;
                        default:
                            cache.Set(names[k], new Entry(costs[k]), costs[k], rnd.Next(4) == 0 ? TimeSpan.FromMilliseconds(rnd.Next(1, 20)) : TimeSpan.Zero);
                            break;
                    }
                    Assert.True(cache.UsedCost >= -1_000_000);   // transiently inconsistent reads are fine, but never wildly off
                }
            }));
        }
        await Task.WhenAll(workers);

        // Quiescent: what is readable must account for UsedCost / Count exactly (entries with a TTL may expire while we look:
        // wait them out first).
        await Task.Delay(50);
        long sum = 0;
        int count = 0;
        for (int i = 0; i < keys; i++)
        {
            if (cache.TryGet(names[i], out var e)) { sum += e.Cost; count++; }
        }
        Assert.Equal(count, cache.Count);
        Assert.Equal(sum, cache.UsedCost);
        Assert.True(cache.UsedCost <= maxCost, "UsedCost " + cache.UsedCost + " > " + maxCost);
        Assert.True(cache.Count <= 1500, "Count " + cache.Count);

        cache.Close();
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.UsedCost);
        Assert.False(cache.TryGet(names[0], out _));
        Assert.False(cache.Set(names[0], new Entry(1), 1, TimeSpan.Zero));
    }
}
