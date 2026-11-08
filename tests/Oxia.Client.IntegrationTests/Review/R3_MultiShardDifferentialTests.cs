// =====================================================================================================================
// PORT NOTE (independent-review regression/differential test; no Go test counterpart)
//   Upstream : oxia/async_client_impl.go (doMultiShardGet/selectResponse, List, RangeScan merge), oxia/key_order.go, oxia/results_heap.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Differential test: a 1-shard server is the oracle for a 4-shard server holding the same random multi-level / non-ASCII / astral keys; compares scans, lists, secondary-index scans, Floor/Ceiling/Lower/Higher/Equal gets (also through an index) and DeleteRange across shards. Verified faithful by review R3; kept as a regression guard.
//   See      : docs/review-findings.md
// =====================================================================================================================
// =====================================================================================================================
// REVIEW R3 -- differential (property) test against the real server.
//   Oracle   : a 1-shard server orders keys / secondary keys / floor-ceiling answers itself (no client merge involved).  A 4-shard
//              server holding the SAME records must give the same answers through the client's merge (KeyOrder, ResultHeap,
//              selectResponse, CompareGetResponse).  Go: oxia/key_order.go, results_heap.go, async_client_impl.go
//              (aggregateAndSortRangeScanAcrossShards, selectResponse, doMultiShardGet).  C#: KeyOrder.cs, ResultsHeap.cs,
//              AsyncClientImpl.cs.  The Go integration tests only use 5-8 hand-picked keys; this one uses random keys with
//              multi-level paths, double/trailing slashes, non-ASCII and astral characters, under both key sortings.
// =====================================================================================================================
using System.Text;
using Oxia.Client;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.IntegrationTests.Review;

public class R3_MultiShardDifferentialTests(OxiaServerFixture oxia)
{
    private static readonly string[] Atoms = ["a", "b", "ab", "A", "Z", "0", "9", "-", "_", ".", "é", "中", "\U0001F600", "x/", "/", "//", "a/b", "b/a"];

    // Secondary keys: no trailing "//" and no leading/trailing '/': the server stores them inside the composed internal key
    // "__oxia/idx/<index>/<secondary><primary>", whose "levels" are not those of the secondary key alone when it ends
    // with "//" -- a corner where the Go client itself disagrees with the Go server (not a port difference).
    private static readonly string[] SecondaryAtoms = ["a", "b", "ab", "A", "Z", "0", "9", "-", "_", ".", "é", "中", "😀", "a/b", "b/a"];

    private static string RandomKey(Random rnd, string[] atomSet)
    {
        var sb = new StringBuilder();
        int atoms = rnd.Next(1, 6);
        for (int i = 0; i < atoms; i++) sb.Append(atomSet[rnd.Next(atomSet.Length)]);
        return sb.ToString();
    }

    private static List<string> RandomKeys(Random rnd, int count, bool secondary = false)
    {
        var atomSet = secondary ? SecondaryAtoms : Atoms;
        var set = new HashSet<string>(StringComparer.Ordinal);
        while (set.Count < count) set.Add(RandomKey(rnd, atomSet));
        return [.. set];
    }

    private static async Task<List<string>> Scan(IAsyncClient c, params RangeScanOption[] options)
    {
        var keys = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await foreach (var r in c.RangeScanAsync(cts.Token, "", "", options).ReadAllAsync(cts.Token))
        {
            Assert.Null(r.Err);
            keys.Add(r.Key);
        }
        return keys;
    }

    private static async Task<List<string>> ListAll(IAsyncClient c)
    {
        var keys = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await foreach (var r in c.ListAsync(cts.Token, "", "").ReadAllAsync(cts.Token))
        {
            Assert.Null(r.Err);
            keys.AddRange(r.Keys!);
        }
        return keys;
    }

    private static async Task<string> GetKey(IAsyncClient c, string probe, params GetOption[] options)
    {
        var r = await c.GetAsync(probe, options);
        if (r.Err is null) return r.Key;
        Assert.True(OxiaErrors.Is(r.Err, ClientErrors.KeyNotFound), "unexpected error " + r.Err);
        return "<not found>";
    }

    public static IEnumerable<object[]> Cases()
    {
        for (int seed = 1; seed <= 8; seed++)
        {
            yield return [ServerKeySorting.Hierarchical, seed];
            yield return [ServerKeySorting.Natural, seed + 100];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task FourShardsGiveTheSameAnswersAsOneShard(ServerKeySorting sorting, int seed)
    {
        var rnd = new Random(seed);
        var keys = RandomKeys(rnd, 150);

        // unique secondary keys (a random permutation of the keys' own space) and colliding ones
        var uniqueSecondary = RandomKeys(rnd, keys.Count, secondary: true);
        var dupSecondary = new List<string>();
        for (int i = 0; i < keys.Count; i++) dupSecondary.Add("s" + SecondaryAtoms[rnd.Next(4)]);

        await using var one = await oxia.StartAsync(OxiaServerProfile.WithShards(1, sorting));
        await using var four = await oxia.StartAsync(OxiaServerProfile.WithShards(4, sorting));
        var c1 = await OxiaClient.CreateAsyncClientAsync(one.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        var c4 = await OxiaClient.CreateAsyncClientAsync(four.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        try
        {
            foreach (var c in new[] { c1, c4 })
            {
                var puts = new List<Task<PutResult>>();
                for (int i = 0; i < keys.Count; i++)
                {
                    puts.Add(c.PutAsync(keys[i], Encoding.UTF8.GetBytes(keys[i]),
                        OxiaOptions.SecondaryIndex("idx", uniqueSecondary[i]), OxiaOptions.SecondaryIndex("dup", dupSecondary[i])).AsTask());
                }
                foreach (var p in await Task.WhenAll(puts)) Assert.Null(p.Err);
            }

            for (int round = 0; round < 2; round++)
            {
                // 1) merged order of the primary keys
                var scan1 = await Scan(c1);
                var scan4 = await Scan(c4);
                Assert.Equal(scan1, scan4);
                if (round == 0) Assert.Equal(keys.Count, scan4.Count);
                AssertSetEqual(await ListAll(c1), await ListAll(c4));

                // 2) merged order through the secondary indexes (unique, and with ties)
                Assert.Equal(await Scan(c1, OxiaOptions.UseIndex("idx")), await Scan(c4, OxiaOptions.UseIndex("idx")));
                Assert.Equal(await Scan(c1, OxiaOptions.UseIndex("dup")), await Scan(c4, OxiaOptions.UseIndex("dup")));

                // 3) floor / ceiling / lower / higher / equal
                var probes = new List<string>(RandomKeys(rnd, 40));
                for (int i = 0; i < 20; i++) probes.Add(keys[rnd.Next(keys.Count)]);
                var secondaryProbes = new List<string>(RandomKeys(rnd, 40, secondary: true));
                for (int i = 0; i < 20; i++) secondaryProbes.Add(uniqueSecondary[rnd.Next(uniqueSecondary.Count)]);
                foreach (var probe in probes)
                {
                    foreach (var (name, option) in new (string, GetOption)[]
                    {
                        ("floor", OxiaOptions.ComparisonFloor()), ("ceiling", OxiaOptions.ComparisonCeiling()),
                        ("lower", OxiaOptions.ComparisonLower()), ("higher", OxiaOptions.ComparisonHigher()),
                        ("equal", OxiaOptions.ComparisonEqual()),
                    })
                    {
                        Assert.True(await GetKey(c1, probe, option) == await GetKey(c4, probe, option),
                            $"{name}({probe}) differs: 1 shard={await GetKey(c1, probe, option)} 4 shards={await GetKey(c4, probe, option)}");
                    }
                }

                foreach (var probe in secondaryProbes)
                {
                    // 4) the same through the unique index: the probe is a secondary key
                    foreach (var (name, option) in new (string, GetOption)[]
                    {
                        ("idx-floor", OxiaOptions.ComparisonFloor()), ("idx-ceiling", OxiaOptions.ComparisonCeiling()),
                        ("idx-lower", OxiaOptions.ComparisonLower()), ("idx-higher", OxiaOptions.ComparisonHigher()),
                    })
                    {
                        GetOption[] opts = [option, OxiaOptions.UseIndex("idx")];
                        Assert.True(await GetKey(c1, probe, opts) == await GetKey(c4, probe, opts),
                            $"{name}({probe}) differs: 1 shard={await GetKey(c1, probe, opts)} 4 shards={await GetKey(c4, probe, opts)}");
                    }
                }

                // 5) a range delete across the shards, then compare again
                var bounds = RandomKeys(rnd, 2);
                bounds.Sort(StringComparer.Ordinal);
                var d1 = await c1.DeleteRangeAsync(bounds[0], bounds[1]);
                var d4 = await c4.DeleteRangeAsync(bounds[0], bounds[1]);
                Assert.True((d1 is null) == (d4 is null), $"delete range [{bounds[0]}, {bounds[1]}) 1 shard: {d1?.Message}; 4 shards: {d4?.Message}");
            }
        }
        finally
        {
            await c1.CloseAsync();
            await c4.CloseAsync();
        }
    }

    private static void AssertSetEqual(List<string> a, List<string> b) =>
        Assert.Equal(a.Order(StringComparer.Ordinal).ToList(), b.Order(StringComparer.Ordinal).ToList());
}
