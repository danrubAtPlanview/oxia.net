// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/key_order_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 3 functions (TestKeyOrder; TestAggregateAndSortRangeScanAcrossShardsKeySorting with 3 t.Run cases;
//             TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys with 5 t.Run cases), ported: 3 functions as 9 facts
//             (1 + 3 + 5), skipped: 0
//   Adaptations:
//     * `t.Run(name, ...)` cases are separate facts named `<Parent>_<name>` (proto enum String() for the first group:
//       KEY_SORTING_NATURAL / KEY_SORTING_HIERARCHICAL / KEY_SORTING_UNKNOWN; the literal case names for the second).
//     * Go's `aggregateAndSortRangeScanAcrossShards(order, channels, outCh)` (async_client_impl.go) is
//       RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync, the merge loop that lives next to the results heap;
//       `chan rangeScanResult` filled and closed up front is an unbounded Channel completed after the writes; outCh is a
//       Channel<GetResult> bounded at 100 like `make(chan GetResult, 100)`.
//     * Go's `new(r.secondaryKey)` (a non-nil pointer, even to "") is a non-null string.
//     * assert.Equal's formatted message ("%s: %q vs %q") is attached by hand.
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests;

public class KeyOrderTests
{
    private static string KeySortingName(KeySorting ks) => ks switch
    {
        KeySorting.Natural => "KEY_SORTING_NATURAL",
        KeySorting.Hierarchical => "KEY_SORTING_HIERARCHICAL",
        _ => "KEY_SORTING_UNKNOWN",
    };

    private static void AssertCompare(int expected, int actual, KeySorting keySorting, string a, string b)
    {
        Assert.True(expected == actual, $"{KeySortingName(keySorting)}: \"{a}\" vs \"{b}\": expected {expected}, got {actual}");
    }

    [Fact]
    public void TestKeyOrder()
    {
        foreach (var (a, b, natural, hierarchical, unknown) in new (string, string, int, int, int)[]
                 {
                     ("a", "a", 0, 0, 0),
                     ("a0", "a/y/z", +1, -1, -1),
                     ("a/x", "ab/y", -1, +1, -1),
                     ("b/x", "a/y/z", +1, -1, +1),
                     ("a/z", "a/y/z", +1, -1, -1),
                     // The internal keys sort after the other keys
                     ("__oxia/x", "z/z", +1, +1, -1),
                 })
        {
            foreach (var (keySorting, expected) in new (KeySorting, int)[]
                     {
                         (KeySorting.Natural, natural),
                         (KeySorting.Hierarchical, hierarchical),
                         (KeySorting.Unknown, unknown),
                     })
            {
                var order = KeyOrder.New(keySorting);
                AssertCompare(expected, order.Compare(a, b), keySorting, a, b);
                AssertCompare(-expected, order.Compare(b, a), keySorting, b, a);
            }
        }
    }

    // A range scan without a partition key merges the records of the shards, which each shard returns in the order of its keys.
    private static async Task AggregateAndSortRangeScanAcrossShardsKeySorting(KeySorting keySorting, string[][] shards, string[] expected)
    {
        var channels = new ChannelReader<RangeScanResult>[shards.Length];
        for (int i = 0; i < shards.Length; i++)
        {
            var ch = Channel.CreateUnbounded<RangeScanResult>();
            foreach (var key in shards[i])
            {
                Assert.True(ch.Writer.TryWrite(new RangeScanResult(new GetResult(key, null, default, null), null)));
            }

            ch.Writer.Complete();
            channels[i] = ch.Reader;
        }

        var outCh = Channel.CreateBounded<GetResult>(100);
        await RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(keySorting), channels, outCh.Writer);

        var merged = new List<string>();
        await foreach (var result in outCh.Reader.ReadAllAsync())
        {
            merged.Add(result.Key);
        }

        Assert.Equal(expected, merged);
    }

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsKeySorting_KEY_SORTING_NATURAL() =>
        AggregateAndSortRangeScanAcrossShardsKeySorting(KeySorting.Natural,
            [["a/x", "a0"], ["a/y/z", "ab/y"]],
            ["a/x", "a/y/z", "a0", "ab/y"]);

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsKeySorting_KEY_SORTING_HIERARCHICAL() =>
        AggregateAndSortRangeScanAcrossShardsKeySorting(KeySorting.Hierarchical,
            [["a0", "a/x"], ["ab/y", "a/y/z"]],
            ["a0", "ab/y", "a/x", "a/y/z"]);

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsKeySorting_KEY_SORTING_UNKNOWN() =>
        AggregateAndSortRangeScanAcrossShardsKeySorting(KeySorting.Unknown,
            [["a0", "a/x"], ["a/y/z", "ab/y"]],
            ["a0", "a/x", "a/y/z", "ab/y"]);

    // A range scan with an index merges the records of the shards, which each shard returns in the order of their secondary keys.
    private static async Task AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting keySorting,
        (string Key, string SecondaryKey)[][] shards, string[] expected)
    {
        var channels = new ChannelReader<RangeScanResult>[shards.Length];
        for (int i = 0; i < shards.Length; i++)
        {
            var ch = Channel.CreateUnbounded<RangeScanResult>();
            foreach (var (key, secondaryKey) in shards[i])
            {
                Assert.True(ch.Writer.TryWrite(new RangeScanResult(new GetResult(key, null, default, null), secondaryKey)));
            }

            ch.Writer.Complete();
            channels[i] = ch.Reader;
        }

        var outCh = Channel.CreateBounded<GetResult>(100);
        await RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(keySorting), channels, outCh.Writer);

        var merged = new List<string>();
        await foreach (var result in outCh.Reader.ReadAllAsync())
        {
            merged.Add(result.Key);
        }

        Assert.Equal(expected, merged);
    }

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys_natural() =>
        AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting.Natural,
            [[("p4", "a/x"), ("p2", "a0")], [("p3", "a/y/z"), ("p1", "ab/y")]],
            ["p4", "p3", "p2", "p1"]);

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys_hierarchical() =>
        AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting.Hierarchical,
            [[("p4", "a0"), ("p2", "a/x")], [("p3", "ab/y"), ("p1", "a/y/z")]],
            ["p4", "p3", "p2", "p1"]);

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys_unknown() =>
        AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting.Unknown,
            [[("p4", "a0"), ("p2", "a/x")], [("p3", "a/y/z"), ("p1", "ab/y")]],
            ["p4", "p2", "p3", "p1"]);

    // The records with the same secondary key are ordered by their keys
    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys_same_secondary_key() =>
        AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting.Natural,
            [[("p2", "a"), ("p3", "b")], [("p1", "b"), ("p4", "c")]],
            ["p2", "p1", "p3", "p4"]);

    [Fact]
    public Task TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys_empty_secondary_key() =>
        AggregateAndSortRangeScanAcrossShardsSecondaryKeys(KeySorting.Natural,
            [[("p2", "")], [("p1", "a")]],
            ["p2", "p1"]);
}
