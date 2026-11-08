// =====================================================================================================================
// PORT NOTE   (extra tests -- not in Go)
//   Upstream : oxia/results_heap.go, oxia/key_order.go, aggregateAndSortRangeScanAcrossShards in oxia/async_client_impl.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Property tests of the range-scan merge: with random data and every key sorting, the merge of the sorted
//              per-shard streams equals a reference sort of all the records (with and without secondary keys), the heap
//              keeps its invariants under Push/Pop/Fix/Init, an error result ends the merge, and sources that fail or are
//              cancelled end the output instead of hanging it.  Also: KeyOrder.Compare agrees with CompareSortKeys over
//              SortKey, and is allocation-free for short keys.
// =====================================================================================================================
using System.Text;
using System.Threading.Channels;
using Oxia.Common.Compare;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests;

public class ResultsHeapExtraTests
{
    // ---------------------------------------------------------------------------------------------------------------
    // Reference orderings, written independently of KeyOrder
    // ---------------------------------------------------------------------------------------------------------------

    private static int Sign(int v) => Math.Sign(v);

    private static int Ordinal(byte[] a, byte[] b) => Sign(a.AsSpan().SequenceCompareTo(b));

    private static IComparer<string> ReferenceComparer(KeySorting ks) => Comparer<string>.Create((a, b) => ks switch
    {
        // natural: plain UTF-8 byte order (the test alphabet has no internal keys)
        KeySorting.Natural => Ordinal(Encoding.UTF8.GetBytes(a), Encoding.UTF8.GetBytes(b)),
        // hierarchical: by depth (number of '/' ignoring one of a trailing "//"), then bytes with '/' sorting after everything
        KeySorting.Hierarchical => HierarchicalCompare(a, b),
        // legacy: CompareWithSlash
        _ => Sign(KeyCompare.CompareWithSlash(Encoding.UTF8.GetBytes(a), Encoding.UTF8.GetBytes(b))),
    });

    private static int Depth(string key)
    {
        int n = key.Count(c => c == '/');
        if (n != 0 && key.Length >= 2 && key.EndsWith("//", StringComparison.Ordinal)) n--;
        return n;
    }

    private static int HierarchicalCompare(string a, string b)
    {
        int c = Depth(a).CompareTo(Depth(b));
        if (c != 0) return Sign(c);
        return Ordinal(SlashLast(a), SlashLast(b));
    }

    // UTF-8 bytes with '/' replaced by 0xff, so that the separator sorts after any other character
    private static byte[] SlashLast(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        for (int i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'/') bytes[i] = 0xff;
        }

        return bytes;
    }

    private static readonly string[] SecondaryKeys = ["", "a", "a/x", "a0", "ab/y", "b", "a/y/z", "z/z"];

    private static readonly string[] Alphabet =["a", "b", "ab", "a0", "0", "x", "z", "y"];

    private static string RandomKey(Random rnd)
    {
        int parts = rnd.Next(1, 5);
        var sb = new StringBuilder();
        for (int i = 0; i < parts; i++)
        {
            if (i > 0) sb.Append('/');
            sb.Append(Alphabet[rnd.Next(Alphabet.Length)]);
        }

        return sb.ToString();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Merge
    // ---------------------------------------------------------------------------------------------------------------

    private static async Task<List<GetResult>> MergeAsync(KeySorting ks, IReadOnlyList<List<RangeScanResult>> shards, bool concurrentProducers)
    {
        var channels = new ChannelReader<RangeScanResult>[shards.Count];
        var producers = new List<Task>();
        for (int i = 0; i < shards.Count; i++)
        {
            var records = shards[i];
            // capacity 1 stands for Go's unbuffered channel: the producer runs in step with the merge
            var ch = concurrentProducers ? Channel.CreateBounded<RangeScanResult>(1) : Channel.CreateUnbounded<RangeScanResult>();
            channels[i] = ch.Reader;
            if (concurrentProducers)
            {
                producers.Add(Task.Run(async () =>
                {
                    foreach (var r in records) await ch.Writer.WriteAsync(r);
                    ch.Writer.Complete();
                }));
            }
            else
            {
                foreach (var r in records) Assert.True(ch.Writer.TryWrite(r));
                ch.Writer.Complete();
            }
        }

        var outCh = Channel.CreateBounded<GetResult>(100);
        var merge = RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(ks), channels, outCh.Writer);
        var merged = new List<GetResult>();
        await foreach (var r in outCh.Reader.ReadAllAsync()) merged.Add(r);
        await merge;
        await Task.WhenAll(producers);
        return merged;
    }

    private static RangeScanResult Rec(string key, string? secondary = null) =>
        new(new GetResult(key, null, default, null), secondary);

    [Theory]
    [InlineData(KeySorting.Natural, false)]
    [InlineData(KeySorting.Hierarchical, false)]
    [InlineData(KeySorting.Unknown, false)]
    [InlineData(KeySorting.Natural, true)]
    [InlineData(KeySorting.Hierarchical, true)]
    [InlineData(KeySorting.Unknown, true)]
    public async Task Merge_OfRandomShards_EqualsAReferenceSort(KeySorting ks, bool concurrentProducers)
    {
        var rnd = new Random(12345 + (int)ks);
        var cmp = ReferenceComparer(ks);

        for (int trial = 0; trial < (concurrentProducers ? 60 : 300); trial++)
        {
            int shardCount = rnd.Next(1, 8);
            var shards = new List<List<RangeScanResult>>();
            var all = new List<string>();
            for (int s = 0; s < shardCount; s++)
            {
                var keys = Enumerable.Range(0, rnd.Next(0, 30)).Select(_ => RandomKey(rnd)).ToList();
                keys.Sort(cmp);                        // each shard returns its records in key order
                shards.Add(keys.Select(k => Rec(k)).ToList());
                all.AddRange(keys);
            }

            all.Sort(cmp);
            var merged = await MergeAsync(ks, shards, concurrentProducers);

            // equal keys are indistinguishable, so comparing the key sequences is exact
            Assert.Equal(all, merged.Select(r => r.Key).ToList());
        }
    }

    [Theory]
    [InlineData(KeySorting.Natural)]
    [InlineData(KeySorting.Hierarchical)]
    [InlineData(KeySorting.Unknown)]
    public async Task Merge_OfRandomShardsWithSecondaryKeys_OrdersBySecondaryKeyThenKey(KeySorting ks)
    {
        var rnd = new Random(777 + (int)ks);
        var cmp = ReferenceComparer(ks);
        var tupleCmp = Comparer<(string Key, string Secondary)>.Create((x, y) =>
        {
            int c = cmp.Compare(x.Secondary, y.Secondary);
            return c != 0 ? c : cmp.Compare(x.Key, y.Key);
        });

        for (int trial = 0; trial < 300; trial++)
        {
            int shardCount = rnd.Next(1, 7);
            var shards = new List<List<RangeScanResult>>();
            var all = new List<(string Key, string Secondary)>();
            for (int s = 0; s < shardCount; s++)
            {
                // few distinct secondary keys so that the tie-break by key is exercised a lot (the empty one included)
                var records = Enumerable.Range(0, rnd.Next(0, 25))
                    .Select(_ => (Key: "k" + rnd.Next(0, 50), Secondary: SecondaryKeys[rnd.Next(SecondaryKeys.Length)]))
                    .ToList();
                records.Sort(tupleCmp);                // each shard returns its records in (secondary key, key) order
                shards.Add(records.Select(r => Rec(r.Key, r.Secondary)).ToList());
                all.AddRange(records);
            }

            all.Sort(tupleCmp);
            var merged = await MergeAsync(ks, shards, concurrentProducers: false);
            Assert.Equal(all.Select(r => r.Key).ToList(), merged.Select(r => r.Key).ToList());
        }
    }

    [Fact]
    public async Task Merge_WithoutSecondaryKeyOnOneSide_FallsBackToTheKeys()
    {
        // Less() compares the secondary keys only when BOTH results have one (older servers send none)
        var merged = await MergeAsync(KeySorting.Natural, [[Rec("b", "z")], [Rec("a")]], false);
        Assert.Equal(new[] { "a", "b" }, merged.Select(r => r.Key));
    }

    [Fact]
    public async Task Merge_OfNoShardsOrEmptyShards_CompletesTheOutput()
    {
        Assert.Empty(await MergeAsync(KeySorting.Natural, [], false));
        Assert.Empty(await MergeAsync(KeySorting.Natural, [[], [], []], false));
        Assert.Equal(new[] { "a" }, (await MergeAsync(KeySorting.Natural, [[], [Rec("a")], []], false)).Select(r => r.Key));
    }

    [Fact]
    public async Task Merge_StopsAtTheFirstErrorResult_AndCompletesTheOutput()
    {
        var boom = new OxiaException("boom");
        var shards = new List<List<RangeScanResult>>
        {
            new() { Rec("a"), Rec("c"), Rec("e") },
            // an error result has no key: it is the lowest and is emitted as soon as it reaches the head of its channel
            new() { Rec("b"), new RangeScanResult(new GetResult("", null, default, boom), null), Rec("d") },
        };

        var merged = await MergeAsync(KeySorting.Natural, shards, false);

        // a, then the head of shard 2 is "b" < "c" -> b; then shard 2's head is the error ("" sorts first) -> emitted last
        Assert.Equal(new[] { "a", "b", "" }, merged.Select(r => r.Key));
        Assert.Same(boom, merged[^1].Err);
        Assert.All(merged.Take(merged.Count - 1), r => Assert.Null(r.Err));
    }

    [Fact]
    public async Task Merge_WhenASourceFails_CompletesTheOutputWithThatException()
    {
        var failing = Channel.CreateUnbounded<RangeScanResult>();
        failing.Writer.Complete(new InvalidOperationException("source failed"));
        var outCh = Channel.CreateBounded<GetResult>(100);

        await RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(KeySorting.Natural), [failing.Reader], outCh.Writer);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in outCh.Reader.ReadAllAsync()) { }
        });
        Assert.Equal("source failed", ex.Message);
    }

    [Fact]
    public async Task Merge_WhenCancelled_CompletesTheOutputWithoutError()
    {
        using var cts = new CancellationTokenSource();
        var never = Channel.CreateUnbounded<RangeScanResult>();   // never produces nor completes
        var outCh = Channel.CreateBounded<GetResult>(100);

        var merge = RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(KeySorting.Natural), [never.Reader], outCh.Writer, cts.Token);
        Assert.False(merge.IsCompleted);
        cts.Cancel();
        await merge.WaitAsync(TimeSpan.FromSeconds(10));

        var drained = new List<GetResult>();
        await foreach (var r in outCh.Reader.ReadAllAsync()) drained.Add(r);
        Assert.Empty(drained);
    }

    [Fact]
    public async Task Merge_AppliesBackPressureOnAFullOutput()
    {
        var src = Channel.CreateUnbounded<RangeScanResult>();
        for (int i = 0; i < 10; i++) src.Writer.TryWrite(Rec("k" + i.ToString("D2")));
        src.Writer.Complete();
        var outCh = Channel.CreateBounded<GetResult>(2);

        var merge = RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder.New(KeySorting.Natural), [src.Reader], outCh.Writer);
        await Task.Delay(100);
        Assert.False(merge.IsCompleted);                       // blocked on the full output
        Assert.Equal(2, outCh.Reader.Count);

        var got = new List<string>();
        await foreach (var r in outCh.Reader.ReadAllAsync()) got.Add(r.Key);
        await merge;
        Assert.Equal(Enumerable.Range(0, 10).Select(i => "k" + i.ToString("D2")), got);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The heap itself
    // ---------------------------------------------------------------------------------------------------------------

    private static ResultAndChannel Item(KeyOrder order, string key, string? secondary = null) =>
        ResultAndChannel.New(order, Rec(key, secondary), Channel.CreateUnbounded<RangeScanResult>().Reader);

    [Fact]
    public void Heap_PushPop_YieldsAscendingOrder_AndClearsPoppedSlots()
    {
        var order = KeyOrder.New(KeySorting.Natural);
        var rnd = new Random(1);
        var keys = Enumerable.Range(0, 200).Select(_ => rnd.Next(0, 1000).ToString("D4")).ToList();

        var h = new ResultHeap(order, 1);   // grows from capacity 1
        foreach (var k in keys) h.Push(Item(order, k));
        Assert.Equal(200, h.Count);

        var popped = new List<string>();
        while (h.Count > 0) popped.Add(h.Pop().Gr.Key);

        Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), popped);
        Assert.All(h.Results, r => Assert.Null(r));
    }

    [Fact]
    public void Heap_InitAndFix_RestoreTheInvariants()
    {
        var order = KeyOrder.New(KeySorting.Natural);
        var rnd = new Random(2);
        var h = new ResultHeap(order, 64);
        var items = Enumerable.Range(0, 50).Select(_ => Item(order, rnd.Next(0, 1000).ToString("D4"))).ToArray();

        // place the elements directly (unordered), then Init
        for (int i = 0; i < items.Length; i++) h.Push(items[i]);
        // Fix after changing the top in place, like the merge does
        for (int round = 0; round < 100; round++)
        {
            var top = h.Results[0];
            top.Reset(order, Rec(rnd.Next(0, 1000).ToString("D4")));
            h.Fix(0);
            for (int i = 1; i < h.Count; i++)
            {
                Assert.False(h.Less(i, (i - 1) / 2), $"heap property violated at {i}");
            }
        }

        // Init on an arbitrary arrangement (the constructor from elements runs it)
        var raw = items.OrderByDescending(x => x.Gr.Key, StringComparer.Ordinal).ToArray();
        var h2 = new ResultHeap(order, raw);
        Assert.Equal(raw.Length, h2.Count);
        for (int i = 1; i < h2.Count; i++)
        {
            Assert.False(h2.Less(i, (i - 1) / 2), $"heap property violated at {i}");
        }

        var order2 = new List<string>();
        while (h2.Count > 0) order2.Add(h2.Pop().Gr.Key);
        Assert.Equal(raw.Select(x => x.Gr.Key).OrderBy(k => k, StringComparer.Ordinal), order2);
    }

    [Fact]
    public void ResultAndChannel_ReusesItsBuffers_AndTracksTheSecondaryKey()
    {
        var order = KeyOrder.New(KeySorting.Hierarchical);
        var rc = Item(order, "a/b/c", "x/y");
        Assert.True(rc.HasSecondaryKey);
        Assert.Equal(order.SortKey("a/b/c"), rc.SortKey.ToArray());
        Assert.Equal(order.SortKey("x/y"), rc.SortSecondaryKey.ToArray());

        rc.Reset(order, Rec("z"));
        Assert.False(rc.HasSecondaryKey);
        Assert.Equal(order.SortKey("z"), rc.SortKey.ToArray());
        Assert.True(rc.SortSecondaryKey.IsEmpty);

        // an empty secondary key is still a secondary key (Go: a pointer to "")
        rc.Reset(order, Rec("z", ""));
        Assert.True(rc.HasSecondaryKey);
        Assert.Equal(order.SortKey(""), rc.SortSecondaryKey.ToArray());

        // after the buffers are big enough, Reset allocates nothing
        rc.Reset(order, Rec("a/very/long/key/for/the/buffer", "another/long/secondary/key"));
        var r1 = Rec("short", "s");
        var r2 = Rec("a/very/long/key/for/the/buffer", "another/long/secondary/key");
        for (int i = 0; i < 3; i++) { rc.Reset(order, r1); rc.Reset(order, r2); }
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++) { rc.Reset(order, r1); rc.Reset(order, r2); }
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // KeyOrder
    // ---------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(KeySorting.Natural)]
    [InlineData(KeySorting.Hierarchical)]
    [InlineData(KeySorting.Unknown)]
    public void KeyOrder_CompareAgreesWithCompareSortKeysOverSortKey_AndReturnsExactlyMinusOneZeroOne(KeySorting ks)
    {
        var order = KeyOrder.New(ks);
        var reference = ReferenceComparer(ks);
        var rnd = new Random(99);
        Span<byte> buf = stackalloc byte[256];
        for (int i = 0; i < 2000; i++)
        {
            string a = RandomKey(rnd), b = RandomKey(rnd);
            int expected = order.CompareSortKeys(order.SortKey(a), order.SortKey(b));
            Assert.Equal(expected, order.Compare(a, b));
            Assert.InRange(expected, -1, 1);
            Assert.Equal(-expected, order.Compare(b, a));

            // the allocation-free encode agrees with the allocating one
            int n = order.SortKeyTo(a, buf);
            Assert.Equal(order.SortKey(a), buf.Slice(0, n).ToArray());
            Assert.Equal(n, order.SortKeyLength(a));

            // and with the independent reference ordering
            Assert.Equal(Math.Sign(reference.Compare(a, b)), expected);
        }
    }

    [Fact]
    public void KeyOrder_DefaultIsTheLegacyOrder_AndUnknownKeySortingsMapToIt()
    {
        var legacy = default(KeyOrder);
        Assert.Equal(legacy.Compare("a/z", "a/y/z"), KeyOrder.New(KeySorting.Unknown).Compare("a/z", "a/y/z"));
        Assert.Equal(-1, legacy.Compare("a/z", "a/y/z"));          // CompareWithSlash: fewer path segments first
        Assert.Equal(-1, KeyOrder.New((KeySorting)99).Compare("a/z", "a/y/z"));
        Assert.Equal(KeyOrder.NewKeyOrder(KeySorting.Natural).Compare("a0", "a/y/z"), KeyOrder.New(KeySorting.Natural).Compare("a0", "a/y/z"));
    }

    [Fact]
    public void KeyOrder_Compare_IsAllocationFreeForShortKeys_AndHandlesLongOnes()
    {
        foreach (var ks in new[] { KeySorting.Natural, KeySorting.Hierarchical, KeySorting.Unknown })
        {
            var order = KeyOrder.New(ks);
            for (int i = 0; i < 3; i++) order.Compare("tenant/users/alice", "tenant/users/bob");
            long before = GC.GetAllocatedBytesForCurrentThread();
            int sum = 0;
            for (int i = 0; i < 1000; i++) sum += order.Compare("tenant/users/alice", "tenant/users/bob");
            Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
            Assert.Equal(-1000, sum);

            // beyond the stack buffer: pooled, still correct
            var longA = new string('a', 600) + "/x";
            var longB = new string('a', 600) + "/y";
            Assert.Equal(-1, order.Compare(longA, longB));
            Assert.Equal(1, order.Compare(longB, longA));
            Assert.Equal(0, order.Compare(longA, longA));
        }
    }

    [Fact]
    public void KeyOrder_InternalKeys_SortAfterUserKeysInNaturalAndHierarchical()
    {
        foreach (var ks in new[] { KeySorting.Natural, KeySorting.Hierarchical })
        {
            var order = KeyOrder.New(ks);
            Assert.Equal(1, order.Compare("__oxia/x", "z/z"));
            Assert.Equal(1, order.Compare("__oxia/x", "\u007f"));
        }

        // legacy order: plain CompareWithSlash, '_' < 'z'
        Assert.Equal(-1, KeyOrder.New(KeySorting.Unknown).Compare("__oxia/x", "z/z"));
    }
}
