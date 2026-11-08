// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : oxia/internal/shard_manager.go, shard_strategy_impl.go, convert.go, dizzy_shard_manager.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Edge cases, properties and concurrency of the lock free shard snapshot that replaces Go's RWMutex + map scan:
//                * Get (bucket table / binary search) equals the Go linear scan on random layouts, gaps and boundaries;
//                * the listener runs before the new map is visible, Changed wakes every waiter and is replaced;
//                * shards that are not in an update are kept, a re-sent shard id does not delete overlaps (Go semantics);
//                * readers never observe a half applied update while the shard map is split and merged;
//                * GetAll is cached; the receive loop applies successive responses and retries after an end of stream.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;
using Oxia.Common.Hash;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Xunit;

namespace Oxia.Client.Tests.Internal;

public class ShardManagerExtraTests
{
    private static HashRange Range(uint min, uint max) => new(min, max);

    /// <summary>A strategy whose "hash" is the number in the key, so that tests choose the hash code of each probe.</summary>
    private static ShardStrategy NumericHash() => new(static key => uint.Parse(key, System.Globalization.CultureInfo.InvariantCulture));

    private static ShardManager NewManager(IShardStrategy? strategy = null, ShardsReplacedListener? listener = null,
        IRpcProvider? rpc = null, Microsoft.Extensions.Logging.ILogger? logger = null) =>
        new(strategy ?? ShardStrategy.New(), rpc, "service:6648", "default", TimeSpan.FromSeconds(10), listener, logger);

    // ----- Get ------------------------------------------------------------------------------------------------------

    /// <summary>Random disjoint layout of the whole uint space in <paramref name="count"/> ranges; distribution picks where the cuts fall.</summary>
    private static List<Shard> RandomLayout(Random random, int count, int distribution)
    {
        var cuts = new SortedSet<uint>();
        while (cuts.Count < count - 1)
        {
            uint cut = distribution switch
            {
                0 => (uint)random.NextInt64(1, 1L << 32),                       // uniform
                1 => (uint)random.NextInt64(1, 1L << 20),                       // everything clustered at the bottom
                2 => (uint)Math.Min((1L << 32) - 1, (long)Math.Pow(2, random.NextDouble() * 32)), // log-uniform
                _ => (uint)((1L << 32) / count * (cuts.Count + 1)),             // even split
            };
            if (cut == 0) cut = 1;
            cuts.Add(cut);
            if (distribution == 3 && cuts.Count == count - 1) break;
        }

        var shards = new List<Shard>();
        uint start = 0;
        long id = 0;
        foreach (uint cut in cuts)
        {
            shards.Add(new Shard(id++, $"leader-{id}", Range(start, cut - 1)));
            start = cut;
        }

        shards.Add(new Shard(id, $"leader-{id}", Range(start, uint.MaxValue)));
        return shards;
    }

    private static long? LinearScan(IReadOnlyList<Shard> shards, uint code)
    {
        foreach (var shard in shards)
        {
            if (shard.HashRange.MinInclusive <= code && code <= shard.HashRange.MaxInclusive) return shard.Id;
        }

        return null;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(256)]
    [InlineData(1000)]
    public void Get_MatchesTheLinearScanOnRandomLayouts(int count)
    {
        var random = new Random(count * 7919);
        for (int distribution = 0; distribution < 4; distribution++)
        {
            var layout = RandomLayout(random, count, distribution);
            // a layout with gaps: drop some shards (the codes of the dropped ones must fail, not land on a neighbour)
            var withGaps = layout.Where(_ => count < 3 || random.Next(10) != 0).ToList();
            foreach (var shards in new[] { layout, withGaps })
            {
                if (shards.Count == 0) continue;
                var sm = NewManager(NumericHash());
                // the update order must not matter
                sm.Update(shards.OrderBy(_ => random.Next()).ToArray());

                var codes = new List<uint> { 0, 1, uint.MaxValue, uint.MaxValue - 1 };
                foreach (var shard in layout)
                {
                    uint min = shard.HashRange.MinInclusive, max = shard.HashRange.MaxInclusive;
                    codes.AddRange([min, max, min == 0 ? 0 : min - 1, max == uint.MaxValue ? max : max + 1]);
                }

                for (int i = 0; i < 2000; i++) codes.Add((uint)random.NextInt64(0, 1L << 32));

                foreach (uint code in codes)
                {
                    var expected = LinearScan(shards, code);
                    string key = code.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    if (expected is { } id)
                    {
                        Assert.Equal(id, sm.Get(key));
                    }
                    else
                    {
                        Assert.Throws<InvalidOperationException>(() => sm.Get(key));
                    }
                }
            }
        }
    }

    [Fact]
    public void Get_UsesTheXxh3HashOfTheKey()
    {
        var sm = NewManager();
        sm.Update([
            new Shard(10, "a", Range(0, 0x7FFFFFFF)),
            new Shard(11, "b", Range(0x80000000, uint.MaxValue)),
        ]);
        for (int i = 0; i < 500; i++)
        {
            string key = $"/some/key/{i}";
            long expected = Hasher.Xxh332(key) <= 0x7FFFFFFF ? 10 : 11;
            Assert.Equal(expected, sm.Get(key));
        }

        // The empty key and keys with non ASCII characters hash like in Go (utf-8 bytes)
        Assert.Equal(Hasher.Xxh332("") <= 0x7FFFFFFF ? 10 : 11, sm.Get(""));
        Assert.Equal(Hasher.Xxh332("clé-ключ-鍵") <= 0x7FFFFFFF ? 10 : 11, sm.Get("clé-ключ-鍵"));
    }

    [Fact]
    public void Get_WithOverlappingRanges_FindsAMatchAndNeverThrowsForACoveredCode()
    {
        // A re-sent shard id does not delete overlaps (Go: only new ids do), so a server bug can leave overlapping ranges.
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, 49)), new Shard(1, "b", Range(100, 149))]);
        sm.Update([new Shard(0, "a", Range(0, 120))]);

        Assert.Equal(0, sm.Get("10"));
        Assert.Equal(1, sm.Get("140"));
        Assert.Contains(sm.Get("110"), new long[] { 0, 1 });
        Assert.Throws<InvalidOperationException>(() => sm.Get("200"));
    }

    [Fact]
    public void Get_WithTheDefaultStrategyDoesNotAllocate()
    {
        var sm = NewManager();
        var shards = new Shard[16];
        for (int i = 0; i < shards.Length; i++)
        {
            shards[i] = new Shard(i, "l", Range((uint)((ulong)i << 28), i == 15 ? uint.MaxValue : (uint)(((ulong)(i + 1) << 28) - 1)));
        }

        sm.Update(shards);
        string[] keys = Enumerable.Range(0, 200).Select(static i => $"/benchmark/key-{i}").ToArray();
        foreach (string key in keys) sm.Get(key); // warm up (JIT, tiering)

        long before = GC.GetAllocatedBytesForCurrentThread();
        long sum = 0;
        for (int round = 0; round < 50; round++)
        {
            foreach (string key in keys) sum += sm.Get(key);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(sum >= 0);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void GetAll_IsCachedUntilTheNextUpdateAndSortedByHashRange()
    {
        var sm = NewManager();
        Assert.Empty(sm.GetAll());
        sm.Update([new Shard(7, "a", Range(100, 199)), new Shard(3, "b", Range(0, 99)), new Shard(9, "c", Range(200, 299))]);

        var all = sm.GetAll();
        Assert.Equal([3L, 7L, 9L], all);
        Assert.Same(all, sm.GetAll());

        sm.Update([new Shard(10, "d", Range(300, 399))]);
        Assert.NotSame(all, sm.GetAll());
        Assert.Equal([3L, 7L, 9L, 10L], sm.GetAll());
        Assert.Equal([3L, 7L, 9L], all); // the array handed out earlier is never modified
    }

    [Fact]
    public void GetAll_ModifyingTheReturnedArrayDoesNotCorruptRouting()
    {
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, 99)), new Shard(1, "b", Range(100, 199))]);
        var all = sm.GetAll();
        all[0] = 42;
        all[1] = 43;
        Assert.Equal(0, sm.Get("10"));
        Assert.Equal(1, sm.Get("150"));
    }

    // ----- Update ---------------------------------------------------------------------------------------------------

    [Fact]
    public void Update_KeepsTheShardsThatAreNotInTheUpdate()
    {
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, 99)), new Shard(1, "b", Range(100, 199))]);
        sm.Update([new Shard(0, "a2", Range(0, 99))]);

        Assert.True(sm.Exists(1));
        Assert.Equal("a2", sm.Leader(0));
        Assert.Equal("b", sm.Leader(1));
        Assert.Equal(1, sm.Get("150"));
    }

    [Fact]
    public void Update_ASendIdThatAlreadyExistsDoesNotDeleteOverlappingShards()
    {
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, 99)), new Shard(1, "b", Range(100, 199))]);
        sm.Update([new Shard(0, "a", Range(0, 150))]); // overlaps shard 1, but its id exists: nothing is deleted

        Assert.True(sm.Exists(0));
        Assert.True(sm.Exists(1));
        Assert.Null(sm.GetSuccessors(1));
    }

    [Fact]
    public void Update_Merge_RecordsTheNewShardAsSuccessorOfBothAndNotifiesOnce()
    {
        var calls = new List<IReadOnlyDictionary<long, long[]>>();
        var sm = NewManager(NumericHash(), calls.Add);
        sm.Update([new Shard(0, "a", Range(0, 99)), new Shard(1, "b", Range(100, 199))]);
        sm.Update([new Shard(2, "c", Range(0, 199))]);

        Assert.False(sm.Exists(0));
        Assert.False(sm.Exists(1));
        Assert.Equal<long>([2L], sm.GetSuccessors(0)!);
        Assert.Equal<long>([2L], sm.GetSuccessors(1)!);
        Assert.Single(calls);
        Assert.Equal(2, calls[0].Count);
        Assert.Equal([2L], calls[0][0]);
        Assert.Equal([2L], calls[0][1]);
        Assert.True(sm.GetSuccessor(1, "150", out long successor));
        Assert.Equal(2, successor);
        Assert.Equal(2, sm.Get("150"));
    }

    [Fact]
    public void Update_WithoutRemovalsDoesNotInvokeTheListener()
    {
        int calls = 0;
        var sm = NewManager(NumericHash(), _ => calls++);
        sm.Update([new Shard(0, "a", Range(0, 99))]);
        sm.Update([new Shard(1, "b", Range(100, 199))]);
        sm.Update([new Shard(1, "b2", Range(100, 199))]);
        Assert.Equal(0, calls);
    }

    [Fact]
    public void Update_TheListenerRunsBeforeTheNewMapIsVisible()
    {
        ShardManager? sm = null;
        Task? changedBefore = null;
        bool newShardVisible = true, oldShardGone = false;
        bool changedCompleted = true;
        sm = NewManager(NumericHash(), _ =>
        {
            newShardVisible = sm!.Exists(2);
            oldShardGone = !sm.Exists(0);
            changedCompleted = changedBefore!.IsCompleted;
        });
        sm.Update([new Shard(0, "a", Range(0, 99))]);
        changedBefore = sm.Changed();

        sm.Update([new Shard(2, "c", Range(0, 99))]);

        Assert.False(newShardVisible);
        Assert.False(oldShardGone);
        Assert.False(changedCompleted);
        Assert.True(sm.Exists(2));
        Assert.False(sm.Exists(0));
        Assert.True(changedBefore.IsCompleted);
    }

    [Fact]
    public void Update_NewIdDeletesOnlyTheShardsItOverlaps()
    {
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, 99)), new Shard(1, "b", Range(100, 199)), new Shard(2, "c", Range(200, 299))]);
        sm.Update([new Shard(5, "e", Range(50, 120))]);

        Assert.False(sm.Exists(0));
        Assert.False(sm.Exists(1));
        Assert.True(sm.Exists(2));
        Assert.True(sm.Exists(5));
        Assert.Equal<long>([5L], sm.GetSuccessors(0)!);
        Assert.Equal<long>([5L], sm.GetSuccessors(1)!);
        Assert.Null(sm.GetSuccessors(2));
    }

    [Fact]
    public async Task Changed_WakesEveryWaiterAndIsReplacedOnEveryUpdate()
    {
        var sm = NewManager(NumericHash());
        var first = sm.Changed();
        Assert.Same(first, sm.Changed());
        var waiters = Enumerable.Range(0, 10).Select(_ => first.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken)).ToArray();
        Assert.All(waiters, static w => Assert.False(w.IsCompleted));

        sm.Update([new Shard(0, "a", Range(0, 99))]);
        await Task.WhenAll(waiters);

        var second = sm.Changed();
        Assert.NotSame(first, second);
        Assert.False(second.IsCompleted);
        sm.Update([new Shard(0, "a2", Range(0, 99))]);
        Assert.True(second.IsCompleted);
        Assert.NotSame(second, sm.Changed());
    }

    [Fact]
    public void KeySorting_IsUnknownUntilTheServerReportsIt()
    {
        var sm = NewManager();
        Assert.Equal(KeySorting.Unknown, sm.KeySorting);
    }

    [Fact]
    public void Leader_AndExists_AreAnsweredFromTheSnapshot()
    {
        var sm = NewManager();
        Assert.False(sm.Exists(0));
        Assert.Equal("service:6648", sm.Leader(0));
        sm.Update([new Shard(0, "leader:1", Range(0, uint.MaxValue))]);
        Assert.True(sm.Exists(0));
        Assert.Equal("leader:1", sm.Leader(0));
    }

    // ----- concurrency ----------------------------------------------------------------------------------------------

    [Fact]
    public async Task Readers_NeverObserveAHalfAppliedUpdateWhileTheMapIsSplitAndMerged()
    {
        var sm = NewManager(NumericHash());
        sm.Update([new Shard(0, "a", Range(0, uint.MaxValue))]);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        long reads = 0;
        var readers = Enumerable.Range(0, 4).Select(r => Task.Run(() =>
        {
            var random = new Random(r);
            string[] keys = Enumerable.Range(0, 64).Select(_ => ((uint)random.NextInt64(0, 1L << 32)).ToString(System.Globalization.CultureInfo.InvariantCulture)).ToArray();
            while (!stop.IsCancellationRequested)
            {
                foreach (string key in keys)
                {
                    long id = sm.Get(key); // must never throw "shard not found": every published map covers the whole hash space
                    Assert.True(sm.Exists(id) || sm.GetSuccessors(id) is not null);
                    Interlocked.Increment(ref reads);
                }

                _ = sm.GetAll();
            }
        }, TestContext.Current.CancellationToken)).ToArray();

        var writer = Task.Run(() =>
        {
            long next = 1;
            long current = 0;
            for (int i = 0; i < 300; i++)
            {
                // split the current shard in two, then merge both back into a new shard
                long left = next++, right = next++;
                sm.Update([new Shard(left, "l", Range(0, 0x7FFFFFFF)), new Shard(right, "r", Range(0x80000000, uint.MaxValue))]);
                long merged = next++;
                sm.Update([new Shard(merged, "m", Range(0, uint.MaxValue))]);
                current = merged;
            }

            return current;
        }, TestContext.Current.CancellationToken);

        long last = await writer;
        await Task.Delay(50, TestContext.Current.CancellationToken);
        await stop.CancelAsync();
        await Task.WhenAll(readers);
        Assert.True(Interlocked.Read(ref reads) > 0);
        Assert.Equal([last], sm.GetAll());
    }

    // ----- receive loop ---------------------------------------------------------------------------------------------

    private static ShardAssignments AssignmentsOf(string ns, KeySorting sorting, params (long Id, string Leader, uint Min, uint Max)[] shards)
    {
        var assignments = new ShardAssignments();
        var nsa = new NamespaceShardsAssignment { KeySorting = sorting };
        foreach (var (id, leader, min, max) in shards)
        {
            nsa.Assignments.Add(new ShardAssignment
            {
                Shard = id,
                Leader = leader,
                Int32HashRange = new Int32HashRange { MinHashInclusive = min, MaxHashInclusive = max },
            });
        }

        assignments.Namespaces[ns] = nsa;
        return assignments;
    }

    [Fact]
    public async Task ReceiveAsync_AppliesSuccessiveResponsesAndDisposesTheStreamWhenItFails()
    {
        var stream = new FakeServerStream<ShardAssignments>();
        var provider = new FakeRpcProvider
        {
            OnGetShardAssignments = (_, target, request) =>
            {
                Assert.Equal("service:6648", target);
                Assert.Equal("default", request.Namespace);
                return ValueTask.FromResult<IServerStreamingClient<ShardAssignments>>(stream);
            },
        };
        var sm = NewManager(NumericHash(), rpc: provider);
        var changed = sm.Changed();

        stream.Push(AssignmentsOf("default", KeySorting.Natural, (0, "leader-a:1", 0, 99)));
        var receive = sm.ReceiveAsync(new Oxia.Common.Time.ZeroBackOff());
        await changed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal("leader-a:1", sm.Leader(0));
        Assert.Equal(KeySorting.Natural, sm.KeySorting);

        var changed2 = sm.Changed();
        stream.Push(AssignmentsOf("default", KeySorting.Hierarchical, (0, "leader-b:2", 0, 99)));
        await changed2.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal("leader-b:2", sm.Leader(0));
        Assert.Equal(KeySorting.Hierarchical, sm.KeySorting);

        stream.Fail(new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "gone")));
        var ex = await Assert.ThrowsAsync<RpcException>(() => receive);
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(stream.Disposed);
    }

    [Fact]
    public async Task ReceiveAsync_AnEndOfStreamIsAnErrorThatIsRetried()
    {
        int calls = 0;
        var logs = new CapturingLogger();
        var provider = new FakeRpcProvider
        {
            OnGetShardAssignments = (_, _, _) =>
            {
                int call = Interlocked.Increment(ref calls);
                var stream = new FakeServerStream<ShardAssignments>();
                if (call == 1) stream.Complete(); // the server closes the stream
                else stream.Push(AssignmentsOf("default", default, (0, "leader:1", 0, 99)));
                return ValueTask.FromResult<IServerStreamingClient<ShardAssignments>>(stream);
            },
        };
        var sm = NewManager(rpc: provider, logger: logs);
        var loop = sm.ReceiveWithRecoveryAsync();
        try
        {
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            wait.CancelAfter(TimeSpan.FromSeconds(10));
            while (!sm.Exists(0)) await Task.Delay(10, wait.Token);
            Assert.Equal(2, Volatile.Read(ref calls));
            Assert.True(logs.Warned.IsCompleted);
        }
        finally
        {
            sm.Dispose();
            await loop.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task CreateAsync_CompletesWithTheFirstAssignmentsWhileTheStreamStaysOpen()
    {
        var provider = new SingleAssignmentsRpcProvider(KeySorting.Hierarchical);
        var sm = await ShardManager.CreateAsync(ShardStrategy.New(), provider, "service:6648", "default", TimeSpan.FromSeconds(10), null);
        try
        {
            Assert.Single(sm.GetAll());
            Assert.Equal(KeySorting.Hierarchical, sm.KeySorting);
        }
        finally
        {
            sm.Dispose();
        }
    }

    [Fact]
    public async Task CreateAsync_FailsWithTheTimeoutWhenNoAssignmentsArriveAndKeepsNoLoopRunning()
    {
        var provider = new PendingAssignmentsRpcProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ShardManager.CreateAsync(ShardStrategy.New(), provider, "service:6648", "default", TimeSpan.FromMilliseconds(50), null));
        Assert.Contains("failed to retrieve the initial list of shard assignments", ex.Message);
        Assert.True(OxiaErrors.IsDeadlineExceeded(ex));
        Assert.False(OxiaErrors.IsCanceled(ex)); // a deadline is not a cancellation (Go: context.DeadlineExceeded != context.Canceled)
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var sm = NewManager();
        sm.Dispose();
        sm.Dispose();
    }

    // ----- strategy and conversion ----------------------------------------------------------------------------------

    [Fact]
    public void ShardStrategy_DefaultHashIsXxh3OfTheKeyAndThePredicateRangesAreInclusive()
    {
        var strategy = ShardStrategy.New();
        foreach (string key in new[] { "", "a", "/some/key", "clé-ключ-鍵" })
        {
            Assert.Equal(Hasher.Xxh332(key), strategy.Hash(key));
            uint code = strategy.Hash(key);
            var predicate = strategy.Get(key);
            Assert.True(predicate(new Shard(0, "", Range(code, code))));
            Assert.True(predicate(new Shard(0, "", Range(0, uint.MaxValue))));
            if (code > 0) Assert.False(predicate(new Shard(0, "", Range(0, code - 1))));
            if (code < uint.MaxValue) Assert.False(predicate(new Shard(0, "", Range(code + 1, uint.MaxValue))));
        }
    }

    [Fact]
    public void ShardConvert_ReportsTheShardIdOfAnAssignmentWithoutBoundaries()
    {
        Assert.False(ShardConvert.TryToHashRange(new ShardAssignment { Shard = 5 }, out _, out var error));
        Assert.Equal("shard 5 has an unsupported hash range boundary <nil>", error.Message);
        Assert.True(ShardConvert.TryToHashRange(new ShardAssignment { Int32HashRange = new Int32HashRange { MinHashInclusive = 3, MaxHashInclusive = 9 } },
            out var range, out _));
        Assert.Equal(Range(3, 9), range);
    }

    [Fact]
    public async Task DizzyShardManager_ForwardsEverythingButTheLeader()
    {
        var sm = await DizzyShardManager.CreateAsync(NumericHash(), new SingleAssignmentsRpcProvider(KeySorting.Natural), "lookup:6648",
            "default", TimeSpan.FromSeconds(10), null);
        try
        {
            Assert.Equal(KeySorting.Natural, sm.KeySorting);
            long shard = sm.GetAll().Single();
            Assert.True(sm.Exists(shard));
            Assert.Equal(shard, sm.Get("0"));
            Assert.Null(sm.GetSuccessors(shard));
            Assert.False(sm.GetSuccessor(shard, "0", out _));
            Assert.False(sm.Changed().IsCompleted);
            Assert.Equal("lookup:6648", sm.Leader(shard));
            Assert.Equal("lookup:6648", sm.Leader(12345)); // unknown shards too
        }
        finally
        {
            sm.Dispose();
        }
    }
}
