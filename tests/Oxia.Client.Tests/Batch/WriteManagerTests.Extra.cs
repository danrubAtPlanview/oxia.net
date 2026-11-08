// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/internal/batch/write_manager.go and manager.go behaviour beyond their tests
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : HandOff must not block a thread and must keep the order of the hand-offs and of predecessorDone even when the
//              batcher of the successor is full; concurrency stress of Add vs ShardsReplaced vs Close; the managers hand out
//              closed batchers after Dispose; Manager creates one batcher per shard under concurrency; error aggregation.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Oxia.Common.Constant;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Tests.Batch;

public class WriteManagerExtraTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    /// <summary>A batcher whose AddAsync waits for a gate: models a full queue.</summary>
    private sealed class SlowBatcher : IBatcher
    {
        public readonly ConcurrentQueue<object> Added = new();
        public readonly ConcurrentQueue<Barrier> Barriers = new();
        private TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Blocking = true;
        public bool Closed;

        public void Open()
        {
            Blocking = false;
            _gate.TrySetResult();
        }

        public async ValueTask AddAsync(object call)
        {
            if (Blocking) await _gate.Task;
            if (call is Barrier b) Barriers.Enqueue(b);
            else Added.Enqueue(call);
        }

        public Task RunAsync() => Task.CompletedTask;
        public void Dispose() => Closed = true;
    }

    [Fact]
    public async Task HandOff_DoesNotBlock_KeepsTheOrder_AndHoldsPredecessorDoneBehindTheHandOffs()
    {
        var batchers = new ConcurrentDictionary<long, SlowBatcher>();
        var manager = new WriteManager(CancellationToken.None, (_, id) =>
        {
            var b = new SlowBatcher { Blocking = id != 0 && id != 1 };
            return batchers[id] = b;
        }, (_, _) => { });
        await manager.AddAsync(0, "x"); // shard 0 gets a batcher that accepts right away
        manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        var shard0 = batchers[0];
        await BatchTestUtil.Eventually(() => !shard0.Barriers.IsEmpty, TenSeconds, TimeSpan.FromMilliseconds(1));
        var shard1 = batchers[1];
        shard1.Blocking = true; // the queue of shard 1 is full from now on

        var heldWrite = Task.Run(async () => await manager.AddAsync(1, "held"));

        // HandOff returns immediately even though the batcher of shard 1 does not accept anything yet
        var handOffs = Task.Run(() =>
        {
            for (int i = 0; i < 5; i++) manager.HandOff(1, "h" + i);
            // the barrier of shard 0 completes: shard 1 may stop holding, but only after the hand-offs were queued
            shard0.Barriers.TryDequeue(out var barrier);
            barrier!.Done();
        });
        await BatchTestUtil.Receive(handOffs, TenSeconds, "HandOff blocked");
        Assert.False(heldWrite.IsCompleted);

        shard1.Open();
        await BatchTestUtil.Receive(heldWrite, TenSeconds, "the held write was not released");

        Assert.Equal(new object[] { "h0", "h1", "h2", "h3", "h4", "held" }, shard1.Added.ToArray());
        manager.Dispose();
    }

    [Fact]
    public async Task Stress_EveryWriteIsAddedExactlyOnce_WhileShardsAreReplaced()
    {
        const int producers = 8, perProducer = 2000;
        var received = new ConcurrentDictionary<long, ConcurrentQueue<object>>();
        var created = new ConcurrentDictionary<long, IBatcher>();
        WriteManager? manager = null;
        manager = new WriteManager(CancellationToken.None, (_, id) =>
        {
            var b = new SlowBatcher { Blocking = false };
            created[id] = b;
            received[id] = b.Added;
            return b;
        }, async (removed, call) =>
        {
            // shard 0 -> 1 -> 2: forward to the successor
            await manager!.AddAsync(removed + 1, call);
        });

        // Both shards have a gate (and a batcher) before the shard map changes
        await manager.AddAsync(0, -1);
        await manager.AddAsync(1, -2);

        var start = new TaskCompletionSource();
        var tasks = new List<Task>();
        long next = 0;
        for (int p = 0; p < producers; p++)
        {
            int producer = p;
            tasks.Add(Task.Run(async () =>
            {
                await start.Task;
                for (int i = 0; i < perProducer; i++)
                {
                    // Route with a stale shard map sometimes: shard 0 until it is removed, then 1 until it is removed, then 2
                    long shard = Volatile.Read(ref next) switch { 0 => 0, 1 => 1, _ => 2 };
                    await manager.AddAsync(shard, producer * perProducer + i);
                }
            }));
        }
        tasks.Add(Task.Run(async () =>
        {
            await start.Task;
            await Task.Delay(5);
            manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
            Volatile.Write(ref next, 1);
            await Task.Delay(5);
            manager.ShardsReplaced(new Dictionary<long, long[]> { [1] = [2] });
            Volatile.Write(ref next, 2);
            // shards 0 and 1 are drained by their barriers: complete them like a batcher loop would
            foreach (var id in new long[] { 0, 1 })
            {
                var batcher = (SlowBatcher)created[id];
                await BatchTestUtil.Eventually(() => !batcher.Barriers.IsEmpty, TenSeconds, TimeSpan.FromMilliseconds(1));
                batcher.Barriers.TryDequeue(out var barrier);
                barrier!.Done();
            }
        }));

        start.SetResult();
        await BatchTestUtil.Receive(Task.WhenAll(tasks), TenSeconds, "an add never completed");
        manager.Dispose();

        // The writes stay where they were added (nothing is handed off here: no batch failed) or were forwarded; none is lost or duplicated
        var all = received.Values.SelectMany(q => q).Cast<int>().OrderBy(x => x).ToArray();
        Assert.Equal(Enumerable.Range(0, producers * perProducer).Prepend(-1).Prepend(-2).OrderBy(x => x), all);
    }

    [Fact]
    public async Task AfterDispose_NewGatesGetClosedBatchers()
    {
        var batchers = new ConcurrentDictionary<long, SlowBatcher>();
        var manager = new WriteManager(CancellationToken.None, (_, id) => batchers[id] = new SlowBatcher { Blocking = false }, (_, _) => { });
        manager.Dispose();

        await manager.AddAsync(5, "late");

        Assert.True(batchers[5].Closed);
    }

    [Fact]
    public async Task Dispose_ThrowsTheCombinedErrorsAfterClosingEveryBatcher()
    {
        var errors = new[] { new OxiaException("e0"), new OxiaException("e1") };
        var closed = new ConcurrentBag<long>();
        IBatcher Create(CancellationToken ct, long id) => new ThrowingBatcher(id, errors[id % 2], closed);

        var manager = new WriteManager(CancellationToken.None, Create, (_, _) => { });
        await manager.AddAsync(0, "a");
        await manager.AddAsync(1, "b");
        await manager.AddAsync(2, "c");

        var err = Assert.Throws<AggregateException>(manager.Dispose);
        Assert.Equal(3, err.InnerExceptions.Count);
        Assert.True(OxiaErrors.Is(err, errors[0]));
        Assert.True(OxiaErrors.Is(err, errors[1]));
        Assert.Equal(3, closed.Count);
    }

    private sealed class ThrowingBatcher(long id, Exception error, ConcurrentBag<long> closed) : IBatcher
    {
        public ValueTask AddAsync(object call) => default;
        public Task RunAsync() => Task.CompletedTask;

        public void Dispose()
        {
            closed.Add(id);
            throw error;
        }
    }

    [Fact]
    public async Task Manager_CreatesOneBatcherPerShard_UnderConcurrency()
    {
        int created = 0;
        var manager = new Manager(CancellationToken.None, (_, id) =>
        {
            Interlocked.Increment(ref created);
            return new SlowBatcher { Blocking = false };
        });

        var results = new ConcurrentBag<(long, IBatcher)>();
        var start = new TaskCompletionSource();
        var tasks = Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            await start.Task;
            for (long shard = 0; shard < 50; shard++) results.Add((shard, manager.Get(shard)));
        })).ToArray();
        start.SetResult();
        await Task.WhenAll(tasks);

        Assert.Equal(50, created);
        foreach (var group in results.GroupBy(r => r.Item1))
        {
            Assert.Single(group.Select(r => r.Item2).Distinct());
        }
    }

    [Fact]
    public void Manager_AfterDispose_HandsOutClosedBatchers_AndAggregatesErrors()
    {
        var closed = new ConcurrentBag<long>();
        var errors = new[] { new OxiaException("e0"), new OxiaException("e1") };
        var manager = new Manager(CancellationToken.None, (_, id) => new ThrowingBatcher(id, errors[id % 2], closed));
        manager.Get(0);
        manager.Get(1);

        var err = Assert.Throws<AggregateException>(manager.Dispose);
        Assert.Equal(2, err.InnerExceptions.Count);

        // a batcher created after Dispose is created closed (here: its Dispose throws on creation)
        Assert.Throws<OxiaException>(() => manager.Get(2));
    }
}
