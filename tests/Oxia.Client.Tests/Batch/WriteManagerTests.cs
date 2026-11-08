// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/write_manager_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 12 (TestWriteManagerAdd, TestWriteManagerHoldsWritesUntilHandOff, TestWriteManagerForwardsWritesToRemovedShard,
//              TestWriteManagerRemovedShardWithoutWrites, TestWriteManagerRetireWaitsForWritesBeingAdded,
//              TestWriteManagerNestedSplit, TestWriteManagerShardsReplacedAfterClose, TestWriteManagerCloseReleasesHeldWrites,
//              TestWriteManagerCloseTwice, TestWriteManagerHandOffToRemovedShardWithoutWrites,
//              TestWriteManagerHoldsWritesUntilAllPredecessorsHandOff, TestWriteManagerExistingSuccessorDoesNotHoldWrites)
//              ported: 12   skipped: 0
//   Adaptations: the blocking `m.Add` is `await m.AddAsync` and the test `forward` (which calls `m.Add` and can block) is the
//              ValueTask-returning constructor overload; the recording batcher blocks inside AddAsync with a
//              TaskCompletionSource instead of a channel; `batcher.Run()` is an already completed task; `Close()` is
//              `Dispose()`.  `batch.Barrier` values travel through a 1-buffered channel exactly like in Go.  Assertions and
//              timings (50 ms "still blocked" windows, 10 s "eventually" bounds) are unchanged.
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Tests.Batch;

public class WriteManagerTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    /// <summary>
    /// recordingBatcher records the calls added to it, and the ones added after it was closed, which a batcher fails. The
    /// test completes the barriers.
    /// </summary>
    private sealed class RecordingBatcher : IBatcher
    {
        private readonly object _lock = new();
        public readonly List<object> Calls = new();
        public readonly List<object> Failed = new();
        public bool Closed;
        public readonly Channel<Barrier> Barriers = Channel.CreateBounded<Barrier>(1);

        // If set, adding a call blocks until unblock is completed
        public TaskCompletionSource? Entered;
        public TaskCompletionSource? Unblock;

        public object Lock => _lock;

        public async ValueTask AddAsync(object call)
        {
            if (call is Barrier barrier)
            {
                await Barriers.Writer.WriteAsync(barrier);
                return;
            }
            if (Unblock is not null)
            {
                Entered!.TrySetResult();
                await Unblock.Task;
            }
            lock (_lock)
            {
                if (Closed)
                {
                    Failed.Add(call);
                }
                else
                {
                    Calls.Add(call);
                }
            }
        }

        public List<object> Recorded()
        {
            lock (_lock) return Calls.ToList();
        }

        public Task RunAsync() => Task.CompletedTask;

        public void Dispose()
        {
            lock (_lock) Closed = true;
        }
    }

    private sealed class TestWriteManager
    {
        private readonly object _lock = new();
        private readonly Dictionary<long, RecordingBatcher> _batchers = new();
        private readonly List<object> _forwarded = new();
        public readonly WriteManager Manager;

        /// <summary>Creates a WriteManager that forwards the writes of a removed shard to the successor given for it.</summary>
        public TestWriteManager(Dictionary<long, long>? successors)
        {
            Manager = new WriteManager(CancellationToken.None, (_, shardId) =>
            {
                lock (_lock)
                {
                    var b = new RecordingBatcher();
                    _batchers[shardId] = b;
                    return b;
                }
            }, async (removedShardId, call) =>
            {
                lock (_lock)
                {
                    _forwarded.Add(call);
                }
                long successor = 0;
                successors?.TryGetValue(removedShardId, out successor);
                await Manager!.AddAsync(successor, call);
            });
        }

        public RecordingBatcher? Batcher(long shardId)
        {
            lock (_lock) return _batchers.GetValueOrDefault(shardId);
        }

        public List<object> ForwardedCalls()
        {
            lock (_lock) return _forwarded.ToList();
        }

        public async ValueTask Add(long shardId, object call) => await Manager.AddAsync(shardId, call);

        public void HandOff(long shardId, object call) => Manager.HandOff(shardId, call);

        public void ShardsReplaced(Dictionary<long, long[]> replaced) => Manager.ShardsReplaced(replaced);

        public void Close() => Manager.Dispose();

        /// <summary>addAsync adds a write from another goroutine: the returned task completes once Add returns.</summary>
        public Task AddAsync(long shardId, object call) => Task.Run(async () => await Manager.AddAsync(shardId, call));
    }

    private static async Task RequireBlocked(Task done)
    {
        if (await Task.WhenAny(done, Task.Delay(TimeSpan.FromMilliseconds(50))) == done)
        {
            Assert.Fail("not blocked");
        }
    }

    private static Task RequireDone(Task done) => BatchTestUtil.Receive(done, TenSeconds, "still blocked");

    private static async Task<Barrier> ReceiveBarrier(RecordingBatcher b)
    {
        using var cts = new CancellationTokenSource(TenSeconds);
        try
        {
            return await b.Barriers.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("no barrier");
            throw;
        }
    }

    /// <summary>select { case &lt;-b.barriers: fail; case &lt;-time.After(50ms): }</summary>
    private static async Task RequireNoBarrier(RecordingBatcher b, string message)
    {
        var waiting = b.Barriers.Reader.WaitToReadAsync().AsTask();
        if (await Task.WhenAny(waiting, Task.Delay(TimeSpan.FromMilliseconds(50))) == waiting)
        {
            Assert.Fail(message);
        }
    }

    private static void AssertRecorded(RecordingBatcher? batcher, params object[] expected) =>
        Assert.Equal(expected, batcher!.Recorded());

    [Fact]
    public async Task TestWriteManagerAdd()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        await m.Add(0, "b");
        await m.Add(1, "c");

        AssertRecorded(m.Batcher(0), "a", "b");
        AssertRecorded(m.Batcher(1), "c");
        m.Close();
    }

    [Fact]
    public async Task TestWriteManagerHoldsWritesUntilHandOff()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");

        // Shard 0 is split into shards 1 and 2
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1, 2] });
        var added = m.AddAsync(1, "c");
        await RequireBlocked(added);

        // The batch of shard 0 fails and hands off its write, which is not held
        m.HandOff(1, "a");
        AssertRecorded(m.Batcher(1), "a");

        // Shard 0 has completed all its batches
        var barrier = await ReceiveBarrier(m.Batcher(0)!);
        await RequireBlocked(added);
        barrier.Done();

        await RequireDone(added);
        AssertRecorded(m.Batcher(1), "a", "c");
        await m.Add(2, "d");
        AssertRecorded(m.Batcher(2), "d");
        Assert.Empty(m.ForwardedCalls());
        m.Close();
    }

    [Fact]
    public async Task TestWriteManagerForwardsWritesToRemovedShard()
    {
        var m = new TestWriteManager(new Dictionary<long, long> { [0] = 1 });
        await m.Add(0, "a");
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        (await ReceiveBarrier(m.Batcher(0)!)).Done();

        // A write routed with the old shard map is forwarded before Add returns,
        // ahead of any write issued after it
        await m.Add(0, "b");
        await m.Add(1, "c");
        Assert.Equal(new object[] { "b" }, m.ForwardedCalls());
        AssertRecorded(m.Batcher(0), "a");
        AssertRecorded(m.Batcher(1), "b", "c");
        m.Close();
    }

    [Fact]
    public async Task TestWriteManagerRemovedShardWithoutWrites()
    {
        var m = new TestWriteManager(new Dictionary<long, long> { [0] = 1 });

        // Shard 0 has nothing to hand off, so shard 1 does not hold its writes
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        await m.Add(1, "a");
        await m.Add(0, "b");

        Assert.Null(m.Batcher(0));
        Assert.Equal(new object[] { "b" }, m.ForwardedCalls());
        AssertRecorded(m.Batcher(1), "a", "b");
        m.Close();
    }

    // Retiring a shard waits for the writes being added to its batcher, so they
    // are handed off before the shards that replaced it stop holding their writes.
    [Fact]
    public async Task TestWriteManagerRetireWaitsForWritesBeingAdded()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        var b = m.Batcher(0)!;
        b.Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        b.Unblock = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The batcher of shard 0 is full: adding the write blocks
        var added = m.AddAsync(0, "b");
        await b.Entered.Task;

        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        await RequireNoBarrier(b, "the shard was retired while a write was being added");

        b.Unblock.SetResult();
        await RequireDone(added);
        await ReceiveBarrier(b);
        AssertRecorded(b, "a", "b");
        m.Close();
    }

    // A shard that replaced another one is split in turn before that one has handed
    // off all its writes: the writes keep their order across both hand-offs.
    [Fact]
    public async Task TestWriteManagerNestedSplit()
    {
        var m = new TestWriteManager(new Dictionary<long, long> { [1] = 3 });
        await m.Add(0, "a");
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1, 2] });
        var barrier0 = await ReceiveBarrier(m.Batcher(0)!);
        m.HandOff(1, "a");
        var heldOn1 = m.AddAsync(1, "b");
        await RequireBlocked(heldOn1);

        // Shard 1 is split into shards 3 and 4. The write held by shard 1 is
        // forwarded, and held by shard 3 until shard 1 has handed off its writes.
        m.ShardsReplaced(new Dictionary<long, long[]> { [1] = [3, 4] });
        await BatchTestUtil.Eventually(() => m.ForwardedCalls().Count == 1, TenSeconds, TimeSpan.FromMilliseconds(1));
        var heldOn3 = m.AddAsync(3, "c");
        await RequireBlocked(heldOn1);
        await RequireBlocked(heldOn3);

        // Shard 1 hands off its writes only after it received the ones of shard 0
        await RequireNoBarrier(m.Batcher(1)!, "shard 1 was drained before shard 0 handed off its writes");
        barrier0.Done();
        var barrier1 = await ReceiveBarrier(m.Batcher(1)!);
        m.HandOff(3, "a");
        await RequireBlocked(heldOn3);
        barrier1.Done();

        await RequireDone(heldOn1);
        await RequireDone(heldOn3);
        var recorded = m.Batcher(3)!.Recorded();
        Assert.Equal(3, recorded.Count);
        Assert.Equal("a", recorded[0]);
        Assert.Equivalent(new object[] { "b", "c" }, recorded.Skip(1).ToArray());
        Assert.Equal(new object[] { "b" }, m.ForwardedCalls());
        m.Close();
    }

    [Fact]
    public async Task TestWriteManagerShardsReplacedAfterClose()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        m.Close();

        // The shard manager applies an update after the client was closed: no
        // batcher is created, as none would be closed
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1, 2] });
        Assert.Null(m.Batcher(1));
        Assert.Null(m.Batcher(2));
    }

    [Fact]
    public async Task TestWriteManagerCloseReleasesHeldWrites()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        var added = m.AddAsync(1, "b");
        await RequireBlocked(added);

        m.Close();
        await RequireDone(added);

        // The closed batcher fails the write
        var b = m.Batcher(1)!;
        lock (b.Lock)
        {
            Assert.Empty(b.Calls);
            Assert.Equal(new object[] { "b" }, b.Failed);
        }
    }

    [Fact]
    public async Task TestWriteManagerCloseTwice()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");

        m.Close();
        m.Close();
        var b = m.Batcher(0)!;
        lock (b.Lock)
        {
            Assert.True(b.Closed);
        }
    }

    [Fact]
    public void TestWriteManagerHandOffToRemovedShardWithoutWrites()
    {
        var m = new TestWriteManager(new Dictionary<long, long> { [0] = 1 });
        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });

        // Shard 0 never had a batcher: the write is forwarded to its successor
        m.HandOff(0, "a");

        Assert.Null(m.Batcher(0));
        Assert.Equal(new object[] { "a" }, m.ForwardedCalls());
        AssertRecorded(m.Batcher(1), "a");
        m.Close();
    }

    // Two shards are merged into one: the new shard holds its writes until both
    // have handed off theirs.
    [Fact]
    public async Task TestWriteManagerHoldsWritesUntilAllPredecessorsHandOff()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        await m.Add(1, "b");

        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [2], [1] = [2] });
        var added = m.AddAsync(2, "c");
        await RequireBlocked(added);

        (await ReceiveBarrier(m.Batcher(0)!)).Done();
        await RequireBlocked(added);

        (await ReceiveBarrier(m.Batcher(1)!)).Done();
        await RequireDone(added);
        AssertRecorded(m.Batcher(2), "c");
        m.Close();
    }

    // A shard that already received writes before it replaced a removed one does
    // not hold its writes for the hand-off.
    [Fact]
    public async Task TestWriteManagerExistingSuccessorDoesNotHoldWrites()
    {
        var m = new TestWriteManager(null);
        await m.Add(0, "a");
        await m.Add(1, "b");

        m.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        await m.Add(1, "c");
        AssertRecorded(m.Batcher(1), "b", "c");

        (await ReceiveBarrier(m.Batcher(0)!)).Done();
        await m.Add(1, "d");
        AssertRecorded(m.Batcher(1), "b", "c", "d");
        m.Close();
    }
}
