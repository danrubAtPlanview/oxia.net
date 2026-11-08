// =====================================================================================================================
// PORT NOTE (independent-review regression/differential test; no Go test counterpart)
//   Upstream : oxia/internal/batch/write_manager.go (writeGate.add / predecessorDone: Go's Put blocks inside Add, so one caller's writes reach the batcher in issue order)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins review finding R1-1: writes held by a new shard after a split must reach the batcher in the order one caller issued them (a later write used to overtake the still-waking held ones).
//   See      : docs/review-findings.md
// =====================================================================================================================
// =====================================================================================================================
// REVIEW R1 (fidelity review of batching / call model / write ordering) -- reproduction tests.
//   Defect   : R1-1  A shard that HOLDS its writes (WriteGate.AddSlowAsync, `await Task.WhenAny(held, closed)`) does not release
//                    the parked writes in arrival order, and a write issued after the hold was released overtakes the writes that
//                    are still waking up.
//   Go       : oxia/internal/batch/write_manager.go:203-226 (writeGate.add): the Go `Put` BLOCKS in `Add` until the write is in
//              the batcher, so one goroutine's consecutive Puts reach the batcher in issue order, hold or no hold.
//   C#       : src/Oxia.Client/Internal/Batch/WriteManager.cs:336-386 (AddSlowAsync) + 472 (PredecessorDoneCore): every parked
//              write wakes on its own thread-pool hop; nothing orders them.  PutAsync returns the (incomplete) ValueTask
//              immediately (AsyncClientImpl.cs:451-453), so a caller that pipelines `PutAsync(k, v1); PutAsync(k, v2)` has both
//              parked at once.
//   Expect   : the writes of ONE caller reach the batcher (and the server) in issue order.  These tests FAIL today.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Text;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Tests.F2;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Tests.Review;

public class R1_HoldOrderTests
{
    /// <summary>End to end: pipelined puts to one key, issued while the child shard holds its writes (right after a split).</summary>
    [Fact]
    public async Task PipelinedPutsIssuedWhileTheChildShardHoldsWrites_AreAppliedInIssueOrder()
    {
        const int N = 300;
        var shardManager = new SplittableShardManager("");
        var executor = new SplitExecutor { ShardManager = shardManager, FrozenShard = 0 };
        var client = SplitExecutor.NewRerouteTestClient(shardManager, executor);
        try
        {
            // v0 is in flight to shard 0 (frozen) when the shard splits: it is rerouted later, and the child holds the rest
            var first = client.PutAsync("k", Encoding.UTF8.GetBytes("v0"));
            await F2Util.ReadAsync(executor.FrozenWrite.Reader);
            shardManager.Split(0, 1, 2);

            // ONE caller, no awaits in between: Go's blocking Put would have queued these in order
            var pending = new List<ValueTask<Oxia.Client.PutResult>>(N);
            for (int i = 1; i <= N; i++)
            {
                pending.Add(client.PutAsync("k", Encoding.UTF8.GetBytes("v" + i)));
            }

            executor.Release.SetResult();
            Assert.Null((await F2Util.WithTimeout(first.AsTask())).Err);
            foreach (var p in pending)
            {
                Assert.Null((await F2Util.WithTimeout(p.AsTask())).Err);
            }

            var expected = Enumerable.Range(0, N + 1).Select(i => "k=v" + i).ToList();
            var applied = executor.AppliedWrites();
            Assert.Equal(expected.Count, applied.Count);
            Assert.True(expected.SequenceEqual(applied),
                "writes of one caller were applied out of order, first mismatch at index " +
                Enumerable.Range(0, expected.Count).FirstOrDefault(i => expected[i] != applied[i]) +
                ": expected " + string.Join(",", expected.Take(12)) + " ... got " + string.Join(",", applied.Take(12)) + " ...");
        }
        finally
        {
            client.WriteBatchManager.Dispose();
        }
    }

    private sealed class RecordingBatcher : IBatcher
    {
        public readonly ConcurrentQueue<object> Added = new();
        public readonly ConcurrentQueue<Barrier> Barriers = new();
        public ValueTask AddAsync(object call)
        {
            if (call is Barrier b) Barriers.Enqueue(b);
            else Added.Enqueue(call);
            return default;
        }
        public Task RunAsync() => Task.CompletedTask;
        public void Dispose() { }
    }

    /// <summary>
    /// Unit level: writes parked on a hold, plus one more issued by the same caller right after the hold was released, must reach
    /// the batcher in issue order (the Go caller was blocked in Add, so none of them could have been issued out of order).
    /// </summary>
    [Fact]
    public async Task WritesParkedOnAHold_ReachTheBatcherInIssueOrder_EvenWhenALaterWriteArrivesAfterTheRelease()
    {
        int reordered = 0;
        const int Iterations = 300;
        for (int iteration = 0; iteration < Iterations; iteration++)
        {
            var batchers = new ConcurrentDictionary<long, RecordingBatcher>();
            var manager = new WriteManager(CancellationToken.None, (_, id) => batchers[id] = new RecordingBatcher(), (_, _) => { });
            await manager.AddAsync(0, "seed"); // shard 0 has a gate, so the successor will hold
            manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
            var removed = batchers[0];
            while (removed.Barriers.IsEmpty) await Task.Yield();

            // Parked on the hold, issued by one caller, never awaited
            var parked = new List<ValueTask>();
            for (int i = 0; i < 8; i++) parked.Add(manager.AddAsync(1, i));

            // The removed shard has handed off: the hold is released ...
            removed.Barriers.TryDequeue(out var barrier);
            barrier!.Done();
            // ... and the same caller issues the next write at once
            var last = manager.AddAsync(1, 8);

            foreach (var p in parked) await p;
            await last;
            var order = batchers[1].Added.Cast<int>().ToArray();
            if (!order.SequenceEqual(Enumerable.Range(0, 9))) reordered++;
            manager.Dispose();
        }

        Assert.True(reordered == 0, $"{reordered} of {Iterations} iterations added one caller's writes to the batcher out of issue order");
    }
}
