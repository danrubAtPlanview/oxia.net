// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : oxia/batch/batcher.go (one batch in flight per shard).  BatcherFactory.ExperimentalMaxInFlight is a MEASUREMENT switch
//              (docs/performance.md, "proposal: pipelined write batches"), off by default.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : (1) the default factory keeps the contract -- one batch in flight; (2) with the switch on, batches are STARTED in the order
//              they were formed, at most K overlap, a Barrier is only completed after every earlier batch finished, Close does not lose
//              or duplicate a call.  What the switch does NOT give (and what a real implementation would need) is an ordered retry: that is
//              the open point of the proposal, documented there.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Tests.Throughput;

public class PipelineExperimentTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private sealed class GatedBatch(ConcurrentQueue<GatedBatch> started, Action<GatedBatch> onStart) : IBatch
    {
        public readonly List<int> Calls = new();
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool CanAdd(object call) => true;
        public void Add(object call) => Calls.Add((int)call);
        public int Size => Calls.Count;

        public async ValueTask CompleteAsync()
        {
            started.Enqueue(this);
            onStart(this);
            await Release.Task.ConfigureAwait(false);
        }

        public void Fail(Exception error) { }
    }

    [Fact]
    public async Task Default_OneBatchInFlight()
    {
        var started = new ConcurrentQueue<GatedBatch>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 2 };
        Assert.Equal(1, factory.ExperimentalMaxInFlight);
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "t", () => new GatedBatch(started, _ => { }));
        for (int i = 0; i < 6; i++) await batcher.AddAsync(i);

        await Eventually(() => started.Count == 1);
        await Task.Delay(100);
        Assert.Single(started); // the second batch waits for the first
        batcher.Dispose();
        foreach (var b in started) b.Release.TrySetResult();
    }

    [Fact]
    public async Task WithTheSwitch_BatchesOverlap_UpToK_AndStartInOrder()
    {
        var started = new ConcurrentQueue<GatedBatch>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 2, ExperimentalMaxInFlight = 3 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "t", () => new GatedBatch(started, _ => { }));
        for (int i = 0; i < 12; i++) await batcher.AddAsync(i);

        await Eventually(() => started.Count == 3);
        await Task.Delay(100);
        Assert.Equal(3, started.Count); // K batches in flight, the fourth waits for the window

        var first = started.ToArray();
        Assert.Equal([[0, 1], [2, 3], [4, 5]], first.Select(b => b.Calls.ToArray()).ToArray());

        first[0].Release.SetResult(); // the window moves by one
        await Eventually(() => started.Count == 4);
        Assert.Equal([6, 7], started.ToArray()[3].Calls.ToArray());

        foreach (var b in started.ToArray()) b.Release.TrySetResult();
        await Eventually(() => started.Count == 6);
        foreach (var b in started) b.Release.TrySetResult();
        batcher.Dispose();
    }

    [Fact]
    public async Task ABarrier_CompletesOnlyAfterEveryBatchInFlightFinished()
    {
        var started = new ConcurrentQueue<GatedBatch>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 2, ExperimentalMaxInFlight = 4 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "t", () => new GatedBatch(started, _ => { }));
        for (int i = 0; i < 4; i++) await batcher.AddAsync(i);
        await Eventually(() => started.Count == 2);

        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(new Barrier(() => done.TrySetResult()));
        await Task.Delay(100);
        Assert.False(done.Task.IsCompleted);

        var batches = started.ToArray();
        batches[0].Release.SetResult();
        await Task.Delay(50);
        Assert.False(done.Task.IsCompleted); // one batch is still in flight
        batches[1].Release.SetResult();
        await done.Task.WaitAsync(Ten);
        batcher.Dispose();
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Ten;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("condition not reached");
            await Task.Delay(2);
        }
    }
}
