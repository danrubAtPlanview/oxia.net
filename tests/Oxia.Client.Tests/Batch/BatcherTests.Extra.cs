// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/batch/batcher.go behaviour, exercised beyond batcher_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : concurrency and contract tests for the batcher loop: exactly-once completion with many producers, ordering
//              within a shard, one batch at a time, linger semantics, barriers, Close racing with Add, back-pressure, a
//              failing batch not killing the loop.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Oxia.Common.Constant;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;

namespace Oxia.Client.Tests.Batch;

public class BatcherExtraTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    /// <summary>A batch driven by delegates.</summary>
    private sealed class FakeBatch(Func<int, object, bool>? canAdd = null, Func<List<object>, ValueTask>? complete = null,
        Action<List<object>, Exception>? fail = null) : IBatch
    {
        public readonly List<object> Calls = new();
        public bool CanAdd(object call) => canAdd?.Invoke(Calls.Count, call) ?? true;
        public void Add(object call) => Calls.Add(call);
        public int Size => Calls.Count;
        public ValueTask CompleteAsync() => complete?.Invoke(Calls) ?? default;
        public void Fail(Exception error) => fail?.Invoke(Calls, error);
    }

    [Fact]
    public async Task ManyProducers_EveryCallIsCompletedExactlyOnce()
    {
        const int producers = 16, perProducer = 5000;
        var completions = new int[producers * perProducer];
        int maxBatch = 0;
        var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: calls =>
        {
            maxBatch = Math.Max(maxBatch, calls.Count);
            foreach (var call in calls) Interlocked.Increment(ref completions[(int)call]);
            return default;
        }));

        var tasks = new List<Task>();
        for (int p = 0; p < producers; p++)
        {
            int producer = p;
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < perProducer; i++)
                {
                    await batcher.AddAsync(producer * perProducer + i);
                }
            }));
        }
        await Task.WhenAll(tasks);

        await BatchTestUtil.Eventually(() => completions.Sum() == completions.Length, TenSeconds, TimeSpan.FromMilliseconds(5));
        batcher.Dispose();
        await batcher.RunAsync();

        Assert.All(completions, count => Assert.Equal(1, count));
        Assert.True(maxBatch <= 100, $"batch of {maxBatch}");
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(7, 0)]
    [InlineData(100, 1)]
    public async Task SingleProducer_OrderIsPreservedAcrossBatches(int maxRequests, int lingerMillis)
    {
        const int total = 20000;
        var completed = new List<int>(total);
        var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(lingerMillis), MaxRequestsPerBatch = maxRequests };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: calls =>
        {
            foreach (var call in calls) completed.Add((int)call);
            return default;
        }));

        for (int i = 0; i < total; i++)
        {
            await batcher.AddAsync(i);
        }
        await BatchTestUtil.Eventually(() => { lock (completed) return completed.Count == total; }, TenSeconds, TimeSpan.FromMilliseconds(5));
        batcher.Dispose();
        await batcher.RunAsync();

        Assert.Equal(Enumerable.Range(0, total), completed);
    }

    [Fact]
    public async Task OnlyOneBatchIsCompletedAtATime()
    {
        int running = 0, maxRunning = 0, completedBatches = 0;
        var factory = new BatcherFactory { MaxRequestsPerBatch = 3 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: async _ =>
        {
            int now = Interlocked.Increment(ref running);
            maxRunning = Math.Max(maxRunning, now);
            await Task.Delay(1);
            Interlocked.Decrement(ref running);
            Interlocked.Increment(ref completedBatches);
        }));

        for (int i = 0; i < 60; i++)
        {
            await batcher.AddAsync(i);
        }
        await BatchTestUtil.Eventually(() => Volatile.Read(ref completedBatches) == 60, TenSeconds, TimeSpan.FromMilliseconds(5));
        batcher.Dispose();

        Assert.Equal(1, maxRunning);
    }

    [Fact]
    public async Task LingerZero_CompletesEveryCallImmediately()
    {
        var sizes = new ConcurrentQueue<int>();
        var factory = new BatcherFactory { Linger = TimeSpan.Zero, MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: calls =>
        {
            sizes.Enqueue(calls.Count);
            return default;
        }));

        for (int i = 0; i < 50; i++)
        {
            await batcher.AddAsync(i);
        }
        await BatchTestUtil.Eventually(() => sizes.Count == 50, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        Assert.All(sizes, size => Assert.Equal(1, size));
    }

    [Fact]
    public async Task Linger_CompletesTheBatchAfterTheLingerTime_AndNotBefore()
    {
        var linger = TimeSpan.FromMilliseconds(200);
        var completions = new ConcurrentQueue<(long Ticks, int Size)>();
        var factory = new BatcherFactory { Linger = linger, MaxRequestsPerBatch = 1000 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: calls =>
        {
            completions.Enqueue((System.Diagnostics.Stopwatch.GetTimestamp(), calls.Count));
            return default;
        }));

        // Two lingering batches in a row: the timer must be re-armed for the second one
        for (int round = 0; round < 2; round++)
        {
            long start = System.Diagnostics.Stopwatch.GetTimestamp();
            await batcher.AddAsync(1);
            await batcher.AddAsync(2);
            await BatchTestUtil.Eventually(() => completions.Count == round + 1, TenSeconds, TimeSpan.FromMilliseconds(1));

            Assert.True(completions.TryPeek(out _));
            var (ticks, size) = completions.ToArray()[round];
            var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(start, ticks);
            Assert.Equal(2, size);
            // The timer never fires early (the loop compares the deadline with the clock itself)
            Assert.True(elapsed >= linger - TimeSpan.FromMilliseconds(5), $"completed after {elapsed}");
            Assert.True(elapsed < TimeSpan.FromSeconds(5), $"completed after {elapsed}");
        }
        batcher.Dispose();
    }

    [Fact]
    public async Task CanAddFalse_CompletesTheBatchAndStartsANewOne()
    {
        var batches = new List<List<object>>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(
            canAdd: (size, call) => size < 3,
            complete: calls =>
            {
                lock (batches) batches.Add(calls.ToList());
                return default;
            }));

        for (int i = 0; i < 7; i++)
        {
            await batcher.AddAsync(i);
        }
        // 7 calls, 3 per batch: two batches completed by CanAdd, the third is lingering
        await BatchTestUtil.Eventually(() => { lock (batches) return batches.Count == 2; }, TenSeconds, TimeSpan.FromMilliseconds(1));
        var barrierDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(new Barrier(() => barrierDone.TrySetResult()));
        await BatchTestUtil.Receive(barrierDone.Task, TenSeconds, "barrier");
        batcher.Dispose();

        lock (batches)
        {
            Assert.Equal(3, batches.Count);
            Assert.Equal(new object[] { 0, 1, 2 }, batches[0]);
            Assert.Equal(new object[] { 3, 4, 5 }, batches[1]);
            Assert.Equal(new object[] { 6 }, batches[2]);
        }
    }

    [Fact]
    public async Task Barrier_CompletesTheOpenBatchFirst_AndLaterCallsStartANewBatch()
    {
        var events = new List<string>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: calls =>
        {
            lock (events) events.Add("batch:" + string.Join(",", calls));
            return default;
        }));

        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(1);
        await batcher.AddAsync(2);
        await batcher.AddAsync(new Barrier(() => { lock (events) events.Add("barrier1"); }));
        await batcher.AddAsync(3);
        await batcher.AddAsync(new Barrier(() => { lock (events) events.Add("barrier2"); }));
        // A barrier with no open batch just runs
        await batcher.AddAsync(new Barrier(() => done.TrySetResult()));
        await BatchTestUtil.Receive(done.Task, TenSeconds, "barrier");
        batcher.Dispose();

        lock (events)
        {
            Assert.Equal(new[] { "batch:1,2", "barrier1", "batch:3", "barrier2" }, events);
        }
    }

    [Fact]
    public async Task AddAfterClose_FailsTheCallRightAway_WithoutAnyLoop()
    {
        var failures = new List<(List<object> Calls, Exception Error)>();
        var batcher = new BatcherImpl(() => new FakeBatch(fail: (calls, err) => failures.Add((calls.ToList(), err))), 4,
            TimeSpan.Zero, 10);

        batcher.Dispose();
        batcher.Dispose(); // idempotent
        await batcher.AddAsync("late");

        var failure = Assert.Single(failures);
        Assert.Equal(new object[] { "late" }, failure.Calls);
        Assert.True(OxiaErrors.Is(failure.Error, ClientErrors.ShuttingDown));
    }

    [Fact]
    public async Task RunAsync_CompletesOnceClosedAndDrained_AndIsIdempotent()
    {
        var failed = new ConcurrentBag<object>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(fail: (calls, err) =>
        {
            foreach (var c in calls) failed.Add(c);
        }));

        var run = batcher.RunAsync();
        Assert.Same(run, batcher.RunAsync());
        Assert.False(run.IsCompleted);

        await batcher.AddAsync(1);
        batcher.Dispose();
        await BatchTestUtil.Receive(run, TenSeconds, "the loop did not end");

        Assert.Equal(new object[] { 1 }, failed.ToArray());
    }

    [Fact]
    public async Task CloseRacingWithAdd_EveryCallEndsExactlyOnce()
    {
        for (int iteration = 0; iteration < 100; iteration++)
        {
            const int producers = 4, perProducer = 200;
            var ended = new int[producers * perProducer];
            var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(1), MaxRequestsPerBatch = 10, QueueCapacity = 2 };
            var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(
                complete: calls =>
                {
                    foreach (var c in calls) Interlocked.Increment(ref ended[(int)c]);
                    return default;
                },
                fail: (calls, err) =>
                {
                    foreach (var c in calls) Interlocked.Increment(ref ended[(int)c]);
                }));

            var start = new TaskCompletionSource();
            var tasks = new List<Task>();
            for (int p = 0; p < producers; p++)
            {
                int producer = p;
                tasks.Add(Task.Run(async () =>
                {
                    await start.Task;
                    for (int i = 0; i < perProducer; i++)
                    {
                        await batcher.AddAsync(producer * perProducer + i);
                    }
                }));
            }
            tasks.Add(Task.Run(async () =>
            {
                await start.Task;
                await Task.Delay(iteration % 5);
                batcher.Dispose();
            }));

            start.SetResult();
            await BatchTestUtil.Receive(Task.WhenAll(tasks), TenSeconds, "an add blocked after close");
            await BatchTestUtil.Receive(batcher.RunAsync(), TenSeconds, "the loop did not end");

            Assert.All(ended, count => Assert.Equal(1, count));
        }
    }

    [Fact]
    public async Task FullQueue_BlocksTheProducerUntilThereIsRoom()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = new BatcherFactory { MaxRequestsPerBatch = 1, QueueCapacity = 2 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(complete: async _ => await release.Task));
        var impl = (BatcherImpl)batcher;

        await batcher.AddAsync(0); // taken by the loop, blocked completing
        await BatchTestUtil.Eventually(() => impl.QueueCount == 0, TenSeconds, TimeSpan.FromMilliseconds(1));
        await batcher.AddAsync(1);
        await batcher.AddAsync(2);
        var blocked = batcher.AddAsync(3);
        Assert.False(blocked.IsCompleted);

        release.SetResult();
        await BatchTestUtil.Receive(blocked.AsTask(), TenSeconds, "the add was not released");
        batcher.Dispose();
    }

    [Fact]
    public async Task ABatchThatThrowsIsFailed_AndTheLoopSurvives()
    {
        var failed = new ConcurrentQueue<(object Call, Exception Error)>();
        var completed = new ConcurrentQueue<object>();
        var factory = new BatcherFactory { MaxRequestsPerBatch = 1 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new FakeBatch(
            complete: calls =>
            {
                if ((int)calls[0] == 1) throw new InvalidOperationException("boom");
                completed.Enqueue(calls[0]);
                return default;
            },
            fail: (calls, err) => failed.Enqueue((calls[0], err))));

        await batcher.AddAsync(0);
        await batcher.AddAsync(1);
        await batcher.AddAsync(2);
        await BatchTestUtil.Eventually(() => completed.Count == 2 && failed.Count == 1, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        Assert.Equal(new object[] { 0, 2 }, completed.ToArray());
        Assert.Equal(1, failed.Single().Call);
        Assert.IsType<InvalidOperationException>(failed.Single().Error);
    }
}
