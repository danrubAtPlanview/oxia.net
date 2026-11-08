// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/batch/batcher_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 4 (TestBatcherBarrier, TestBatcherCloseWithConcurrentAdds, TestBatcherCloseFailsOpenBatch, TestBatcher with
//              3 subtests)   ported: 4 (TestBatcher is a [Theory] with the 3 subtests as rows)   skipped: 0
//   Adaptations: Go channels used as one-shot signals are TaskCompletionSource; `batcher.Add` -> `await AddAsync`;
//              `batcherChannelBufferSize` / `len(queue) == cap(queue)` -> BatcherFactory.BatcherChannelBufferSize and the
//              internal BatcherImpl.QueueCount / QueueCapacity (the queue is a bounded Channel).  `Close()` is `Dispose()`.
//              Assertions are unchanged.
// =====================================================================================================================
using Oxia.Client.Batch;
using Oxia.Common.Constant;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier; // System.Threading.Barrier is imported by ImplicitUsings

namespace Oxia.Client.Tests.Batch;

public class BatcherTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    private sealed class TestBatch : IBatch
    {
        private int _count;
        public TaskCompletionSource<Exception?> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool CanAdd(object call) => true;
        public void Add(object call) => _count++;
        public int Size => _count;
        public ValueTask CompleteAsync()
        {
            Result.TrySetResult(null); // close(b.result)
            return default;
        }

        public void Fail(Exception error) => Result.TrySetResult(error);
    }

    /// <summary>recordingBatch appends its calls to completed when it completes.</summary>
    private sealed class RecordingBatch(List<object> completed) : IBatch
    {
        private readonly List<object> _calls = new();
        public bool CanAdd(object call) => true;
        public void Add(object call) => _calls.Add(call);
        public int Size => _calls.Count;
        public ValueTask CompleteAsync()
        {
            completed.AddRange(_calls);
            return default;
        }

        public void Fail(Exception error) { }
    }

    [Fact]
    public async Task TestBatcherBarrier()
    {
        var completed = new List<object>();
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test-write", () => new RecordingBatch(completed));

        // The barrier completes the batch with the calls added before it, without
        // waiting for the linger time
        var done = new TaskCompletionSource<object[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(1);
        await batcher.AddAsync(2);
        await batcher.AddAsync(new Barrier(() => done.TrySetResult(completed.ToArray())));
        var calls = await BatchTestUtil.Receive(done.Task, TenSeconds, "the barrier was not completed");
        Assert.Equal(new object[] { 1, 2 }, calls);
        batcher.Dispose();

        // A closed batcher completes the barriers right away
        var closedDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(new Barrier(() => closedDone.TrySetResult()));
        await BatchTestUtil.Receive(closedDone.Task, TenSeconds, "the barrier was not completed");
    }

    /// <summary>endingBatch reports its calls when it completes or fails. Completing it waits for release.</summary>
    private sealed class EndingBatch(Task release, Action<List<object>> ended) : IBatch
    {
        private readonly List<object> _calls = new();
        public bool CanAdd(object call) => true;
        public void Add(object call) => _calls.Add(call);
        public int Size => _calls.Count;
        public void Fail(Exception error) => ended(_calls);

        public async ValueTask CompleteAsync()
        {
            await release;
            ended(_calls);
        }
    }

    // Close fails the calls being added concurrently, also the ones blocked on a
    // full queue while the batcher waits for a batch to complete, and every call
    // is completed or failed exactly once.
    [Fact]
    public async Task TestBatcherCloseWithConcurrentAdds()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mutex = new object();
        var ended = new Dictionary<object, int>();
        var factory = new BatcherFactory { MaxRequestsPerBatch = 1 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test-write", () =>
            new EndingBatch(release.Task, calls =>
            {
                lock (mutex)
                {
                    foreach (var call in calls)
                    {
                        ended[call] = ended.GetValueOrDefault(call) + 1;
                    }
                }
            }));

        // The first call blocks the batcher, the next ones fill the queue, and the
        // last ones block adding to it
        int calls = 2 * BatcherFactory.BatcherChannelBufferSize + 2;
        var adding = new List<Task>();
        for (int i = 0; i < calls; i++)
        {
            int call = i;
            adding.Add(Task.Run(async () => await batcher.AddAsync(call)));
        }

        var impl = (BatcherImpl)batcher;
        await BatchTestUtil.Eventually(() => impl.QueueCount == impl.QueueCapacity, TenSeconds, TimeSpan.FromMilliseconds(1));

        batcher.Dispose();
        var added = Task.WhenAll(adding);
        await BatchTestUtil.Receive(added, TenSeconds, "adding a call blocked after the batcher was closed");

        release.SetResult();
        await BatchTestUtil.Eventually(() =>
        {
            lock (mutex) return ended.Count == calls;
        }, TenSeconds, TimeSpan.FromMilliseconds(1));
        lock (mutex)
        {
            foreach (var (call, count) in ended)
            {
                Assert.True(count == 1, $"call {call}: {count}");
            }
        }
    }

    /// <summary>signalingBatch signals on added when a call is added to it, and sends its failure on failed.</summary>
    private sealed class SignalingBatch(TaskCompletionSource added, TaskCompletionSource<Exception> failed) : IBatch
    {
        private int _calls;
        public bool CanAdd(object call) => true;
        public int Size => _calls;
        public ValueTask CompleteAsync() => default;
        public void Fail(Exception error) => failed.TrySetResult(error);

        public void Add(object call)
        {
            _calls++;
            added.TrySetResult();
        }
    }

    [Fact]
    public async Task TestBatcherCloseFailsOpenBatch()
    {
        var added = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        int batches = 0;
        var factory = new BatcherFactory { Linger = TimeSpan.FromHours(1), MaxRequestsPerBatch = 100 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test-write", () =>
        {
            Interlocked.Increment(ref batches);
            return new SignalingBatch(added, failed);
        });

        await batcher.AddAsync(1);
        await added.Task;
        batcher.Dispose();

        Assert.True(OxiaErrors.Is(await failed.Task, ClientErrors.ShuttingDown));
        // Run failed the batch it was lingering on, instead of failing the call in a new batch
        Assert.Equal(1, Volatile.Read(ref batches));
    }

    [Theory]
    [InlineData("complete on maxRequestsPerBatch", 1000, 1, false, false)]
    [InlineData("complete on linger", 1, 2, false, false)]
    [InlineData("fail on close", 1000, 2, true, true)]
    public async Task TestBatcher(string name, int lingerMillis, int maxSize, bool closeImmediately, bool expectShuttingDown)
    {
        _ = name;
        var testBatch = new TestBatch();
        var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(lingerMillis), MaxRequestsPerBatch = maxSize };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test-write", () => testBatch);
        await batcher.AddAsync(1);

        if (closeImmediately)
        {
            batcher.Dispose();
        }

        var err = await testBatch.Result.Task.WaitAsync(TenSeconds);
        if (expectShuttingDown)
        {
            Assert.True(OxiaErrors.Is(err, ClientErrors.ShuttingDown));
        }
        else
        {
            Assert.Null(err);
        }

        if (!closeImmediately)
        {
            batcher.Dispose();
        }
    }
}
