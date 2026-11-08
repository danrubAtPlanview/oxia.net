// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : -
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : the steady-state add -> complete path of the pooled batches must not allocate: DESIGN.md "Performance rules".
//              Deterministic where it can be (everything on the current thread: GC.GetAllocatedBytesForCurrentThread), a
//              process-wide budget for the end-to-end lock-step ping-pong through the real batcher loop (the collection runs
//              alone: no other test class runs while it measures).
//   NOTE     : only meaningful in an OPTIMIZED build (`dotnet test -c Release`): the C# compiler turns async methods into
//              classes in Debug, which allocate a state machine per call.  In Debug the tests skip themselves.
// =====================================================================================================================
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Threading.Tasks.Sources;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Batch;

[CollectionDefinition("Allocations", DisableParallelization = true)]
public class AllocationsCollection;

[Collection("Allocations")]
public class AllocationTests(ITestOutputHelper output)
{
    private static void RequireOptimizedBuild()
    {
        var debuggable = typeof(BatcherImpl).Assembly.GetCustomAttribute<DebuggableAttribute>();
        Assert.SkipWhen(debuggable?.IsJITOptimizerDisabled == true,
            "allocation assertions need an optimized build (async state machines are classes in Debug): run `dotnet test -c Release`");
    }

    /// <summary>A pooled put whose completion resolves a ValueTask (what the async client will hand to the batcher).</summary>
    private sealed class PooledPut : PutCall, IValueTaskSource<PutResponse?>
    {
        private ManualResetValueTaskSourceCore<PutResponse?> _core;

        public ValueTask<PutResponse?> Task => new(this, _core.Version);

        public override void Complete(PutResponse? response, Exception? error)
        {
            if (error is null) _core.SetResult(response);
            else _core.SetException(error);
        }

        public PutResponse? GetResult(short token)
        {
            try
            {
                return _core.GetResult(token);
            }
            finally
            {
                _core.Reset();
            }
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    /// <summary>An executor that answers synchronously with a cached response.</summary>
    private static FakeExecutor SyncExecutor()
    {
        var response = new WriteResponse();
        response.Puts.Add(new PutResponse { Status = Status.Ok });
        var cached = ValueTask.FromResult(response);
        return new FakeExecutor
        {
            Write = (ct, shard, prepare) =>
            {
                prepare();
                return cached;
            },
        };
    }

    private sealed class CountingBatch : IBatch
    {
        public bool CanAdd(object call) => true;
        public void Add(object call) { }
        public int Size => 0;
        public ValueTask CompleteAsync() => default;
        public void Fail(Exception error) { }
    }

    [Fact]
    public async Task BatcherAddFastPath_DoesNotAllocate()
    {
        RequireOptimizedBuild();

        // linger 0, one call per batch, a loop that consumes the bursts below
        var factory = new Oxia.Client.Batch.BatcherFactory { Linger = TimeSpan.Zero, MaxRequestsPerBatch = 1, QueueCapacity = 4096 };
        var batcher = factory.NewBatcher(CancellationToken.None, 1, "test", () => new CountingBatch());
        var impl = (BatcherImpl)batcher;
        object call = new object();

        int notCompleted = 0;
        long Burst()
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1000; i++)
            {
                if (!batcher.AddAsync(call).IsCompletedSuccessfully) notCompleted++;
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        async Task Drain()
        {
            while (impl.QueueCount > 0) await Task.Delay(1);
        }

        // warm-up: the buffer of the channel grows up to the burst size
        for (int round = 0; round < 5; round++)
        {
            Burst();
            await Drain();
        }

        long allocated = 0;
        for (int round = 0; round < 20; round++)
        {
            allocated += Burst();
            await Drain();
        }
        batcher.Dispose();

        output.WriteLine($"AddAsync fast path: {allocated} bytes for 20000 adds (queue full {notCompleted} times)");
        Assert.Equal(0, notCompleted);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void WriteBatchCycle_AddCompleteRecycle_DoesNotAllocateInSteadyState()
    {
        RequireOptimizedBuild();
        var factory = new WriteBatchFactory
        {
            Execute = SyncExecutor().ExecuteWrite,
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(30),
            MaxByteSize = 1 << 20,
        };
        var call = new PooledPut { Key = "key", Value = new byte[16] };
        int failures = 0;

        void Cycle()
        {
            var batch = factory.NewBatch(1);
            batch.Add(call);
            var t = batch.CompleteAsync();
            if (!t.IsCompletedSuccessfully) failures++;
            batch.Recycle();
            var result = call.Task;
            if (!result.IsCompletedSuccessfully) failures++;
            result.GetAwaiter().GetResult();
        }

        for (int i = 0; i < 1000; i++) Cycle(); // warm-up: JIT, pool, timeout source

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10000; i++) Cycle();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        output.WriteLine($"write batch cycle: {allocated} bytes for 10000 batches");
        Assert.Equal(0, failures);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void ReadBatchCycle_AddCompleteRecycle_DoesNotAllocateInSteadyState()
    {
        RequireOptimizedBuild();
        var response = new ReadResponse();
        response.Gets.Add(new GetResponse { Status = Status.Ok });
        var cached = ValueTask.FromResult(response);
        var factory = new ReadBatchFactory
        {
            Execute = (ct, request) => cached,
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(30),
        };
        var call = new GetCall { Key = "key", Callback = static (_, _) => { } };
        int failures = 0;

        void Cycle()
        {
            var batch = factory.NewBatch(1);
            batch.Add(call);
            var t = batch.CompleteAsync();
            if (!t.IsCompletedSuccessfully) failures++;
            batch.Recycle();
        }

        for (int i = 0; i < 1000; i++) Cycle();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10000; i++) Cycle();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        output.WriteLine($"read batch cycle: {allocated} bytes for 10000 batches");
        Assert.Equal(0, failures);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void MetricsRecording_DoesNotAllocate_WithAndWithoutListeners()
    {
        RequireOptimizedBuild();
        using var meterFactory = new Metrics.TestMeterFactory();
        var metrics = new OxiaMetrics(meterFactory);
        var request = new WriteRequest();
        request.Puts.Add(new PutRequest { Value = new byte[10] });
        var response = new ReadResponse();
        response.Gets.Add(new GetResponse { Value = new byte[3] });
        var failure = new Exception();

        void Record()
        {
            metrics.RecordWrite(1, 2, request, null);
            metrics.RecordRead(1, 2, response, null);
            metrics.RecordWrite(1, 2, request, failure);
        }

        for (int i = 0; i < 100; i++) Record();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++) Record();
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);

        // with a listener that does not allocate itself
        long measurements = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, metrics.Meter)) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((i, v, tags, s) => measurements++);
        listener.SetMeasurementEventCallback<double>((i, v, tags, s) => measurements++);
        listener.Start();

        for (int i = 0; i < 100; i++) Record();
        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++) Record();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        output.WriteLine($"metrics recording with a listener: {allocated} bytes, {measurements} measurements");
        Assert.True(measurements > 0);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public async Task EndToEnd_LockStepPingPongThroughTheBatcherLoop_StaysWithinAByteBudget()
    {
        RequireOptimizedBuild();
        var factory = new Oxia.Client.Internal.Batch.BatcherFactory(SyncExecutor(), "default", TimeSpan.Zero, 100, new OxiaMetrics(null),
            TimeSpan.FromSeconds(30));
        var batcher = factory.NewWriteBatcher(CancellationToken.None, 1, 1 << 20);
        var call = new PooledPut { Key = "key", Value = new byte[16] };

        async Task Run(int count)
        {
            for (int i = 0; i < count; i++)
            {
                await batcher.AddAsync(call);
                await call.Task;
            }
        }

        await Run(20000); // warm-up
        GC.Collect();
        long before = GC.GetTotalAllocatedBytes(precise: true);
        const int ops = 100000;
        await Run(ops);
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        batcher.Dispose();

        output.WriteLine($"end to end: {allocated} bytes for {ops} operations ({(double)allocated / ops:F2} B/op)");
        // Budget: the loop, the batch, the request, the timeout source and the call are all reused.  What is left (measured:
        // ~0.02 B/op) is the occasional box of an async state machine when the producer / the loop actually suspend.
        Assert.True(allocated < ops, $"{allocated} bytes for {ops} operations");
    }
}
