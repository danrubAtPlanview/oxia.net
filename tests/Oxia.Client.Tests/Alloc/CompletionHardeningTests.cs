// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : oxia/internal/batch/write_batch.go, read_batch.go (handle / Fail: every call is completed exactly once)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wave 2 / Z1 (+ the R1 review, finding R1-2).  Completing a call runs code of the application (a Callback, a metrics
//              observer).  An exception from it must
//                * not stop the batch from completing the OTHER calls,
//                * not reach BatcherImpl.CompleteAsync's catch-all `batch.Fail(e)`, which would complete EVERY call of the batch again --
//                  calls that were completed already and, with pooled calls, may be recycled and owned by another operation by now;
//              and an exception from the rerouter (which owns the calls from the moment it is invoked) must not do that either.
//              Also pins the ownership protocol of the response messages at these seams: a batch releases its response after the calls
//              took what they need, and a response that is not released is never handed out to anybody else.
//   Divergences: n/a (new tests).
// =====================================================================================================================
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Alloc;

// (the pooled responses are process-wide: tests that inspect a released response run alone)
[Collection("Z1 serial")]
public class CompletionHardeningTests
{
    private static WriteBatchFactory WriteFactory(Func<WriteRequest, WriteResponse> answer, WriteRerouter? reroute = null) => new()
    {
        Metrics = new OxiaMetrics(null),
        MaxByteSize = 1 << 20,
        RequestTimeout = TimeSpan.FromSeconds(30),
        Reroute = reroute,
        Execute = (ct, shard, prepare) => ValueTask.FromResult(answer(prepare())),
    };

    private static WriteResponse Ok(WriteRequest request)
    {
        var response = WriteResponse.Rent(); // like the transport: a pooled response
        for (int i = 0; i < request.Puts.Count; i++) response.Puts.Add(new PutResponse { Status = Status.Ok });
        for (int i = 0; i < request.Deletes.Count; i++) response.Deletes.Add(new DeleteResponse { Status = Status.Ok });
        return response;
    }

    [Fact]
    public async Task AThrowingCallback_DoesNotStopTheOtherCallsOfAWriteBatch_NorMakesCompleteAsyncThrow()
    {
        var factory = WriteFactory(Ok);
        var batch = factory.NewBatch(1);
        var completions = new List<string>();
        batch.Add(new PutCall { Key = "a", Callback = (r, e) => completions.Add("a:" + (e is null ? "ok" : "err")) });
        batch.Add(new PutCall { Key = "b", Callback = (r, e) => throw new InvalidOperationException("callback boom") });
        batch.Add(new DeleteCall { Key = "c", Callback = (r, e) => completions.Add("c:" + (e is null ? "ok" : "err")) });
        batch.Add(new PutCall { Key = "d", Callback = (r, e) => completions.Add("d:" + (e is null ? "ok" : "err")) });

        await batch.CompleteAsync(); // does not throw

        Assert.Contains("a:ok", completions);
        Assert.Contains("c:ok", completions);
        Assert.Contains("d:ok", completions);
        Assert.Equal(3, completions.Count); // each exactly once: no call was completed a second time with the callback's exception
    }

    [Fact]
    public async Task AThrowingCallback_DoesNotStopTheOtherCallsOfAFailingWriteBatch()
    {
        var failure = new Exception("server down");
        var factory = new WriteBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 1 << 20,
            RequestTimeout = TimeSpan.FromSeconds(30),
            Execute = (ct, shard, prepare) => { prepare(); return ValueTask.FromException<WriteResponse>(failure); },
        };
        var batch = factory.NewBatch(1);
        int completed = 0;
        for (int i = 0; i < 5; i++)
        {
            int index = i;
            batch.Add(new PutCall
            {
                Key = "k" + i,
                Callback = (r, e) =>
                {
                    Interlocked.Increment(ref completed);
                    if (index == 1) throw new InvalidOperationException("callback boom");
                },
            });
        }

        await batch.CompleteAsync();

        Assert.Equal(5, completed);
    }

    [Fact]
    public async Task AThrowingCallback_DoesNotStopTheOtherGetsOfAReadBatch()
    {
        var factory = new ReadBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(30),
            Execute = (ct, request) =>
            {
                var response = ReadResponse.Rent();
                for (int i = 0; i < request.Gets.Count; i++) response.Gets.Add(new GetResponse { Status = Status.KeyNotFound });
                return ValueTask.FromResult(response);
            },
        };
        var batch = factory.NewBatch(1);
        var seen = new List<int>();
        for (int i = 0; i < 4; i++)
        {
            int index = i;
            batch.Add(new GetCall
            {
                Key = "k" + i,
                Callback = (r, e) =>
                {
                    seen.Add(index);
                    if (index == 0) throw new InvalidOperationException("callback boom");
                },
            });
        }

        await batch.CompleteAsync();

        Assert.Equal([0, 1, 2, 3], seen);
    }

    [Fact]
    public async Task AThrowingRerouter_DoesNotCompleteTheCallsItOwns()
    {
        var shardNotFound = OxiaErrors.ToThrowable(OxiaErrors.ShardNotFound);
        int rerouted = 0;
        var factory = new WriteBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 1 << 20,
            RequestTimeout = TimeSpan.FromSeconds(30),
            Execute = (ct, shard, prepare) => { prepare(); return ValueTask.FromException<WriteResponse>(shardNotFound); },
            Reroute = (shard, puts, deletes, deleteRanges) =>
            {
                // the first call is handed to a successor, then the rerouter fails
                rerouted++;
                throw new InvalidOperationException("reroute boom");
            },
        };
        var batch = factory.NewBatch(1);
        int completions = 0;
        for (int i = 0; i < 3; i++) batch.Add(new PutCall { Key = "k" + i, Callback = (r, e) => Interlocked.Increment(ref completions) });

        await batch.CompleteAsync(); // does not throw: the batcher's catch-all would fail all three calls again

        Assert.Equal(1, rerouted);
        Assert.Equal(0, completions); // the batch no longer owns them: it completed none
    }

    [Fact]
    public async Task AThrowingReadRerouter_DoesNotCompleteTheGetsItOwns()
    {
        var shardNotFound = OxiaErrors.ToThrowable(OxiaErrors.ShardNotFound);
        var factory = new ReadBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(30),
            Execute = (ct, request) => ValueTask.FromException<ReadResponse>(shardNotFound),
            Reroute = gets => throw new InvalidOperationException("reroute boom"),
        };
        var batch = factory.NewBatch(1);
        int completions = 0;
        batch.Add(new GetCall { Key = "k", Callback = (r, e) => Interlocked.Increment(ref completions) });

        await batch.CompleteAsync();

        Assert.Equal(0, completions);
    }

    [Fact]
    public async Task TheBatcherSurvivesAThrowingCallback_AndCompletesEveryCallExactlyOnce()
    {
        // through the real batcher: 2 calls per batch, the second call of each batch throws from its callback
        var executor = new AnsweringExecutor();
        var batcherFactory = new BatcherFactory(executor, Constants.DefaultNamespace, TimeSpan.FromMinutes(10), 2, new OxiaMetrics(null),
            TimeSpan.FromSeconds(30));
        var batcher = batcherFactory.NewWriteBatcher(CancellationToken.None, 0, 1 << 20);
        var counts = new int[6];
        var done = new SemaphoreSlim(0);
        for (int i = 0; i < 6; i++)
        {
            int index = i;
            await batcher.AddAsync(new PutCall
            {
                Key = "k" + i,
                Callback = (r, e) =>
                {
                    Interlocked.Increment(ref counts[index]);
                    done.Release();
                    if (index % 2 == 1) throw new InvalidOperationException("callback boom");
                },
            });
        }

        for (int i = 0; i < 6; i++) Assert.True(await done.WaitAsync(TimeSpan.FromSeconds(10)), "a call was never completed");
        await Task.Delay(100); // a wrong second completion would show up now
        Assert.All(counts, c => Assert.Equal(1, c));
        batcher.Dispose();
    }

    private sealed class AnsweringExecutor : Oxia.Client.Internal.IExecutor
    {
        public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) =>
            ValueTask.FromResult(Ok(prepare()));

        public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
        public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> c) => throw new NotImplementedException();
        public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> c) => throw new NotImplementedException();
    }

    // ----- ownership of the response messages ----------------------------------------------------------------------------

    [Fact]
    public async Task TheBatchReleasesItsPooledResponse_AfterTheCallsCompleted()
    {
        WriteResponse? seen = null;
        var factory = WriteFactory(request => seen = Ok(request));
        var batch = factory.NewBatch(1);
        batch.Add(new PutCall { Key = "a", Callback = (r, e) => { Assert.Null(e); Assert.NotNull(r); } });

        await batch.CompleteAsync();

        Assert.NotNull(seen);
        Assert.Equal(PoolStates.Pooled, seen!.PoolState); // released: it is in the pool now
    }

    [Fact]
    public async Task TheBatchLeavesAPlainResponseAlone()
    {
        var plain = new WriteResponse();
        plain.Puts.Add(new PutResponse { Status = Status.Ok });
        var factory = WriteFactory(_ => plain);
        var batch = factory.NewBatch(1);
        batch.Add(new PutCall { Key = "a" });

        await batch.CompleteAsync();

        Assert.Equal(PoolStates.Plain, plain.PoolState);
        Assert.Single(plain.Puts); // untouched: a fake that keeps its response can still inspect it
    }
}
