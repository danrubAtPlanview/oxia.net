// =====================================================================================================================
// REVIEW R1 -- reproduction tests.
//   Defect   : R1-2  An exception thrown by a metrics observer (MeterListener callback) escapes from the batch completion path.
//                    (a) a write the server applied is reported to the caller as FAILED (and not as "not sent"), and
//                    (b) the batcher then calls batch.Fail(e) on a batch some of whose calls were already completed and
//                        RECYCLED: the Fail completes the pooled call objects again -- which by then belong to OTHER operations
//                        (pool reuse / ABA) -- so an unrelated, not even sent, write is failed with the listener's exception.
//   Go       : oxia/internal/metrics/metrics.go -- OTel instruments never throw into the caller; write_batch.go:handle/Fail
//              complete every call exactly once.
//   C#       : src/Oxia.Client/Internal/Metrics/Metrics.cs:158-162 (MeteredPutCall.OnComplete: `_inner.Complete` THEN RecordOp),
//              :253-261 (RecordWrite) ; src/Oxia.Client/Internal/Batch/WriteBatch.cs:209-227 ; src/Oxia.Client/Batch/Batcher.cs:312-331
//              (CompleteAsync: `catch { batch.Fail(e); }` re-fails ALL calls) ; src/Oxia.Client/PooledCalls.cs:118-125 (Recycle
//              resets `_completed`, so a stale reference completes the next user of the object).
//   Expect   : observers cannot change the outcome of an operation.  These tests FAIL today.
// =====================================================================================================================
using System.Diagnostics.Metrics;
using System.Text;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Tests.F2;
using Oxia.Common.Proto;
using Xunit;
using MetricsImpl = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Review;

// The pooled call objects are process-wide statics and the listener sees every "oxia_client" Meter: run alone.
[CollectionDefinition("R1 serial", DisableParallelization = true)]
public sealed class R1SerialCollection;

[Collection("R1 serial")]
public class R1_MetricsListenerTests
{
    private sealed class OkExecutor : Oxia.Client.Internal.IExecutor
    {
        public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare)
        {
            var request = prepare();
            var response = new WriteResponse();
            foreach (var put in request.Puts)
            {
                response.Puts.Add(new PutResponse { Status = Status.Ok, Version = new ProtoVersion(), Key = put.Key });
            }
            return new ValueTask<WriteResponse>(response);
        }

        public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
        public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> c) => throw new NotImplementedException();
        public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> c) => throw new NotImplementedException();
    }

    private static AsyncClientImpl NewClient(TimeSpan linger, int maxRequestsPerBatch)
    {
        var c = new AsyncClientImpl { ShardManager = new SplittableShardManager("") };
        var bf = new BatcherFactory(new OkExecutor(), Oxia.Common.Constant.Constants.DefaultNamespace, linger, maxRequestsPerBatch,
            new MetricsImpl(null), TimeSpan.FromSeconds(30));
        bf.WriteRerouter = c.RerouteWrites;
        c.WriteBatchManager = new WriteManager(CancellationToken.None,
            (ct, shard) => bf.NewWriteBatcher(ct, shard, OxiaOptions.DefaultMaxBatchSize), c.ForwardWriteAsync);
        return c;
    }

    private static MeterListener Listen(Action<Instrument, double> onDouble)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == MetricsImpl.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) => onDouble(instrument, measurement));
        listener.SetMeasurementEventCallback<long>((_, _, _, _) => { });
        listener.Start();
        return listener;
    }

    /// <summary>(a) The write is applied by the server; the caller must not be told it failed because an observer threw.</summary>
    [Fact]
    public async Task AThrowingMetricsListener_DoesNotFailAWriteTheServerApplied()
    {
        var client = NewClient(TimeSpan.Zero, 1);
        using var listener = Listen((_, _) => throw new InvalidOperationException("listener boom"));
        try
        {
            var result = await F2Util.WithTimeout(client.PutAsync("k", Encoding.UTF8.GetBytes("v")).AsTask(), TimeSpan.FromSeconds(10));
            Assert.Null(result.Err);
        }
        finally
        {
            client.WriteBatchManager.Dispose();
        }
    }

    /// <summary>
    /// (b) Put A and Put B share a batch.  After A completed and its caller reused the pooled call object for put C, the observer
    /// throws while B is being completed: the batcher's catch-all Fail then fails "A" -- i.e. C, which was never sent.
    /// </summary>
    [Fact]
    public async Task AThrowingMetricsListener_DoesNotFailAnUnrelatedOperationThatReusedAPooledCall()
    {
        var client = NewClient(TimeSpan.FromMinutes(10), 2);
        using var released = new ManualResetEventSlim(false);
        int opMeasurements = 0;
        using var listener = Listen((instrument, _) =>
        {
            // one oxia_client_op measurement per operation, recorded right after its caller was woken up
            if (instrument.Name == "oxia_client_op" && Interlocked.Increment(ref opMeasurements) == 2)
            {
                released.Wait(TimeSpan.FromSeconds(10));
                throw new InvalidOperationException("listener boom");
            }
        });
        try
        {
            var a = client.PutAsync("a", Encoding.UTF8.GetBytes("1"));
            var b = client.PutAsync("b", Encoding.UTF8.GetBytes("2"));

            Assert.Null((await F2Util.WithTimeout(a.AsTask(), TimeSpan.FromSeconds(10))).Err); // A is consumed: its object is back in the pool
            var c = client.PutAsync("c", Encoding.UTF8.GetBytes("3"));                       // ... and C (LIFO pool) takes it over
            released.Set();                                                                  // now the observer throws while B completes

            // C sits in an open batch (linger 10 min, 1 of 2 slots used): nothing may complete it
            var cTask = c.AsTask();
            var winner = await Task.WhenAny(cTask, Task.Delay(500));
            if (winner == cTask)
            {
                var r = await cTask;
                Assert.Fail("put C was never sent but it completed with: " + r.Err?.Message);
            }
        }
        finally
        {
            released.Set();
            client.WriteBatchManager.Dispose();
        }
    }
}
