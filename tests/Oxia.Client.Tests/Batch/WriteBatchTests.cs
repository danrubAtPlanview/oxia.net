// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/write_batch_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 10 (TestWriteBatchAdd [4 cases], TestWriteBatchComplete [3 cases], TestWriteBatchRerouteOnShardDeleted,
//              TestWriteBatchOpIndex, TestWriteBatchNoRerouteOnOtherError, TestWriteBatchCanAdd [3 subtests],
//              TestWriteBatchCanAddPanicsOnRead, TestWriteBatchDropsTheCallsWhoseCallerGaveUp,
//              TestWriteBatchWithAllTheCallsDropped, TestWriteBatchFailureTellsIfTheRequestWasSent [2 subtests])
//              ported: 10   skipped: 0
//   Adaptations: `metrics.NewMetrics(noop.NewMeterProvider())` -> `new Metrics(null)` (nobody listens: a no-op, like the noop
//              provider); `batch.Complete()` -> `await batch.CompleteAsync()`; the executor's `(response, err)` is a
//              ValueTask<WriteResponse> that throws and `prepare func() (*WriteRequest, error)` is a Func<WriteRequest> that
//              throws; Go's struct equality on the request / responses -> ProtoAssert.Equal (field by field);
//              `assert.ErrorIs(err, context.Canceled)` -> an OperationCanceledException in the error chain; context.WithCancel
//              -> CancellationTokenSource; context.DeadlineExceeded -> a TimeoutException instance (matched by reference);
//              `assert.PanicsWithValue(t, "invalid call", ...)` -> InvalidOperationException("invalid call").
//              The batch is not recycled by these tests (nobody gives it back), so the request captured by the fake executors
//              can be inspected after CompleteAsync exactly like in Go.
// =====================================================================================================================
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Batch;

public class WriteBatchTests
{
    public static TheoryData<string, bool, int> AddCases() => new()
    {
        { nameof(PutCall), false, 1 },
        { nameof(DeleteCall), false, 1 },
        { nameof(DeleteRangeCall), false, 1 },
        { nameof(GetCall), true, 0 },
    };

    [Theory]
    [MemberData(nameof(AddCases))]
    public void TestWriteBatchAdd(string callType, bool expectPanic, int expectedSize)
    {
        object call = callType switch
        {
            nameof(PutCall) => new PutCall(),
            nameof(DeleteCall) => new DeleteCall(),
            nameof(DeleteRangeCall) => new DeleteRangeCall(),
            _ => new GetCall(),
        };
        var factory = new WriteBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        bool panicked = UtilsTests.Add(batch, call);

        Assert.True(expectPanic == panicked, callType);
        Assert.True(expectedSize == batch.Size, callType);
    }

    public static TheoryData<int> CompleteCases() => new() { 0, 1, 2 };

    [Theory]
    [MemberData(nameof(CompleteCases))]
    public async Task TestWriteBatchComplete(int caseIndex)
    {
        var errFailure = new Exception("failure");
        var putResponseOk = new PutResponse
        {
            Status = Status.Ok,
            Version = new ProtoVersion
            {
                VersionId = 1,
                CreatedTimestamp = 2,
                ModifiedTimestamp = 3,
                ModificationsCount = 1,
            },
        };

        WriteResponse? response;
        Exception? err;
        PutResponse? expectedPutResponse;
        DeleteResponse? expectedDeleteResponse;
        DeleteRangeResponse? expectedDeleteRangeResponse;
        Exception? expectedErr;
        switch (caseIndex)
        {
            case 0:
                response = new WriteResponse();
                response.Puts.Add(putResponseOk);
                response.Deletes.Add(new DeleteResponse { Status = Status.Ok });
                response.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
                err = null;
                expectedPutResponse = putResponseOk;
                expectedDeleteResponse = new DeleteResponse { Status = Status.Ok };
                expectedDeleteRangeResponse = new DeleteRangeResponse { Status = Status.Ok };
                expectedErr = null;
                break;
            case 1:
                response = new WriteResponse();
                response.Puts.Add(new PutResponse { Status = Status.UnexpectedVersionId });
                response.Deletes.Add(new DeleteResponse { Status = Status.KeyNotFound });
                response.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
                err = null;
                expectedPutResponse = new PutResponse { Status = Status.UnexpectedVersionId };
                expectedDeleteResponse = new DeleteResponse { Status = Status.KeyNotFound };
                expectedDeleteRangeResponse = new DeleteRangeResponse { Status = Status.Ok };
                expectedErr = null;
                break;
            default:
                response = null;
                err = errFailure;
                expectedPutResponse = null;
                expectedDeleteResponse = null;
                expectedDeleteRangeResponse = null;
                expectedErr = errFailure;
                break;
        }

        ValueTask<WriteResponse> Execute(CancellationToken ct, long shard, Func<WriteRequest> prepare)
        {
            var request = prepare();
            var expected = new WriteRequest { Shard = UtilsTests.ShardId };
            expected.Puts.Add(new PutRequest
            {
                Key = "/a",
                Value = new byte[] { 0 },
                ExpectedVersionId = UtilsTests.One,
            });
            expected.Deletes.Add(new DeleteRequest
            {
                Key = "/b",
                ExpectedVersionId = UtilsTests.Two,
                OpIndex = 1,
            });
            expected.DeleteRanges.Add(new DeleteRangeRequest
            {
                StartInclusive = "/callC",
                EndExclusive = "/d",
                OpIndex = 2,
            });
            ProtoAssert.Equal(expected, request);
            return err is not null ? ValueTask.FromException<WriteResponse>(err) : ValueTask.FromResult(response!);
        }

        var factory = new WriteBatchFactory
        {
            Execute = Execute,
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        int callbacks = 0;
        PutResponse? putResponse = null;
        Exception? putErr = null;
        DeleteResponse? deleteResponse = null;
        Exception? deleteErr = null;
        DeleteRangeResponse? deleteRangeResponse = null;
        Exception? deleteRangeErr = null;

        batch.Add(new PutCall
        {
            Key = "/a",
            Value = new byte[] { 0 },
            ExpectedVersionId = UtilsTests.One,
            Callback = (r, e) => { putResponse = r; putErr = e; callbacks++; },
        });
        batch.Add(new DeleteCall
        {
            Key = "/b",
            ExpectedVersionId = UtilsTests.Two,
            Callback = (r, e) => { deleteResponse = r; deleteErr = e; callbacks++; },
        });
        batch.Add(new DeleteRangeCall
        {
            MinKeyInclusive = "/callC",
            MaxKeyExclusive = "/d",
            Callback = (r, e) => { deleteRangeResponse = r; deleteRangeErr = e; callbacks++; },
        });
        Assert.Equal(3, batch.Size);

        await batch.CompleteAsync();

        Assert.Equal(3, callbacks);

        ProtoAssert.Equal(expectedPutResponse, putResponse);
        AssertErrorIs(putErr, expectedErr);

        ProtoAssert.Equal(expectedDeleteResponse, deleteResponse);
        AssertErrorIs(deleteErr, expectedErr);

        ProtoAssert.Equal(expectedDeleteRangeResponse, deleteRangeResponse);
        AssertErrorIs(deleteRangeErr, expectedErr);
    }

    /// <summary>assert.ErrorIs(t, err, target) -- a nil target requires a nil error.</summary>
    private static void AssertErrorIs(Exception? err, Exception? target)
    {
        if (target is null)
        {
            Assert.Null(err);
        }
        else
        {
            Assert.True(OxiaErrors.Is(err, target), $"expected {target}, actual {err}");
        }
    }

    [Fact]
    public async Task TestWriteBatchRerouteOnShardDeleted()
    {
        int executeCount = 0;

        ValueTask<WriteResponse> Execute(CancellationToken ct, long shard, Func<WriteRequest> prepare)
        {
            executeCount++;
            return ValueTask.FromException<WriteResponse>(OxiaErrors.ShardNotFound);
        }

        long reroutedShardId = 0;
        List<PutCall>? reroutedPuts = null;
        List<DeleteCall>? reroutedDeletes = null;
        List<DeleteRangeCall>? reroutedDeleteRanges = null;

        var factory = new WriteBatchFactory
        {
            Execute = Execute,
            Reroute = (id, puts, deletes, deleteRanges) =>
            {
                reroutedShardId = id;
                reroutedPuts = puts;
                reroutedDeletes = deletes;
                reroutedDeleteRanges = deleteRanges;
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        static void PutCallback(PutResponse? r, Exception? e) { }
        static void DeleteCallback(DeleteResponse? r, Exception? e) { }
        static void DeleteRangeCallback(DeleteRangeResponse? r, Exception? e) { }

        batch.Add(new PutCall { Key = "key-1", Value = "v1"u8.ToArray(), Callback = PutCallback });
        batch.Add(new PutCall { Key = "key-2", Value = "v2"u8.ToArray(), Callback = PutCallback });
        batch.Add(new DeleteCall { Key = "key-3", Callback = DeleteCallback });
        batch.Add(new DeleteRangeCall { MinKeyInclusive = "a", MaxKeyExclusive = "z", Callback = DeleteRangeCallback });

        await batch.CompleteAsync();

        var puts = reroutedPuts!;
        var deletes = reroutedDeletes!;
        var deleteRanges = reroutedDeleteRanges!;
        Assert.Equal(UtilsTests.ShardId, reroutedShardId);
        Assert.Equal(2, puts.Count);
        Assert.Equal("key-1", puts[0].Key);
        Assert.Equal("key-2", puts[1].Key);
        Assert.Single(deletes);
        Assert.Equal("key-3", deletes[0].Key);
        Assert.Single(deleteRanges);
        Assert.Equal(1, executeCount);

        // The rerouted calls keep their op index, to be re-added in the same order
        Assert.Equal(0u, puts[0].OpIndex);
        Assert.Equal(1u, puts[1].OpIndex);
        Assert.Equal(2u, deletes[0].OpIndex);
        Assert.Equal(3u, deleteRanges[0].OpIndex);
    }

    [Fact]
    public async Task TestWriteBatchOpIndex()
    {
        WriteRequest? request = null;
        var factory = new WriteBatchFactory
        {
            Execute = (ct, shard, prepare) =>
            {
                request = prepare();
                return ValueTask.FromException<WriteResponse>(new Exception("failure"));
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        static void PutCallback(PutResponse? r, Exception? e) { }
        static void DeleteCallback(DeleteResponse? r, Exception? e) { }
        static void DeleteRangeCallback(DeleteRangeResponse? r, Exception? e) { }

        batch.Add(new PutCall { Key = "a", Callback = PutCallback });
        batch.Add(new DeleteCall { Key = "a", Callback = DeleteCallback });
        batch.Add(new PutCall { Key = "a", Callback = PutCallback });
        batch.Add(new DeleteRangeCall { MinKeyInclusive = "a", MaxKeyExclusive = "b", Callback = DeleteRangeCallback });
        batch.Add(new DeleteCall { Key = "a", Callback = DeleteCallback });

        await batch.CompleteAsync();

        Assert.NotNull(request);
        Assert.Equal(2, request.Puts.Count);
        Assert.Equal(0u, request.Puts[0].OpIndex);
        Assert.Equal(2u, request.Puts[1].OpIndex);
        Assert.Equal(2, request.Deletes.Count);
        Assert.Equal(1u, request.Deletes[0].OpIndex);
        Assert.Equal(4u, request.Deletes[1].OpIndex);
        Assert.Single(request.DeleteRanges);
        Assert.Equal(3u, request.DeleteRanges[0].OpIndex);
    }

    [Fact]
    public async Task TestWriteBatchNoRerouteOnOtherError()
    {
        int callCount = 0;
        ValueTask<WriteResponse> Execute(CancellationToken ct, long shard, Func<WriteRequest> prepare)
        {
            callCount++;
            prepare();
            if (callCount == 1)
            {
                return ValueTask.FromException<WriteResponse>(OxiaErrors.InvalidStatus);
            }
            var response = new WriteResponse();
            response.Puts.Add(new PutResponse { Status = Status.Ok, Version = new ProtoVersion { VersionId = 1 } });
            return ValueTask.FromResult(response);
        }

        bool rerouted = false;
        var factory = new WriteBatchFactory
        {
            Execute = Execute,
            Reroute = (id, puts, deletes, deleteRanges) => rerouted = true,
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        int callbacks = 0;
        batch.Add(new PutCall
        {
            Key = "key-1",
            Value = "v1"u8.ToArray(),
            Callback = (resp, err) =>
            {
                Assert.Null(resp);
                Assert.True(OxiaErrors.Is(err, OxiaErrors.InvalidStatus));
                callbacks++;
            },
        });

        await batch.CompleteAsync();

        Assert.Equal(1, callbacks);
        Assert.False(rerouted);
        Assert.Equal(1, callCount);
    }

    [Theory]
    [InlineData("larger than maxBatchSize", 128, false)]
    [InlineData("add to next", 50, false)]
    [InlineData("add to current", 1, true)]
    public void TestWriteBatchCanAdd(string name, int dataSize, bool expectCanAdd)
    {
        _ = name;
        var factory = new WriteBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 100,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);
        batch.Add(new PutCall
        {
            Key = "a",
            Value = new byte[50],
        });

        bool canAdd = batch.CanAdd(new PutCall
        {
            Key = "b",
            Value = new byte[dataSize],
        });

        Assert.Equal(expectCanAdd, canAdd);
    }

    [Fact]
    public void TestWriteBatchCanAddPanicsOnRead()
    {
        var factory = new WriteBatchFactory
        {
            Metrics = new OxiaMetrics(null),
            MaxByteSize = 100,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        var panic = Assert.Throws<InvalidOperationException>(() => batch.CanAdd(new GetCall { Key = "a" }));
        Assert.Equal("invalid call", panic.Message);
    }

    // A call whose caller gave up on it before the request is first handed to gRPC
    // is dropped: it fails with an error telling that it was not sent, and the
    // other calls are sent without it, keeping their op index.
    [Fact]
    public async Task TestWriteBatchDropsTheCallsWhoseCallerGaveUp()
    {
        using var gaveUpCts = new CancellationTokenSource();
        gaveUpCts.Cancel();
        using var waitingCts = new CancellationTokenSource();
        var gaveUpCall = new CallContext(gaveUpCts.Token);
        var waitingCall = new CallContext(waitingCts.Token);

        WriteRequest? request = null;
        var factory = new WriteBatchFactory
        {
            Execute = (ct, shard, prepare) =>
            {
                request = prepare();
                var response = new WriteResponse();
                response.Puts.Add(new PutResponse { Status = Status.Ok });
                response.Puts.Add(new PutResponse { Status = Status.Ok });
                response.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
                return ValueTask.FromResult(response);
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        var results = new Dictionary<string, Exception?>();
        Action<PutResponse?, Exception?> PutCallback(string key) => (_, err) => results[key] = err;
        batch.Add(new PutCall { Key = "a", Callback = PutCallback("a") });
        batch.Add(new PutCall { Key = "b", CallContext = gaveUpCall, Callback = PutCallback("b") });
        batch.Add(new PutCall { Key = "c", CallContext = waitingCall, Callback = PutCallback("c") });
        batch.Add(new DeleteCall
        {
            Key = "d",
            CallContext = gaveUpCall,
            Callback = (_, err) => results["d"] = err,
        });
        batch.Add(new DeleteRangeCall
        {
            MinKeyInclusive = "e",
            MaxKeyExclusive = "f",
            CallContext = waitingCall,
            Callback = (_, err) => results["e"] = err,
        });
        await batch.CompleteAsync();

        Assert.NotNull(request);
        Assert.Equal(2, request.Puts.Count);
        Assert.Equal("a", request.Puts[0].Key);
        Assert.Equal(0u, request.Puts[0].OpIndex);
        Assert.Equal("c", request.Puts[1].Key);
        Assert.Equal(2u, request.Puts[1].OpIndex);
        Assert.Empty(request.Deletes);
        Assert.Single(request.DeleteRanges);
        Assert.Equal(4u, request.DeleteRanges[0].OpIndex);

        Assert.Equal(5, results.Count);
        foreach (var key in new[] { "a", "c", "e" })
        {
            Assert.True(results[key] is null, key);
        }
        foreach (var key in new[] { "b", "d" })
        {
            Assert.True(Model.CallContextTests.IsCanceled(results[key]), key);
            Assert.True(CallContext.IsNotSent(results[key]), key);
        }

        // The calls that were sent cannot be dropped anymore
        waitingCts.Cancel();
        Assert.False(CallContext.IsNotSent(waitingCall.Cancel()));
    }

    [Fact]
    public async Task TestWriteBatchWithAllTheCallsDropped()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var callContext = new CallContext(cts.Token);

        Exception? prepareErr = null;
        var factory = new WriteBatchFactory
        {
            Execute = (ct, shard, prepare) =>
            {
                try
                {
                    prepare();
                }
                catch (Exception e)
                {
                    prepareErr = e;
                    return ValueTask.FromException<WriteResponse>(e);
                }
                return ValueTask.FromResult(new WriteResponse());
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        Exception? putErr = null, deleteErr = null;
        batch.Add(new PutCall { Key = "a", CallContext = callContext, Callback = (_, err) => putErr = err });
        batch.Add(new DeleteCall { Key = "b", CallContext = callContext, Callback = (_, err) => deleteErr = err });
        await batch.CompleteAsync();

        // Nothing is left to send
        Assert.NotNull(prepareErr);
        Assert.True(CallContext.IsNotSent(putErr));
        Assert.True(CallContext.IsNotSent(deleteErr));
    }

    // A request that was never handed to gRPC, e.g. because the leader could not
    // be reached before the timeout, was not applied: its calls fail with an error
    // telling that it was not sent. Once it was sent, the outcome of a failure is
    // unknown.
    [Theory]
    [InlineData("not sent", false)]
    [InlineData("sent", true)]
    public async Task TestWriteBatchFailureTellsIfTheRequestWasSent(string name, bool sent)
    {
        _ = name;
        var deadlineExceeded = new TimeoutException("context deadline exceeded");
        var factory = new WriteBatchFactory
        {
            Execute = (ct, shard, prepare) =>
            {
                if (sent)
                {
                    prepare();
                }
                return ValueTask.FromException<WriteResponse>(deadlineExceeded);
            },
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxByteSize = 1024,
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        Exception? putErr = null;
        batch.Add(new PutCall { Key = "a", Callback = (_, err) => putErr = err });
        await batch.CompleteAsync();

        Assert.True(OxiaErrors.Is(putErr, deadlineExceeded));
        Assert.Equal(!sent, CallContext.IsNotSent(putErr));
    }
}
