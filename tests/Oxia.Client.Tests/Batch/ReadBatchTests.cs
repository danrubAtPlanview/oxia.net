// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/read_batch_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 3 (TestReadBatchAdd [4 cases], TestReadBatchComplete [3 cases], TestReadBatchRerouteOnShardDeleted)
//              ported: 3   skipped: 0
//   Adaptations: `metrics.NewMetrics(noop.NewMeterProvider())` -> `new Metrics(null)` (nobody listens: a no-op, like the noop
//              provider); `batch.Complete()` -> `await batch.CompleteAsync()`; the executor's `(response, err)` is a
//              ValueTask<ReadResponse> that throws; `reflect.TypeOf(call)` -> the call type name; Go's struct equality on the
//              request / response -> ProtoAssert.Equal (field by field); `assert.ErrorIs(err, X)` -> OxiaErrors.Is.
// =====================================================================================================================
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Batch;

public class ReadBatchTests
{
    public static TheoryData<string, bool, int> AddCases() => new()
    {
        { nameof(PutCall), true, 0 },
        { nameof(DeleteCall), true, 0 },
        { nameof(DeleteRangeCall), true, 0 },
        { nameof(GetCall), false, 1 },
    };

    [Theory]
    [MemberData(nameof(AddCases))]
    public void TestReadBatchAdd(string callType, bool expectPanic, int expectedSize)
    {
        object call = callType switch
        {
            nameof(PutCall) => new PutCall(),
            nameof(DeleteCall) => new DeleteCall(),
            nameof(DeleteRangeCall) => new DeleteRangeCall(),
            _ => new GetCall(),
        };
        var factory = new ReadBatchFactory
        {
            Metrics = new OxiaMetrics(null),
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        bool panicked = UtilsTests.Add(batch, call);

        Assert.True(expectPanic == panicked, callType);
        Assert.True(expectedSize == batch.Size, callType);
    }

    public static TheoryData<int> CompleteCases() => new() { 0, 1, 2 };

    [Theory]
    [MemberData(nameof(CompleteCases))]
    public async Task TestReadBatchComplete(int caseIndex)
    {
        var errFailure = new Exception("failure");
        var getResponseOk = new GetResponse
        {
            Value = new byte[] { 0 },
            Status = Status.Ok,
            Version = new ProtoVersion
            {
                VersionId = 1,
                CreatedTimestamp = 2,
                ModifiedTimestamp = 3,
                ModificationsCount = 1,
            },
        };

        ReadResponse? response;
        Exception? err;
        GetResponse? expectedGetResponse;
        Exception? expectedGetErr;
        switch (caseIndex)
        {
            case 0:
                response = new ReadResponse();
                response.Gets.Add(getResponseOk);
                err = null;
                expectedGetResponse = getResponseOk;
                expectedGetErr = null;
                break;
            case 1:
                response = new ReadResponse();
                response.Gets.Add(new GetResponse { Status = Status.KeyNotFound });
                err = null;
                expectedGetResponse = new GetResponse { Status = Status.KeyNotFound };
                expectedGetErr = null;
                break;
            default:
                response = null;
                err = errFailure;
                expectedGetResponse = null;
                expectedGetErr = errFailure;
                break;
        }

        ValueTask<ReadResponse> Execute(CancellationToken ct, ReadRequest request)
        {
            var expected = new ReadRequest { Shard = UtilsTests.ShardId };
            expected.Gets.Add(new GetRequest { Key = "/a", IncludeValue = true });
            ProtoAssert.Equal(expected, request);
            return err is not null ? ValueTask.FromException<ReadResponse>(err) : ValueTask.FromResult(response!);
        }

        var factory = new ReadBatchFactory
        {
            Execute = Execute,
            Metrics = new OxiaMetrics(null),
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        GetResponse? getResponse = null;
        Exception? getErr = null;
        int callbacks = 0;

        void GetCallback(GetResponse? r, Exception? e)
        {
            getResponse = r;
            getErr = e;
            callbacks++;
        }

        batch.Add(new GetCall
        {
            Key = "/a",
            Callback = GetCallback,
            IncludeValue = true,
        });
        Assert.Equal(1, batch.Size);

        await batch.CompleteAsync();

        Assert.Equal(1, callbacks);
        ProtoAssert.Equal(expectedGetResponse, getResponse);
        if (expectedGetErr is null)
        {
            Assert.Null(getErr);
        }
        else
        {
            Assert.True(OxiaErrors.Is(getErr, expectedGetErr));
        }
    }

    [Fact]
    public async Task TestReadBatchRerouteOnShardDeleted()
    {
        int executeCount = 0;

        ValueTask<ReadResponse> Execute(CancellationToken ct, ReadRequest request)
        {
            executeCount++;
            return ValueTask.FromException<ReadResponse>(OxiaErrors.ShardNotFound);
        }

        List<GetCall>? reroutedGets = null;

        var factory = new ReadBatchFactory
        {
            Execute = Execute,
            Reroute = gets => reroutedGets = gets,
            Metrics = new OxiaMetrics(null),
            RequestTimeout = TimeSpan.FromSeconds(5),
        };
        var batch = factory.NewBatch(UtilsTests.ShardId);

        static void GetCallback(GetResponse? r, Exception? e) { }
        batch.Add(new GetCall { Key = "key-1", Callback = GetCallback, IncludeValue = true });
        batch.Add(new GetCall { Key = "key-2", Callback = GetCallback, IncludeValue = true });

        await batch.CompleteAsync();

        Assert.NotNull(reroutedGets);
        Assert.Equal(2, reroutedGets.Count);
        Assert.Equal("key-1", reroutedGets[0].Key);
        Assert.Equal("key-2", reroutedGets[1].Key);
        Assert.Equal(1, executeCount);
    }
}
