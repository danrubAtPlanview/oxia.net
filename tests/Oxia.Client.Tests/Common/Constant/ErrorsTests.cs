// =====================================================================================================================
// PORT NOTE
//   Upstream : common/constant/errors_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Round trips between Oxia sentinels and gRPC statuses with google.rpc.ErrorInfo details, leader hints, and
//              IsRetryable.
//   Go tests: 13, ported: 13, skipped: 0.
//   Divergences: `okStatusError` (a Go error whose GRPCStatus() is OK) is an RpcException with StatusCode.OK;
//              `status.Convert(err).Code()/Message()` is `RpcException.StatusCode / Status.Detail`;
//              `assert.Empty(metadata)` accepts null or an empty ErrorMetadata (Go: nil map).
//              TestIntoGrpcStatusError is one [Fact] looping over the table, exactly like the Go test (no t.Run).
// =====================================================================================================================
using Google.Protobuf.WellKnownTypes;
using ErrorInfo = Google.Rpc.ErrorInfo;
using Status = Grpc.Core.Status;
using Grpc.Core;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class ErrorsTests
{
    private static void AssertEmpty(ErrorMetadata? metadata) => Assert.True(metadata is null || metadata.Count == 0);

    [Fact]
    public void TestWithLeaderHint()
    {
        var err = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(1, "leader:6648"));
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(err);
        Assert.NotNull(metadata);
        var ok = metadata.TryGetLeaderHint(out var shard, out var leader);

        Assert.True(OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader));
        Assert.True(ok);
        Assert.Equal(1L, shard);
        Assert.Equal("leader:6648", leader);
    }

    [Fact]
    public void TestWithCoordinatorLeaderHint()
    {
        var err = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithCoordinatorLeaderHint("coordinator:6651"));
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(err);
        Assert.NotNull(metadata);
        var ok = metadata.TryGetCoordinatorLeaderHint(out var leader);

        Assert.True(OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader));
        Assert.True(ok);
        Assert.Equal("coordinator:6651", leader);
    }

    [Fact]
    public void TestErrorMetadataGetLeaderHintInvalidShard()
    {
        var metadata = new ErrorMetadata
        {
            [OxiaErrors.ErrorMetadataLeader] = "leader:6648",
            [OxiaErrors.ErrorMetadataShard] = "not-a-number",
        };
        var ok = metadata.TryGetLeaderHint(out var shard, out var leader);

        Assert.False(ok);
        Assert.Equal(0L, shard);
        Assert.Equal(string.Empty, leader);
    }

    [Fact]
    public void TestWithLeaderHintEmptyLeader()
    {
        var err = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(1, ""));
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(err);
        Assert.NotNull(metadata);
        var ok = metadata.TryGetLeaderHint(out _, out _);

        Assert.True(OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader));
        AssertEmpty(metadata);
        Assert.False(ok);
    }

    [Fact]
    public void TestWithCoordinatorLeaderHintEmptyLeader()
    {
        var err = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithCoordinatorLeaderHint(""));
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(err);
        Assert.NotNull(metadata);
        var ok = metadata.TryGetCoordinatorLeaderHint(out _);

        Assert.True(OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader));
        AssertEmpty(metadata);
        Assert.False(ok);
    }

    [Fact]
    public void TestIntoGrpcStatusError()
    {
        (Exception Err, StatusCode Code)[] tests =
        [
            (OxiaErrors.Aborted, StatusCode.Aborted),
            (OxiaErrors.InvalidSessionTimeout, StatusCode.InvalidArgument),
            (OxiaErrors.SessionNotFound, StatusCode.NotFound),
            (OxiaErrors.NamespaceNotFound, StatusCode.NotFound),
            (OxiaErrors.ShardNotFound, StatusCode.NotFound),
            (OxiaErrors.InvalidTerm, StatusCode.FailedPrecondition),
            (OxiaErrors.InvalidStatus, StatusCode.FailedPrecondition),
            (OxiaErrors.NotificationsNotEnabled, StatusCode.FailedPrecondition),
            (OxiaErrors.NodeIsNotMember, StatusCode.Aborted),
            (OxiaErrors.NodeIsNotLeader, StatusCode.Aborted),
            (OxiaErrors.NotInitialized, StatusCode.Unavailable),
            (OxiaErrors.ResourceConflict, StatusCode.FailedPrecondition),
            (OxiaErrors.ResourceUnavailable, StatusCode.Unavailable),
            (OxiaErrors.UnsupportedFeatures, StatusCode.FailedPrecondition),
        ];

        foreach (var (tErr, tCode) in tests)
        {
            var err = OxiaErrors.IntoGrpcStatusError(tErr);
            Assert.NotNull(err);

            Assert.Equal(tCode, err.StatusCode);
            Assert.Equal(tErr.Message, err.Status.Detail);
            var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(err);
            Assert.True(OxiaErrors.Is(oxiaErr, tErr));
            AssertEmpty(metadata);
        }
    }

    [Fact]
    public void TestIntoGrpcStatusUnknown()
    {
        var err = new Exception("other");
        var grpcErr = OxiaErrors.IntoGrpcStatusError(err);
        Assert.NotNull(grpcErr);

        Assert.Equal(StatusCode.Unknown, grpcErr.StatusCode);
        Assert.Equal(err.Message, grpcErr.Status.Detail);
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(grpcErr);
        Assert.Same(grpcErr, oxiaErr); // assert.EqualError(oxiaErr, grpcErr.Error())
        AssertEmpty(metadata);
    }

    [Fact]
    public void TestIntoGrpcStatusNil()
    {
        Assert.Null(OxiaErrors.IntoGrpcStatusError(null));
    }

    [Fact]
    public void TestIntoGrpcStatusReturnsUnknownGrpcStatusError()
    {
        var err = new RpcException(new Status(StatusCode.InvalidArgument, "invalid"));
        var grpcErr = OxiaErrors.IntoGrpcStatusError(err);
        Assert.NotNull(grpcErr);

        Assert.Equal(StatusCode.InvalidArgument, grpcErr.StatusCode);
        Assert.Equal("invalid", grpcErr.Status.Detail);
    }

    [Fact]
    public void TestIntoGrpcStatusAddsErrorInfoToGrpcStatusError()
    {
        var err = new RpcException(new Status(StatusCode.Aborted, OxiaErrors.NodeIsNotLeader.Message));
        var grpcErr = OxiaErrors.IntoGrpcStatusError(err, OxiaErrors.WithLeaderHint(1, "leader:6648"));
        Assert.NotNull(grpcErr);
        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(grpcErr);
        Assert.NotNull(metadata);
        var ok = metadata.TryGetLeaderHint(out var shard, out var leader);

        Assert.Equal(StatusCode.Aborted, grpcErr.StatusCode);
        Assert.Equal(OxiaErrors.NodeIsNotLeader.Message, grpcErr.Status.Detail);
        Assert.Same(grpcErr, oxiaErr); // assert.EqualError(oxiaErr, grpcErr.Error()): reason UNKNOWN keeps the original error
        Assert.True(ok);
        Assert.Equal(1L, shard);
        Assert.Equal("leader:6648", leader);
    }

    // A status with code OK cannot carry details, and gRPC maps it to a nil error.
    [Fact]
    public void TestIntoGrpcStatusErrorOkStatus()
    {
        Assert.Null(OxiaErrors.IntoGrpcStatusError(new RpcException(new Status(StatusCode.OK, "ok"))));
    }

    [Fact]
    public void TestFromGrpcErrorIgnoresForeignErrorInfo()
    {
        var st = new Google.Rpc.Status { Code = (int)StatusCode.Aborted, Message = OxiaErrors.NodeIsNotLeader.Message };
        st.Details.Add(Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonNodeIsNotLeader, Domain = "other.domain" }));

        var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(st.ToRpcException());
        Assert.False(OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader));
        Assert.Equal(StatusCode.Aborted, OxiaErrors.GrpcCode(oxiaErr));
        Assert.Null(metadata);
    }

    [Fact]
    public void TestIsRetryable()
    {
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.EndOfStream)); // io.EOF
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.Aborted));
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.ResourceUnavailable));
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.NodeIsNotMember));
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.NodeIsNotLeader));
        Assert.True(OxiaErrors.IsRetryable(OxiaErrors.NotInitialized));

        Assert.False(OxiaErrors.IsRetryable(OxiaErrors.InvalidTerm));
        Assert.False(OxiaErrors.IsRetryable(OxiaErrors.SessionNotFound));
        Assert.False(OxiaErrors.IsRetryable(new Exception("other")));
        Assert.False(OxiaErrors.IsRetryable(OxiaErrors.IntoGrpcStatusError(OxiaErrors.ResourceUnavailable)));
    }
}
