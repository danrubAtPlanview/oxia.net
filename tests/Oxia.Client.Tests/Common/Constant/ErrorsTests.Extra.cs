// =====================================================================================================================
// PORT NOTE
//   Upstream : (none -- extra tests for common/constant/errors.go, see ErrorsTests.cs for the ported ones)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire compatibility of the grpc-status-details-bin trailer with the Go server, malformed details, chain walking
//              (errors.As / errors.Is), GrpcCode / IsCanceled / IsDeadlineExceeded, leader-hint parsing edge cases.
// =====================================================================================================================
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Oxia.Common.Constant;
using Xunit;
using ErrorInfo = Google.Rpc.ErrorInfo;
using Status = Grpc.Core.Status;

namespace Oxia.Client.Tests.Common;

public class ErrorsExtraTests
{
    private const string DetailsTrailer = "grpc-status-details-bin";

    private static RpcException WithDetails(StatusCode code, string message, params Any[] details)
    {
        var st = new Google.Rpc.Status { Code = (int)code, Message = message };
        foreach (var d in details) st.Details.Add(d);
        return st.ToRpcException();
    }

    [Fact]
    public void IntoGrpcStatusError_WritesGoCompatibleStatusDetailsTrailer()
    {
        var err = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader,
            OxiaErrors.WithLeaderHint(7, "node-1:6648"), OxiaErrors.WithCoordinatorLeaderHint("coord:6651"));
        Assert.NotNull(err);

        var entry = err.Trailers.Single(e => e.Key == DetailsTrailer);
        Assert.True(entry.IsBinary);

        // Decode with the raw google.rpc.Status descriptor, as the Go client would
        var st = Google.Rpc.Status.Parser.ParseFrom(entry.ValueBytes);
        Assert.Equal((int)StatusCode.Aborted, st.Code);
        Assert.Equal("oxia: node is not leader", st.Message);
        var detail = Assert.Single(st.Details);
        Assert.Equal("type.googleapis.com/google.rpc.ErrorInfo", detail.TypeUrl);

        var info = ErrorInfo.Parser.ParseFrom(detail.Value);
        Assert.Equal("NODE_IS_NOT_LEADER", info.Reason);
        Assert.Equal("oxia.io", info.Domain);
        Assert.Equal(3, info.Metadata.Count);
        Assert.Equal("7", info.Metadata["shard"]);
        Assert.Equal("node-1:6648", info.Metadata["leader"]);
        Assert.Equal("coord:6651", info.Metadata["coordinator-leader"]);
    }

    [Fact]
    public void FromGrpcError_DecodesBytesProducedByAGoServer()
    {
        // google.rpc.Status{code: 10, message: "oxia: node is not leader",
        //   details: [Any{type_url: ".../google.rpc.ErrorInfo", value: ErrorInfo{reason, domain, metadata}}]} built by hand
        var info = new ErrorInfo { Reason = "SHARD_NOT_FOUND", Domain = "oxia.io" };
        info.Metadata["shard"] = "3";
        var any = new Any { TypeUrl = "type.googleapis.com/google.rpc.ErrorInfo", Value = info.ToByteString() };
        var status = new Google.Rpc.Status { Code = 5, Message = "oxia: shard not found" };
        status.Details.Add(any);

        var trailers = new Metadata { { DetailsTrailer, status.ToByteArray() } };
        var rpc = new RpcException(new Status(StatusCode.NotFound, "oxia: shard not found"), trailers);

        var (err, md) = OxiaErrors.FromGrpcError(rpc);
        Assert.Same(OxiaErrors.ShardNotFound, err);
        Assert.NotNull(md);
        Assert.Equal("3", md["shard"]);
    }

    [Fact]
    public void FromGrpcError_NullInNullOut()
    {
        var (err, md) = OxiaErrors.FromGrpcError(null);
        Assert.Null(err);
        Assert.Null(md);
    }

    [Fact]
    public void FromGrpcError_NonGrpcErrorPassesThrough()
    {
        var original = new InvalidOperationException("boom");
        var (err, md) = OxiaErrors.FromGrpcError(original);
        Assert.Same(original, err);
        Assert.Null(md);
    }

    [Fact]
    public void FromGrpcError_SentinelPassesThrough()
    {
        var (err, md) = OxiaErrors.FromGrpcError(OxiaErrors.Aborted);
        Assert.Same(OxiaErrors.Aborted, err);
        Assert.Null(md);
    }

    [Fact]
    public void FromGrpcError_RpcExceptionWithoutDetails()
    {
        var rpc = new RpcException(new Status(StatusCode.Unavailable, "down"));
        var (err, md) = OxiaErrors.FromGrpcError(rpc);
        Assert.Same(rpc, err);
        Assert.Null(md);
    }

    [Fact]
    public void FromGrpcError_MalformedDetailsNeverThrow()
    {
        foreach (byte[] garbage in new byte[][]
                 {
                     [],
                     [0xFF],
                     [0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F], // length-delimited field with an impossible length
                     [0x1A, 0x03, 0x01, 0x02], // truncated nested message
                     "this is not a protobuf"u8.ToArray(),
                 })
        {
            var trailers = new Metadata { { DetailsTrailer, garbage } };
            var rpc = new RpcException(new Status(StatusCode.Aborted, "x"), trailers);

            var (err, md) = OxiaErrors.FromGrpcError(rpc);
            Assert.Same(rpc, err);
            Assert.Null(md);
        }
    }

    [Fact]
    public void FromGrpcError_DetailThatDoesNotUnpackIsSkipped()
    {
        // an Any claiming to be an ErrorInfo whose payload is garbage, followed by a valid oxia ErrorInfo
        var bad = new Any { TypeUrl = "type.googleapis.com/google.rpc.ErrorInfo", Value = ByteString.CopyFrom(0xFF, 0xFF, 0xFF) };
        var good = Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonSessionNotFound, Domain = OxiaErrors.OxiaErrorDomain });

        var (err, _) = OxiaErrors.FromGrpcError(WithDetails(StatusCode.NotFound, "m", bad, good));
        Assert.Same(OxiaErrors.SessionNotFound, err);
    }

    [Fact]
    public void FromGrpcError_SkipsOtherDetailTypesAndForeignDomains()
    {
        var other = Any.Pack(new Google.Rpc.DebugInfo { Detail = "d" });
        var foreign = Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonAborted, Domain = "other" });
        var good = Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonAborted, Domain = OxiaErrors.OxiaErrorDomain });

        var (err, _) = OxiaErrors.FromGrpcError(WithDetails(StatusCode.Aborted, "m", other, foreign, good));
        Assert.Same(OxiaErrors.Aborted, err);
    }

    [Fact]
    public void FromGrpcError_FirstOxiaErrorInfoWins()
    {
        var first = Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonInvalidTerm, Domain = OxiaErrors.OxiaErrorDomain });
        var second = Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonAborted, Domain = OxiaErrors.OxiaErrorDomain });

        var (err, _) = OxiaErrors.FromGrpcError(WithDetails(StatusCode.Aborted, "m", first, second));
        Assert.Same(OxiaErrors.InvalidTerm, err);
    }

    [Fact]
    public void FromGrpcError_UnknownReasonKeepsTheOriginalErrorAndMetadata()
    {
        var info = new ErrorInfo { Reason = "SOMETHING_NEW", Domain = OxiaErrors.OxiaErrorDomain };
        info.Metadata["leader"] = "l:1";
        info.Metadata["shard"] = "9";
        var rpc = WithDetails(StatusCode.Internal, "m", Any.Pack(info));

        var (err, md) = OxiaErrors.FromGrpcError(rpc);
        Assert.Same(rpc, err);
        Assert.NotNull(md);
        Assert.True(md.TryGetLeaderHint(out var shard, out var leader));
        Assert.Equal(9L, shard);
        Assert.Equal("l:1", leader);
    }

    [Fact]
    public void FromGrpcError_EveryReasonMapsToItsSentinel()
    {
        (string Reason, Exception Sentinel)[] all =
        [
            (OxiaErrors.ReasonAborted, OxiaErrors.Aborted),
            (OxiaErrors.ReasonInvalidSessionTimeout, OxiaErrors.InvalidSessionTimeout),
            (OxiaErrors.ReasonSessionNotFound, OxiaErrors.SessionNotFound),
            (OxiaErrors.ReasonNamespaceNotFound, OxiaErrors.NamespaceNotFound),
            (OxiaErrors.ReasonShardNotFound, OxiaErrors.ShardNotFound),
            (OxiaErrors.ReasonInvalidTerm, OxiaErrors.InvalidTerm),
            (OxiaErrors.ReasonInvalidStatus, OxiaErrors.InvalidStatus),
            (OxiaErrors.ReasonNotificationsNotEnabled, OxiaErrors.NotificationsNotEnabled),
            (OxiaErrors.ReasonNodeIsNotMember, OxiaErrors.NodeIsNotMember),
            (OxiaErrors.ReasonNodeIsNotLeader, OxiaErrors.NodeIsNotLeader),
            (OxiaErrors.ReasonNotInitialized, OxiaErrors.NotInitialized),
            (OxiaErrors.ReasonResourceConflict, OxiaErrors.ResourceConflict),
            (OxiaErrors.ReasonResourceUnavailable, OxiaErrors.ResourceUnavailable),
            (OxiaErrors.ReasonUnsupportedFeatures, OxiaErrors.UnsupportedFeatures),
        ];

        foreach (var (reason, sentinel) in all)
        {
            var rpc = WithDetails(StatusCode.Unknown, "m", Any.Pack(new ErrorInfo { Reason = reason, Domain = OxiaErrors.OxiaErrorDomain }));
            var (err, md) = OxiaErrors.FromGrpcError(rpc);
            Assert.Same(sentinel, err);
            Assert.NotNull(md);
            Assert.Empty(md);
        }

        // ReasonUnknown is not a sentinel
        var unknown = WithDetails(StatusCode.Unknown, "m", Any.Pack(new ErrorInfo { Reason = OxiaErrors.ReasonUnknown, Domain = OxiaErrors.OxiaErrorDomain }));
        Assert.Same(unknown, OxiaErrors.FromGrpcError(unknown).Error);
    }

    [Fact]
    public void FromGrpcError_FindsAnRpcExceptionWrappedInAnotherException()
    {
        var rpc = OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(2, "x:1"))!;
        var wrapped = new InvalidOperationException("wrapped", rpc);
        var aggregate = new AggregateException(new Exception("a"), wrapped);

        Assert.Same(OxiaErrors.NodeIsNotLeader, OxiaErrors.FromGrpcError(wrapped).Error);
        Assert.Same(OxiaErrors.NodeIsNotLeader, OxiaErrors.FromGrpcError(aggregate).Error);
        Assert.Equal(StatusCode.Aborted, OxiaErrors.GrpcCode(aggregate));
    }

    [Fact]
    public void IntoGrpcStatusError_WrappedSentinelKeepsTheWrapperMessage()
    {
        var wrapped = OxiaErrors.Wrap(OxiaErrors.SessionNotFound, "while renewing");
        var rpc = OxiaErrors.IntoGrpcStatusError(wrapped);
        Assert.NotNull(rpc);

        Assert.Equal(StatusCode.NotFound, rpc.StatusCode);
        Assert.Equal("while renewing: oxia: session not found", rpc.Status.Detail);
        Assert.Same(OxiaErrors.SessionNotFound, OxiaErrors.FromGrpcError(rpc).Error);
    }

    [Fact]
    public void IntoGrpcStatusError_AppendsToExistingDetailsAndKeepsThem()
    {
        var existing = Any.Pack(new Google.Rpc.DebugInfo { Detail = "kept" });
        var source = WithDetails(StatusCode.Aborted, "msg", existing);

        var rpc = OxiaErrors.IntoGrpcStatusError(source, OxiaErrors.WithCoordinatorLeaderHint("c:1"))!;
        var st = rpc.Trailers.GetRpcStatus()!;
        Assert.Equal(2, st.Details.Count);
        Assert.True(st.Details[0].Is(Google.Rpc.DebugInfo.Descriptor));
        var info = st.Details[1].Unpack<ErrorInfo>();
        Assert.Equal(OxiaErrors.ReasonUnknown, info.Reason);
        Assert.Equal("c:1", info.Metadata["coordinator-leader"]);
    }

    [Fact]
    public void IntoGrpcStatusError_WrappedRpcExceptionKeepsCodeAndUsesTheWrapperMessage()
    {
        var inner = new RpcException(new Status(StatusCode.NotFound, "inner message"));
        var rpc = OxiaErrors.IntoGrpcStatusError(new InvalidOperationException("outer message", inner));
        Assert.NotNull(rpc);
        Assert.Equal(StatusCode.NotFound, rpc.StatusCode);
        Assert.Equal("outer message", rpc.Status.Detail);
    }

    [Fact]
    public void IntoGrpcStatusError_NullOptionIsIgnored()
    {
        var rpc = OxiaErrors.IntoGrpcStatusError(OxiaErrors.Aborted, null!, OxiaErrors.WithLeaderHint(1, "l:1"));
        var (_, md) = OxiaErrors.FromGrpcError(rpc);
        Assert.NotNull(md);
        Assert.True(md.TryGetLeaderHint(out var shard, out _));
        Assert.Equal(1L, shard);
    }

    [Fact]
    public void IntoGrpcStatusError_OtherErrorsAreUnknownWithoutDetails()
    {
        var rpc = OxiaErrors.IntoGrpcStatusError(new InvalidOperationException("nope"))!;
        Assert.Equal(StatusCode.Unknown, rpc.StatusCode);
        Assert.Equal("nope", rpc.Status.Detail);
        Assert.DoesNotContain(rpc.Trailers, e => e.Key == DetailsTrailer);
    }

    [Theory]
    [InlineData("1", true, 1L)]
    [InlineData("0", true, 0L)]
    [InlineData("-5", true, -5L)]
    [InlineData("+5", true, 5L)]
    [InlineData("9223372036854775807", true, long.MaxValue)]
    [InlineData("9223372036854775808", false, 0L)]
    [InlineData("", false, 0L)]
    [InlineData(" 1", false, 0L)]
    [InlineData("1 ", false, 0L)]
    [InlineData("1.0", false, 0L)]
    [InlineData("0x10", false, 0L)]
    [InlineData("1_000", false, 0L)]
    public void TryGetLeaderHint_ParsesShardLikeStrconvParseInt(string shardText, bool ok, long expected)
    {
        var md = new ErrorMetadata { ["leader"] = "l:1", ["shard"] = shardText };
        Assert.Equal(ok, md.TryGetLeaderHint(out var shard, out var leader));
        Assert.Equal(expected, shard);
        Assert.Equal(ok ? "l:1" : string.Empty, leader);
    }

    [Fact]
    public void TryGetLeaderHint_RequiresLeaderAndShard()
    {
        Assert.False(new ErrorMetadata { ["shard"] = "1" }.TryGetLeaderHint(out _, out _));
        Assert.False(new ErrorMetadata { ["leader"] = "" , ["shard"] = "1" }.TryGetLeaderHint(out _, out _));
        Assert.False(new ErrorMetadata { ["leader"] = "l:1" }.TryGetLeaderHint(out _, out _)); // missing shard parses as "" -> error
        Assert.False(new ErrorMetadata().TryGetLeaderHint(out var shard, out var leader));
        Assert.Equal(0L, shard);
        Assert.Equal(string.Empty, leader);
    }

    [Fact]
    public void TryGetCoordinatorLeaderHint_RequiresNonEmptyLeader()
    {
        Assert.False(new ErrorMetadata().TryGetCoordinatorLeaderHint(out var none));
        Assert.Equal(string.Empty, none);
        Assert.False(new ErrorMetadata { ["coordinator-leader"] = "" }.TryGetCoordinatorLeaderHint(out _));
        Assert.True(new ErrorMetadata { ["coordinator-leader"] = "c:1" }.TryGetCoordinatorLeaderHint(out var leader));
        Assert.Equal("c:1", leader);
    }

    [Fact]
    public void WithLeaderHint_FormatsNegativeShardsInvariantly()
    {
        var md = new ErrorMetadata();
        OxiaErrors.WithLeaderHint(-12345678901L, "l:1")(md);
        Assert.Equal("-12345678901", md["shard"]);
    }

    [Fact]
    public void GrpcCode_FollowsStatusCodeSemantics()
    {
        Assert.Equal(StatusCode.OK, OxiaErrors.GrpcCode(null));
        Assert.Equal(StatusCode.Unknown, OxiaErrors.GrpcCode(new Exception("x")));
        Assert.Equal(StatusCode.Unknown, OxiaErrors.GrpcCode(OxiaErrors.Aborted));
        Assert.Equal(StatusCode.NotFound, OxiaErrors.GrpcCode(new RpcException(new Status(StatusCode.NotFound, "x"))));
        Assert.Equal(StatusCode.Unavailable,
            OxiaErrors.GrpcCode(new Exception("w", new RpcException(new Status(StatusCode.Unavailable, "x")))));
    }

    [Fact]
    public void IsCanceled_And_IsDeadlineExceeded()
    {
        Assert.False(OxiaErrors.IsCanceled(null));
        Assert.False(OxiaErrors.IsCanceled(new Exception("x")));
        Assert.True(OxiaErrors.IsCanceled(new OperationCanceledException()));
        Assert.True(OxiaErrors.IsCanceled(new TaskCanceledException()));
        Assert.True(OxiaErrors.IsCanceled(new RpcException(new Status(StatusCode.Cancelled, "c"))));
        Assert.True(OxiaErrors.IsCanceled(new Exception("w", new OperationCanceledException())));
        Assert.True(OxiaErrors.IsCanceled(new AggregateException(new Exception("a"), new OperationCanceledException())));
        Assert.False(OxiaErrors.IsCanceled(new RpcException(new Status(StatusCode.DeadlineExceeded, "d"))));

        Assert.False(OxiaErrors.IsDeadlineExceeded(null));
        Assert.False(OxiaErrors.IsDeadlineExceeded(new Exception("x")));
        Assert.True(OxiaErrors.IsDeadlineExceeded(new TimeoutException()));
        Assert.True(OxiaErrors.IsDeadlineExceeded(new RpcException(new Status(StatusCode.DeadlineExceeded, "d"))));
        Assert.True(OxiaErrors.IsDeadlineExceeded(new Exception("w", new TimeoutException())));
        Assert.False(OxiaErrors.IsDeadlineExceeded(new RpcException(new Status(StatusCode.Cancelled, "c"))));
        Assert.False(OxiaErrors.IsDeadlineExceeded(new OperationCanceledException()));
    }

    [Fact]
    public void Is_WalksInnerAndAggregateAndUsesCustomIs()
    {
        var wrapped = OxiaErrors.Wrap(OxiaErrors.Aborted, "ctx");
        Assert.True(OxiaErrors.Is(wrapped, OxiaErrors.Aborted));
        Assert.False(OxiaErrors.Is(wrapped, OxiaErrors.InvalidTerm));
        Assert.True(OxiaErrors.Is(new AggregateException(new Exception("a"), wrapped), OxiaErrors.Aborted));
        Assert.False(OxiaErrors.Is(null, OxiaErrors.Aborted));
        Assert.True(OxiaErrors.IsRetryable(wrapped));
    }
}
