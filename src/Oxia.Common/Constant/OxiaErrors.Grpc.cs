// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "common utilities" component; replace the bodies, keep the signatures ***
//   Upstream : common/constant/errors.go  (IntoGrpcStatusError, FromGrpcError, ErrorMetadata, GrpcStatusOption)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Translate between Oxia sentinel errors and gRPC statuses carrying a google.rpc.ErrorInfo detail
//              (domain "oxia.io", reason = ReasonXxx, metadata = leader hints).  The status travels in the
//              `grpc-status-details-bin` trailer (google.rpc.Status).  Use Grpc.StatusProto for (de)serialisation.
// =====================================================================================================================
using Grpc.Core;

namespace Oxia.Common.Constant;

/// <summary>Go: <c>constant.ErrorMetadata</c> (map[string]string) with leader-hint helpers.</summary>
public sealed class ErrorMetadata : Dictionary<string, string>
{
    public ErrorMetadata() { }
    public ErrorMetadata(IDictionary<string, string> source) : base(source) { }

    /// <summary>Go: GetLeaderHint -> (shard, leader, ok).</summary>
    public bool TryGetLeaderHint(out long shard, out string leader)
        => throw new NotImplementedException();

    /// <summary>Go: GetCoordinatorLeaderHint -> (leader, ok).</summary>
    public bool TryGetCoordinatorLeaderHint(out string leader)
        => throw new NotImplementedException();
}

/// <summary>Go: <c>GrpcStatusOption</c> (func(ErrorMetadata)).</summary>
public delegate void GrpcStatusOption(ErrorMetadata metadata);

public static partial class OxiaErrors
{
    /// <summary>Go: WithLeaderHint(shard, leader).</summary>
    public static GrpcStatusOption WithLeaderHint(long shard, string leader) => throw new NotImplementedException();

    /// <summary>Go: WithCoordinatorLeaderHint(leader).</summary>
    public static GrpcStatusOption WithCoordinatorLeaderHint(string leader) => throw new NotImplementedException();

    /// <summary>Go: IntoGrpcStatusError(err, opts...) -- builds the server-side RpcException (used by mock servers in tests).</summary>
    public static RpcException? IntoGrpcStatusError(Exception? err, params GrpcStatusOption[] opts)
        => throw new NotImplementedException();

    /// <summary>
    /// Go: FromGrpcError(err) -> (translatedErr, metadata).  If <paramref name="err"/> is an RpcException carrying an
    /// ErrorInfo in domain oxia.io, returns the matching sentinel (and the ErrorInfo metadata); otherwise returns
    /// <paramref name="err"/> unchanged with null metadata.  Never throws.  null in -> null out.
    /// </summary>
    public static (Exception? Error, ErrorMetadata? Metadata) FromGrpcError(Exception? err)
        => throw new NotImplementedException();

    /// <summary>Go: <c>status.Code(err)</c> -- gRPC status code of the RpcException found in the chain, or Unknown/OK per Go semantics (nil -> OK).</summary>
    public static StatusCode GrpcCode(Exception? err) => throw new NotImplementedException();

    /// <summary>Go: <c>errors.Is(err, context.Canceled)</c> -- OperationCanceledException anywhere in the chain, or RpcException(Cancelled).</summary>
    public static bool IsCanceled(Exception? err) => throw new NotImplementedException();

    /// <summary>Go: <c>errors.Is(err, context.DeadlineExceeded)</c> -- TimeoutException or RpcException(DeadlineExceeded) in the chain.</summary>
    public static bool IsDeadlineExceeded(Exception? err) => throw new NotImplementedException();
}
