// =====================================================================================================================
// PORT NOTE
//   Upstream : common/constant/errors.go  (ErrorMetadata, WithLeaderHint, WithCoordinatorLeaderHint, IntoGrpcStatusError,
//              FromGrpcError, withErrorInfoDetails)  -- the sentinels / IsRetryable half is in OxiaErrors.cs.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Translate between Oxia sentinel errors and gRPC statuses carrying a google.rpc.ErrorInfo detail
//              (domain "oxia.io", reason = ReasonXxx, metadata = leader hints).  The status travels in the
//              `grpc-status-details-bin` trailer (serialized google.rpc.Status), byte-compatible with the Go server/client.
//              Built on Grpc.StatusProto (Google.Rpc.Status <-> RpcException) and Google.Api.CommonProtos (ErrorInfo).
//   Divergences:
//      * Go's `error` holding a gRPC status (`status.FromError`) is an `RpcException` here (found anywhere in the
//        InnerException/AggregateException chain, like Go's errors.As); the *message* of a wrapped status is the wrapper's
//        message, like Go (`p.Message = err.Error()`).
//      * IntoGrpcStatusError returns `RpcException?` (null for a null error and for an OK status, which cannot carry details).
//      * FromGrpcError returns a non-null (possibly empty) `ErrorMetadata` whenever the error carried an oxia.io ErrorInfo
//        (Go returns a nil map that is still safe to query); it returns null for errors without an oxia.io ErrorInfo (Go: nil).
//      * Malformed `grpc-status-details-bin` payloads / details that do not unpack are skipped, never thrown (Go's
//        Status.Details() puts an error in the slice, which FromGrpcError ignores).
//      * GrpcCode / IsCanceled / IsDeadlineExceeded map Go's status.Code(err) / errors.Is(err, context.Canceled|DeadlineExceeded)
//        onto the .NET exception types (see each method).
// =====================================================================================================================
using System.Globalization;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;

namespace Oxia.Common.Constant;

/// <summary>Go: <c>constant.ErrorMetadata</c> (map[string]string) with leader-hint helpers.</summary>
public sealed class ErrorMetadata : Dictionary<string, string>
{
    public ErrorMetadata() { }
    public ErrorMetadata(IDictionary<string, string> source) : base(source) { }

    /// <summary>
    /// Go: GetLeaderHint -> (shard, leader, ok).  Requires a non-empty leader and a shard that parses as a base-10 int64;
    /// on failure <paramref name="shard"/> is 0 and <paramref name="leader"/> is empty.
    /// </summary>
    public bool TryGetLeaderHint(out long shard, out string leader)
    {
        shard = 0;
        leader = string.Empty;

        if (!TryGetValue(OxiaErrors.ErrorMetadataLeader, out var l) || l.Length == 0) return false;
        TryGetValue(OxiaErrors.ErrorMetadataShard, out var s);
        // strconv.ParseInt(s, 10, 64): optional sign, decimal digits only
        if (s is null || !long.TryParse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        shard = parsed;
        leader = l;
        return true;
    }

    /// <summary>Go: GetCoordinatorLeaderHint -> (leader, ok).</summary>
    public bool TryGetCoordinatorLeaderHint(out string leader)
    {
        if (TryGetValue(OxiaErrors.ErrorMetadataCoordinatorLeader, out var l) && l.Length > 0)
        {
            leader = l;
            return true;
        }

        leader = string.Empty;
        return false;
    }
}

/// <summary>Go: <c>GrpcStatusOption</c> (func(ErrorMetadata)).</summary>
public delegate void GrpcStatusOption(ErrorMetadata metadata);

public static partial class OxiaErrors
{
    /// <summary>Go: WithLeaderHint(shard, leader).  A no-op for an empty leader.</summary>
    public static GrpcStatusOption WithLeaderHint(long shard, string leader) => metadata =>
    {
        if (string.IsNullOrEmpty(leader)) return;
        metadata[ErrorMetadataShard] = shard.ToString(CultureInfo.InvariantCulture);
        metadata[ErrorMetadataLeader] = leader;
    };

    /// <summary>Go: WithCoordinatorLeaderHint(leader).  A no-op for an empty leader.</summary>
    public static GrpcStatusOption WithCoordinatorLeaderHint(string leader) => metadata =>
    {
        if (string.IsNullOrEmpty(leader)) return;
        metadata[ErrorMetadataCoordinatorLeader] = leader;
    };

    /// <summary>
    /// Go: IntoGrpcStatusError(err, opts...) -- builds the server-side RpcException (used by mock servers in tests).
    /// Returns null for a null error, and for an error that already is an OK status (Go: an OK status cannot carry details).
    /// </summary>
    public static RpcException? IntoGrpcStatusError(Exception? err, params GrpcStatusOption[] opts)
    {
        if (err is null) return null;

        ErrorMetadata? metadata = null;
        foreach (var opt in opts)
        {
            if (opt is null) continue;
            metadata ??= new ErrorMetadata();
            opt(metadata);
        }

        if (metadata is { Count: 0 }) metadata = null;

        var rpc = FindRpcException(err);
        if (rpc is not null)
        {
            return WithErrorInfoDetails(StatusOf(rpc, err), ReasonUnknown, metadata);
        }

        var msg = err.Message;
        if (Is(err, Aborted)) return Build(StatusCode.Aborted, msg, ReasonAborted, metadata);
        if (Is(err, InvalidSessionTimeout)) return Build(StatusCode.InvalidArgument, msg, ReasonInvalidSessionTimeout, metadata);
        if (Is(err, SessionNotFound)) return Build(StatusCode.NotFound, msg, ReasonSessionNotFound, metadata);
        if (Is(err, NamespaceNotFound)) return Build(StatusCode.NotFound, msg, ReasonNamespaceNotFound, metadata);
        if (Is(err, ShardNotFound)) return Build(StatusCode.NotFound, msg, ReasonShardNotFound, metadata);
        if (Is(err, InvalidTerm)) return Build(StatusCode.FailedPrecondition, msg, ReasonInvalidTerm, metadata);
        if (Is(err, InvalidStatus)) return Build(StatusCode.FailedPrecondition, msg, ReasonInvalidStatus, metadata);
        if (Is(err, NotificationsNotEnabled)) return Build(StatusCode.FailedPrecondition, msg, ReasonNotificationsNotEnabled, metadata);
        if (Is(err, NodeIsNotMember)) return Build(StatusCode.Aborted, msg, ReasonNodeIsNotMember, metadata);
        if (Is(err, NodeIsNotLeader)) return Build(StatusCode.Aborted, msg, ReasonNodeIsNotLeader, metadata);
        if (Is(err, NotInitialized)) return Build(StatusCode.Unavailable, msg, ReasonNotInitialized, metadata);
        if (Is(err, ResourceConflict)) return Build(StatusCode.FailedPrecondition, msg, ReasonResourceConflict, metadata);
        if (Is(err, ResourceUnavailable)) return Build(StatusCode.Unavailable, msg, ReasonResourceUnavailable, metadata);
        if (Is(err, UnsupportedFeatures)) return Build(StatusCode.FailedPrecondition, msg, ReasonUnsupportedFeatures, metadata);

        // default: status.Error(codes.Unknown, err.Error()) -- no details
        return new RpcException(new Grpc.Core.Status(StatusCode.Unknown, msg));

        static RpcException? Build(StatusCode code, string message, string reason, ErrorMetadata? metadata)
            => WithErrorInfoDetails(new Google.Rpc.Status { Code = (int)code, Message = message }, reason, metadata);
    }

    /// <summary>Go: withErrorInfoDetails -- appends an oxia.io ErrorInfo to the status; an OK status maps to a null error.</summary>
    private static RpcException? WithErrorInfoDetails(Google.Rpc.Status status, string reason, ErrorMetadata? metadata)
    {
        if (status.Code == (int)StatusCode.OK) return null;

        var info = new ErrorInfo { Reason = reason, Domain = OxiaErrorDomain };
        if (metadata is not null)
        {
            foreach (var (k, v) in metadata) info.Metadata[k] = v;
        }

        status.Details.Add(Any.Pack(info));
        return status.ToRpcException();
    }

    /// <summary>
    /// Go: FromGrpcError(err) -> (translatedErr, metadata).  If <paramref name="err"/> is (or wraps) an RpcException carrying an
    /// ErrorInfo in domain oxia.io, returns the matching sentinel (and the ErrorInfo metadata); otherwise returns
    /// <paramref name="err"/> unchanged with null metadata.  Never throws.  null in -> null out.
    /// </summary>
    public static (Exception? Error, ErrorMetadata? Metadata) FromGrpcError(Exception? err)
    {
        if (err is null) return (null, null);

        var rpc = FindRpcException(err);
        if (rpc is null) return (err, null);

        Google.Rpc.Status? status;
        try
        {
            status = rpc.Trailers.GetRpcStatus(ignoreParseError: true);
        }
        catch (Exception)
        {
            return (err, null);
        }

        if (status is null) return (err, null);

        foreach (var detail in status.Details)
        {
            ErrorInfo info;
            try
            {
                if (!detail.Is(ErrorInfo.Descriptor)) continue;
                info = detail.Unpack<ErrorInfo>();
            }
            catch (Exception)
            {
                // Go: Status.Details() holds the unmarshal error for such an item, which is not an *ErrorInfo
                continue;
            }

            if (info.Domain != OxiaErrorDomain) continue;

            var metadata = new ErrorMetadata(info.Metadata);
            return (SentinelForReason(info.Reason) ?? err, metadata);
        }

        return (err, null);
    }

    private static Exception? SentinelForReason(string reason) => reason switch
    {
        ReasonAborted => Aborted,
        ReasonInvalidSessionTimeout => InvalidSessionTimeout,
        ReasonSessionNotFound => SessionNotFound,
        ReasonNamespaceNotFound => NamespaceNotFound,
        ReasonShardNotFound => ShardNotFound,
        ReasonInvalidTerm => InvalidTerm,
        ReasonInvalidStatus => InvalidStatus,
        ReasonNotificationsNotEnabled => NotificationsNotEnabled,
        ReasonNodeIsNotMember => NodeIsNotMember,
        ReasonNodeIsNotLeader => NodeIsNotLeader,
        ReasonNotInitialized => NotInitialized,
        ReasonResourceConflict => ResourceConflict,
        ReasonResourceUnavailable => ResourceUnavailable,
        ReasonUnsupportedFeatures => UnsupportedFeatures,
        _ => null, // ReasonUnknown and anything else: the original error (+ metadata)
    };

    /// <summary>
    /// Go: <c>status.Code(err)</c> -- gRPC status code of the first RpcException found in the chain (outermost first);
    /// <see cref="StatusCode.OK"/> for a null error and <see cref="StatusCode.Unknown"/> when no status is found.
    /// </summary>
    public static StatusCode GrpcCode(Exception? err)
    {
        if (err is null) return StatusCode.OK;
        return FindRpcException(err)?.StatusCode ?? StatusCode.Unknown;
    }

    /// <summary>Go: <c>errors.Is(err, context.Canceled)</c> -- OperationCanceledException anywhere in the chain, or RpcException(Cancelled).</summary>
    public static bool IsCanceled(Exception? err) => AnyInChain(err, static e =>
        e is OperationCanceledException || e is RpcException { StatusCode: StatusCode.Cancelled });

    /// <summary>Go: <c>errors.Is(err, context.DeadlineExceeded)</c> -- TimeoutException or RpcException(DeadlineExceeded) in the chain.</summary>
    public static bool IsDeadlineExceeded(Exception? err) => AnyInChain(err, static e =>
        e is TimeoutException || e is RpcException { StatusCode: StatusCode.DeadlineExceeded });

    // ----- chain helpers (errors.As) --------------------------------------------------------------------------------
    private static RpcException? FindRpcException(Exception? err)
    {
        while (err is not null)
        {
            if (err is RpcException rpc) return rpc;
            if (err is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions)
                {
                    var found = FindRpcException(inner);
                    if (found is not null) return found;
                }

                return null;
            }

            err = err.InnerException;
        }

        return null;
    }

    private static bool AnyInChain(Exception? err, Func<Exception, bool> predicate)
    {
        while (err is not null)
        {
            if (predicate(err)) return true;
            if (err is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions)
                {
                    if (AnyInChain(inner, predicate)) return true;
                }

                return false;
            }

            err = err.InnerException;
        }

        return false;
    }

    /// <summary>
    /// Go: <c>status.FromError(err)</c> for an error known to hold an RpcException: the status of the exception, with the
    /// details of its trailers; when the error only wraps the RpcException the message is the wrapper's (errors.As branch).
    /// </summary>
    private static Google.Rpc.Status StatusOf(RpcException rpc, Exception outer)
    {
        Google.Rpc.Status? fromTrailers = null;
        try
        {
            fromTrailers = rpc.Trailers.GetRpcStatus(ignoreParseError: true);
        }
        catch (Exception)
        {
            // malformed details are dropped
        }

        var status = new Google.Rpc.Status
        {
            Code = (int)rpc.StatusCode,
            Message = ReferenceEquals(rpc, outer) ? rpc.Status.Detail : outer.Message,
        };
        if (fromTrailers is not null) status.Details.Add(fromTrailers.Details);
        return status;
    }
}
