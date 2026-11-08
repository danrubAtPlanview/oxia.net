// =====================================================================================================================
// PORT NOTE
//   Upstream : common/constant/errors.go   (sentinel errors, IsRetryable)  -- the gRPC status mapping half of that
//              file (IntoGrpcStatusError / FromGrpcError / ErrorMetadata) lives in OxiaErrors.Grpc.cs (partial).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go reports errors as values compared with `errors.Is(err, ErrX)`.  The .NET port keeps that model:
//              * every sentinel is a shared, never-thrown `OxiaException` instance (zero allocation when a sentinel is
//                stored in a result struct, exactly like Go returning the package-level error variable);
//              * `OxiaErrors.Is(err, sentinel)` is `errors.Is`: it walks the InnerException chain (and AggregateException
//                members) looking for the sentinel by reference;
//              * `OxiaErrors.Wrap(sentinel, "context")` is `errors.Wrap(sentinel, "context")` -> a NEW exception whose
//                InnerException is the sentinel, so `Is` still matches;
//              * `OxiaErrors.ToThrowable(err)` produces a fresh exception to actually `throw` (never throw a shared sentinel:
//                the CLR would overwrite its stack trace concurrently).
//              Messages are byte-identical to the Go strings ("oxia: operation was aborted", ...).
// =====================================================================================================================
namespace Oxia.Common.Constant;

/// <summary>Base class of every error that the Oxia client/common code produces on its own.</summary>
public class OxiaException : Exception
{
    public OxiaException(string message) : base(message) { }
    public OxiaException(string message, Exception? inner) : base(message, inner) { }
}

public static partial class OxiaErrors
{
    public const string OxiaErrorDomain = "oxia.io";

    public const string ErrorMetadataShard = "shard";
    public const string ErrorMetadataLeader = "leader";
    public const string ErrorMetadataCoordinatorLeader = "coordinator-leader";

    // Reasons carried in google.rpc.ErrorInfo.reason
    public const string ReasonAborted = "ABORTED";
    public const string ReasonInvalidSessionTimeout = "INVALID_SESSION_TIMEOUT";
    public const string ReasonSessionNotFound = "SESSION_NOT_FOUND";
    public const string ReasonNamespaceNotFound = "NAMESPACE_NOT_FOUND";
    public const string ReasonShardNotFound = "SHARD_NOT_FOUND";
    public const string ReasonInvalidTerm = "INVALID_TERM";
    public const string ReasonInvalidStatus = "INVALID_STATUS";
    public const string ReasonNotificationsNotEnabled = "NOTIFICATIONS_NOT_ENABLED";
    public const string ReasonNodeIsNotMember = "NODE_IS_NOT_MEMBER";
    public const string ReasonNodeIsNotLeader = "NODE_IS_NOT_LEADER";
    public const string ReasonNotInitialized = "NOT_INITIALIZED";
    public const string ReasonResourceConflict = "RESOURCE_CONFLICT";
    public const string ReasonResourceUnavailable = "RESOURCE_UNAVAILABLE";
    public const string ReasonUnsupportedFeatures = "UNSUPPORTED_FEATURES";
    public const string ReasonUnknown = "UNKNOWN";

    public static readonly OxiaException Aborted = new("oxia: operation was aborted");
    public static readonly OxiaException InvalidSessionTimeout = new("oxia: invalid session timeout");
    public static readonly OxiaException SessionNotFound = new("oxia: session not found");
    public static readonly OxiaException NamespaceNotFound = new("oxia: namespace not found");
    public static readonly OxiaException ShardNotFound = new("oxia: shard not found");
    public static readonly OxiaException InvalidTerm = new("oxia: invalid term");
    public static readonly OxiaException InvalidStatus = new("oxia: invalid status");
    public static readonly OxiaException NotificationsNotEnabled = new("oxia: notifications not enabled on namespace");
    public static readonly OxiaException NodeIsNotMember = new("oxia: node is not a member");
    public static readonly OxiaException NodeIsNotLeader = new("oxia: node is not leader");
    public static readonly OxiaException NotInitialized = new("oxia: server not initialized yet");
    public static readonly OxiaException ResourceConflict = new("oxia: resource conflict");
    public static readonly OxiaException ResourceUnavailable = new("oxia: resource unavailable");
    public static readonly OxiaException UnsupportedFeatures = new("oxia: unsupported features");

    /// <summary>Go's <c>io.EOF</c>: a stream ended normally.  Used where Go code does <c>errors.Is(err, io.EOF)</c>.</summary>
    public static readonly OxiaException EndOfStream = new("EOF");

    /// <summary>
    /// <c>errors.Is(err, target)</c>: true if <paramref name="err"/> is <paramref name="target"/> or wraps it anywhere in
    /// its InnerException chain (AggregateException members included).  Reference equality, like Go for sentinel values.
    /// An exception type may customise matching by implementing <see cref="IErrorIs"/> (Go's <c>Is(target error) bool</c>).
    /// </summary>
    public static bool Is(Exception? err, Exception target)
    {
        while (err is not null)
        {
            if (ReferenceEquals(err, target)) return true;
            if (err is IErrorIs custom && custom.Is(target)) return true;
            if (err is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions)
                {
                    if (Is(inner, target)) return true;
                }
                return false;
            }
            err = err.InnerException;
        }
        return false;
    }

    /// <summary><c>errors.Wrap(sentinel, message)</c>: message + ": " + sentinel text, matchable with <see cref="Is"/>.</summary>
    public static OxiaException Wrap(Exception cause, string message) => new($"{message}: {cause.Message}", cause);

    /// <summary>Returns a fresh exception that is safe to throw and still satisfies <c>Is(thrown, err)</c>.</summary>
    public static Exception ToThrowable(Exception err) => err is OxiaException ? new OxiaException(err.Message, err) : err;

    /// <summary><c>constant.IsRetryable</c>.</summary>
    public static bool IsRetryable(Exception? err) =>
        Is(err, EndOfStream) ||
        Is(err, Aborted) ||
        Is(err, NodeIsNotMember) ||
        Is(err, NodeIsNotLeader) ||
        Is(err, NotInitialized) ||
        Is(err, ResourceUnavailable);
}

/// <summary>Go's optional <c>Is(target error) bool</c> method on an error type.</summary>
public interface IErrorIs
{
    bool Is(Exception target);
}
