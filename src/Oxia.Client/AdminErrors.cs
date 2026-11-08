// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/admin_errors.go   (ErrUnauthorized/ErrUnauthenticated/ErrUnknown live in ClientErrors.cs; adminError,
//              wrapAdminError, mapAdminError here)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Maps the failure of an admin RPC to one of three "kind" sentinels (permission denied -> Unauthorized,
//              unauthenticated -> Unauthenticated, everything else -> Unknown) while keeping the original cause reachable:
//              `OxiaErrors.Is(err, ClientErrors.Unauthorized)` matches the kind (Go: adminError.Is), and
//              `OxiaErrors.Is(err, cause)` / `OxiaErrors.GrpcCode(err)` still see the underlying gRPC failure
//              (Go: adminError.Unwrap).
//   Divergences: adminError -> AdminError : Exception, IErrorIs (Go's `Is(target) bool` method == IErrorIs; `Unwrap` ==
//              InnerException).  `status.Code(err)` -> OxiaErrors.GrpcCode(err).  Error() text is the Go format
//              "<kind>: <cause>" (the cause text is the .NET exception message, not grpc-go's "rpc error: ..." text).
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Constant;

namespace Oxia.Client;

/// <summary>Go: adminError -- an admin failure of a given kind (one of the ErrUnauthorized/ErrUnauthenticated/ErrUnknown sentinels) with its cause.</summary>
public sealed class AdminError : Exception, IErrorIs
{
    private readonly Exception _kind;

    internal AdminError(Exception kind, Exception? cause) : base(null, cause) => _kind = kind;

    /// <summary>The sentinel that classifies the failure.</summary>
    public Exception Kind => _kind;

    /// <summary>Go: Error() -- the kind alone when there is no cause, "kind: cause" otherwise.</summary>
    public override string Message => InnerException is null ? _kind.Message : $"{_kind.Message}: {InnerException.Message}";

    /// <summary>Go: Is(target) -- matches the kind only; the cause is matched through InnerException by OxiaErrors.Is.</summary>
    public bool Is(Exception target) => ReferenceEquals(target, _kind);
}

internal static class AdminErrors
{
    /// <summary>Go: wrapAdminError -- the kind itself when there is no cause.</summary>
    internal static Exception WrapAdminError(Exception kind, Exception? cause)
        => cause is null ? kind : new AdminError(kind, cause);

    /// <summary>Go: mapAdminError -- null stays null.</summary>
    internal static Exception? MapAdminError(Exception? err)
    {
        if (err is null) return null;

        return OxiaErrors.GrpcCode(err) switch
        {
            StatusCode.PermissionDenied => WrapAdminError(ClientErrors.Unauthorized, err),
            StatusCode.Unauthenticated => WrapAdminError(ClientErrors.Unauthenticated, err),
            _ => WrapAdminError(ClientErrors.Unknown, err),
        };
    }
}
