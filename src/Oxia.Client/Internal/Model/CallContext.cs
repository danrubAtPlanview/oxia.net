// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/model/call_context.go   (CallContext, NotSent, IsNotSent)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The context of the caller of a write call.  The caller can give up on the call until it is handed to gRPC:
//              once the token is cancelled, the call is DROPPED instead, and it fails with an error telling that it was
//              not sent ("NotSent"): unlike a failure after the call was sent (e.g. a timeout waiting for the response),
//              it tells that the server did not apply the call and never will.
//              State machine (atomic): pending -> sent | dropped.  First transition wins.
//   Mapping  : context.Context -> CancellationToken; ctx.Err() -> OperationCanceledException (the token cannot tell
//              Canceled from DeadlineExceeded).  `*CallContext == nil` is valid in Go (always sent): use the static helpers.
// =====================================================================================================================
using Oxia.Common.Constant;

namespace Oxia.Client.Internal.Model;

public sealed class CallContext
{
    private const int Pending = 0, Sent = 1, Dropped = 2;

    private readonly CancellationToken _ct;
    private int _state;

    public CallContext(CancellationToken ct) => _ct = ct;

    /// <summary>Invoked right before the call is handed to gRPC. False if the call must be dropped instead (token done).</summary>
    public bool MarkSent()
    {
        if (_ct.IsCancellationRequested)
            Interlocked.CompareExchange(ref _state, Dropped, Pending);
        else
            Interlocked.CompareExchange(ref _state, Sent, Pending);
        return Volatile.Read(ref _state) == Sent;
    }

    /// <summary>Null-safe <see cref="MarkSent()"/>: a call without CallContext is always sent.</summary>
    public static bool MarkSent(CallContext? c) => c is null || c.MarkSent();

    /// <summary>
    /// Invoked by the caller once the token is done, if the call did not complete yet. Returns the error of the call: a call
    /// that was not handed to gRPC yet is dropped (NotSent error), while one that was may still be applied by the server.
    /// </summary>
    public Exception Cancel()
    {
        Interlocked.CompareExchange(ref _state, Dropped, Pending);
        if (Volatile.Read(ref _state) == Dropped) return Err();
        return new OperationCanceledException(_ct);
    }

    /// <summary>The error of a call that was dropped.</summary>
    public Exception Err() => NotSent(new OperationCanceledException(_ct))!;

    /// <summary>Marks err as the failure of a call that was never handed to gRPC.</summary>
    public static Exception? NotSent(Exception? err) => err is null ? null : new NotSentException(err);

    /// <summary>Tells if err is the failure of a call that was never handed to gRPC.</summary>
    public static bool IsNotSent(Exception? err)
    {
        while (err is not null)
        {
            if (err is NotSentException) return true;
            err = err.InnerException;
        }
        return false;
    }
}

/// <summary>Go: notSentError.</summary>
internal sealed class NotSentException : OxiaException
{
    public NotSentException(Exception inner) : base(inner.Message, inner) { }
}
