// =====================================================================================================================
// PORT NOTE
//   Upstream : github.com/cenkalti/backoff/v4 v4.3.0  retry.go (Retry, RetryNotify, RetryNotifyWithTimer[AndData]),
//              error.go (PermanentError, Permanent), timer.go (Timer interface).  The default timer is SystemRetryTimer.cs.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The retry loop, with cenkalti's exact control flow:
//                 timer := timer ?? system timer; defer timer.Stop(); ctx := getContext(backOff); backOff.Reset();
//                 loop { res, err = operation(); if err == nil return res;
//                        if err is (wraps) a PermanentError return its inner error;
//                        next = backOff.NextBackOff(); if next == Stop { return ctx.Err() ?? err }
//                        notify?.(err, next); timer.Start(next); wait for timer.C or ctx.Done -> ctx.Err() }
//              The operation is always run at least once (a cancelled token is only looked at after a failure).
//   Divergences:
//      * `error` return -> the operation signals failure by THROWING; the loop rethrows (with the original stack trace) when it
//        gives up.  Any exception counts as a failure (including OperationCanceledException: like Go retrying a returned
//        context.Canceled until the backoff context itself is done).
//      * PermanentError -> PermanentException, looked up in the InnerException / AggregateException chain (errors.As).  The
//        loop throws OxiaErrors.ToThrowable(inner): a fresh exception that still satisfies OxiaErrors.Is(thrown, inner) when the
//        inner error is a shared sentinel (never throw a shared sentinel instance), the inner exception itself otherwise.
//      * ctx.Err() -> OperationCanceledException(token).  The loop honours both the token of an IBackOffContext (Go's
//        getContext) and the optional explicit `ct` argument (checked right after a failure and while waiting).
//      * backoff.Timer.C() channel -> IRetryTimer.WaitAsync(ct) (completes when the started timer fires, cancellation ->
//        OperationCanceledException).  The loop always calls timer.Stop() when it exits, for caller-supplied timers too.
//      * The caller-supplied timer is used for every wait, even for a zero interval (it may fire early on purpose).
// =====================================================================================================================
using System.Runtime.ExceptionServices;
using Oxia.Common.Constant;

namespace Oxia.Common.Time;

/// <summary>cenkalti <c>backoff.Timer</c> -- waits between attempts; Fires early if the owner wants (see ShardMapTimer).</summary>
public interface IRetryTimer
{
    void Start(TimeSpan duration);
    void Stop();
    /// <summary>Completes when the started timer fires. Cancellation -> OperationCanceledException.</summary>
    ValueTask WaitAsync(CancellationToken ct);
}

/// <summary>cenkalti <c>backoff.Permanent(err)</c>: stops the retry loop; the loop throws/returns <see cref="Inner"/>.</summary>
public sealed class PermanentException : Exception
{
    public Exception Inner { get; }
    public PermanentException(Exception inner) : base(inner.Message, inner) => Inner = inner;
}

/// <summary>
/// cenkalti retry loop.  Semantics (must match v4.3.0): Reset() the backoff once; run the operation; on success return;
/// on PermanentException throw its Inner; otherwise next = backOff.NextBackOff(); if next == Stop then (if the token
/// of an IBackOffContext is cancelled throw OperationCanceledException, else rethrow the last error); call notify(err,
/// next); start the timer; wait for the timer or token (token -> throw OperationCanceledException).
/// The operation signals failure by THROWING (RpcException etc.); callers translate with OxiaErrors.FromGrpcError.
/// </summary>
public static class Retry
{
    public static async ValueTask<T> RetryNotifyWithTimerAsync<T>(Func<ValueTask<T>> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, IRetryTimer? timer, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(backOff);

        var backOffCt = BackOffFactory.GetContext(backOff);
        SystemRetryTimer? ownTimer = null;
        CancellationTokenSource? linked = null;
        try
        {
            backOff.Reset();
            while (true)
            {
                Exception err;
                try
                {
                    return await operation().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    err = e;
                }

                var permanent = FindPermanent(err);
                if (permanent is not null)
                {
                    throw OxiaErrors.ToThrowable(permanent.Inner);
                }

                // The explicit token plays the role of an extra context of the backoff
                var next = ct.IsCancellationRequested ? BackOffConstants.Stop : backOff.NextBackOff();
                if (next == BackOffConstants.Stop)
                {
                    if (backOffCt.IsCancellationRequested) throw new OperationCanceledException(backOffCt);
                    if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
                    ExceptionDispatchInfo.Throw(err);
                }

                notify?.Invoke(err, next);

                var waitCt = CombineTokens(backOffCt, ct, ref linked);
                if (waitCt.IsCancellationRequested) throw new OperationCanceledException(CancelledOf(backOffCt, ct));

                timer ??= ownTimer = new SystemRetryTimer();
                timer.Start(next);
                try
                {
                    await timer.WaitAsync(waitCt).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (waitCt.IsCancellationRequested)
                {
                    throw new OperationCanceledException(CancelledOf(backOffCt, ct));
                }
            }
        }
        finally
        {
            timer?.Stop();
            ownTimer?.Dispose();
            linked?.Dispose();
        }
    }

    public static async ValueTask RetryNotifyWithTimerAsync(Func<ValueTask> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, IRetryTimer? timer, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        await RetryNotifyWithTimerAsync<bool>(async () =>
        {
            await operation().ConfigureAwait(false);
            return true;
        }, backOff, notify, timer, ct).ConfigureAwait(false);
    }

    public static ValueTask<T> RetryNotifyAsync<T>(Func<ValueTask<T>> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, CancellationToken ct = default)
        => RetryNotifyWithTimerAsync(operation, backOff, notify, null, ct);

    public static ValueTask RetryNotifyAsync(Func<ValueTask> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, CancellationToken ct = default)
        => RetryNotifyWithTimerAsync(operation, backOff, notify, null, ct);

    public static ValueTask<T> RetryAsync<T>(Func<ValueTask<T>> operation, IBackOff backOff, CancellationToken ct = default)
        => RetryNotifyWithTimerAsync(operation, backOff, null, null, ct);

    public static ValueTask RetryAsync(Func<ValueTask> operation, IBackOff backOff, CancellationToken ct = default)
        => RetryNotifyWithTimerAsync(operation, backOff, null, null, ct);

    /// <summary>errors.As(err, &amp;permanent).</summary>
    private static PermanentException? FindPermanent(Exception? err)
    {
        while (err is not null)
        {
            if (err is PermanentException p) return p;
            if (err is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions)
                {
                    var found = FindPermanent(inner);
                    if (found is not null) return found;
                }

                return null;
            }

            err = err.InnerException;
        }

        return null;
    }

    private static CancellationToken CancelledOf(CancellationToken backOffCt, CancellationToken ct)
        => backOffCt.IsCancellationRequested ? backOffCt : ct;

    /// <summary>One token that is cancelled when either is; no allocation unless both can be cancelled (then once per call).</summary>
    private static CancellationToken CombineTokens(CancellationToken a, CancellationToken b, ref CancellationTokenSource? linked)
    {
        if (!b.CanBeCanceled || a == b) return a;
        if (!a.CanBeCanceled) return b;
        linked ??= CancellationTokenSource.CreateLinkedTokenSource(a, b);
        return linked.Token;
    }
}
