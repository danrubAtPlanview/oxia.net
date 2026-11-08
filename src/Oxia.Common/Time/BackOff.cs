// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "common utilities" component; replace bodies, keep signatures ***
//   Upstream : common/time/backoff.go, common/time/jitter.go, and the subset of github.com/cenkalti/backoff/v4 v4.3.0
//              that the client uses (ExponentialBackOff, WithContext, Retry, RetryNotify, RetryNotifyWithTimer, Permanent,
//              ZeroBackOff, Timer).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Exponential backoff with jitter + a retry loop with cenkalti semantics (see Retry.cs notes below).
// =====================================================================================================================
namespace Oxia.Common.Time;

/// <summary>cenkalti <c>backoff.BackOff</c>.  <see cref="BackOffConstants.Stop"/> from NextBackOff means "give up".</summary>
public interface IBackOff
{
    TimeSpan NextBackOff();
    void Reset();
}

/// <summary>cenkalti <c>backoff.BackOffContext</c>: a backoff bound to a cancellation token. NextBackOff returns Stop once cancelled.</summary>
public interface IBackOffContext : IBackOff
{
    CancellationToken Context { get; }
}

public static class BackOffConstants
{
    /// <summary>backoff.Stop (-1ns in Go).</summary>
    public static readonly TimeSpan Stop = TimeSpan.FromTicks(-1);
    public static readonly TimeSpan DefaultInitialInterval = TimeSpan.FromMilliseconds(500);
    public const double DefaultRandomizationFactor = 0.5;
    public const double DefaultMultiplier = 1.5;
    public static readonly TimeSpan DefaultMaxInterval = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan DefaultMaxElapsedTime = TimeSpan.FromMinutes(15);
}

/// <summary>cenkalti ExponentialBackOff (same field names/semantics; MaxElapsedTime == 0 means never stop).</summary>
public sealed class ExponentialBackOff : IBackOff
{
    public TimeSpan InitialInterval = BackOffConstants.DefaultInitialInterval;
    public double RandomizationFactor = BackOffConstants.DefaultRandomizationFactor;
    public double Multiplier = BackOffConstants.DefaultMultiplier;
    public TimeSpan MaxInterval = BackOffConstants.DefaultMaxInterval;
    public TimeSpan MaxElapsedTime = BackOffConstants.DefaultMaxElapsedTime;
    public TimeSpan NextBackOff() => throw new NotImplementedException();
    public void Reset() => throw new NotImplementedException();
}

/// <summary>cenkalti ZeroBackOff: retries immediately, forever (until the context ends).</summary>
public sealed class ZeroBackOff : IBackOff
{
    public TimeSpan NextBackOff() => TimeSpan.Zero;
    public void Reset() { }
}

/// <summary>Wraps a backoff.BackOff with a token (cenkalti <c>backoff.WithContext</c>).</summary>
public static class BackOffFactory
{
    public static IBackOffContext WithContext(IBackOff backOff, CancellationToken ct) => throw new NotImplementedException();

    /// <summary>Go: time.NewBackOff(ctx) -- 100ms initial interval, never stops.</summary>
    public static IBackOffContext NewBackOff(CancellationToken ct) => throw new NotImplementedException();

    /// <summary>Go: time.NewBackOffWithInitialInterval(ctx, interval).</summary>
    public static IBackOffContext NewBackOffWithInitialInterval(CancellationToken ct, TimeSpan initialInterval)
        => throw new NotImplementedException();

    public static readonly TimeSpan DefaultInitialInterval = TimeSpan.FromMilliseconds(100);
    public static readonly TimeSpan SlowInitialInterval = TimeSpan.FromSeconds(10);
}

/// <summary>Go: time.ConcurrentBackOff -- mutex-protected wrapper.</summary>
public sealed class ConcurrentBackOff : IBackOffContext
{
    public ConcurrentBackOff(IBackOff inner) => throw new NotImplementedException();
    public CancellationToken Context => throw new NotImplementedException();
    public TimeSpan NextBackOff() => throw new NotImplementedException();
    public void Reset() => throw new NotImplementedException();
}

/// <summary>Go: time.Jitter(interval, maxDeviation).</summary>
public static class Jitter
{
    public static TimeSpan Apply(TimeSpan interval, TimeSpan maxDeviation) => throw new NotImplementedException();
}

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
    public static ValueTask<T> RetryNotifyWithTimerAsync<T>(Func<ValueTask<T>> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, IRetryTimer? timer, CancellationToken ct = default)
        => throw new NotImplementedException();

    public static ValueTask RetryNotifyWithTimerAsync(Func<ValueTask> operation, IBackOff backOff,
        Action<Exception, TimeSpan>? notify, IRetryTimer? timer, CancellationToken ct = default)
        => throw new NotImplementedException();

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
}
