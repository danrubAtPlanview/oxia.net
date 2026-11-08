// =====================================================================================================================
// PORT NOTE
//   Upstream : common/time/backoff.go  (NewBackOff, NewBackOffWithInitialInterval, ConcurrentBackOff)
//              + github.com/cenkalti/backoff/v4 v4.3.0: backoff.go (BackOff, Stop, ZeroBackOff), context.go (BackOffContext,
//              WithContext, getContext).  ExponentialBackOff is in ExponentialBackOff.cs, the retry loop in Retry.cs, the
//              default timer in SystemRetryTimer.cs.  common/time/clock.go (Clock, MockedClock) is not used by the client.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The backoff abstraction the client's retry loops are written against.  `IBackOff.NextBackOff` returns the
//              wait before the next attempt or `BackOffConstants.Stop`; an `IBackOffContext` is a backoff bound to a
//              cancellation token (Go's context.Context) that returns Stop once the token is cancelled.
//   Divergences:
//      * time.Duration (ns) -> TimeSpan (100ns ticks); `backoff.Stop` is TimeSpan.FromTicks(-1) like Go's Duration(-1).
//      * context.Context -> CancellationToken.  `Context` of a backoff that is not bound to a token is
//        CancellationToken.None (Go: context.Background()).
//      * `WithContext` on an already-wrapped backoff re-wraps the inner backoff with the new token (as cenkalti does).
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

/// <summary>cenkalti ZeroBackOff: retries immediately, forever (until the context ends).</summary>
public sealed class ZeroBackOff : IBackOff
{
    public TimeSpan NextBackOff() => TimeSpan.Zero;
    public void Reset() { }
}

/// <summary>Wraps a backoff.BackOff with a token (cenkalti <c>backoff.WithContext</c>).</summary>
public static class BackOffFactory
{
    /// <summary>cenkalti <c>backoff.WithContext(b, ctx)</c>.</summary>
    public static IBackOffContext WithContext(IBackOff backOff, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(backOff);
        if (backOff is BackOffContext existing)
        {
            // cenkalti: re-wrap the inner backoff with the new context instead of stacking wrappers
            return new BackOffContext(existing.Inner, ct);
        }

        return new BackOffContext(backOff, ct);
    }

    /// <summary>Go: time.NewBackOff(ctx) -- 100ms initial interval, never stops.</summary>
    public static IBackOffContext NewBackOff(CancellationToken ct) => NewBackOffWithInitialInterval(ct, DefaultInitialInterval);

    /// <summary>Go: time.NewBackOffWithInitialInterval(ctx, interval).</summary>
    public static IBackOffContext NewBackOffWithInitialInterval(CancellationToken ct, TimeSpan initialInterval)
        => WithContext(new ExponentialBackOff
        {
            InitialInterval = initialInterval,
            RandomizationFactor = BackOffConstants.DefaultRandomizationFactor,
            Multiplier = BackOffConstants.DefaultMultiplier,
            MaxInterval = BackOffConstants.DefaultMaxInterval,
            MaxElapsedTime = TimeSpan.Zero, // Never stop trying
        }, ct);

    public static readonly TimeSpan DefaultInitialInterval = TimeSpan.FromMilliseconds(100);
    public static readonly TimeSpan SlowInitialInterval = TimeSpan.FromSeconds(10);

    /// <summary>cenkalti's unexported <c>backOffContext</c>.</summary>
    private sealed class BackOffContext : IBackOffContext
    {
        public BackOffContext(IBackOff inner, CancellationToken ct)
        {
            Inner = inner;
            Context = ct;
        }

        public IBackOff Inner { get; }
        public CancellationToken Context { get; }

        public TimeSpan NextBackOff() => Context.IsCancellationRequested ? BackOffConstants.Stop : Inner.NextBackOff();

        public void Reset() => Inner.Reset();
    }

    /// <summary>cenkalti <c>getContext(b)</c>.</summary>
    internal static CancellationToken GetContext(IBackOff backOff)
        => backOff is IBackOffContext bc ? bc.Context : CancellationToken.None;
}

/// <summary>Go: time.ConcurrentBackOff -- mutex-protected wrapper.</summary>
public sealed class ConcurrentBackOff : IBackOffContext
{
    private readonly object _mu = new();
    private readonly IBackOff _bo;

    public ConcurrentBackOff(IBackOff inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _bo = inner;
    }

    /// <summary>
    /// Exposes the context of the wrapped backoff.  The retry loop only interrupts its wait between retries through an
    /// <see cref="IBackOffContext"/>, so without it a cancelled token would not stop the wait.
    /// </summary>
    public CancellationToken Context
    {
        get
        {
            lock (_mu)
            {
                return _bo is IBackOffContext bc ? bc.Context : CancellationToken.None;
            }
        }
    }

    public TimeSpan NextBackOff()
    {
        lock (_mu)
        {
            return _bo.NextBackOff();
        }
    }

    public void Reset()
    {
        lock (_mu)
        {
            _bo.Reset();
        }
    }
}
