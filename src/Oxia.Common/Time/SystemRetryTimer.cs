// =====================================================================================================================
// PORT NOTE
//   Upstream : github.com/cenkalti/backoff/v4 v4.3.0  timer.go  (defaultTimer: a time.Timer that is created on the first
//              Start and Reset on the following ones)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The timer the retry loop uses when the caller does not supply one.  Reusable: one System.Threading.Timer
//              created lazily and re-armed by every Start; the wait is a ValueTask backed by this object (an
//              IValueTaskSource over ManualResetValueTaskSourceCore), so a Start/WaitAsync cycle allocates nothing but the
//              cancellation registration of the token.
//   Divergences: `C() <-chan time.Time` -> `WaitAsync(CancellationToken)`.  Stop() on an armed timer fails a pending wait with
//              OperationCanceledException (a Go receiver would block forever).  Negative durations fire immediately; durations
//              beyond the Timer limit (~49 days) are clamped.
// =====================================================================================================================
using System.Threading.Tasks.Sources;

namespace Oxia.Common.Time;

/// <summary>
/// Default <see cref="IRetryTimer"/>: a re-armable one-shot timer.  One <c>Start</c> must be followed by at most one
/// <c>WaitAsync</c>; <c>Start</c> may be called again after the wait completed (or instead of it).  Not for concurrent use
/// by several waiters.
/// </summary>
public sealed class SystemRetryTimer : IRetryTimer, IValueTaskSource, IDisposable
{
    private const int Idle = 0;
    private const int Armed = 1;
    private const int Completed = 2;

    private static readonly Action<object?, CancellationToken> CancelCallback =
        static (state, token) => ((SystemRetryTimer)state!).OnCancel(token);

    private static readonly TimerCallback FireCallback = static state => ((SystemRetryTimer)state!).OnFire();

    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
    private Timer? _timer;
    private CancellationTokenRegistration _registration;
    private int _state;
    private bool _disposed;

    public void Start(TimeSpan duration)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _registration.Dispose();
        _registration = default;
        _core.Reset();
        Volatile.Write(ref _state, Armed);

        // Timer.Change rejects negative values (other than -1ms == infinite) and anything above uint.MaxValue - 1 ms
        if (duration < TimeSpan.Zero) duration = TimeSpan.Zero;
        else if (duration.TotalMilliseconds > uint.MaxValue - 1) duration = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

        if (_timer is null) _timer = new Timer(FireCallback, this, duration, Timeout.InfiniteTimeSpan);
        else _timer.Change(duration, Timeout.InfiniteTimeSpan);
    }

    public void Stop()
    {
        if (Interlocked.CompareExchange(ref _state, Completed, Armed) != Armed) return;

        _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _core.SetException(new OperationCanceledException());
    }

    public ValueTask WaitAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _state) == Idle) throw new InvalidOperationException("The timer was not started");

        var version = _core.Version;
        if (ct.CanBeCanceled)
        {
            // Runs inline when the token is already cancelled
            _registration = ct.UnsafeRegister(CancelCallback, this);
        }

        return new ValueTask(this, version);
    }

    public void Dispose()
    {
        if (_disposed) return;
        Stop();
        _disposed = true;
        _registration.Dispose();
        _registration = default;
        _timer?.Dispose();
    }

    private void OnFire()
    {
        if (Interlocked.CompareExchange(ref _state, Completed, Armed) == Armed)
        {
            _core.SetResult(true);
        }
    }

    private void OnCancel(CancellationToken token)
    {
        if (Interlocked.CompareExchange(ref _state, Completed, Armed) == Armed)
        {
            try
            {
                _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
            catch (ObjectDisposedException)
            {
                // disposed concurrently: nothing to disarm
            }

            _core.SetException(new OperationCanceledException(token));
        }
    }

    void IValueTaskSource.GetResult(short token)
    {
        try
        {
            _core.GetResult(token);
        }
        finally
        {
            // the wait is over: stop listening to the token
            _registration.Dispose();
            _registration = default;
        }
    }

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
