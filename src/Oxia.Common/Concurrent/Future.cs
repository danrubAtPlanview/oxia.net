// =====================================================================================================================
// PORT NOTE
//   Upstream : common/concurrent/future.go   (Future[T], NewFuture)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A single-assignment result.  Go: a channel with a buffer of 1 and a non-blocking push, so the FIRST
//              Complete/Fail wins and later ones are dropped; Wait(ctx) prefers an already-available value over a done ctx
//              (select would pick at random) and otherwise waits for the value or the ctx.  A Wait that gives up because of
//              the ctx leaves the future untouched: it can be completed afterwards and waited on again.
//   Implementation: sealed class; the result lives in plain fields published by one Interlocked flag, and the waiter is a
//              ManualResetValueTaskSourceCore<T> (the future is its own IValueTaskSource<T>): no Task, no TaskCompletionSource,
//              no allocation on the success path.  The waiter slot is re-armed by every WaitAsync (waits are sequential,
//              "you should only call Wait once at a time"); `Reset()` lets a caller pool instances.
//              First-wins is a CAS shared by Complete and Fail; a waiter is resolved exactly once by whoever wins the CAS on the
//              waiter state (the completer or the cancellation callback of the token).
//   Divergences:
//      * ctx.Err() -> OperationCanceledException(token).
//      * The result stays available after it was consumed (Go's channel would be empty: a second Wait blocks forever).
//      * Fail(err) of a shared sentinel (OxiaErrors.*) makes WaitAsync throw OxiaErrors.ToThrowable(err) -- a fresh exception
//        that still satisfies OxiaErrors.Is(thrown, err); other exceptions are rethrown as they are.
//      * Continuations run asynchronously by default (a Go receiver is woken through the scheduler, never inline in the
//        sender); pass `runContinuationsAsynchronously: false` only when the completer is known to be safe to run them.
//      * Added: Reset() (reuse), IsCompleted.
// =====================================================================================================================
using System.Threading.Tasks.Sources;
using Oxia.Common.Constant;

namespace Oxia.Common.Concurrent;

public sealed class Future<T> : IValueTaskSource<T>
{
    private const int NoWaiter = 0;
    private const int Waiting = 1;
    private const int Resolved = 2;

    private static readonly Action<object?, CancellationToken> CancelCallback =
        static (state, token) => ((Future<T>)state!).OnCancel(token);

    private ManualResetValueTaskSourceCore<T> _core;
    private CancellationTokenRegistration _registration;

    private T? _value;
    private Exception? _error;
    private int _claimed; // 1 once a Complete / Fail won
    private int _ready; // 1 once _value / _error are published
    private int _waiter; // NoWaiter / Waiting / Resolved

    public Future(bool runContinuationsAsynchronously = true)
    {
        _core = new ManualResetValueTaskSourceCore<T> { RunContinuationsAsynchronously = runContinuationsAsynchronously };
    }

    /// <summary>True once a result or failure has been recorded.</summary>
    public bool IsCompleted => Volatile.Read(ref _ready) != 0;

    /// <summary>Waits until completed or failed; throws the failure; honours <paramref name="ct"/> (throws OperationCanceledException). Call once.</summary>
    public ValueTask<T> WaitAsync(CancellationToken ct)
    {
        // A ready value wins over a done token: do not even look at the token when something is already recorded
        if (Volatile.Read(ref _ready) != 0)
        {
            return ReadyResult();
        }

        // Arm the waiter slot, then re-check: a completer that published before the slot was armed would not see the waiter
        _core.Reset();
        var version = _core.Version;
        Interlocked.Exchange(ref _waiter, Waiting);

        if (Volatile.Read(ref _ready) != 0)
        {
            TryResolveWaiter();
        }
        else if (ct.CanBeCanceled)
        {
            // Runs inline when the token is already cancelled
            _registration = ct.UnsafeRegister(CancelCallback, this);
        }

        return new ValueTask<T>(this, version);
    }

    /// <summary>Completes the future with <paramref name="result"/>, unless it is already completed or failed.</summary>
    public void Complete(T result)
    {
        if (Interlocked.CompareExchange(ref _claimed, 1, 0) != 0) return;

        _value = result;
        Publish();
    }

    /// <summary>Signals that the operation failed, unless the future is already completed or failed.</summary>
    public void Fail(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (Interlocked.CompareExchange(ref _claimed, 1, 0) != 0) return;

        _error = OxiaErrors.ToThrowable(error);
        Publish();
    }

    /// <summary>
    /// Makes the instance reusable.  Only valid once the previous result was consumed (or abandoned) and no other thread
    /// can still call Complete/Fail for the previous round.
    /// </summary>
    public void Reset()
    {
        _registration.Dispose();
        _registration = default;
        _core.Reset();
        _value = default;
        _error = null;
        Volatile.Write(ref _waiter, NoWaiter);
        Volatile.Write(ref _ready, 0);
        Volatile.Write(ref _claimed, 0);
    }

    private void Publish()
    {
        Interlocked.Exchange(ref _ready, 1); // full fence: pairs with the one in WaitAsync
        TryResolveWaiter();
    }

    private ValueTask<T> ReadyResult()
    {
        if (_error is null)
        {
            return new ValueTask<T>(_value!);
        }

        _core.Reset();
        _core.SetException(_error);
        return new ValueTask<T>(this, _core.Version);
    }

    private void TryResolveWaiter()
    {
        if (Interlocked.CompareExchange(ref _waiter, Resolved, Waiting) != Waiting) return;

        if (_error is null) _core.SetResult(_value!);
        else _core.SetException(_error);
    }

    private void OnCancel(CancellationToken token)
    {
        // Only resolves the waiter: the future itself stays pending and can still be completed
        if (Interlocked.CompareExchange(ref _waiter, Resolved, Waiting) == Waiting)
        {
            _core.SetException(new OperationCanceledException(token));
        }
    }

    T IValueTaskSource<T>.GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            _registration.Dispose();
            _registration = default;
        }
    }

    ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<T>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
