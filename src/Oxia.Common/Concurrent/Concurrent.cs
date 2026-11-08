// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "common utilities" component; replace bodies, keep signatures ***
//   Upstream : common/concurrent/future.go, common/concurrent/wait_group.go, common/channel/noblock.go (PushNoBlock)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Future<T>  -- single-assignment result; first Complete/Fail wins, later ones are ignored (Go: non-blocking
//                            push on a 1-buffered channel).  Wait honours a cancellation token, but a value that is already
//                            available WINS over an already-cancelled token (Go comment in future.go).
//              WaitGroup  -- like sync.WaitGroup but returns the first failure and honours cancellation.  Parties are
//                            counted at construction; Done() beyond the party count is ignored (Go: select/default).
//              Zero-allocation goal: implement Future<T> over ManualResetValueTaskSourceCore<T> (single awaiter), keep
//              it a sealed class so it can be pooled by callers.
// =====================================================================================================================
namespace Oxia.Common.Concurrent;

public sealed class Future<T>
{
    /// <summary>Waits until completed or failed; throws the failure; honours <paramref name="ct"/> (throws OperationCanceledException). Call once.</summary>
    public ValueTask<T> WaitAsync(CancellationToken ct) => throw new NotImplementedException();
    public void Complete(T result) => throw new NotImplementedException();
    public void Fail(Exception error) => throw new NotImplementedException();
}

public sealed class WaitGroup
{
    public WaitGroup(int parties) => throw new NotImplementedException();

    /// <summary>Go: Wait(ctx) error. Returns the first failure, or null when every party is done; cancellation returns an OperationCanceledException value (does not throw).</summary>
    public ValueTask<Exception?> WaitAsync(CancellationToken ct) => throw new NotImplementedException();
    public void Done() => throw new NotImplementedException();
    public void Fail(Exception error) => throw new NotImplementedException();
    /// <summary>Go: Go(f) -- runs f on the pool, Done on success / Fail on exception.</summary>
    public void Go(Func<Task> f) => throw new NotImplementedException();
}
