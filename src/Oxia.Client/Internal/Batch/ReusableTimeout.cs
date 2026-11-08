// =====================================================================================================================
// PORT NOTE   (no upstream counterpart)
//   Upstream : oxia/internal/batch/read_batch.go, write_batch.go  (`ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)`)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The per-batch request timeout.  Go creates a context with a timer per batch; allocating a
//              CancellationTokenSource (+ its timer) per batch would be the direct translation.  A pooled batch owns ONE
//              CancellationTokenSource that is re-armed with CancelAfter and recycled with TryReset (which fails -- and a new
//              source is created -- only when the timeout actually fired, i.e. when the request timed out anyway).
//              The timer is armed only AFTER the executor was invoked and did not complete synchronously (the synchronous
//              prefix of a call cannot be waiting for anything): an executor that completes synchronously -- fakes, cached
//              answers -- never touches the timer.  A non-positive timeout is armed up front (an expired context).
//   Contract : the token handed out by Token is valid until Stop; the executor must not keep it after its ValueTask completed.
// =====================================================================================================================
namespace Oxia.Client.Internal.Batch;

internal sealed class ReusableTimeout
{
    private CancellationTokenSource? _cts;

    /// <summary>The token of the request (not armed yet).</summary>
    public CancellationToken Token => (_cts ??= new CancellationTokenSource()).Token;

    /// <summary>Go: the deadline of context.WithTimeout(context.Background(), timeout), counted from now.</summary>
    public void Arm(TimeSpan timeout)
    {
        var cts = _cts ??= new CancellationTokenSource();
        // CancelAfter accepts at most uint.MaxValue - 1 milliseconds (~49 days): anything longer never fires in practice.
        cts.CancelAfter(timeout.TotalMilliseconds >= uint.MaxValue - 1 ? Timeout.InfiniteTimeSpan : timeout);
    }

    /// <summary>Go: cancel().  Makes the source reusable, or drops it if it was already cancelled.</summary>
    public void Stop()
    {
        var cts = _cts;
        if (cts is null) return;

        if (!cts.TryReset())
        {
            cts.Dispose();
            _cts = null;
        }
    }
}
