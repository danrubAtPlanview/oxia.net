// =====================================================================================================================
// PORT NOTE
//   Upstream : common/concurrent/wait_group.go   (WaitGroup, NewWaitGroup)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Like sync.WaitGroup, plus (1) the first failure is returned and (2) the wait honours a cancellation token.
//              Go: a channel of `parties` error slots.  Done pushes nil without blocking (dropped when full), Fail pushes the
//              error; Wait receives `parties` responses in order and returns the first non-nil one, or ctx.Err().
//   Implementation: the same shape -- a bounded System.Threading.Channels channel of `parties` responses
//              (null = done, non-null = failure), read by WaitAsync.  Wait may be called again after a cancelled Wait (the
//              cancelled Wait consumed nothing it had not been given, exactly like the Go select).
//   Divergences:
//      * Fail beyond the party count blocks forever in Go (full channel, no select); here it is dropped like Done.
//      * Wait(ctx) error -> `ValueTask<Exception?>`: null when every party is done, the first failure, or an
//        OperationCanceledException VALUE (not thrown) when the token is cancelled.  A ctx deadline is also an
//        OperationCanceledException (the token has no deadline notion).
//      * Go(f func() error) -> Go(Func<Task>): an exception thrown by f is the failure.
//      * `parties` below 0 is rejected (Go panics in make(chan, -1)); 0 parties makes Wait return null immediately.
// =====================================================================================================================
using System.Threading.Channels;

namespace Oxia.Common.Concurrent;

public sealed class WaitGroup
{
    private readonly int _parties;
    private readonly Channel<Exception?> _responses;

    public WaitGroup(int parties)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(parties);
        _parties = parties;
        _responses = Channel.CreateBounded<Exception?>(new BoundedChannelOptions(Math.Max(parties, 1))
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait, // TryWrite fails when full (Go: select/default)
        });
    }

    /// <summary>Go: Wait(ctx) error. Returns the first failure, or null when every party is done; cancellation returns an OperationCanceledException value (does not throw).</summary>
    public async ValueTask<Exception?> WaitAsync(CancellationToken ct)
    {
        var reader = _responses.Reader;
        for (var i = 0; i < _parties; i++)
        {
            if (!reader.TryRead(out var response))
            {
                try
                {
                    response = await reader.ReadAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException e) when (ct.IsCancellationRequested)
                {
                    return e;
                }
            }

            if (response is not null)
            {
                return response;
            }
        }

        // Everyone has completed successfully
        return null;
    }

    /// <summary>Signals that one party in the group is done (ignored when every party already responded).</summary>
    public void Done() => _responses.Writer.TryWrite(null);

    /// <summary>Signals that one party has failed in the operation.</summary>
    public void Fail(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        _responses.Writer.TryWrite(error);
    }

    /// <summary>Go: Go(f) -- runs f on the pool, Done on success / Fail on exception.</summary>
    public void Go(Func<Task> f)
    {
        ArgumentNullException.ThrowIfNull(f);
        _ = Task.Run(async () =>
        {
            try
            {
                await f().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Fail(e);
                return;
            }

            Done();
        });
    }
}
