// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/rpc_provider.go  (type ShardMapTimer: Start / Stop / C) -- plus the cenkalti `backoff.Timer`
//              contract it implements.  The Go file executor.go only declares the Executor interface (InternalContracts.cs).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Waits between the attempts of a retry like the system timer, but it fires as soon as the shard map changes:
//              the next attempt goes to the shard's new leader, or finds that the shard was split.  `Changed` is the "shard
//              map changed" task of the shard manager as it was when the last attempt started; the value current at
//              Start() is the one that counts (Go captures the channel in Start).
//   Divergences:
//      * Go starts a goroutine per Start() that selects on {timer, changed, stop} and pushes into a 1-buffered channel.  Here
//        WaitAsync awaits Task.Delay against the `Changed` task directly -- no goroutine, no channel, and a timer instance is
//        reused for every attempt of an operation (Start/WaitAsync/Stop cycles).  If the shard map had already changed when the
//        wait begins, WaitAsync completes synchronously without touching a timer.
//      * Go's Stop() leaves a pending wait blocked forever; a wait interrupted by Stop() throws OperationCanceledException
//        (the retry loop never does this: it calls Stop() only when it is done).
// =====================================================================================================================
using Oxia.Common.Time;

namespace Oxia.Client.Internal;

/// <summary>Go: ShardMapTimer.</summary>
public sealed class ShardMapTimer : IRetryTimer
{
    private TimeSpan _duration;
    private Task? _changedAtStart;
    private CancellationTokenSource? _stop;

    /// <summary>The "shard map changed" task of the shard manager when the last attempt started (null: never fires early).</summary>
    public Task? Changed { get; set; }

    public void Start(TimeSpan duration)
    {
        _duration = duration;
        _changedAtStart = Changed;
        if (_stop is null || !_stop.TryReset()) _stop = new CancellationTokenSource();
    }

    public void Stop() => _stop?.Cancel();

    public ValueTask WaitAsync(CancellationToken ct)
    {
        var changed = _changedAtStart;
        if (changed is { IsCompleted: true }) return ValueTask.CompletedTask;
        return new ValueTask(WaitCoreAsync(changed, _duration, _stop?.Token ?? default, ct));
    }

    private static async Task WaitCoreAsync(Task? changed, TimeSpan duration, CancellationToken stop, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, stop);
        var delay = Task.Delay(duration, linked.Token);
        if (changed is not null && ReferenceEquals(await Task.WhenAny(delay, changed).ConfigureAwait(false), changed))
        {
            // fires like the timer would: the retry loop goes ahead with the next attempt
            await linked.CancelAsync().ConfigureAwait(false);
            return;
        }

        await delay.ConfigureAwait(false);
    }
}
