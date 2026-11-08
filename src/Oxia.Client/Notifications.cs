// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/notifications.go   (notifications, shardInit, shardNotificationsManager, convertNotification[Type])
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The feed of changes of the database.  One subscription manager per shard multiplexes the notification batches
//              of its shard into the single user-facing channel; the very first batch of a NEW subscription only confirms that
//              the server created the notification cursor and is discarded (it initializes the subscription, which
//              NotificationsImpl.CreateAsync waits for, bounded by the request timeout); a resumed subscription starts after the last
//              batch received, and a confirmation batch (empty, timestamp 0) that starts after the requested offset tells that
//              the retention deleted notifications in between: a NotificationsMissed event is delivered.  When a shard is
//              split (it leaves the shard map and others replace it) its stream is cancelled and the subscriptions to the
//              shards that replaced it take over, from the last batch received (or, if the parent was not initialized yet,
//              they initialize it in its place: ShardInit counts the subscriptions that still must receive their first batch).
//              The user-facing channel is completed once every subscription stopped.
//   Divergences:
//      * `chan *Notification` (buffered 100) -> bounded Channel<Notification>(100); sending honours the token.  One Notification
//        object is allocated per entry, nothing else on the delivery path.
//      * sync.WaitGroup `running` (Add after creation) -> an Interlocked counter that completes the channel at zero; the
//        goroutine that waited on it is not needed.  ctxMultiplexChanClosed (a context closed on completion) -> a Task.
//      * Go `context.DeadlineExceeded` of the initialization timeout -> TimeoutException.
//      * Errors returned by getNotifications()/deliver() are THROWN (the retry loop's contract); io.EOF is OxiaErrors.EndOfStream
//        and backoff.Permanent(err) is PermanentException.  The `Fail`ed init error is thrown by CreateAsync (Go: returned).
//      * The panic("Invalid notification type") of convertNotificationType is an InvalidOperationException with that message.
//      * Fields of the manager are internal and settable: the Go tests build the structs by hand.
// =====================================================================================================================
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;
using WireNotification = Oxia.Common.Proto.Notification;
using WireNotificationType = Oxia.Common.Proto.NotificationType;

namespace Oxia.Client;

internal sealed class NotificationsImpl : INotifications
{
    /// <summary>errShardSplit ends the subscription to a shard that was split: the subscriptions to the shards that replaced it take over.</summary>
    internal static readonly OxiaException ErrShardSplit = new("oxia: the shard was split");

    internal Channel<Notification> MultiplexCh = null!;
    internal IShardManager ShardManager = null!;
    internal IRpcProvider RpcProvider = null!;

    internal WaitGroup InitWaitGroup = null!;
    internal CancellationToken Ctx;
    internal Action Cancel = static () => { };

    // The subscriptions to the shards: the user-facing channel is completed once they all stopped
    private int _running;

    // Go: ctxMultiplexChanClosed -- done once the user-facing channel is completed
    internal Task MultiplexChanClosed = Task.CompletedTask;
    private TaskCompletionSource? _multiplexClosed;
    private CancellationTokenSource? _cts;

    // Tests build the struct field by field, like Go.
    internal NotificationsImpl() { }

    /// <summary>Go: newNotifications.  Throws if the subscription to any shard cannot be established (non-retryable error) or the request timeout elapses.</summary>
    public static async ValueTask<NotificationsImpl> CreateAsync(CancellationToken clientCtx, ClientOptions options,
        IRpcProvider rpc, IShardManager sm)
    {
        var nm = new NotificationsImpl
        {
            MultiplexCh = Channel.CreateBounded<Notification>(new BoundedChannelOptions(100)
            {
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false,
            }),
            ShardManager = sm,
            RpcProvider = rpc,
        };

        var cts = CancellationTokenSource.CreateLinkedTokenSource(clientCtx);
        nm._cts = cts;
        nm.Ctx = cts.Token;
        nm.Cancel = () =>
        {
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
        };
        nm._multiplexClosed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        nm.MultiplexChanClosed = nm._multiplexClosed.Task;

        // Bounds the wait for the initialization (created first: the token of nm may be disposed once the channel is completed)
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(nm.Ctx);
        timeoutCts.CancelAfter(options.RequestTimeout);

        // Create a notification manager for each shard
        var shards = sm.GetAll();
        nm.InitWaitGroup = new WaitGroup(shards.Length);

        nm._running = shards.Length;
        foreach (long shard in shards)
        {
            var init = new ShardInit();
            init.Store(1);
            ShardNotificationsManager.Start(shard, nm, null, init);
        }

        // Go: a goroutine waits for the managers and then closes the channel; with no shard that is immediate
        if (shards.Length == 0) nm.CompleteMultiplexChannel();

        // Wait for the notifications on all the shards to be initialized
        Exception? err = await nm.InitWaitGroup.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
        if (err is not null)
        {
            // Stop the per-shard managers that may still be retrying
            nm.Cancel();
            if (err is OperationCanceledException && timeoutCts.IsCancellationRequested && !nm.Ctx.IsCancellationRequested)
                err = new TimeoutException("context deadline exceeded");
            throw OxiaErrors.ToThrowable(err);
        }

        return nm;
    }

    public ChannelReader<Notification> Ch => MultiplexCh.Reader;

    public async ValueTask CloseAsync()
    {
        // Interrupt the go-routines receiving notifications on all the shards
        Cancel();

        // Wait until the all the go-routines are stopped and the user-facing channel is closed
        await MultiplexChanClosed.ConfigureAwait(false);

        // Ensure the channel is empty, so that the user will not see any notifications after the close
        while (MultiplexCh.Reader.TryRead(out _)) { }
    }

    /// <summary>Go: nm.running.Add(n).  Called before the subscription that adds them is done.</summary>
    internal void AddRunning(int n) => Interlocked.Add(ref _running, n);

    /// <summary>Go: nm.running.Done().</summary>
    internal void RunningDone()
    {
        if (Interlocked.Decrement(ref _running) == 0) CompleteMultiplexChannel();
    }

    private void CompleteMultiplexChannel()
    {
        MultiplexCh.Writer.TryComplete();
        _multiplexClosed?.TrySetResult();
        try { _cts?.Dispose(); } catch (ObjectDisposedException) { }
    }
}

/// <summary>
/// The initialization of the subscription to a shard of the initial shard map, which CreateAsync waits for: the subscription
/// is established with the first batch it receives, or, when the shard is split before, with those of the subscriptions to the
/// shards that replaced it.
/// </summary>
internal sealed class ShardInit
{
    // The subscriptions that haven't received their first batch yet
    private long _pending;

    public void Store(long value) => Interlocked.Exchange(ref _pending, value);
    public long Load() => Interlocked.Read(ref _pending);
    public long Add(long delta) => Interlocked.Add(ref _pending, delta);
}

/// <summary>Manages the notifications for a specific shard.</summary>
internal sealed class ShardNotificationsManager
{
    internal long Shard;
    internal CancellationToken Ctx;
    internal NotificationsImpl Nm = null!;
    internal IBackOff Backoff = null!;
    internal long LastOffsetReceived;
    internal bool Initialized;
    // The initialization that the subscription counts for, until it is initialized
    internal ShardInit? Init;
    internal ILogger Log = null!;

    // Tests build the struct field by field, like Go.
    internal ShardNotificationsManager() { }

    /// <summary>
    /// Go: newShardNotificationsManager.  Starts the subscription to a shard: from the batch after
    /// <paramref name="startOffsetExclusive"/>, or from the batch that the subscription receives first, which only
    /// initializes it, when startOffsetExclusive is null.
    /// </summary>
    internal static void Start(long shard, NotificationsImpl nm, long? startOffsetExclusive, ShardInit? init)
    {
        var snm = new ShardNotificationsManager
        {
            Shard = shard,
            Ctx = nm.Ctx,
            Nm = nm,
            LastOffsetReceived = -1,
            Init = init,
            Backoff = BackOffFactory.NewBackOffWithInitialInterval(nm.Ctx, TimeSpan.FromSeconds(1)),
            Log = OxiaLog.Create("oxia-notifications-manager"),
        };
        if (startOffsetExclusive is { } start)
        {
            snm.LastOffsetReceived = start;
            snm.Initialized = true;
            snm.Init = null;
        }

        _ = Task.Run(snm.GetNotificationsWithRetriesAsync);
    }

    internal async Task GetNotificationsWithRetriesAsync()
    {
        try
        {
            // A change of the shard map ends the wait for the next attempt: it can be the split of the shard
            var timer = new ShardMapTimer();
            Exception? err = null;
            try
            {
                await Retry.RetryNotifyWithTimerAsync(async () =>
                {
                    timer.Changed = Nm.ShardManager.Changed();
                    await GetNotificationsAsync().ConfigureAwait(false);
                }, Backoff, OnRetry, timer, Ctx).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                err = e;
            }

            if (OxiaErrors.Is(err, NotificationsImpl.ErrShardSplit)) FollowSplit();
        }
        finally
        {
            // Signal that this shard notification manager is now closed
            Nm.RunningDone();
        }
    }

    private void OnRetry(Exception err, TimeSpan duration)
    {
        if (!OxiaErrors.IsCanceled(err))
        {
            Log.LogError(err, "Error while getting notifications shard={Shard} retry-after={RetryAfter}", Shard, duration);
        }

        // Retryable errors (eg. the server has not yet received its shard assignments) are handled by the backoff policy: the
        // overall initialization is still bounded by the request timeout in CreateAsync.
        if (!Initialized && !OxiaErrors.IsRetryable(err))
        {
            Initialized = true;
            Nm.InitWaitGroup.Fail(err);
        }
    }

    /// <summary>Reports whether the shard was split: it is no longer in the shard map, and other shards replaced it.</summary>
    internal bool Split()
    {
        var shardManager = Nm.ShardManager;
        return !shardManager.Exists(Shard) && Sessions.CurrentSuccessors(shardManager, Shard).Count > 0;
    }

    /// <summary>
    /// Subscribes to the shards that replaced the shard after it was split, from the last batch received from it: each of them
    /// holds the batches of the shard up to its last one, and delivers their notifications on its side of the split, before its own.
    /// </summary>
    internal void FollowSplit()
    {
        if (Ctx.IsCancellationRequested) return;
        var successors = Sessions.CurrentSuccessors(Nm.ShardManager, Shard);
        Log.LogInformation("Following the split of the shard shard={Shard} shards={Shards} offset={Offset}",
            Shard, string.Join(",", successors), LastOffsetReceived);

        long? startOffsetExclusive = null;
        if (Initialized)
        {
            startOffsetExclusive = LastOffsetReceived;
        }
        else
        {
            // The subscription had not received any batch: the subscriptions to the shards that replaced the shard
            // initialize it in its place
            Init!.Add(successors.Count - 1);
        }
        // Added before this manager is done, for the user-facing channel to stay open
        Nm.AddRunning(successors.Count);
        foreach (long successor in successors)
        {
            Start(successor, Nm, startOffsetExclusive, Init);
        }
    }

    /// <summary>
    /// Cancels the stream of the subscription when the shard map, from the one whose change completes
    /// <paramref name="changed"/>, no longer has the shard because it was split: the stream doesn't end by itself, until the
    /// shard is deleted.  Never throws.
    /// </summary>
    private async Task CancelOnSplitAsync(CancellationTokenSource cts, Task changed)
    {
        var ct = cts.Token;
        while (true)
        {
            try
            {
                await changed.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            changed = Nm.ShardManager.Changed();
            if (Split())
            {
                cts.Cancel();
                return;
            }
        }
    }

    internal async ValueTask MultiplexNotificationBatchAsync(NotificationBatch nb)
    {
        if (!Initialized)
        {
            if (Log.IsEnabled(LogLevel.Debug)) Log.LogDebug("Initialized the notification manager shard={Shard}", Shard);

            // We need to discard the very first notification, because it's only needed to ensure that the notification cursor
            // is created on the server side.
            Initialized = true;
            if (Init!.Add(-1) == 0)
            {
                Nm.InitWaitGroup.Done();
            }
            Init = null;
            LastOffsetReceived = nb.Offset;
            return;
        }

        var entries = nb.Notifications;
        for (int i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            await DeliverAsync(NotificationsConverter.ConvertNotification(entry.Key ?? "",
                entry.Value ?? throw new InvalidOperationException("Invalid notification: no value"))).ConfigureAwait(false);
        }
    }

    internal ValueTask DeliverAsync(Notification notification)
    {
        // Unblock from channel write when we're closing down
        return Nm.MultiplexCh.Writer.WriteAsync(notification, Ctx);
    }

    /// <summary>The error that ends an attempt whose stream failed with <paramref name="err"/>.</summary>
    private Exception StreamError(Exception err)
    {
        if (Ctx.IsCancellationRequested) return new OperationCanceledException(Ctx);
        if (Split()) return new PermanentException(OxiaErrors.ToThrowable(NotificationsImpl.ErrShardSplit));
        return err;
    }

    /// <summary>One subscription attempt (Go: getNotifications).  Ends by throwing the error that ends the stream.</summary>
    internal async ValueTask GetNotificationsAsync()
    {
        // Taken before the shard is looked up: a split from then on ends the stream
        var changed = Nm.ShardManager.Changed();
        if (Split())
        {
            throw new PermanentException(OxiaErrors.ToThrowable(NotificationsImpl.ErrShardSplit));
        }
        string leader = Nm.ShardManager.Leader(Shard);

        long? startOffsetExclusive = Initialized ? LastOffsetReceived : null;

        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(Ctx);
        var streamCt = streamCts.Token;
        var cancelOnSplit = CancelOnSplitAsync(streamCts, changed);
        try
        {
            IServerStreamingClient<NotificationBatch> notifications;
            try
            {
                notifications = await Nm.RpcProvider.GetNotifications(streamCt, leader,
                    new NotificationsRequest { Shard = Shard, StartOffsetExclusive = startOffsetExclusive }).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw StreamError(e);
            }

            for (bool first = true; ; first = false)
            {
                NotificationBatch? nb;
                try
                {
                    nb = await notifications.RecvAsync(streamCt).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw StreamError(e);
                }

                if (nb is null)
                {
                    if (Ctx.IsCancellationRequested) throw new OperationCanceledException(Ctx);
                    throw OxiaErrors.ToThrowable(OxiaErrors.EndOfStream);
                }

                if (Log.IsEnabled(LogLevel.Debug))
                {
                    Log.LogDebug("Received batch notification shard={Shard} offset={Offset} count={Count}",
                        Shard, nb.Offset, nb.Notifications.Count);
                }

                // The stream gets created even when the server rejects the subscription: the rejection is only reported by
                // the first Recv().  The server confirms an accepted subscription with a first empty batch, so receiving a
                // batch is the signal that it was accepted, and a persistent rejection never reaches here and keeps
                // escalating the retry delay.
                Backoff.Reset();

                // The first batch tells where the subscription starts: after the batches that the retention deleted, if any is
                // after the requested offset.  It is the one that confirms the subscription, empty and without the timestamp
                // of a write: a server of v0.16 confirms only the new subscriptions, and starts a resumed one with its next batch
                if (first && startOffsetExclusive is { } start && nb.Offset > start &&
                    nb.Timestamp == 0 && nb.Notifications.Count == 0)
                {
                    await NotifyMissedAsync(start, nb.Offset).ConfigureAwait(false);
                }

                await MultiplexNotificationBatchAsync(nb).ConfigureAwait(false);

                LastOffsetReceived = nb.Offset;
            }
        }
        finally
        {
            streamCts.Cancel();
            await cancelOnSplit.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tells the user that the subscription missed notifications of the shard, after offset <paramref name="fromExclusive"/> and
    /// up to offset <paramref name="toInclusive"/>: the retention deleted them before the subscription could read them.
    /// </summary>
    private ValueTask NotifyMissedAsync(long fromExclusive, long toInclusive)
    {
        Log.LogWarning("Missed notifications that the retention deleted before they were read from-offset-exclusive={From} to-offset-inclusive={To}",
            fromExclusive, toInclusive);
        return DeliverAsync(new Notification(NotificationType.NotificationsMissed, "", -1, ""));
    }
}

internal static class NotificationsConverter
{
    internal static NotificationType ConvertNotificationType(WireNotificationType t) => t switch
    {
        WireNotificationType.KeyCreated => NotificationType.KeyCreated,
        WireNotificationType.KeyModified => NotificationType.KeyModified,
        WireNotificationType.KeyDeleted => NotificationType.KeyDeleted,
        WireNotificationType.KeyRangeDeleted => NotificationType.KeyRangeRangeDeleted,
        _ => throw new InvalidOperationException("Invalid notification type"),
    };

    internal static Notification ConvertNotification(string key, WireNotification n)
    {
        long versionId = n.VersionId ?? -1;
        return new Notification(ConvertNotificationType(n.Type), key, versionId, n.KeyRangeLast ?? "");
    }
}
