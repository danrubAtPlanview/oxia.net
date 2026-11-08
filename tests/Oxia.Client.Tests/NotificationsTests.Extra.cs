// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : none.  Intent/edge tests for oxia/notifications.go as ported: conversion of every field, per-shard order,
//              back-pressure, close/cancellation semantics of the user-facing channel, zero shards, initialization timeout.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Xunit;
using WireNotification = Oxia.Common.Proto.Notification;
using WireNotificationType = Oxia.Common.Proto.NotificationType;

namespace Oxia.Client.Tests;

public class NotificationsExtraTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    /// <summary>Hands out one controllable subscription per GetNotifications call.</summary>
    private sealed class ScriptedProvider : UnimplementedRpcProvider
    {
        public readonly Channel<Subscription> Subscriptions = Channel.CreateUnbounded<Subscription>();
        public bool Confirm { get; set; } = true;

        public override ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request)
        {
            var sub = new Subscription(ct, request);
            if (Confirm) sub.Items.Writer.TryWrite(new NotificationBatch { Shard = request.Shard, Offset = request.StartOffsetExclusive ?? 0 });
            Subscriptions.Writer.TryWrite(sub);
            return ValueTask.FromResult<IServerStreamingClient<NotificationBatch>>(sub);
        }

        public async Task<Subscription> Next() => await Wait.WithTimeout(Subscriptions.Reader.ReadAsync().AsTask(), Ten, "no subscription");
    }

    private sealed class Subscription : IServerStreamingClient<NotificationBatch>
    {
        public readonly CancellationToken Ctx;
        public readonly NotificationsRequest Request;
        public readonly Channel<NotificationBatch> Items = Channel.CreateUnbounded<NotificationBatch>();

        public Subscription(CancellationToken ctx, NotificationsRequest request) { Ctx = ctx; Request = request; }
        public async ValueTask<NotificationBatch?> RecvAsync(CancellationToken ct = default) => await Items.Reader.ReadAsync(Ctx);
        public void Dispose() { }

        public void Send(long offset, params WireNotificationEntry[] entries)
        {
            var nb = new NotificationBatch { Shard = Request.Shard, Offset = offset, Timestamp = 1 };
            foreach (var e in entries) nb.Notifications.Add(new NotificationEntry { Key = e.Key, Value = e.Value });
            Items.Writer.TryWrite(nb);
        }
    }

    private readonly record struct WireNotificationEntry(string Key, WireNotification Value);

    private static WireNotificationEntry Entry(string key, WireNotificationType type, long? versionId = null, string? rangeEnd = null)
        => new(key, new WireNotification { Type = type, VersionId = versionId, KeyRangeLast = rangeEnd });

    private static ClientOptions Options() => new() { RequestTimeout = Ten };

    [Fact]
    public async Task EveryFieldOfTheEntryIsConverted()
    {
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var provider = new ScriptedProvider();
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, Options(), provider, sm);
        try
        {
            var sub = await provider.Next();
            sub.Send(1,
                Entry("/created", WireNotificationType.KeyCreated, 5),
                Entry("/modified", WireNotificationType.KeyModified, 6),
                Entry("/deleted", WireNotificationType.KeyDeleted),
                Entry("/a", WireNotificationType.KeyRangeDeleted, null, "/z"));

            var got = new List<Notification>();
            for (int i = 0; i < 4; i++) got.Add(await Wait.WithTimeout(nm.Ch.ReadAsync().AsTask(), Ten, "no notification"));

            Assert.Equal(new[] { "/created", "/modified", "/deleted", "/a" }, got.Select(n => n.Key));
            Assert.Equal(new[] { NotificationType.KeyCreated, NotificationType.KeyModified, NotificationType.KeyDeleted, NotificationType.KeyRangeRangeDeleted },
                got.Select(n => n.Type));
            Assert.Equal(new long[] { 5, 6, -1, -1 }, got.Select(n => n.VersionId));   // unset => -1
            Assert.Equal(new[] { "", "", "", "/z" }, got.Select(n => n.KeyRangeEnd));
        }
        finally
        {
            await nm.CloseAsync();
        }
    }

    [Fact]
    public async Task TheNotificationsOfEveryShardAreMultiplexed_KeepingTheOrderOfEachShard()
    {
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0, 1, 2 } };
        var provider = new ScriptedProvider();
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, Options(), provider, sm);
        try
        {
            var subs = new Dictionary<long, Subscription>();
            for (int i = 0; i < 3; i++)
            {
                var sub = await provider.Next();
                subs[sub.Request.Shard] = sub;
            }

            const int perShard = 300;   // more than the 100 of the user-facing buffer: the delivery applies back-pressure
            foreach (var (shard, sub) in subs)
            {
                for (int i = 0; i < perShard; i++) sub.Send(i + 1, Entry($"/s{shard}/{i}", WireNotificationType.KeyCreated, i));
            }

            var perShardSeen = new Dictionary<long, List<int>> { [0] = new(), [1] = new(), [2] = new() };
            for (int i = 0; i < 3 * perShard; i++)
            {
                var n = await Wait.WithTimeout(nm.Ch.ReadAsync().AsTask(), Ten, "no notification");
                var parts = n.Key.Split('/');   // "", "s<shard>", "<i>"
                perShardSeen[long.Parse(parts[1][1..])].Add(int.Parse(parts[2]));
            }
            foreach (var (shard, seen) in perShardSeen)
                Assert.Equal(Enumerable.Range(0, perShard), seen);
        }
        finally
        {
            await nm.CloseAsync();
        }
    }

    [Fact]
    public async Task Close_CancelsTheSubscriptions_DiscardsWhatNobodyRead_AndCompletesTheChannel()
    {
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var provider = new ScriptedProvider();
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, Options(), provider, sm);
        var sub = await provider.Next();
        sub.Send(1, Entry("/a", WireNotificationType.KeyCreated, 1), Entry("/b", WireNotificationType.KeyCreated, 1));
        await Wait.EventuallyAsync(() => nm.Ch.Count == 2, Ten, TimeSpan.FromMilliseconds(5));   // delivered, unread

        await nm.CloseAsync();

        Assert.True(sub.Ctx.IsCancellationRequested);
        Assert.False(nm.Ch.TryRead(out _));               // the user does not see any notification after the close
        Assert.False(await nm.Ch.WaitToReadAsync());      // completed
        await nm.CloseAsync();                            // idempotent
    }

    [Fact]
    public async Task ClosingTheClientContext_CompletesTheChannel()
    {
        using var client = new CancellationTokenSource();
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var provider = new ScriptedProvider();
        var nm = await NotificationsImpl.CreateAsync(client.Token, Options(), provider, sm);
        await provider.Next();

        client.Cancel();

        Assert.False(await Wait.WithTimeout(nm.Ch.WaitToReadAsync().AsTask(), Ten, "the channel was not completed"));
        await nm.CloseAsync();
    }

    [Fact]
    public async Task WithoutShards_TheSubscriptionIsEstablishedAndTheChannelIsCompleteAtOnce()
    {
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, Options(), new ScriptedProvider(), new NotificationsTestShardManager());

        Assert.False(await Wait.WithTimeout(nm.Ch.WaitToReadAsync().AsTask(), Ten, "the channel was not completed"));
        await nm.CloseAsync();
    }

    [Fact]
    public async Task TheInitializationTimesOut_WhenTheServerNeverConfirmsTheSubscription_AndStopsTheSubscription()
    {
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var provider = new ScriptedProvider { Confirm = false };

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await NotificationsImpl.CreateAsync(CancellationToken.None,
            new ClientOptions { RequestTimeout = TimeSpan.FromMilliseconds(300) }, provider, sm));

        Assert.True(OxiaErrors.IsDeadlineExceeded(err), err.ToString());   // context.DeadlineExceeded
        var sub = await provider.Next();
        await Wait.EventuallyAsync(() => sub.Ctx.IsCancellationRequested, Ten, TimeSpan.FromMilliseconds(10), "the subscription was not stopped");
    }

    [Fact]
    public async Task TheFirstBatchOfANewSubscriptionIsDiscarded()
    {
        var sm = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var provider = new ScriptedProvider { Confirm = false };
        var created = Task.Run(async () => await NotificationsImpl.CreateAsync(CancellationToken.None, Options(), provider, sm));
        var sub = await provider.Next();

        // The very first batch only confirms the notification cursor: even with notifications in it, they are not delivered
        sub.Send(7, Entry("/discarded", WireNotificationType.KeyCreated, 1));
        var nm = await Wait.WithTimeout(created, Ten, "not established");
        try
        {
            sub.Send(8, Entry("/delivered", WireNotificationType.KeyCreated, 2));
            Assert.Equal("/delivered", (await Wait.WithTimeout(nm.Ch.ReadAsync().AsTask(), Ten, "no notification")).Key);
        }
        finally
        {
            await nm.CloseAsync();
        }
    }
}
