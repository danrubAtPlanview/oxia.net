// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/notifications_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 14 (TestNotificationsClose, TestNotificationsInitRetriesOnRetryableError, TestNotificationsInitFailsOnNonRetryableError,
//             TestNotificationsRejectionDoesNotResetBackoff, TestNotificationsReceivedBatchResetsBackoff,
//             TestNotificationsReceivedBatchLogsAtDebugLevel, TestNotificationsFollowSplit, TestNotificationsFollowSplitBeforeInitialized,
//             TestNotificationsMissed, TestNotificationsResumedWithoutConfirmation, TestNotificationsFollowSplitAfterClose,
//             TestNotificationsStreamEnd, TestNotificationsDeliverEndsWhenClosed [2 subtests], TestConvertNotificationType),
//             ported: 14 (the 2 subtests are Theory rows), skipped: 0.
//   Adaptations (the assertions are the Go ones):
//      * getNotifications() returns an error in Go; GetNotificationsAsync() throws it: `assert.ErrorIs(err, X)` is
//        `OxiaErrors.Is(thrown, X)`; io.EOF is OxiaErrors.EndOfStream; context.Canceled is OxiaErrors.IsCanceled.
//      * The fake streams are IServerStreamingClient<NotificationBatch>: a `nil` batch / io.EOF is a null return from RecvAsync.
//      * `make(chan *Notification)` (UNbuffered, nobody receives) in TestNotificationsDeliverEndsWhenClosed is a FULL capacity-1
//        channel: a send cannot complete either way.
//      * `assert.Contains(logs.messages(), "Received batch notification")`: the .NET message carries its attributes inline
//        ("Received batch notification shard=.. offset=.. count=.."), so the assertion is on the message prefix.
//      * The Go ctx of the subscription (`parent.ctx.Err()`) is the CancellationToken given to GetNotifications.
//      * assert.Eventually is Wait.EventuallyAsync (F1Support).
// =====================================================================================================================
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;
using Xunit;
using WireNotification = Oxia.Common.Proto.Notification;
using WireNotificationType = Oxia.Common.Proto.NotificationType;

namespace Oxia.Client.Tests;

public class NotificationsTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private static Notification NewNotification(string key) => new(NotificationType.KeyCreated, key, 0, "");

    [Fact]
    public async Task TestNotificationsClose()
    {
        int count = 0;

        var multiplexCh = Channel.CreateBounded<Notification>(100);
        multiplexCh.Writer.TryWrite(NewNotification("key1"));
        multiplexCh.Writer.TryWrite(NewNotification("key2"));
        multiplexCh.Writer.TryComplete();

        var nm = new NotificationsImpl
        {
            Cancel = () => count++,
            MultiplexChanClosed = Task.CompletedTask,   // the context is cancelled: the channel is closed
            MultiplexCh = multiplexCh,
        };

        await nm.CloseAsync();   // assert.NoError(t, err)
        Assert.Equal(1, count);

        Assert.False(multiplexCh.Reader.TryRead(out var n));   // n, ok := <-nm.multiplexCh; assert.Equal(false, ok)
        Assert.Null(n);
        Assert.False(await multiplexCh.Reader.WaitToReadAsync());
    }

    /// <summary>Go: notificationsTestStream.</summary>
    private sealed class NotificationsTestStream : IServerStreamingClient<NotificationBatch>
    {
        private readonly CancellationToken _ctx;
        public readonly Channel<NotificationBatch> Batches = Channel.CreateBounded<NotificationBatch>(1);

        public NotificationsTestStream(CancellationToken ctx) => _ctx = ctx;

        public async ValueTask<NotificationBatch?> RecvAsync(CancellationToken ct = default)
            => await Batches.Reader.ReadAsync(_ctx).ConfigureAwait(false);

        public void Dispose() { }
    }

    /// <summary>Returns each batch in turn (a null batch is a `nil` one), then fails every later Recv() with Err (null Err: ends the stream).</summary>
    private sealed class NotificationsTestScriptedStream : IServerStreamingClient<NotificationBatch>
    {
        private readonly Queue<NotificationBatch?> _batches;
        private readonly Exception? _err;

        public NotificationsTestScriptedStream(IEnumerable<NotificationBatch?> batches, Exception? err)
        {
            _batches = new Queue<NotificationBatch?>(batches);
            _err = err;
        }

        public ValueTask<NotificationBatch?> RecvAsync(CancellationToken ct = default)
        {
            if (_batches.Count == 0)
            {
                if (_err is null) return ValueTask.FromResult<NotificationBatch?>(null);
                return ValueTask.FromException<NotificationBatch?>(OxiaErrors.ToThrowable(_err));
            }
            return ValueTask.FromResult(_batches.Dequeue());
        }

        public void Dispose() { }
    }

    private sealed class NotificationsTestRpcProvider : UnimplementedRpcProvider
    {
        private int _failures;   // Number of initial GetNotifications calls that will fail
        private int _attempts;
        public Exception? Err { get; set; }
        public IServerStreamingClient<NotificationBatch>? Stream { get; set; }   // If set, returned instead of a new stream

        public int Failures { set => Volatile.Write(ref _failures, value); }
        public int Attempts => Volatile.Read(ref _attempts);

        public override ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target,
            NotificationsRequest request)
        {
            Interlocked.Increment(ref _attempts);
            if (Interlocked.Decrement(ref _failures) >= 0)
                return ValueTask.FromException<IServerStreamingClient<NotificationBatch>>(OxiaErrors.ToThrowable(Err!));
            if (Stream is not null) return ValueTask.FromResult(Stream);

            var stream = new NotificationsTestStream(ct);
            // The first batch only confirms that the notification cursor is created
            stream.Batches.Writer.TryWrite(new NotificationBatch { Offset = 0 });
            return ValueTask.FromResult<IServerStreamingClient<NotificationBatch>>(stream);
        }
    }

    [Fact]
    public async Task TestNotificationsInitRetriesOnRetryableError()
    {
        var provider = new NotificationsTestRpcProvider { Err = OxiaErrors.NotInitialized, Failures = 1 };
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };

        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, new ClientOptions { RequestTimeout = Ten },
            provider, shardManager);
        Assert.Equal(2, provider.Attempts);
        await nm.CloseAsync();
    }

    [Fact]
    public async Task TestNotificationsInitFailsOnNonRetryableError()
    {
        var provider = new NotificationsTestRpcProvider { Err = OxiaErrors.NotificationsNotEnabled, Failures = 1 };
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };

        // require.Error(t, err); assert.Nil(t, nm)
        await Assert.ThrowsAnyAsync<Exception>(async () => await NotificationsImpl.CreateAsync(CancellationToken.None,
            new ClientOptions { RequestTimeout = Ten }, provider, shardManager));
        Assert.Equal(1, provider.Attempts);
    }

    private static (ShardNotificationsManager, ResetCountingBackOff) NewTestShardNotificationsManager(IServerStreamingClient<NotificationBatch>? stream)
    {
        var bo = new ResetCountingBackOff(BackOffFactory.NewBackOff(CancellationToken.None));
        return (new ShardNotificationsManager
        {
            Ctx = CancellationToken.None,
            Nm = new NotificationsImpl
            {
                MultiplexCh = Channel.CreateBounded<Notification>(100),
                ShardManager = new NotificationsTestShardManager(),
                RpcProvider = new NotificationsTestRpcProvider { Stream = stream },
            },
            Backoff = bo,
            LastOffsetReceived = -1,
            Log = NullLogger.Instance,
        }, bo);
    }

    private static async Task<Exception> Fails(ShardNotificationsManager snm)
        => await Assert.ThrowsAnyAsync<Exception>(async () => await snm.GetNotificationsAsync());

    [Fact]
    public async Task TestNotificationsRejectionDoesNotResetBackoff()
    {
        // The stream is created even when the server rejects the subscription: the rejection is only reported by the first
        // Recv(), so no batch ever arrives.
        var (snm, bo) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(
            Array.Empty<NotificationBatch?>(), OxiaErrors.NodeIsNotLeader));

        Assert.True(OxiaErrors.Is(await Fails(snm), OxiaErrors.NodeIsNotLeader));
        Assert.Equal(0, bo.Resets);
    }

    [Fact]
    public async Task TestNotificationsReceivedBatchResetsBackoff()
    {
        // A batch arrives only once the subscription was accepted, so a stream that delivered one before failing, eg. because
        // the leader stepped down, should be retried quickly.
        var (snm, bo) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(
            new NotificationBatch?[] { new NotificationBatch { Offset = 5 } }, OxiaErrors.ResourceUnavailable));
        snm.Initialized = true;

        Assert.True(OxiaErrors.Is(await Fails(snm), OxiaErrors.ResourceUnavailable));
        Assert.Equal(1, bo.Resets);
        Assert.Equal(5, snm.LastOffsetReceived);
    }

    [Fact]
    public async Task TestNotificationsReceivedBatchLogsAtDebugLevel()
    {
        var (snm, _) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(
            new NotificationBatch?[] { new NotificationBatch { Offset = 5 } }, OxiaErrors.ResourceUnavailable));
        snm.Initialized = true;
        var logs = new DebugLogs();
        snm.Log = logs;

        Assert.True(OxiaErrors.Is(await Fails(snm), OxiaErrors.ResourceUnavailable));
        Assert.Contains(logs.Messages(), m => m.StartsWith("Received batch notification", StringComparison.Ordinal));
    }

    /// <summary>
    /// notificationsTestServer serves the subscriptions of a client: it confirms each one at the offset it starts from, or at
    /// the shard's commit offset 0, and lets the test send the next batches, or fail the stream.
    /// </summary>
    private sealed class NotificationsTestServer : UnimplementedRpcProvider
    {
        private readonly object _lock = new();
        // The offset that the next subscription to a shard is confirmed at, if not the one it starts from
        private readonly Dictionary<long, long> _confirmedOffsets = new();
        // The error that the subscriptions to a shard fail with
        private readonly Dictionary<long, Exception> _rejections = new();
        // The shards whose subscriptions the test confirms
        private readonly HashSet<long> _unconfirmed = new();
        private readonly Dictionary<long, int> _attempts = new();
        private readonly Channel<NotificationsTestSubscription> _subscriptions = Channel.CreateUnbounded<NotificationsTestSubscription>();

        // Confirms only the new subscriptions, as the servers of v0.16 do: a resumed one starts with the next batch
        public bool OnlyNewConfirmed { get; set; }

        public override ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target,
            NotificationsRequest req)
        {
            lock (_lock)
            {
                _attempts[req.Shard] = _attempts.GetValueOrDefault(req.Shard) + 1;
                if (_rejections.TryGetValue(req.Shard, out var rejection))
                    return ValueTask.FromException<IServerStreamingClient<NotificationBatch>>(OxiaErrors.ToThrowable(rejection));
                if (!_confirmedOffsets.TryGetValue(req.Shard, out long offset)) offset = req.StartOffsetExclusive ?? 0;
                _confirmedOffsets.Remove(req.Shard);

                var subscription = new NotificationsTestSubscription(ct, req);
                if (!_unconfirmed.Contains(req.Shard) && (!OnlyNewConfirmed || req.StartOffsetExclusive is null))
                    subscription.Confirm(offset);
                _subscriptions.Writer.TryWrite(subscription);
                return ValueTask.FromResult<IServerStreamingClient<NotificationBatch>>(subscription);
            }
        }

        /// <summary>Has the test confirm the subscriptions to the shard.</summary>
        public void LeaveUnconfirmed(long shard) { lock (_lock) _unconfirmed.Add(shard); }
        public void ConfirmAt(long shard, long offset) { lock (_lock) _confirmedOffsets[shard] = offset; }
        public void Reject(long shard, Exception err) { lock (_lock) _rejections[shard] = err; }
        public int AttemptsOn(long shard) { lock (_lock) return _attempts.GetValueOrDefault(shard); }

        public async Task<NotificationsTestSubscription> NextSubscription()
            => await Wait.WithTimeout(_subscriptions.Reader.ReadAsync().AsTask(), Ten, "no subscription");
    }

    private sealed class NotificationsTestSubscription : IServerStreamingClient<NotificationBatch>
    {
        public readonly CancellationToken Ctx;
        public readonly NotificationsRequest Request;
        private readonly Channel<(NotificationBatch? Batch, Exception? Err)> _items = Channel.CreateUnbounded<(NotificationBatch?, Exception?)>();

        public NotificationsTestSubscription(CancellationToken ctx, NotificationsRequest request)
        {
            Ctx = ctx;
            Request = request;
        }

        // select { case nb := <-s.batches: ...; case err := <-s.errs: ...; case <-s.ctx.Done(): ... }
        public async ValueTask<NotificationBatch?> RecvAsync(CancellationToken ct = default)
        {
            var item = await _items.Reader.ReadAsync(Ctx).ConfigureAwait(false);
            if (item.Err is not null) throw OxiaErrors.ToThrowable(item.Err);
            return item.Batch;
        }

        /// <summary>Sends the batch that confirms the subscription at the offset: empty, without a timestamp.</summary>
        public void Confirm(long offset) => _items.Writer.TryWrite((new NotificationBatch { Shard = Request.Shard, Offset = offset }, null));

        /// <summary>Fails the stream (Go: s.errs &lt;- err).</summary>
        public void Fail(Exception err) => _items.Writer.TryWrite((null, err));

        /// <summary>Sends the batch of the write at the offset, with the creation notifications of the keys.</summary>
        public void Send(long offset, params string[] keys)
        {
            var nb = new NotificationBatch
            {
                Shard = Request.Shard,
                Offset = offset,
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            };
            foreach (string key in keys)
            {
                nb.Notifications.Add(new NotificationEntry
                {
                    Key = key,
                    Value = new WireNotification { Type = WireNotificationType.KeyCreated, VersionId = offset },
                });
            }
            _items.Writer.TryWrite((nb, null));
        }

        public void Dispose() { }
    }

    private static async Task<Notification> NextNotification(NotificationsImpl nm)
    {
        var n = await Wait.WithTimeout(nm.Ch.ReadAsync().AsTask(), Ten, "no notification");
        Assert.NotNull(n);   // require.NotNil
        return n;
    }

    [Fact]
    public async Task TestNotificationsFollowSplit()
    {
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var server = new NotificationsTestServer();
        server.ConfirmAt(0, 10);
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, new ClientOptions { RequestTimeout = Ten },
            server, shardManager);
        try
        {
            var parent = await server.NextSubscription();
            Assert.Null(parent.Request.StartOffsetExclusive);
            parent.Send(11, "a");
            Assert.Equal("a", (await NextNotification(nm)).Key);

            // The parent keeps its stream open after the split: the subscription cancels it, and subscribes to the children
            // from the last batch it received from the parent
            shardManager.Split(0, 1, 2);
            await Wait.EventuallyAsync(() => parent.Ctx.IsCancellationRequested, Ten, TimeSpan.FromMilliseconds(10));

            var children = new Dictionary<long, NotificationsTestSubscription>();
            for (int i = 0; i < 2; i++)
            {
                var child = await server.NextSubscription();
                children[child.Request.Shard] = child;
                Assert.NotNull(child.Request.StartOffsetExclusive);
                Assert.Equal(11, child.Request.StartOffsetExclusive);
            }
            Assert.Contains(1L, children.Keys);
            Assert.Contains(2L, children.Keys);

            children[1].Send(12, "b");
            children[2].Send(13, "c");
            var keys = new[] { (await NextNotification(nm)).Key, (await NextNotification(nm)).Key };
            Assert.Equal(new[] { "b", "c" }, keys.OrderBy(k => k).ToArray());   // ElementsMatch

            // A child split in turn hands over its subscription the same way
            shardManager.Split(2, 3, 4);
            for (int i = 0; i < 2; i++)
            {
                var grandchild = await server.NextSubscription();
                Assert.Contains(grandchild.Request.Shard, new long[] { 3, 4 });
                Assert.NotNull(grandchild.Request.StartOffsetExclusive);
                Assert.Equal(13, grandchild.Request.StartOffsetExclusive);
            }
        }
        finally
        {
            await nm.CloseAsync();
        }
    }

    [Fact]
    public async Task TestNotificationsFollowSplitBeforeInitialized()
    {
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var server = new NotificationsTestServer();
        server.Reject(0, OxiaErrors.NodeIsNotLeader);
        server.LeaveUnconfirmed(2);

        var created = Task.Run(async () =>
        {
            try
            {
                var nm = await NotificationsImpl.CreateAsync(CancellationToken.None,
                    new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(30) }, server, shardManager);
                return (Nm: (NotificationsImpl?)nm, Err: (Exception?)null);
            }
            catch (Exception e)
            {
                return (null, e);
            }
        });
        await Wait.EventuallyAsync(() => server.AttemptsOn(0) > 0, Ten, TimeSpan.FromMilliseconds(10));

        // The shard was split before the subscription was established on it: the subscriptions to the children establish it in
        // its place, without a batch of the parent to start after
        shardManager.Split(0, 1, 2);
        var children = new Dictionary<long, NotificationsTestSubscription>();
        for (int i = 0; i < 2; i++)
        {
            var child = await server.NextSubscription();
            children[child.Request.Shard] = child;
            Assert.Null(child.Request.StartOffsetExclusive);
        }
        Assert.Contains(1L, children.Keys);
        Assert.Contains(2L, children.Keys);

        // Established once both children confirm their subscription
        var early = await Task.WhenAny(created, Task.Delay(TimeSpan.FromMilliseconds(200)));
        if (ReferenceEquals(early, created))
            throw new Xunit.Sdk.XunitException("the subscription was established before the subscription to shard 2");
        children[2].Confirm(0);

        var res = await Wait.WithTimeout(created, Ten, "the subscription was not established");
        Assert.Null(res.Err);   // require.NoError
        try
        {
            children[1].Send(1, "a");
            Assert.Equal("a", (await NextNotification(res.Nm!)).Key);
        }
        finally
        {
            await res.Nm!.CloseAsync();
        }
    }

    [Fact]
    public async Task TestNotificationsMissed()
    {
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var server = new NotificationsTestServer();
        server.ConfirmAt(0, 10);
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, new ClientOptions { RequestTimeout = Ten },
            server, shardManager);
        try
        {
            var subscription = await server.NextSubscription();
            subscription.Send(11, "a");
            Assert.Equal("a", (await NextNotification(nm)).Key);

            // The subscription starts again after the last batch it received, but the retention deleted the batches up to
            // offset 20: the server confirms it after them
            server.ConfirmAt(0, 20);
            subscription.Fail(OxiaErrors.ResourceUnavailable);
            subscription = await server.NextSubscription();
            Assert.NotNull(subscription.Request.StartOffsetExclusive);
            Assert.Equal(11, subscription.Request.StartOffsetExclusive);
            var missed = await NextNotification(nm);
            Assert.Equal(NotificationType.NotificationsMissed, missed.Type);
            Assert.Empty(missed.Key);
            Assert.Equal(-1, missed.VersionId);

            subscription.Send(21, "b");
            Assert.Equal("b", (await NextNotification(nm)).Key);

            // Confirmed where it starts, a subscription missed nothing
            subscription.Fail(OxiaErrors.ResourceUnavailable);
            subscription = await server.NextSubscription();
            Assert.NotNull(subscription.Request.StartOffsetExclusive);
            Assert.Equal(21, subscription.Request.StartOffsetExclusive);
            subscription.Send(22, "c");
            Assert.Equal("c", (await NextNotification(nm)).Key);
        }
        finally
        {
            await nm.CloseAsync();
        }
    }

    // A server of v0.16 doesn't confirm a resumed subscription: its first batch is the one of the next write, past the offset
    // it starts from, and it missed nothing.
    [Fact]
    public async Task TestNotificationsResumedWithoutConfirmation()
    {
        var shardManager = new NotificationsTestShardManager { Shards = new List<long> { 0 } };
        var server = new NotificationsTestServer { OnlyNewConfirmed = true };
        server.ConfirmAt(0, 10);
        var nm = await NotificationsImpl.CreateAsync(CancellationToken.None, new ClientOptions { RequestTimeout = Ten },
            server, shardManager);
        try
        {
            var subscription = await server.NextSubscription();
            subscription.Send(11, "a");
            Assert.Equal("a", (await NextNotification(nm)).Key);

            subscription.Fail(OxiaErrors.ResourceUnavailable);
            subscription = await server.NextSubscription();
            Assert.NotNull(subscription.Request.StartOffsetExclusive);
            Assert.Equal(11, subscription.Request.StartOffsetExclusive);
            // The next write notified nothing, e.g. a put with an unexpected version: its batch is empty
            subscription.Send(15);
            subscription.Send(16, "b");
            Assert.Equal("b", (await NextNotification(nm)).Key);
        }
        finally
        {
            await nm.CloseAsync();
        }
    }

    // A shard that is split after the subscription was closed is not followed.
    [Fact]
    public void TestNotificationsFollowSplitAfterClose()
    {
        var (snm, _) = NewTestShardNotificationsManager(null);
        snm.Nm.ShardManager = new NotificationsTestShardManager
        {
            Shards = new List<long> { 1, 2 },
            Successors = new Dictionary<long, long[]> { [0] = new long[] { 1, 2 } },
        };
        snm.Init = new ShardInit();
        snm.Init.Store(1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        snm.Ctx = cts.Token;

        snm.FollowSplit();

        // No subscription to the shards that replaced it takes over its initialization
        Assert.Equal(1, snm.Init.Load());
    }

    // A stream that ends without an error is retried.
    [Fact]
    public async Task TestNotificationsStreamEnd()
    {
        var (snm, _) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(new NotificationBatch?[] { null }, null));
        Assert.True(OxiaErrors.Is(await Fails(snm), OxiaErrors.EndOfStream));   // assert.ErrorIs(..., io.EOF)

        // Unless the subscription was closed
        (snm, _) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(new NotificationBatch?[] { null }, null));
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        snm.Ctx = cts.Token;
        Assert.True(OxiaErrors.IsCanceled(await Fails(snm)));   // assert.ErrorIs(..., context.Canceled)
    }

    // A notification that nobody receives does not keep a closed subscription from ending.
    [Theory]
    [InlineData("notification")]
    [InlineData("missed-notifications")]
    public async Task TestNotificationsDeliverEndsWhenClosed(string name)
    {
        string key = "/a";
        NotificationBatch batch;
        long lastOffset, expectedReceived;
        if (name == "notification")
        {
            batch = new NotificationBatch { Offset = 1, Timestamp = 1 };
            batch.Notifications.Add(new NotificationEntry { Key = key, Value = new WireNotification { Type = WireNotificationType.KeyCreated } });
            lastOffset = 0;
            expectedReceived = 0;
        }
        else
        {
            // The confirmation of a resumed subscription after the batches that the retention deleted
            batch = new NotificationBatch { Offset = 10 };
            lastOffset = 5;
            expectedReceived = 5;
        }

        var (snm, _) = NewTestShardNotificationsManager(new NotificationsTestScriptedStream(new NotificationBatch?[] { batch }, null));
        snm.Initialized = true;
        snm.LastOffsetReceived = lastOffset;
        // An unbuffered channel that nobody receives from: a full capacity-1 channel
        var unbuffered = Channel.CreateBounded<Notification>(1);
        Assert.True(unbuffered.Writer.TryWrite(NewNotification("full")));
        snm.Nm.MultiplexCh = unbuffered;
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        snm.Ctx = cts.Token;

        Assert.True(OxiaErrors.IsCanceled(await Fails(snm)));
        Assert.Equal(expectedReceived, snm.LastOffsetReceived);
    }

    [Fact]
    public void TestConvertNotificationType()
    {
        var mapping = new Dictionary<WireNotificationType, NotificationType>
        {
            [WireNotificationType.KeyCreated] = NotificationType.KeyCreated,
            [WireNotificationType.KeyModified] = NotificationType.KeyModified,
            [WireNotificationType.KeyDeleted] = NotificationType.KeyDeleted,
            [WireNotificationType.KeyRangeDeleted] = NotificationType.KeyRangeRangeDeleted,
        };
        foreach (var (protoType, expected) in mapping)
        {
            Assert.Equal(expected, NotificationsConverter.ConvertNotificationType(protoType));
        }
        // assert.PanicsWithValue(t, "Invalid notification type", ...)
        var panic = Assert.Throws<InvalidOperationException>(() => NotificationsConverter.ConvertNotificationType((WireNotificationType)100));
        Assert.Equal("Invalid notification type", panic.Message);
    }
}
