// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : none.  Intent/edge/concurrency tests for oxia/cache.go as ported: allocation-free hit path, eviction rules of
//              Put/Delete/notifications, ReadModifyUpdate retries on conflict, the CacheManager fan-out and close, and a stress
//              of Get/Put/Delete racing with notification eviction (the load-eviction counting must never leave a stale record
//              behind once the notifications are delivered).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests;

public class CacheExtraTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private static CacheImpl<string> NewCache(RecordsClient client) => new(client, null, Json.Marshal, Json.Unmarshal);

    [Fact]
    public async Task GetHitPath_DoesNotAllocate()
    {
        var client = new RecordsClient();
        await client.Write("/found", "value");
        var c = NewCache(client);
        try
        {
            await c.GetAsync(CancellationToken.None, "/found");                       // loads and caches the record
            await c.TryGetAsync(CancellationToken.None, "/missing");                   // loads and caches "not found"

            for (int i = 0; i < 1_000; i++)   // warm-up
            {
                Hit(c, "/found");
                Miss(c, "/missing");
            }
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 10_000; i++)
            {
                Hit(c, "/found");
                Miss(c, "/missing");
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.Equal(0, allocated);
            Assert.Equal(2, client.Gets);   // never went to the server again
        }
        finally
        {
            await c.CloseAsync();
        }

        static void Hit(CacheImpl<string> c, string key)
        {
            var vt = c.TryGetAsync(CancellationToken.None, key);
            Assert.True(vt.IsCompletedSuccessfully);
            var r = vt.Result;
            if (r.Err is not null || r.Value != "value") throw new InvalidOperationException("miss");
        }

        static void Miss(CacheImpl<string> c, string key)
        {
            var vt = c.TryGetAsync(CancellationToken.None, key);
            Assert.True(vt.IsCompletedSuccessfully);
            if (!ReferenceEquals(vt.Result.Err, ClientErrors.KeyNotFound)) throw new InvalidOperationException("not a miss");
        }
    }

    [Fact]
    public async Task TryGetAsync_ReportsKeyNotFoundWithoutThrowing()
    {
        var c = NewCache(new RecordsClient());
        try
        {
            var r = await c.TryGetAsync(CancellationToken.None, "/absent");
            Assert.True(OxiaErrors.Is(r.Err, ClientErrors.KeyNotFound));
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task Notifications_EvictTheKey_RangeDeletedAndMissedEvictEverything()
    {
        var client = new RecordsClient();
        foreach (var k in new[] { "/a", "/b", "/c" }) await client.Write(k, "v1");
        var c = NewCache(client);
        try
        {
            async Task LoadAll() { foreach (var k in new[] { "/a", "/b", "/c" }) await c.GetAsync(CancellationToken.None, k); }

            await LoadAll();
            Assert.Equal(3, client.Gets);

            c.HandleNotification(new Notification(NotificationType.KeyModified, "/b", 1, ""));
            await LoadAll();
            Assert.Equal(4, client.Gets);   // only /b was evicted

            c.HandleNotification(new Notification(NotificationType.KeyDeleted, "/a", -1, ""));
            c.HandleNotification(new Notification(NotificationType.KeyCreated, "/c", 0, ""));
            await LoadAll();
            Assert.Equal(6, client.Gets);

            c.HandleNotification(new Notification(NotificationType.KeyRangeRangeDeleted, "/a", -1, "/b"));
            await LoadAll();
            Assert.Equal(9, client.Gets);   // everything, also keys outside the range (the cache cannot list its keys)

            c.HandleNotification(new Notification(NotificationType.NotificationsMissed, "", -1, ""));
            await LoadAll();
            Assert.Equal(12, client.Gets);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task Put_FailingWithUnexpectedVersionId_KeepsTheCachedRecord_AnyOtherFailureEvictsIt()
    {
        var client = new RecordsClient();
        await client.Write("/key", "v1");
        var c = NewCache(client);
        try
        {
            await c.GetAsync(CancellationToken.None, "/key");
            Assert.Equal(1, client.Gets);

            client.PutErr = ClientErrors.UnexpectedVersionId;
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.PutAsync(CancellationToken.None, "/key", "v2"));
            Assert.True(OxiaErrors.Is(err, ClientErrors.UnexpectedVersionId));
            await c.GetAsync(CancellationToken.None, "/key");
            Assert.Equal(1, client.Gets);   // still cached: the put did not change the record

            client.PutErr = new Exception("boom");
            await Assert.ThrowsAnyAsync<Exception>(async () => await c.PutAsync(CancellationToken.None, "/key", "v2"));
            await c.GetAsync(CancellationToken.None, "/key");
            Assert.Equal(2, client.Gets);   // evicted: the put may have been applied
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task Delete_EvictsTheKeyEvenWhenTheClientFails()
    {
        var client = new RecordsClient();
        await client.Write("/key", "v1");
        var c = NewCache(client);
        try
        {
            await c.GetAsync(CancellationToken.None, "/key");
            await c.DeleteAsync(CancellationToken.None, "/key");
            Assert.True(OxiaErrors.Is(
                await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key")), ClientErrors.KeyNotFound));
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task Get_FailingToDeserialize_IsNotCached_AndWrapsTheError()
    {
        var client = new RecordsClient();
        client.Records["/key"] = "not json"u8.ToArray();
        var c = NewCache(client);
        try
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.Contains("failed to deserialize value", err.Message);
            await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.Equal(2, client.Gets);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task ReadModifyUpdate_RetriesOnConflict_UntilThePutSucceeds()
    {
        var client = new RecordsClient();
        await client.Write("/key", "0");
        int conflicts = 0;
        client.PutHook = key => Interlocked.Increment(ref conflicts) <= 2 && key == "/key" ? ClientErrors.UnexpectedVersionId : null;
        var c = NewCache(client);
        try
        {
            int attempts = 0;
            await c.ReadModifyUpdateAsync(CancellationToken.None, "/key", v =>
            {
                attempts++;
                return v.TryGet(out var existing) ? existing + "+" : "new";
            });

            Assert.Equal(3, attempts);   // two conflicts, then success
            Assert.Equal("0+", System.Text.Json.JsonSerializer.Deserialize<string>(client.Record("/key")));
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task ReadModifyUpdate_StopsRetryingWhenTheTokenIsCancelled()
    {
        var client = new RecordsClient();
        client.PutHook = _ => ClientErrors.UnexpectedVersionId;   // conflicts forever
        var c = NewCache(client);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        try
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await Wait.WithTimeout(c.ReadModifyUpdateAsync(cts.Token, "/key", _ => "v").AsTask(), Ten, "ReadModifyUpdate did not stop"));
            Assert.True(OxiaErrors.IsCanceled(err));
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task CacheManager_FansTheNotificationsOutToEveryCache_AndCloseClosesThemAll()
    {
        var notifications = new TestNotifications();
        var cm = await CacheManager.CreateAsync(new SyncClientImpl(new NeverCompleteAsyncClient { Notifications = notifications }), null);
        var c1 = (CacheImpl<string>)cm.NewCache<string>(Json.Marshal, Json.Unmarshal);
        var c2 = (CacheImpl<string>)cm.NewCache<string>(Json.Marshal, Json.Unmarshal);
        c1.ValueCache.Set("/key", new CachedEntry<string>("v1", default, true), 2, TimeSpan.FromMinutes(5));
        c2.ValueCache.Set("/key", new CachedEntry<string>("v1", default, true), 2, TimeSpan.FromMinutes(5));
        Assert.Equal(2, cm.Caches.Count);

        await notifications.Channel.Writer.WriteAsync(new Notification(NotificationType.KeyModified, "/key", 2, ""));
        await Wait.EventuallyAsync(() => c1.ValueCache.Count == 0 && c2.ValueCache.Count == 0, Ten, TimeSpan.FromMilliseconds(5),
            "the notification was not delivered to every cache");

        await cm.CloseAsync();
        Assert.True(notifications.Closed);
        Assert.False(c1.ValueCache.Set("x", CachedEntry<string>.Absent, 0, TimeSpan.Zero));   // closed
        Assert.False(c2.ValueCache.Set("x", CachedEntry<string>.Absent, 0, TimeSpan.Zero));
    }

    [Fact]
    public async Task CacheManager_Close_CombinesTheErrors()
    {
        var notifications = new FailingCloseNotifications(new Exception("notifications close failed"));
        var cm = await CacheManager.CreateAsync(new SyncClientImpl(new NeverCompleteAsyncClient { Notifications = notifications }), null);

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await cm.CloseAsync());

        Assert.Equal("notifications close failed", err.Message);
    }

    private sealed class FailingCloseNotifications : INotifications
    {
        private readonly Exception _err;
        private readonly System.Threading.Channels.Channel<Notification> _ch = System.Threading.Channels.Channel.CreateUnbounded<Notification>();
        public FailingCloseNotifications(Exception err) => _err = err;
        public System.Threading.Channels.ChannelReader<Notification> Ch => _ch.Reader;
        public ValueTask CloseAsync() => ValueTask.FromException(_err);
    }

    [Fact]
    public async Task CacheNew_BlockingConvenience_BuildsTheCacheOfASyncClient()
    {
        var notifications = new TestNotifications();
        var client = new SyncClientImpl(new NeverCompleteAsyncClient { Notifications = notifications });

        var cache = await Task.Run(() => Cache.New<string>(client, Json.Marshal, Json.Unmarshal));

        Assert.NotNull(cache);
        await client.CloseAsync();
        Assert.True(notifications.Closed);
    }

    /// <summary>
    /// Rounds of: loads of a key that are held right after the server read it, a write (by another client, or through the cache)
    /// that lands while they are held, and its notification.  Whatever the interleaving, the next Get must see the written record:
    /// none of the loads may cache what it read before the write.
    /// </summary>
    [Theory]
    [InlineData("notification")]
    [InlineData("put-through-the-cache")]
    [InlineData("delete-through-the-cache")]
    public async Task Stress_LoadsHeldAfterTheServerReadRacingAWrite_NeverLeaveAStaleRecord(string writeKind)
    {
        var client = new RecordsClient();
        var c = NewCache(client);
        var rnd = new Random(42);
        try
        {
            for (int round = 0; round < 100; round++)
            {
                string key = "/key-" + rnd.Next(4);
                string oldValue = $"old-{round}", newValue = $"new-{round}";
                await client.Write(key, oldValue);
                c.HandleNotification(new Notification(NotificationType.KeyModified, key, 0, ""));   // start from an empty entry

                int hold = rnd.Next(1, 4);
                client.AfterNextRead = () => Task.Delay(hold);
                var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
                {
                    try { await c.GetAsync(CancellationToken.None, key); } catch { /* the record may be deleted meanwhile */ }
                })).ToArray();

                await Task.Delay(rnd.Next(0, hold));   // the write lands while the first load is held (or just before it)
                switch (writeKind)
                {
                    case "notification":
                        await client.Write(key, newValue);
                        c.HandleNotification(new Notification(NotificationType.KeyModified, key, 0, ""));
                        break;
                    case "put-through-the-cache":
                        await c.PutAsync(CancellationToken.None, key, newValue);
                        break;
                    default:
                        await c.DeleteAsync(CancellationToken.None, key);
                        break;
                }
                await Task.WhenAll(readers);

                var r = await c.TryGetAsync(CancellationToken.None, key);
                if (writeKind == "delete-through-the-cache")
                    Assert.True(OxiaErrors.Is(r.Err, ClientErrors.KeyNotFound), $"round {round}: stale record {r.Value}");
                else
                    Assert.True(newValue == r.Value, $"round {round}: expected {newValue}, cache returned {r.Value} ({r.Err})");
                Assert.Equal(0, c.PendingLoads);
            }
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    /// <summary>
    /// A server whose writes become visible immediately, with the notifications of the writes delivered later, like Oxia
    /// does.  Every load is slowed down at random points so that writes and their notifications land inside loads.
    /// </summary>
    [Fact]
    public async Task Stress_GetPutDeleteRacingNotificationEviction_NeverLeavesAStaleRecordOnceTheNotificationsAreDelivered()
    {
        const int keyCount = 8;
        var client = new RecordsClient();
        var c = NewCache(client);
        var pendingNotifications = System.Threading.Channels.Channel.CreateUnbounded<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        string KeyOf(int i) => "/key-" + i;

        // The "network": delivers each notification to the cache a bit after the write
        var deliverer = Task.Run(async () =>
        {
            var rnd = new Random(1);
            await foreach (var key in pendingNotifications.Reader.ReadAllAsync())
            {
                if (rnd.Next(3) == 0) await Task.Yield();
                c.HandleNotification(new Notification(NotificationType.KeyModified, key, 0, ""));
            }
        });

        var writers = Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
        {
            var rnd = new Random(100 + w);
            int n = 0;
            while (!cts.IsCancellationRequested)
            {
                string key = KeyOf(rnd.Next(keyCount));
                switch (rnd.Next(3))
                {
                    case 0:   // another client writes (visible at once; the notification comes later)
                        await client.Write(key, $"w{w}-{n++}");
                        pendingNotifications.Writer.TryWrite(key);
                        break;
                    case 1:   // another client deletes
                        await client.Write(key, null);
                        pendingNotifications.Writer.TryWrite(key);
                        break;
                    default:  // a write through the cache itself (it evicts)
                        await c.PutAsync(CancellationToken.None, key, $"c{w}-{n++}");
                        break;
                }
                if (rnd.Next(4) == 0) await Task.Yield();
            }
        })).ToArray();

        var readers = Enumerable.Range(0, 4).Select(r => Task.Run(async () =>
        {
            var rnd = new Random(200 + r);
            while (!cts.IsCancellationRequested)
            {
                client.AfterNextRead = rnd.Next(3) == 0 ? static async () => await Task.Yield() : null;   // widen the read-to-cache window
                await c.TryGetAsync(CancellationToken.None, KeyOf(rnd.Next(keyCount)));
            }
        })).ToArray();

        await Task.WhenAll(writers.Concat(readers));
        pendingNotifications.Writer.Complete();
        await deliverer;   // every notification has been delivered
        client.AfterNextRead = null;

        // Once the notifications are delivered, the cache must agree with the server for every key
        for (int i = 0; i < keyCount; i++)
        {
            string key = KeyOf(i);
            string? expected;
            try
            {
                expected = JsonSerializerDeserialize(client.Record(key));
            }
            catch (KeyNotFoundException)
            {
                expected = null;
            }
            var r = await c.TryGetAsync(CancellationToken.None, key);
            if (expected is null) Assert.True(OxiaErrors.Is(r.Err, ClientErrors.KeyNotFound), $"{key}: cache has {r.Value}, the server has no record");
            else
            {
                Assert.Null(r.Err);
                Assert.Equal(expected, r.Value);
            }
        }
        Assert.Equal(0, c.PendingLoads);
        await c.CloseAsync();

        static string JsonSerializerDeserialize(byte[] data) => System.Text.Json.JsonSerializer.Deserialize<string>(data)!;
    }
}
