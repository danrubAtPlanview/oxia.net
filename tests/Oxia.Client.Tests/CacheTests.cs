// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/cache_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 16 (TestCache_LoadRacingNotification [5 subtests], TestCache_LoadRacingWrite [2 subtests], TestCacheGetAfterWrite
//             [2 subtests], TestCacheGetLoadingWhileClosing, TestCache_LoadRacingOtherKeyNotification,
//             TestNewCacheRequiresTheClientImplementation, TestNewCacheFailsWithoutNotifications,
//             TestNewCacheSharesTheNotifications, TestNewCacheFailsWhenTheValueCacheFails, TestCacheManagerStopsWhenNotificationsEnd,
//             TestCacheManagerHandleNotificationLogsAtDebugLevel, TestCachePutFailsOnSerializationError, TestCacheGetCachesKeyNotFound,
//             TestCacheGetFailsOnReadError, TestCacheReadModifyUpdateCreatesMissingRecord,
//             TestCacheReadModifyUpdateFailsWithoutRetrying [3 subtests]), ported: 16 (subtests are Theory rows, or Facts named Parent_Sub), skipped: 0.
//   Adaptations (the assertions are the Go ones):
//      * The SyncClient fake (`recordsClient`) and the AsyncClient fake (`neverCompleteAsyncClient`) implement ISyncClient /
//        IAsyncClient; "Runs once, in the next Get, after it read the record" blocks on a Task instead of a channel.
//      * json.Marshal / json.Unmarshal are System.Text.Json adapters (F1Support.Json).
//      * Errors are thrown by the cache (it is a sync-client style API): `assert.ErrorIs(err, X)` is `OxiaErrors.Is(thrown, X)`.
//      * `c.loads` empty (under loadsMutex) is CacheImpl.PendingLoads == 0; `cm.caches` is CacheManager.Caches.
//      * The "goroutine started by newCacheManager ends" check (assertGoroutinesEnd) is the completion of CacheManager.RunTask.
//      * captureDebugLogs replaces slog's default logger; here the logger of the CacheManager is replaced (CacheManager.Log), and
//        the message is matched by prefix because the .NET message carries its attributes inline.
//      * TestNewCacheFailsWhenTheValueCacheFails: failpoints are always enabled (Failpoints.Enabled is a constant true), so the
//        `t.Skip` branch is not ported.
//      * `newSyncClient(asyncClient)` is `new SyncClientImpl(asyncClient)`.
// =====================================================================================================================
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Failpoint;
using Xunit;

namespace Oxia.Client.Tests;

/// <summary>Serves the records from memory. The gets and the puts fail with GetErr and PutErr, if set. (Go: recordsClient)</summary>
internal sealed class RecordsClient : ISyncClient
{
    private readonly object _lock = new();
    public readonly Dictionary<string, byte[]> Records = new();
    private long _gets;
    public long Gets => Interlocked.Read(ref _gets);
    /// <summary>Runs once, in the next Get, after it read the record.</summary>
    public Func<Task>? AfterNextRead { get; set; }
    public Exception? GetErr { get; set; }
    public Exception? PutErr { get; set; }

    public ValueTask CloseAsync() => ValueTask.CompletedTask;

    public ValueTask<(string InsertedKey, RecordVersion Version)> PutAsync(CancellationToken ct, string key, ReadOnlyMemory<byte> value,
        params ReadOnlySpan<PutOption> options)
    {
        lock (_lock)
        {
            if (PutErr is not null)
                return ValueTask.FromException<(string, RecordVersion)>(OxiaErrors.ToThrowable(PutErr));
            Records[key] = value.ToArray();
            return ValueTask.FromResult((key, default(RecordVersion)));
        }
    }

    public ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options)
    {
        lock (_lock) Records.Remove(key);
        return ValueTask.CompletedTask;
    }

    public ValueTask<(string StoredKey, byte[]? Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key,
        params ReadOnlySpan<GetOption> options)
        => GetCoreAsync(key);

    private async ValueTask<(string StoredKey, byte[]? Value, RecordVersion Version)> GetCoreAsync(string key)
    {
        Interlocked.Increment(ref _gets);

        byte[]? value;
        bool found;
        Func<Task>? afterRead;
        Exception? getErr;
        lock (_lock)
        {
            found = Records.TryGetValue(key, out value);
            afterRead = AfterNextRead;
            AfterNextRead = null;
            getErr = GetErr;
        }

        if (getErr is not null) throw OxiaErrors.ToThrowable(getErr);

        if (afterRead is not null) await afterRead().ConfigureAwait(false);
        if (!found) throw OxiaErrors.ToThrowable(ClientErrors.KeyNotFound);
        return (key, value, default);
    }

    public byte[] Record(string key)
    {
        lock (_lock) return Records[key];
    }

    /// <summary>Changes the record as another client would.</summary>
    public async Task Write(string key, string? value)
    {
        if (value is null)
        {
            await DeleteAsync(CancellationToken.None, key);
            return;
        }
        byte[] data = JsonSerializer.SerializeToUtf8Bytes(value);
        await PutAsync(CancellationToken.None, key, data);
    }

    public ValueTask DeleteRangeAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options) => throw new NotImplementedException();
    public ValueTask<List<string>> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<ListOption> options) => throw new NotImplementedException();
    public System.Threading.Channels.ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<RangeScanOption> options) => throw new NotImplementedException();
    public System.Threading.Channels.ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options) => throw new NotImplementedException();
    public ValueTask<INotifications> GetNotificationsAsync() => throw new NotImplementedException();
}

public class CacheTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    // Runs change while a Get of the cache is loading "/key", after the server read the record, and returns once the load is done.
    private static async Task<CacheImpl<string>> LoadRacing(RecordsClient client, Func<CacheImpl<string>, Task> change)
    {
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);

        var read = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AfterNextRead = () =>
        {
            read.SetResult();
            return release.Task;
        };

        var loaded = Task.Run(async () =>
        {
            try { await c.GetAsync(CancellationToken.None, "/key"); } catch { /* _, _, _ = c.Get(...) */ }
        });

        await read.Task;
        await change(c);
        release.SetResult();
        await loaded;
        return c;
    }

    private static async Task AssertRecord(CacheImpl<string> c, string? expected)
    {
        Exception? err = null;
        string? value = null;
        try
        {
            (value, _) = await c.GetAsync(CancellationToken.None, "/key");
        }
        catch (Exception e)
        {
            err = e;
        }

        if (expected is null)
        {
            Assert.True(OxiaErrors.Is(err, ClientErrors.KeyNotFound), $"value: {value}");
        }
        else
        {
            Assert.Null(err);
            Assert.Equal(expected, value);
        }

        Assert.Equal(0, c.PendingLoads);   // assert.Empty(t, c.loads)
    }

    // A notification that evicts a key while a Get is loading it from the server must not let the load cache the record as
    // it was before the change.
    [Theory]
    [InlineData("created")]
    [InlineData("modified")]
    [InlineData("deleted")]
    [InlineData("range-deleted")]
    [InlineData("missed")]
    public async Task TestCache_LoadRacingNotification(string name)
    {
        string v1 = "v1", v2 = "v2";
        (string? before, string? after, Notification notification) = name switch
        {
            "created" => (null, v1, new Notification(NotificationType.KeyCreated, "/key", 0, "")),
            "modified" => (v1, v2, new Notification(NotificationType.KeyModified, "/key", 0, "")),
            "deleted" => (v1, null, new Notification(NotificationType.KeyDeleted, "/key", 0, "")),
            "range-deleted" => (v1, null, new Notification(NotificationType.KeyRangeRangeDeleted, "/a", 0, "/z")),
            "missed" => (v1, v2, new Notification(NotificationType.NotificationsMissed, "", -1, "")),
            _ => throw new ArgumentException(name),
        };

        var client = new RecordsClient();
        await client.Write("/key", before);

        var c = await LoadRacing(client, async c =>
        {
            await client.Write("/key", after);
            c.HandleNotification(notification);
        });
        try
        {
            await AssertRecord(c, after);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    // Nor must a Put or a Delete through the cache.
    [Fact]
    public async Task TestCache_LoadRacingWrite_put()
    {
        string v1 = "v1", v2 = "v2";
        var client = new RecordsClient();
        await client.Write("/key", v1);

        var c = await LoadRacing(client, async c =>
        {
            await c.PutAsync(CancellationToken.None, "/key", v2);   // assert.NoError
        });
        try
        {
            await AssertRecord(c, v2);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task TestCache_LoadRacingWrite_delete()
    {
        var client = new RecordsClient();
        await client.Write("/key", "v1");

        var c = await LoadRacing(client, async c =>
        {
            await c.DeleteAsync(CancellationToken.None, "/key");    // assert.NoError
        });
        try
        {
            await AssertRecord(c, null);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    // A Get after a Put or a Delete through the cache returns the written record, also when a Get loaded the key just before:
    // ristretto buffers the Set of the load, which must not be applied after the eviction of the write.
    [Fact]
    public async Task TestCacheGetAfterWrite_put()
    {
        const int keys = 2_000;
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            for (int i = 0; i < keys; i++)
            {
                string key = $"/key-{i}";
                if (i % 2 == 0) await client.Write(key, "v1");
                try { await c.GetAsync(CancellationToken.None, key); } catch { /* _, _, _ = c.Get(...) */ }
                await c.PutAsync(CancellationToken.None, key, "v2");

                var (value, _) = await c.GetAsync(CancellationToken.None, key);
                Assert.True("v2" == value, key);
            }
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task TestCacheGetAfterWrite_delete()
    {
        const int keys = 2_000;
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            for (int i = 0; i < keys; i++)
            {
                string key = $"/key-{i}";
                await client.Write(key, "v1");
                await c.GetAsync(CancellationToken.None, key);
                await c.DeleteAsync(CancellationToken.None, key);

                Exception? err = null;
                try { await c.GetAsync(CancellationToken.None, key); } catch (Exception e) { err = e; }
                Assert.True(OxiaErrors.Is(err, ClientErrors.KeyNotFound), key);
            }
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    // A Get that loads a key while the cache closes returns.
    [Fact]
    public async Task TestCacheGetLoadingWhileClosing()
    {
        var client = new RecordsClient();
        await client.Write("/key", "v1");
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);

        var read = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AfterNextRead = () =>
        {
            read.SetResult();
            return release.Task;
        };
        var loaded = Task.Run(async () =>
        {
            try { await c.GetAsync(CancellationToken.None, "/key"); return (Exception?)null; }
            catch (Exception e) { return e; }
        });

        await read.Task;
        await c.CloseAsync();   // require.NoError
        release.SetResult();
        var err = await Wait.WithTimeout(loaded, Ten, "the Get did not return");
        Assert.Null(err);
    }

    // The notification of another key must not keep a load from caching the record.
    [Fact]
    public async Task TestCache_LoadRacingOtherKeyNotification()
    {
        var client = new RecordsClient();
        await client.Write("/key", "v1");

        var c = await LoadRacing(client, async c =>
        {
            await client.Write("/other", "v1");
            c.HandleNotification(new Notification(NotificationType.KeyCreated, "/other", 0, ""));
        });
        try
        {
            await AssertRecord(c, "v1");
            Assert.Equal(1, client.Gets);   // the second Get read the server
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task TestNewCacheRequiresTheClientImplementation()
    {
        var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await Cache.NewAsync<string>(new RecordsClient(), Json.Marshal, Json.Unmarshal));
        Assert.Contains("Invalid client implementation", err.Message);   // assert.ErrorContains
    }

    [Fact]
    public async Task TestNewCacheFailsWithoutNotifications()
    {
        var errNotifications = new Exception("notifications not enabled");
        var client = new SyncClientImpl(new NeverCompleteAsyncClient { NotificationsErr = errNotifications });

        var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await Cache.NewAsync<string>(client, Json.Marshal, Json.Unmarshal));
        Assert.True(OxiaErrors.Is(err, errNotifications));
        await client.CloseAsync();   // assert.NoError(t, client.Close())
    }

    // The caches of a client share its subscription to the notifications.
    [Fact]
    public async Task TestNewCacheSharesTheNotifications()
    {
        var notifications = new TestNotifications();
        var asyncClient = new NeverCompleteAsyncClient { Notifications = notifications };
        var client = new SyncClientImpl(asyncClient);

        await Cache.NewAsync<string>(client, Json.Marshal, Json.Unmarshal);
        await Cache.NewAsync<int>(client, Json.Marshal, Json.Unmarshal);
        Assert.Equal(1, asyncClient.NotificationsRequests);

        await client.CloseAsync();
        Assert.True(notifications.Closed);
    }

    [Fact]
    public async Task TestNewCacheFailsWhenTheValueCacheFails()
    {
        var injected = new Exception("injected");
        var notifications = new TestNotifications();
        var cm = await CacheManager.CreateAsync(new SyncClientImpl(new NeverCompleteAsyncClient { Notifications = notifications }),
            Failpoints.New(new Dictionary<string, Func<Exception?>> { ["newCacheImpl.newCache"] = () => injected }));

        var err = Assert.ThrowsAny<Exception>(() => cm.NewCache<string>(Json.Marshal, Json.Unmarshal));

        Assert.True(OxiaErrors.Is(err, injected));
        Assert.Empty(cm.Caches);
        await cm.CloseAsync();
    }

    // The cache manager stops once the notifications end.
    [Fact]
    public async Task TestCacheManagerStopsWhenNotificationsEnd()
    {
        var notifications = new TestNotifications();
        var cm = await CacheManager.CreateAsync(new SyncClientImpl(new NeverCompleteAsyncClient { Notifications = notifications }), null);

        notifications.Channel.Writer.TryComplete();   // close(notifications.ch)
        // assertGoroutinesEnd(t, "newCacheManager")
        await Wait.WithTimeout(cm.RunTask, Ten, "a goroutine started by newCacheManager did not end");
        await cm.CloseAsync();
    }

    [Fact]
    public async Task TestCacheManagerHandleNotificationLogsAtDebugLevel()
    {
        var logs = new DebugLogs();
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            await client.Write("/key", "v1");
            await c.GetAsync(CancellationToken.None, "/key");
            var cm = new CacheManager(null, null) { Log = logs };
            cm.Caches.Add(c);

            await client.Write("/key", "v2");
            cm.HandleNotification(new Notification(NotificationType.KeyModified, "/key", 1, ""));

            Assert.Contains(logs.Messages(), m => m.StartsWith("Received notification", StringComparison.Ordinal));
            var (cached, _) = await c.GetAsync(CancellationToken.None, "/key");
            Assert.Equal("v2", cached);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task TestCachePutFailsOnSerializationError()
    {
        var errSerialize = new Exception("unsupported value");
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, _ => throw errSerialize, Json.Unmarshal);
        try
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.PutAsync(CancellationToken.None, "/key", "v1"));
            Assert.True(OxiaErrors.Is(err, errSerialize));
            Assert.Empty(client.Records);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    // A key that does not exist is cached as such.
    [Fact]
    public async Task TestCacheGetCachesKeyNotFound()
    {
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.True(OxiaErrors.Is(err, ClientErrors.KeyNotFound));
            c.ValueCache.Wait();

            err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.True(OxiaErrors.Is(err, ClientErrors.KeyNotFound));
            Assert.Equal(1, client.Gets);   // the second Get read the server
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    // A failed read is not cached.
    [Fact]
    public async Task TestCacheGetFailsOnReadError()
    {
        var errRead = new Exception("read failed");
        var client = new RecordsClient { GetErr = errRead };
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.True(OxiaErrors.Is(err, errRead));
            c.ValueCache.Wait();

            err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.GetAsync(CancellationToken.None, "/key"));
            Assert.True(OxiaErrors.Is(err, errRead));
            Assert.Equal(2, client.Gets);
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Fact]
    public async Task TestCacheReadModifyUpdateCreatesMissingRecord()
    {
        var client = new RecordsClient();
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            await c.ReadModifyUpdateAsync(CancellationToken.None, "/key", v =>
            {
                Assert.True(v.Empty);
                return "v1";
            });

            byte[] stored = client.Record("/key");
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse("\"v1\""), JsonNode.Parse(stored)));   // assert.JSONEq
        }
        finally
        {
            await c.CloseAsync();
        }
    }

    [Theory]
    [InlineData("read-error")]
    [InlineData("modify-error")]
    [InlineData("write-error")]
    public async Task TestCacheReadModifyUpdateFailsWithoutRetrying(string name)
    {
        var errFailure = new Exception("failure");
        var client = new RecordsClient
        {
            GetErr = name == "read-error" ? errFailure : null,
            PutErr = name == "write-error" ? errFailure : null,
        };
        ModifyFunc<string> modify = name == "modify-error" ? _ => throw errFailure : _ => "v1";
        var c = new CacheImpl<string>(client, null, Json.Marshal, Json.Unmarshal);
        try
        {
            int attempts = 0;
            var err = await Assert.ThrowsAnyAsync<Exception>(async () => await c.ReadModifyUpdateAsync(CancellationToken.None, "/key", v =>
            {
                attempts++;
                return modify(v);
            }));
            Assert.True(OxiaErrors.Is(err, errFailure));
            Assert.True(attempts <= 1);
            Assert.Equal(1, client.Gets);
        }
        finally
        {
            await c.CloseAsync();
        }
    }
}
