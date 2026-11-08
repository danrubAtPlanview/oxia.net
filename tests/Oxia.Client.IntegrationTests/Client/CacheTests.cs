// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/cache_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The notification-invalidated read-through cache (oxia.NewCache) on top of a real sync client: negative caching,
//              ReadModifyUpdate retries on concurrent updates, invalidation by delete-range notifications.
//   Go tests: 8   ported: 8   skipped: 0
//   Divergences:
//     * API used (components F1/F2 own it; it matches their in-progress sources, adapt the `NewCache` + `JsonSerde` helpers at the bottom if it changes):
//         OxiaClient.CreateCache<T>(client, SerializeFunc, DeserializeFunc) -> ValueTask<ICache<T>>; ICache<T>.PutAsync(ct, key, value, options) -> (InsertedKey, Version); GetAsync(ct, key) -> (Value, Version);
//         DeleteAsync(ct, key, options); ReadModifyUpdateAsync(ct, key, ModifyFunc<T>); failures are thrown like the sync client.
//     * Go returns the zero value + zero Version alongside an error and asserts them (`assert.Equal(oxia.Version{}, version)`,
//       `assert.Equal(testStruct{}, value)`); with a throwing API nothing is returned, so these two assertions are implied by the
//       exception and not repeated.
//     * `assert.NotPanics` around cache.Get (regression for a type-assertion panic on the second Get of a missing key) becomes
//       "the second Get throws exactly the not-found error, not an InvalidCastException or any other failure".
//     * encoding/json -> System.Text.Json; `testStruct{A string json:"a"; B int json:"b"}` is TestStruct.
//     * goroutines -> Task.Run; the ModifyFunc blocks the worker thread on the Oxia.Common WaitGroup exactly like the Go goroutine does.
// =====================================================================================================================
using System.Text.Json;
using System.Text.Json.Serialization;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public sealed record TestStruct([property: JsonPropertyName("a")] string A, [property: JsonPropertyName("b")] int B);

public class CacheTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestCache_Empty()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, "/non-existing-key")), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, "/non-existing-key/child")), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.DeleteAsync(CancellationToken.None, "/non-existing-key")), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.DeleteAsync(CancellationToken.None, "/non-existing-key/child")), ClientErrors.KeyNotFound);

        await cache.CloseAsync();
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestCache_InsertionDeletion()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        var k1 = NewKey();
        var v1 = new TestStruct("hello", 1);
        var (_, version1) = await cache.PutAsync(CancellationToken.None, k1, v1, OxiaOptions.ExpectedRecordNotExists());
        Assert.Equal(0L, version1.ModificationsCount);

        var v2 = new TestStruct("hello", 2);
        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.PutAsync(CancellationToken.None, k1, v2, OxiaOptions.ExpectedRecordNotExists())),
            ClientErrors.UnexpectedVersionId);

        var (value, version) = await cache.GetAsync(CancellationToken.None, k1);
        Assert.Equal(0L, version.ModificationsCount);
        Assert.Equal(v1, value);

        await cache.ReadModifyUpdateAsync(CancellationToken.None, k1, existingValue => new TestStruct(existingValue.MustGet().A, 3));

        (value, version) = await cache.GetAsync(CancellationToken.None, k1);
        Assert.Equal(1L, version.ModificationsCount);
        Assert.Equal(new TestStruct("hello", 3), value);

        await cache.ReadModifyUpdateAsync(CancellationToken.None, k1, existingValue =>
        {
            var ok = existingValue.TryGet(out var ev);
            Assert.True(ok);
            return new TestStruct(ev.A, 4);
        });

        (value, version) = await cache.GetAsync(CancellationToken.None, k1);
        Assert.Equal(2L, version.ModificationsCount);
        Assert.Equal(new TestStruct("hello", 4), value);

        await cache.CloseAsync();
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestCache_PutOutSideTheCache()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        var k1 = NewKey();
        var v1 = new TestStruct("hello", 1);
        var data = JsonSerializer.SerializeToUtf8Bytes(v1);
        var (_, version1) = await client.PutAsync(CancellationToken.None, k1, data, OxiaOptions.ExpectedRecordNotExists());
        Assert.Equal(0L, version1.ModificationsCount);

        var (value, version) = await cache.GetAsync(CancellationToken.None, k1);
        Assert.Equal(0L, version.ModificationsCount);
        Assert.Equal(v1, value);
    }

    [Fact]
    public async Task TestCache_DeserializationFailure()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        var k1 = NewKey();
        await client.PutAsync(CancellationToken.None, k1, B("invalid json"));

        // assert.Error(t, err): any failure (here a deserialization error)
        var err = await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, k1));
        Assert.NotNull(err);
    }

    [Fact]
    public async Task TestCache_ConcurrentUpdate()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client1 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var client2 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache1 = await NewCache(client1);

        var cache2 = await NewCache(client2);

        var k1 = NewKey();
        var v1 = new TestStruct("hello", 1);
        var (_, version1) = await cache1.PutAsync(CancellationToken.None, k1, v1, OxiaOptions.ExpectedRecordNotExists());
        Assert.Equal(0L, version1.ModificationsCount);

        var wg = new WaitGroup(2);
        var wg1 = new WaitGroup(1);
        var wg2 = new WaitGroup(1);
        var wgResult = new WaitGroup(2);
        var isFirstTime = 1; // atomic.Bool (1 == true)

        // Go: the goroutines `assert.NoError(t, err)` and then `wgResult.Done()`.  An xUnit assertion thrown on a worker thread
        // would be lost, so the goroutine reports success with Done() and failure with Fail(err); the test asserts below.
        void ReadModifyUpdate(ICache<TestStruct> cache, WaitGroup release) => _ = Task.Run(async () =>
        {
            try
            {
                await cache.ReadModifyUpdateAsync(CancellationToken.None, k1, existingValue =>
                {
                    if (Volatile.Read(ref isFirstTime) == 1)
                    {
                        wg.Done();
                        _ = release.WaitAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
                    }

                    var ev = existingValue.MustGet();
                    return new TestStruct(ev.A, ev.B + 1);
                });
                wgResult.Done();
            }
            catch (Exception e)
            {
                wgResult.Fail(e);
            }
        });

        ReadModifyUpdate(cache1, wg1);
        ReadModifyUpdate(cache2, wg2);

        // Go waits with context.Background(); the 60 s limit only keeps a broken implementation from hanging the run
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        // Wait for both go-routines to read the existing version
        _ = await wg.WaitAsync(limit.Token);

        // Unblock both of them to and cause the conflict
        Volatile.Write(ref isFirstTime, 0);
        wg1.Done();
        wg2.Done();

        // Wait for both operations to complete
        var resultErr = await wgResult.WaitAsync(limit.Token);
        AssertNoError(resultErr);

        var (value, version) = await cache1.GetAsync(CancellationToken.None, k1);
        Assert.Equal(2L, version.ModificationsCount);
        Assert.Equal(new TestStruct("hello", 3), value);

        (value, version) = await cache2.GetAsync(CancellationToken.None, k1);
        Assert.Equal(2L, version.ModificationsCount);
        Assert.Equal(new TestStruct("hello", 3), value);

        await cache1.CloseAsync();
        await client1.CloseAsync();

        await cache2.CloseAsync();
        await client2.CloseAsync();
    }

    // TestCache_GetNotFoundTwice verifies that calling Get() twice on a non-existing
    // key does not panic. This is a regression test for a bug where the negative cache
    // entry was stored with a double-wrapped optional type (*optional[Optional[...]]),
    // causing a type assertion panic on the second Get() when the entry was served from
    // the ristretto cache.
    [Fact]
    public async Task TestCache_GetNotFoundTwice()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        var key = "/non-existing-key-twice";

        // First Get: triggers load(), which stores a negative cache entry
        AssertErrorIs(await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, key)), ClientErrors.KeyNotFound);

        // Wait for ristretto to commit the async Set, then verify the second Get
        // doesn't panic (regression: double-wrapped optional type caused panic)
        await TestUtil.Eventually(async () =>
        {
            // assert.NotPanics + assert.ErrorIs: the only acceptable outcome is the not-found error
            var err = await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, key));
            Assert.IsNotType<InvalidCastException>(err);
            return OxiaErrors.Is(err, ClientErrors.KeyNotFound);
        }, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        await cache.CloseAsync();
        await client.CloseAsync();
    }

    // TestCache_GetNotFoundThenPut verifies that a key can transition from "not found"
    // (negative cache entry) to a valid value without type assertion issues.
    [Fact]
    public async Task TestCache_GetNotFoundThenPut()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var cache = await NewCache(client);

        var key = NewKey();

        // Get non-existing key to populate negative cache entry
        var err = await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, key));
        AssertErrorIs(err, ClientErrors.KeyNotFound);

        // Wait for ristretto async set, then verify still not found from cache
        await TestUtil.Eventually(async () =>
        {
            err = await TestUtil.CatchAsync(async () => await cache.GetAsync(CancellationToken.None, key));
            return err is not null;
        }, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        AssertErrorIs(err, ClientErrors.KeyNotFound);

        // Now put a value for this key
        var v = new TestStruct("hello", 42);
        await cache.PutAsync(CancellationToken.None, key, v);

        // Get should now return the value (Put invalidates the cache, so load() is called)
        var (value, _) = await cache.GetAsync(CancellationToken.None, key);
        Assert.Equal(v, value);

        await cache.CloseAsync();
        await client.CloseAsync();
    }

    // TestCache_DeleteRange verifies that the notification of a range deletion made
    // by another client evicts all the cached records in the range, not only the
    // one at its start.
    [Fact]
    public async Task TestCache_DeleteRange()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var otherClient = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;
        var values = new Dictionary<string, TestStruct>
        {
            ["/a"] = new("a", 1),
            ["/b"] = new("b", 2),
            ["/c"] = new("c", 3),
        };
        foreach (var (key, value) in values)
        {
            var data = JsonSerializer.SerializeToUtf8Bytes(value);
            await otherClient.PutAsync(ctx, key, data);
        }

        // The cache only receives the notifications of the writes made after its
        // creation, so the puts above can't evict the records it caches here
        var cache = await NewCache(client);

        foreach (var (key, value) in values)
        {
            var (cachedValue, _) = await cache.GetAsync(ctx, key);
            Assert.Equal(value, cachedValue);
        }

        await otherClient.DeleteRangeAsync(ctx, "/a", "/c");

        foreach (var key in new[] { "/a", "/b" })
        {
            var k = key;
            await TestUtil.EventuallyWithT(async () =>
            {
                var err = await TestUtil.CatchAsync(async () => await cache.GetAsync(ctx, k));
                AssertErrorIs(err, ClientErrors.KeyNotFound, k);
            }, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
        }

        var (last, _) = await cache.GetAsync(ctx, "/c");
        Assert.Equal(values["/c"], last);

        await cache.CloseAsync();
        await client.CloseAsync();
        await otherClient.CloseAsync();
    }

    // ---- adaptation points for the cache API (F1 `Cache`, F2 `OxiaClient.CreateCache`) ---------------------------------------

    /// <summary>Go: <c>oxia.NewCache[testStruct](client, json.Marshal, json.Unmarshal)</c>.</summary>
    private static async ValueTask<ICache<TestStruct>> NewCache(ISyncClient client) =>
        await OxiaClient.CreateCache<TestStruct>(client, JsonSerde.Marshal, JsonSerde.Unmarshal);

    /// <summary>Go: <c>json.Marshal</c> / <c>json.Unmarshal</c> as SerializeFunc / DeserializeFunc.</summary>
    private static class JsonSerde
    {
        public static byte[] Marshal(object? value) => JsonSerializer.SerializeToUtf8Bytes(value, value!.GetType());

        public static object? Unmarshal(ReadOnlyMemory<byte> data, Type type) => JsonSerializer.Deserialize(data.Span, type);
    }
}
