// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/async_client_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : End-to-end tests of the async client (and, in the same Go file, of the sync client: key sorting, floor/ceiling gets,
//              range scans, partition routing, sequences, notifications, sessions) against a real standalone Oxia server.
//              Go: every test starts its own in-process server with `dataserver.NewStandalone(dataserver.NewTestConfig(dir))`
//              (+ `config.NumShards` / `KeySorting` as needed); here `oxia.StartAsync(profile)` starts a fresh empty
//              `oxia standalone` with the same settings in the reusable Docker container (see README.md).
//   Go tests: 23 (+ 18 sub-tests)   ported: 23 (all sub-tests as [Theory] cases)   skipped: 0
//   Divergences:
//     * Go channels: `<-client.Put(...)` is `await client.PutAsync(...)`; `<-ch` / `select` with `time.After` use TestUtil.Receive /
//       ReceiveWithin / TryReceive; `for result := range client.RangeScan(...)` is `await foreach` over the ChannelReader.
//     * assert.ErrorIs(t, oxia.ErrX, err) (Go passes the arguments swapped in several places) is asserted in the natural
//       direction: the thrown exception must contain the sentinel (OxiaErrors.Is).
//     * assert.Equal(GetResult{...}, got) is asserted field by field (TestHelpers.AssertGetResult).
//     * t.Run sub-tests of TestSyncClientImpl_FloorCeilingGetKeySorting / RangeScanKeySorting / RangeScanUseIndexKeySorting /
//       ListAndRangeScanEndInclusive / RangeScanWithoutValues are [Theory] cases, each with its own fresh server (Go shares one server
//       between the sub-tests of RangeScanWithoutValues; the reads do not interact).
//     * `t.Context()` is TestContext.Current.CancellationToken; `context.Background()` is CancellationToken.None.
// =====================================================================================================================
using System.Text;
using Oxia.Common.Constant;
using Oxia.Common.Hash;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public class AsyncClientTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestAsyncClientImpl()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        var putResultA = await client.PutAsync("/a", new byte[] { 0 }, OxiaOptions.ExpectedRecordNotExists());
        Assert.Equal(0L, putResultA.Version.VersionId);
        Assert.Equal(0L, putResultA.Version.ModificationsCount);

        var getResult = await client.GetAsync("/a");
        AssertGetResult("/a", new byte[] { 0 }, putResultA.Version, null, getResult);

        var putResultC1 = await client.PutAsync("/c", new byte[] { 0 }, OxiaOptions.ExpectedRecordNotExists());
        Assert.Equal(1L, putResultC1.Version.VersionId);
        Assert.Equal(0L, putResultC1.Version.ModificationsCount);

        var putResultC2 = await client.PutAsync("/c", new byte[] { 1 }, OxiaOptions.ExpectedVersionId(putResultC1.Version.VersionId));
        Assert.Equal(2L, putResultC2.Version.VersionId);
        Assert.Equal(1L, putResultC2.Version.ModificationsCount);

        var listResult = await client.ListAsync(CancellationToken.None, "/y", "/z").Recv();
        Assert.Equal(0, listResult.Keys?.Count ?? 0);

        listResult = await client.ListAsync(CancellationToken.None, "/a", "/d").Recv();
        AssertListResult(["/a", "/c"], listResult);

        var deleteErr = await client.DeleteAsync("/a", OxiaOptions.ExpectedVersionId(putResultA.Version.VersionId));
        AssertNoError(deleteErr);

        getResult = await client.GetAsync("/a");
        AssertGetResult("", null, default, ClientErrors.KeyNotFound, getResult);

        var deleteRangeResult = await client.DeleteRangeAsync("/c", "/d");
        AssertNoError(deleteRangeResult);

        getResult = await client.GetAsync("/d");
        AssertGetResult("", null, default, ClientErrors.KeyNotFound, getResult);

        await client.CloseAsync();

        await standaloneServer.CloseAsync();
    }

    // https://github.com/oxia-db/oxia/issues/834
    [Fact]
    public async Task TestAsyncClientImpl_PutDeletePutOrder()
    {
        await using var standaloneServer = await oxia.StartAsync();

        // A long linger makes the operations below go in the same write batch
        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.FromSeconds(1)));

        var putResult1 = client.PutAsync("/k", B("v1"));
        var deleteResult = client.DeleteAsync("/k");
        var putResult2 = client.PutAsync("/k", B("v2"));
        AssertNoError((await putResult1).Err);
        AssertNoError(await deleteResult);
        AssertNoError((await putResult2).Err);

        var getResult = await client.GetAsync("/k");
        AssertNoError(getResult.Err);
        Assert.Equal(B("v2"), getResult.Value);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_Notifications()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        var notifications = await client.GetNotificationsAsync();

        var ctx = CancellationToken.None;

        var (_, s1) = await client.PutAsync(ctx, "/a", B("0"));

        var n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyCreated, n.Type);
        Assert.Equal("/a", n.Key);
        Assert.Equal(s1.VersionId, n.VersionId);

        var (_, s2) = await client.PutAsync(ctx, "/a", B("1"));

        n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyModified, n.Type);
        Assert.Equal("/a", n.Key);
        Assert.Equal(s2.VersionId, n.VersionId);

        var (_, s3) = await client.PutAsync(ctx, "/b", B("0"));
        await client.DeleteAsync(ctx, "/a");

        n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyCreated, n.Type);
        Assert.Equal("/b", n.Key);
        Assert.Equal(s3.VersionId, n.VersionId);

        n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyDeleted, n.Type);
        Assert.Equal("/a", n.Key);
        Assert.Equal(-1L, n.VersionId);

        // Create a 2nd notifications channel
        // This will only receive new updates
        var notifications2 = await client.GetNotificationsAsync();

        var (timedOut, _, _) = await notifications2.Ch.ReceiveWithin(TimeSpan.FromMilliseconds(100));
        Assert.True(timedOut, "shouldn't have received any notifications");
        // Ok, we expect it to time out

        var (_, s4) = await client.PutAsync(ctx, "/x", B("1"));

        n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyCreated, n.Type);
        Assert.Equal("/x", n.Key);
        Assert.Equal(s4.VersionId, n.VersionId);

        n = (await notifications2.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyCreated, n.Type);
        Assert.Equal("/x", n.Key);
        Assert.Equal(s4.VersionId, n.VersionId);

        await client.CloseAsync();

        // Channels should be closed after the client is closed
        Assert.True(notifications.Ch.TryReceive().Ready, "should have been closed");
        Assert.True(notifications2.Ch.TryReceive().Ready, "should have been closed");

        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_NotificationsClose()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        var notifications = await client.GetNotificationsAsync();

        await notifications.CloseAsync();

        var (timedOut, _, n) = await notifications.Ch.ReceiveWithin(TimeSpan.FromSeconds(1));
        Assert.False(timedOut, "Shouldn't have timed out");
        Assert.Null(n);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    // A subscription that falls behind by more than the retention time misses the
    // notifications that the server deletes meanwhile: it is told, before the
    // notifications that follow them.
    [Fact]
    public async Task TestSyncClientImpl_NotificationsMissed()
    {
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.Default with { NotificationsRetention = TimeSpan.FromSeconds(1) });

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        var notifications = await client.GetNotificationsAsync();

        // The subscription doesn't read: the notifications of these writes fill its
        // channel and the flow-control window of its stream, with their long keys,
        // and the retention deletes the ones still on the server
        var ctx = CancellationToken.None;
        var padding = new string('x', 1000);
        for (var i = 0; i < 1000; i++)
        {
            await client.PutAsync(ctx, $"/unread-{i:D4}-{padding}", B("0"));
        }

        await Task.Delay(TimeSpan.FromSeconds(3));

        // It reads the ones it got before, then is told that it missed the others
        async Task<Notification> NextNotification()
        {
            var (timedOut, _, n) = await notifications.Ch.ReceiveWithin(TimeSpan.FromSeconds(30));
            Assert.False(timedOut, "no notification");
            Assert.NotNull(n);
            return n;
        }

        var read = 0;
        for (var n = await NextNotification(); n.Type != NotificationType.NotificationsMissed; n = await NextNotification())
        {
            Assert.True(n.Key.StartsWith("/unread-", StringComparison.Ordinal), n.Key);
            read++;
        }

        Assert.True(read < 1000, $"expected fewer than 1000 notifications to be read, got {read}");

        // Then it gets the next ones
        var (_, version) = await client.PutAsync(ctx, "/after", B("0"));
        var next = await NextNotification();
        Assert.Equal("/after", next.Key);
        Assert.Equal(version.VersionId, next.VersionId);

        await client.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_Sessions()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr,
            OxiaOptions.WithBatchLinger(TimeSpan.Zero), OxiaOptions.WithSessionTimeout(TimeSpan.FromSeconds(5)));

        var putCh = client.PutAsync("/x", B("x"), OxiaOptions.Ephemeral()).AsTask();
        long versionId = 0;

        if (await putCh.CompletesWithin(TimeSpan.FromSeconds(1)))
        {
            var res = await putCh;
            AssertNoError(res.Err);
            Assert.Equal(0L, res.Version.ModificationsCount);
            versionId = res.Version.VersionId;
        }
        else
        {
            Assert.Fail("Shouldn't have timed out");
        }

        var getCh = client.GetAsync("/x").AsTask();
        if (await getCh.CompletesWithin(TimeSpan.FromSeconds(1)))
        {
            var res = await getCh;
            AssertNoError(res.Err);
            Assert.Equal(0L, res.Version.ModificationsCount);
            Assert.Equal(versionId, res.Version.VersionId);
        }
        else
        {
            Assert.Fail("Shouldn't have timed out");
        }

        await client.CloseAsync();
        // Go: slog.Debug("First client closed")

        client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        var secondClient = client;
        await TestUtil.Eventually(async () =>
        {
            var pending = secondClient.GetAsync("/x").AsTask();
            if (await pending.CompletesWithin(TimeSpan.FromSeconds(1)))
            {
                var res = await pending;
                return OxiaErrors.Is(res.Err, ClientErrors.KeyNotFound);
            }

            Assert.Fail("Shouldn't have timed out");
            return false;
        }, TimeSpan.FromSeconds(8), TimeSpan.FromMilliseconds(500));

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_OverrideEphemeral()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithSessionTimeout(TimeSpan.FromSeconds(5)));

        var k = NewKey();
        var (_, version) = await client.PutAsync(CancellationToken.None, k, B("v1"), OxiaOptions.Ephemeral());

        Assert.True(version.Ephemeral);

        // Override with non-ephemeral value
        (_, version) = await client.PutAsync(CancellationToken.None, k, B("v2"));

        Assert.False(version.Ephemeral);
        Assert.Equal("", version.ClientIdentity);

        await client.CloseAsync();

        // Reopen
        client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var (_, res, version2) = await client.GetAsync(CancellationToken.None, k);
        Assert.Equal(1L, version2.ModificationsCount);
        Assert.Equal("v2", Encoding.UTF8.GetString(res!));
        Assert.False(version2.Ephemeral);
        Assert.Equal("", version2.ClientIdentity);
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_ClientIdentity()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client1 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithIdentity("client-1"));

        var k = NewKey();
        var (_, version) = await client1.PutAsync(CancellationToken.None, k, B("v1"), OxiaOptions.Ephemeral());

        Assert.True(version.Ephemeral);
        Assert.Equal("client-1", version.ClientIdentity);

        var client2 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr,
            OxiaOptions.WithSessionTimeout(TimeSpan.FromSeconds(2)), OxiaOptions.WithIdentity("client-2"));

        var (_, res, version2) = await client2.GetAsync(CancellationToken.None, k);
        Assert.Equal(0L, version2.ModificationsCount);
        Assert.Equal("v1", Encoding.UTF8.GetString(res!));
        Assert.True(version2.Ephemeral);
        Assert.Equal("client-1", version2.ClientIdentity);

        (_, version) = await client2.PutAsync(CancellationToken.None, k, B("v2"), OxiaOptions.Ephemeral());

        Assert.True(version.Ephemeral);
        Assert.Equal("client-2", version.ClientIdentity);

        await client1.CloseAsync();
        await client2.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_SessionNotifications()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client1 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithIdentity("client-1"));

        var client2 = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithIdentity("client-1"));

        var notifications = await client2.GetNotificationsAsync();

        var ctx = CancellationToken.None;

        var (_, s1) = await client1.PutAsync(ctx, "/a", B("0"), OxiaOptions.Ephemeral());

        var n = (await notifications.Ch.Recv())!;
        Assert.Equal(NotificationType.KeyCreated, n.Type);
        Assert.Equal("/a", n.Key);
        Assert.Equal(s1.VersionId, n.VersionId);

        await client1.CloseAsync();

        var (timedOut, _, received) = await notifications.Ch.ReceiveWithin(TimeSpan.FromSeconds(3));
        if (!timedOut)
        {
            n = received!;
            Assert.Equal(NotificationType.KeyDeleted, n.Type);
            Assert.Equal("/a", n.Key);
        }
        else
        {
            Assert.Fail("read from channel timed out");
        }

        await client2.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_FloorCeilingGet()
    {
        // Test with multiple shards to ensure correctness across shards
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(10));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;
        await client.PutAsync(ctx, "a", B("0"));
        // await client.PutAsync(ctx, "b", B("1")); // Skipped intentionally
        await client.PutAsync(ctx, "c", B("2"));
        await client.PutAsync(ctx, "d", B("3"));
        await client.PutAsync(ctx, "e", B("4"));
        // await client.PutAsync(ctx, "f", B("5")); // Skipped intentionally
        await client.PutAsync(ctx, "g", B("6"));

        var (key, value, _) = await client.GetAsync(ctx, "a");
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonEqual());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonFloor());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonCeiling());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "a", OxiaOptions.ComparisonLower())), ClientErrors.KeyNotFound);

        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonHigher());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        // ---------------------------------------------------------------

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "b")), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "b", OxiaOptions.ComparisonEqual())), ClientErrors.KeyNotFound);

        (key, value, _) = await client.GetAsync(ctx, "b", OxiaOptions.ComparisonFloor());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "b", OxiaOptions.ComparisonCeiling());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "b", OxiaOptions.ComparisonLower());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "b", OxiaOptions.ComparisonHigher());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        // ---------------------------------------------------------------

        (key, value, _) = await client.GetAsync(ctx, "c");
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "c", OxiaOptions.ComparisonEqual());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "c", OxiaOptions.ComparisonFloor());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "c", OxiaOptions.ComparisonCeiling());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "c", OxiaOptions.ComparisonLower());
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "c", OxiaOptions.ComparisonHigher());
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        // ---------------------------------------------------------------

        (key, value, _) = await client.GetAsync(ctx, "d");
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "d", OxiaOptions.ComparisonEqual());
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "d", OxiaOptions.ComparisonFloor());
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "d", OxiaOptions.ComparisonCeiling());
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "d", OxiaOptions.ComparisonLower());
        Assert.Equal("c", key);
        Assert.Equal("2", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "d", OxiaOptions.ComparisonHigher());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        // ---------------------------------------------------------------

        (key, value, _) = await client.GetAsync(ctx, "e");
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "e", OxiaOptions.ComparisonEqual());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "e", OxiaOptions.ComparisonFloor());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "e", OxiaOptions.ComparisonCeiling());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "e", OxiaOptions.ComparisonLower());
        Assert.Equal("d", key);
        Assert.Equal("3", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "e", OxiaOptions.ComparisonHigher());
        Assert.Equal("g", key);
        Assert.Equal("6", TestUtil.Utf8(value));

        // ---------------------------------------------------------------

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "f")), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "f", OxiaOptions.ComparisonEqual())), ClientErrors.KeyNotFound);

        (key, value, _) = await client.GetAsync(ctx, "f", OxiaOptions.ComparisonFloor());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "f", OxiaOptions.ComparisonCeiling());
        Assert.Equal("g", key);
        Assert.Equal("6", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "f", OxiaOptions.ComparisonLower());
        Assert.Equal("e", key);
        Assert.Equal("4", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "f", OxiaOptions.ComparisonHigher());
        Assert.Equal("g", key);
        Assert.Equal("6", TestUtil.Utf8(value));

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    // A floor, lower, ceiling or higher get without a partition key goes to every
    // shard, and the client picks one of their answers. Each case stores two keys
    // on different shards: both shards answer with their key, and the client must
    // pick the right one in the key sorting of the namespace.
    //
    // Go: a table of {name, keySorting, keys, key, comparison, expected} run with t.Run(name).
    [Theory]
    // Natural sorting compares the bytes: "a/x" < "b" < "c"
    [InlineData("natural floor", ServerKeySorting.Natural, "a/x", "b", "c", "Floor", "b")]
    [InlineData("natural lower", ServerKeySorting.Natural, "a/x", "b", "c", "Lower", "b")]
    // "b" < "b/x" < "c"
    [InlineData("natural ceiling", ServerKeySorting.Natural, "b/x", "c", "b", "Ceiling", "b/x")]
    [InlineData("natural higher", ServerKeySorting.Natural, "b/x", "c", "b", "Higher", "b/x")]
    // Hierarchical sorting puts the keys with fewer '/' first, then sorts '/'
    // after any other byte: "aa/z" < "ab/y" < "a/x" < "a/z"
    [InlineData("hierarchical floor", ServerKeySorting.Hierarchical, "a/x", "ab/y", "a/z", "Floor", "a/x")]
    [InlineData("hierarchical lower", ServerKeySorting.Hierarchical, "a/x", "ab/y", "a/z", "Lower", "a/x")]
    [InlineData("hierarchical ceiling", ServerKeySorting.Hierarchical, "a/x", "ab/y", "aa/z", "Ceiling", "ab/y")]
    [InlineData("hierarchical higher", ServerKeySorting.Hierarchical, "a/x", "ab/y", "aa/z", "Higher", "ab/y")]
    // "z" < "b/x" < "a/y/z" < "c/c/c"
    [InlineData("hierarchical floor across levels", ServerKeySorting.Hierarchical, "b/x", "a/y/z", "c/c/c", "Floor", "a/y/z")]
    [InlineData("hierarchical ceiling across levels", ServerKeySorting.Hierarchical, "b/x", "a/y/z", "z", "Ceiling", "b/x")]
    public async Task TestSyncClientImpl_FloorCeilingGetKeySorting(string name, ServerKeySorting keySorting, string key0, string key1,
        string key, string comparisonName, string expected)
    {
        Assert.NotEmpty(name);
        var comparison = comparisonName switch
        {
            "Floor" => OxiaOptions.ComparisonFloor(),
            "Lower" => OxiaOptions.ComparisonLower(),
            "Ceiling" => OxiaOptions.ComparisonCeiling(),
            "Higher" => OxiaOptions.ComparisonHigher(),
            _ => throw new ArgumentException(comparisonName),
        };

        var profile = OxiaServerProfile.WithShards(4, keySorting);
        Assert.NotEqual(Sharding.ShardOf(key0, profile.Shards), Sharding.ShardOf(key1, profile.Shards));

        await using var standaloneServer = await oxia.StartAsync(profile);

        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        foreach (var k in new[] { key0, key1 })
        {
            await client.PutAsync(TestCtx, k, B(k));
        }

        var (got, _, _) = await client.GetAsync(TestCtx, key, comparison);
        Assert.Equal(expected, got);

        await client.CloseAsync();
    }

    // A range scan without a partition key goes to every shard, and the client
    // merges the records of the shards. The merged records must follow the key
    // sorting of the namespace, like the records of a single shard.
    [Theory]
    // Natural sorting compares the bytes: '/' sorts before '0'
    [InlineData(ServerKeySorting.Natural, new[] { "a/x", "a/y/z", "a0", "ab/y", "b" })]
    // Hierarchical sorting puts the keys with fewer '/' first, then sorts '/'
    // after any other byte
    [InlineData(ServerKeySorting.Hierarchical, new[] { "a0", "b", "ab/y", "a/x", "a/y/z" })]
    public async Task TestSyncClientImpl_RangeScanKeySorting(ServerKeySorting keySorting, string[] expected)
    {
        string[] keys = ["b", "a/y/z", "ab/y", "a0", "a/x"];

        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(4, keySorting));

        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        foreach (var key in keys)
        {
            await client.PutAsync(TestCtx, key, B(key));
        }

        var scanned = new List<string>();
        await foreach (var result in client.RangeScanAsync(TestCtx, "", "").ReadAllAsync(TestCtx))
        {
            AssertNoError(result.Err);
            scanned.Add(result.Key);
        }

        Assert.Equal(expected, scanned);

        await client.CloseAsync();
    }

    // A range scan with an index and without a partition key goes to every shard,
    // and each shard returns its records in the order of their secondary keys. The
    // client must merge the records of the shards in that order too, not in the
    // order of their primary keys.
    [Theory]
    [InlineData(ServerKeySorting.Natural)]
    [InlineData(ServerKeySorting.Hierarchical)]
    public async Task TestSyncClientImpl_RangeScanUseIndexKeySorting(ServerKeySorting keySorting)
    {
        // The secondary keys sort in the opposite order of the primary keys
        (string key, string secondaryKey)[] records =
        [
            ("p1", "h"), ("p2", "g"), ("p3", "f"), ("p4", "e"),
            ("p5", "d"), ("p6", "c"), ("p7", "b"), ("p8", "a"),
        ];
        string[] expected = ["p8", "p7", "p6", "p5", "p4", "p3", "p2", "p1"];

        var profile = OxiaServerProfile.WithShards(4, keySorting);

        // The records are spread over all the shards
        var shards = new HashSet<long>();
        foreach (var record in records)
        {
            shards.Add(Sharding.ShardOf(record.key, profile.Shards));
        }

        Assert.Equal((int)profile.Shards, shards.Count);

        await using var standaloneServer = await oxia.StartAsync(profile);

        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        foreach (var record in records)
        {
            await client.PutAsync(TestCtx, record.key, B(record.key), OxiaOptions.SecondaryIndex("idx", record.secondaryKey));
        }

        var scanned = new List<string>();
        await foreach (var result in client.RangeScanAsync(TestCtx, "a", "z", OxiaOptions.UseIndex("idx")).ReadAllAsync(TestCtx))
        {
            AssertNoError(result.Err);
            scanned.Add(result.Key);
        }

        Assert.Equal(expected, scanned);

        await client.CloseAsync();
    }

    // A list or a range scan with an inclusive end returns the records of the end,
    // from every shard, in either key sorting. Through an index, a range from a
    // secondary key to the same one returns the records with that secondary key,
    // and none of the ones whose secondary key extends it.
    [Theory]
    [InlineData(ServerKeySorting.Natural)]
    [InlineData(ServerKeySorting.Hierarchical)]
    public async Task TestSyncClientImplListAndRangeScanEndInclusive(ServerKeySorting keySorting)
    {
        (string key, string secondaryKey)[] records =
        [
            ("p-0", "/t/k-1"), ("p-3", "/t/k-1"), ("p-4", "/t/k-1"),
            ("p-1", "/t/k-12"), ("p-10", "/t/k-1:a"), ("p-12", "/t/k-1_b"),
            ("p-6", "/t/k-0"), ("p-7", "/t/k-2"),
        ];

        var profile = OxiaServerProfile.WithShards(4, keySorting);

        // The records of "/t/k-1" are on several shards
        var shards = new HashSet<long>();
        foreach (var record in records[..3])
        {
            shards.Add(Sharding.ShardOf(record.key, profile.Shards));
        }

        Assert.True(shards.Count > 1);

        await using var standaloneServer = await oxia.StartAsync(profile);

        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        foreach (var record in records)
        {
            await client.PutAsync(TestCtx, record.key, B(record.key), OxiaOptions.SecondaryIndex("idx", record.secondaryKey));
        }

        foreach (var key in new[] { "q-1", "q-2" })
        {
            await client.PutAsync(TestCtx, key, B(key), OxiaOptions.PartitionKey("q"));
        }

        async Task<List<string>> List(string start, string end, params ListOption[] options) =>
            await client.ListAsync(TestCtx, start, end, options); //nolint:staticcheck

        async Task<List<string>> Scan(string start, string end, params RangeScanOption[] options)
        {
            var keys = new List<string>();
            await foreach (var result in client.RangeScanAsync(TestCtx, start, end, options).ReadAllAsync(TestCtx))
            {
                AssertNoError(result.Err);
                keys.Add(result.Key);
            }

            return keys;
        }

        var index = OxiaOptions.UseIndex("idx");
        var endInclusive = OxiaOptions.EndInclusive();

        AssertElementsMatch(["p-0", "p-3", "p-4"], await List("/t/k-1", "/t/k-1", index, endInclusive));
        Assert.Equal(["p-0", "p-3", "p-4"], await Scan("/t/k-1", "/t/k-1", index, endInclusive));
        Assert.Empty(await List("/t/k-1", "/t/k-1", index));

        AssertElementsMatch(["p-6", "p-0", "p-3", "p-4"], await List("/t/k-0", "/t/k-1", index, endInclusive));
        Assert.Equal(["p-6", "p-0", "p-3", "p-4"], await Scan("/t/k-0", "/t/k-1", index, endInclusive));

        // On the primary keys
        AssertElementsMatch(["p-1", "p-10", "p-12", "p-3"], await List("p-1", "p-3", endInclusive));
        Assert.Equal(["p-1", "p-10", "p-12", "p-3"], await Scan("p-1", "p-3", endInclusive));
        Assert.Equal(["q-1", "q-2"], await List("q-1", "q-2", OxiaOptions.PartitionKey("q"), endInclusive));
        Assert.Equal(["q-1", "q-2"], await Scan("q-1", "q-2", OxiaOptions.PartitionKey("q"), endInclusive));

        await client.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_PartitionRouting()
    {
        // Test with multiple shards to ensure correctness across shards
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(10));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;

        await client.PutAsync(ctx, "a", B("0"), OxiaOptions.PartitionKey("x"));
        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "a")), ClientErrors.KeyNotFound);

        var (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.PartitionKey("x"));
        Assert.Equal("a", key);
        Assert.Equal("0", TestUtil.Utf8(value));

        await client.PutAsync(ctx, "a", B("0"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "b", B("1"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "c", B("2"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "d", B("3"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "e", B("4"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "f", B("5"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "g", B("6"), OxiaOptions.PartitionKey("x"));

        // Listing must yield the same results
        var keys = await client.ListAsync(ctx, "a", "d"); //nolint:staticcheck
        Assert.Equal(["a", "b", "c"], keys);

        keys = await client.ListAsync(ctx, "a", "d", OxiaOptions.PartitionKey("x")); //nolint:staticcheck
        Assert.Equal(["a", "b", "c"], keys);

        // Searching with wrong partition-key will return empty list
        keys = await client.ListAsync(ctx, "a", "d", OxiaOptions.PartitionKey("wrong-partition-key")); //nolint:staticcheck
        Assert.NotNull(keys);
        Assert.Empty(keys);

        // Delete with wrong partition key would fail
        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.DeleteAsync(ctx, "g", OxiaOptions.PartitionKey("wrong-partition-key"))),
            ClientErrors.KeyNotFound);

        await client.DeleteAsync(ctx, "g", OxiaOptions.PartitionKey("x"));

        // Get tests
        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonHigher());
        Assert.Equal("b", key);
        Assert.Equal("1", TestUtil.Utf8(value));

        (key, value, _) = await client.GetAsync(ctx, "a", OxiaOptions.ComparisonHigher(), OxiaOptions.PartitionKey("x"));
        Assert.Equal("b", key);
        Assert.Equal("1", TestUtil.Utf8(value));

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
                await client.GetAsync(ctx, "a", OxiaOptions.ComparisonHigher(), OxiaOptions.PartitionKey("wrong-partition-key"))),
            ClientErrors.KeyNotFound);

        // Delete with wrong partition key would fail to delete all keys
        await client.DeleteRangeAsync(ctx, "c", "e", OxiaOptions.PartitionKey("wrong-partition-key"));

        keys = await client.ListAsync(ctx, "c", "f"); //nolint:staticcheck
        Assert.Equal(["c", "d", "e"], keys);

        await client.DeleteRangeAsync(ctx, "c", "e", OxiaOptions.PartitionKey("x"));

        keys = await client.ListAsync(ctx, "c", "f"); //nolint:staticcheck
        Assert.Equal(["e"], keys);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_SequentialKeys()
    {
        // Test with multiple shards to ensure correctness across shards
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(10));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.PutAsync(ctx, "a", B("0"), OxiaOptions.SequenceKeysDeltas(1))),
            ClientErrors.InvalidOptions);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.PutAsync(ctx, "a", B("0"),
            OxiaOptions.SequenceKeysDeltas(1),
            OxiaOptions.PartitionKey("x"),
            OxiaOptions.ExpectedVersionId(1))), ClientErrors.InvalidOptions);

        var (key, _) = await client.PutAsync(ctx, "a", B("0"),
            OxiaOptions.SequenceKeysDeltas(1),
            OxiaOptions.PartitionKey("x"));
        Assert.Equal($"a-{1:D20}", key);

        (key, _) = await client.PutAsync(ctx, "a", B("1"),
            OxiaOptions.SequenceKeysDeltas(3),
            OxiaOptions.PartitionKey("x"));
        Assert.Equal($"a-{4:D20}", key);

        (key, _) = await client.PutAsync(ctx, "a", B("2"),
            OxiaOptions.SequenceKeysDeltas(1, 6),
            OxiaOptions.PartitionKey("x"));
        Assert.Equal($"a-{5:D20}-{6:D20}", key);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "a")), ClientErrors.KeyNotFound);

        var (_, value, _) = await client.GetAsync(ctx, $"a-{1:D20}", OxiaOptions.PartitionKey("x"));
        Assert.Equal(B("0"), value);

        (_, value, _) = await client.GetAsync(ctx, $"a-{4:D20}", OxiaOptions.PartitionKey("x"));
        Assert.Equal(B("1"), value);

        (_, value, _) = await client.GetAsync(ctx, $"a-{5:D20}-{6:D20}", OxiaOptions.PartitionKey("x"));
        Assert.Equal(B("2"), value);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_RangeScan()
    {
        // Test with multiple shards to ensure correctness across shards
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(10));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;

        await client.PutAsync(ctx, "a", B("0"));
        await client.PutAsync(ctx, "b", B("1"));
        await client.PutAsync(ctx, "c", B("2"));
        await client.PutAsync(ctx, "d", B("3"));
        await client.PutAsync(ctx, "e", B("4"));
        await client.PutAsync(ctx, "f", B("5"));
        await client.PutAsync(ctx, "g", B("6"));

        var ch = client.RangeScanAsync(ctx, "b", "f");

        var gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("b", gr.Key);
        Assert.Equal(B("1"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("c", gr.Key);
        Assert.Equal(B("2"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("d", gr.Key);
        Assert.Equal(B("3"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("e", gr.Key);
        Assert.Equal(B("4"), gr.Value);

        var (more, _) = await ch.Receive();
        Assert.False(more);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Theory]
    [InlineData("primary", "a", "d", false, new[] { "a", "b", "c" })]
    [InlineData("index", "0", "3", true, new[] { "c", "b", "a" })]
    public async Task TestSyncClientImplRangeScanWithoutValues(string name, string start, string end, bool useIndex, string[] expected)
    {
        Assert.NotEmpty(name);
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(3));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;
        var versions = new Dictionary<string, RecordVersion>();
        var i = 0;
        foreach (var key in new[] { "a", "b", "c" })
        {
            var (_, version) = await client.PutAsync(ctx, key, B(key), OxiaOptions.SecondaryIndex("idx", (2 - i).ToString()));
            versions[key] = version;
            i++;
        }

        // Go: append(test.options, oxia.IncludeValue(false))...
        RangeScanOption[] options = useIndex
            ? [OxiaOptions.UseIndex("idx"), OxiaOptions.IncludeValue(false)]
            : [OxiaOptions.IncludeValue(false)];

        var keys = new List<string>();
        await foreach (var result in client.RangeScanAsync(ctx, start, end, options).ReadAllAsync(ctx))
        {
            AssertNoError(result.Err);
            Assert.Null(result.Value);
            Assert.Equal(versions[result.Key].VersionId, result.Version.VersionId);
            keys.Add(result.Key);
        }

        Assert.Equal(expected, keys);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_RangeScanOnPartition()
    {
        // Test with multiple shards to ensure correctness across shards
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(10));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;

        await client.PutAsync(ctx, "a", B("0"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "b", B("1"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "c", B("2"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "d", B("3"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "e", B("4"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "f", B("5"), OxiaOptions.PartitionKey("x"));
        await client.PutAsync(ctx, "g", B("6"), OxiaOptions.PartitionKey("x"));

        var ch = client.RangeScanAsync(ctx, "b", "f", OxiaOptions.PartitionKey("x"));

        var gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("b", gr.Key);
        Assert.Equal(B("1"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("c", gr.Key);
        Assert.Equal(B("2"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("d", gr.Key);
        Assert.Equal(B("3"), gr.Value);

        gr = await ch.Recv();
        AssertNoError(gr.Err);
        Assert.Equal("e", gr.Key);
        Assert.Equal(B("4"), gr.Value);

        var (more, _) = await ch.Receive();
        Assert.False(more);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_SequenceOrdering()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithMaxRequestsPerBatch(1));

        var responses = new List<ValueTask<PutResult>>();

        for (var i = 0; i < 100; i++)
        {
            var r = client.PutAsync("a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
            responses.Add(r);
        }

        for (var i = 0; i < 100; i++)
        {
            var r = await responses[i];

            Assert.Equal($"a-{i + 1:D20}", r.Key);
        }

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestAsyncClientImpl_versionId()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr);

        var ch0 = client.PutAsync("/a", B("0"));
        var ch1 = client.PutAsync("/a", B("1"));
        var ch2 = client.PutAsync("/a", B("2"));

        var chb0 = client.PutAsync("/b", B("0"));

        var r0 = await ch0;
        var r1 = await ch1;
        var r2 = await ch2;
        var rb0 = await chb0;

        AssertNoError(r0.Err);
        AssertNoError(r1.Err);
        AssertNoError(r2.Err);
        AssertNoError(rb0.Err);

        Assert.Equal(0L, r0.Version.VersionId);
        Assert.Equal(1L, r1.Version.VersionId);
        Assert.Equal(2L, r2.Version.VersionId);
        Assert.Equal(3L, rb0.Version.VersionId);

        var ch3 = client.PutAsync("/a", B("3"));
        var r3 = await ch3;
        Assert.Equal(4L, r3.Version.VersionId);

        // Go does not close the client here; closing it keeps the server's shutdown quiet
        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestGetValueWithSessionId()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr);

        var ch0 = client.PutAsync("/TestGetValueWithSessionId", B("0"), OxiaOptions.Ephemeral());
        var r0 = await ch0;
        AssertNoError(r0.Err);
        Assert.Equal(1L, r0.Version.VersionId);

        var ch1 = client.GetAsync("/TestGetValueWithSessionId");
        var r1 = await ch1;
        AssertNoError(r1.Err);
        Assert.Equal(r1.Version.SessionId, r0.Version.SessionId);

        // cleanup
        await client.CloseAsync();

        client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr);

        ch0 = client.PutAsync("/TestGetValueWithSessionId", B("0"), OxiaOptions.Ephemeral());
        r0 = await ch0;
        AssertNoError(r0.Err);
        Assert.Equal(3L, r0.Version.VersionId);

        ch1 = client.GetAsync("/TestGetValueWithSessionId");
        r1 = await ch1;
        AssertNoError(r1.Err);
        Assert.NotEqual(0L, r0.Version.SessionId);
        Assert.Equal(r1.Version.SessionId, r0.Version.SessionId);

        await client.CloseAsync();
    }

    [Fact]
    public async Task TestGetWithoutValue()
    {
        await using var standaloneServer = await oxia.StartAsync();

        await using var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr);

        var key = "stream";

        var keys = new List<string>(2);

        var putResult = await client.PutAsync(key, B("0"), OxiaOptions.PartitionKey(key), OxiaOptions.SequenceKeysDeltas(1));
        Assert.NotNull(putResult.Key);
        AssertNoError(putResult.Err);
        keys.Add(putResult.Key);

        putResult = await client.PutAsync(key, B("1"), OxiaOptions.PartitionKey(key), OxiaOptions.SequenceKeysDeltas(1));
        Assert.NotNull(putResult.Key);
        AssertNoError(putResult.Err);
        keys.Add(putResult.Key);

        foreach (var subKey in keys)
        {
            var result = await client.GetAsync(subKey, OxiaOptions.PartitionKey(key), OxiaOptions.IncludeValue(true));
            Assert.NotNull(result.Value);
            result = await client.GetAsync(subKey, OxiaOptions.PartitionKey(key), OxiaOptions.IncludeValue(false));
            Assert.Null(result.Value);
        }

        var result2 = await client.GetAsync(keys[0], OxiaOptions.PartitionKey(key), OxiaOptions.IncludeValue(false), OxiaOptions.ComparisonHigher());
        Assert.Null(result2.Value);
        Assert.Equal(result2.Key, keys[1]);

        result2 = await client.GetAsync(keys[1], OxiaOptions.PartitionKey(key), OxiaOptions.IncludeValue(false), OxiaOptions.ComparisonLower());
        Assert.Null(result2.Value);
        Assert.Equal(result2.Key, keys[0]);
    }
}
