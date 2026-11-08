// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/sync_client_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Sync client against a real standalone server: secondary indexes (list / range scan / get with all comparison types),
//              GetSequenceUpdates (client close, cancellation, multiple shards), internal keys, large values.
//   Go tests: 9   ported: 9   skipped: 0
//   Divergences:
//     * Go `dataserver.NewStandalone(config)` is `oxia.StartAsync(profile)` (fresh empty `oxia standalone` in the Docker container);
//       `config.NumShards = 10` is `OxiaServerProfile.WithShards(10)`, `config.NotificationsEnabled = false` is
//       `Default with { NotificationsEnabled = false }`.
//     * `ch1, err := client.GetSequenceUpdates(ctx, "a")` (nil channel + ErrInvalidOptions) throws in C# (see Client.cs): the test
//       asserts the thrown error contains ErrInvalidOptions, which also implies there is no channel.
//     * `for _, ok := <-ch` / `select { default: }` idioms use TestUtil.Receive / TryReceive / IsEmpty.
// =====================================================================================================================
using Oxia.Common.Hash;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public class SyncClientTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestSyncClientImpl_SecondaryIndexes()
    {
        // Go: "Test with multiple shards to ensure correctness across shards" -- but NumShards = 1
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(1));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        // ////////////////////////////////////////////////////////////////////////

        var ctx = CancellationToken.None;
        for (var i = 0; i < 10; i++)
        {
            var primKey = ((char)('a' + i)).ToString();
            var val = i.ToString();
            await client.PutAsync(ctx, primKey, B(val), OxiaOptions.SecondaryIndex("val-idx", val));
        }

        // ////////////////////////////////////////////////////////////////////////

        var l = await client.ListAsync(ctx, "1", "4", OxiaOptions.UseIndex("val-idx")); //nolint:staticcheck
        Assert.Equal(["b", "c", "d"], l);

        // ////////////////////////////////////////////////////////////////////////

        var resCh = client.RangeScanAsync(ctx, "1", "4", OxiaOptions.UseIndex("val-idx"));
        var n = 1;
        await foreach (var res in resCh.ReadAllAsync(ctx))
        {
            AssertNoError(res.Err);

            var primKey = ((char)('a' + n)).ToString();
            var val = n.ToString();

            Assert.Equal(primKey, res.Key);
            Assert.Equal(val, TestUtil.Utf8(res.Value));
            n++;
        }

        Assert.Equal(4, n);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_SecondaryIndexesRepeated()
    {
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(1));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        // ////////////////////////////////////////////////////////////////////////

        var ctx = CancellationToken.None;
        for (var i = 0; i < 10; i++)
        {
            var primKey = "/" + (char)('a' + i);
            var val = ((char)('a' + i)).ToString();
            await client.PutAsync(ctx, primKey, B(val),
                OxiaOptions.SecondaryIndex("val-idx", val),
                OxiaOptions.SecondaryIndex("val-idx", val.ToUpperInvariant()));
        }

        // ////////////////////////////////////////////////////////////////////////

        var l = await client.ListAsync(ctx, "b", "e", OxiaOptions.UseIndex("val-idx")); //nolint:staticcheck
        Assert.Equal(["/b", "/c", "/d"], l);

        l = await client.ListAsync(ctx, "I", "d", OxiaOptions.UseIndex("val-idx")); //nolint:staticcheck
        Assert.Equal(["/i", "/j", "/a", "/b", "/c"], l);

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_SecondaryIndexes_Get()
    {
        await DoSecondaryIndexesGet(OxiaServerProfile.WithShards(10));
    }

    [Fact]
    public async Task TestSyncClientImpl_SecondaryIndexes_Get_NoNotifications()
    {
        await DoSecondaryIndexesGet(OxiaServerProfile.Default with { NotificationsEnabled = false });
    }

    private async Task DoSecondaryIndexesGet(OxiaServerProfile config)
    {
        await using var standaloneServer = await oxia.StartAsync(config);

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        // ////////////////////////////////////////////////////////////////////////

        var ctx = CancellationToken.None;
        for (var i = 1; i < 10; i++)
        {
            var primKey = ((char)('a' + i)).ToString();
            var val = $"{i:D3}";
            await client.PutAsync(ctx, primKey, B(val), OxiaOptions.SecondaryIndex("val-idx", val));
        }

        string primaryKey;
        byte[]? val2;
        // ////////////////////////////////////////////////////////////////////////

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "000", OxiaOptions.UseIndex("val-idx"))), ClientErrors.KeyNotFound);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "001", OxiaOptions.UseIndex("val-idx"));
        Assert.Equal("b", primaryKey);
        Assert.Equal(B("001"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "005", OxiaOptions.UseIndex("val-idx"));
        Assert.Equal("f", primaryKey);
        Assert.Equal(B("005"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "009", OxiaOptions.UseIndex("val-idx"));
        Assert.Equal("j", primaryKey);
        Assert.Equal(B("009"), val2);

        AssertErrorIs(await TestUtil.CatchAsync(async () => await client.GetAsync(ctx, "999", OxiaOptions.UseIndex("val-idx"))), ClientErrors.KeyNotFound);

        // ////////////////////////////////////////////////////////////////////////

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "000", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonLower())), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "001", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonLower())), ClientErrors.KeyNotFound);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "005", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonLower());
        Assert.Equal("e", primaryKey);
        Assert.Equal(B("004"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "009", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonLower());
        Assert.Equal("i", primaryKey);
        Assert.Equal(B("008"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "999", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonLower());
        Assert.Equal("j", primaryKey);
        Assert.Equal(B("009"), val2);

        // ////////////////////////////////////////////////////////////////////////

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "000", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonFloor())), ClientErrors.KeyNotFound);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "001", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonFloor());
        Assert.Equal("b", primaryKey);
        Assert.Equal(B("001"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "005", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonFloor());
        Assert.Equal("f", primaryKey);
        Assert.Equal(B("005"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "009", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonFloor());
        Assert.Equal("j", primaryKey);
        Assert.Equal(B("009"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "999", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonFloor());
        Assert.Equal("j", primaryKey);
        Assert.Equal(B("009"), val2);

        // ////////////////////////////////////////////////////////////////////////

        (primaryKey, val2, _) = await client.GetAsync(ctx, "000", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonHigher());
        Assert.Equal("b", primaryKey);
        Assert.Equal(B("001"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "001", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonHigher());
        Assert.Equal("c", primaryKey);
        Assert.Equal(B("002"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "005", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonHigher());
        Assert.Equal("g", primaryKey);
        Assert.Equal(B("006"), val2);

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "009", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonHigher())), ClientErrors.KeyNotFound);

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "999", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonHigher())), ClientErrors.KeyNotFound);

        // ////////////////////////////////////////////////////////////////////////

        (primaryKey, val2, _) = await client.GetAsync(ctx, "000", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonCeiling());
        Assert.Equal("b", primaryKey);
        Assert.Equal(B("001"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "001", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonCeiling());
        Assert.Equal("b", primaryKey);
        Assert.Equal(B("001"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "005", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonCeiling());
        Assert.Equal("f", primaryKey);
        Assert.Equal(B("005"), val2);

        (primaryKey, val2, _) = await client.GetAsync(ctx, "009", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonCeiling());
        Assert.Equal("j", primaryKey);
        Assert.Equal(B("009"), val2);

        AssertErrorIs(await TestUtil.CatchAsync(async () =>
            await client.GetAsync(ctx, "999", OxiaOptions.UseIndex("val-idx"), OxiaOptions.ComparisonCeiling())), ClientErrors.KeyNotFound);

        // ////////////////////////////////////////////////////////////////////////

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_GetSequenceUpdates_ClientClose()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        var (k1, _) = await client.PutAsync(CancellationToken.None, "a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));

        // The subscription context is never canceled
        var updates = client.GetSequenceUpdates(CancellationToken.None, "a", OxiaOptions.PartitionKey("x"));
        Assert.Equal(k1, await updates.Recv());

        // Closing the client ends the subscription
        await client.CloseAsync();

        var (timedOut, closed, _) = await updates.ReceiveWithin(TimeSpan.FromSeconds(10));
        if (timedOut)
        {
            Assert.Fail("the subscription should have been closed with the client");
        }
        else
        {
            Assert.True(closed, "the subscription should have been closed with the client");
        }

        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_GetSequenceUpdates()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr, OxiaOptions.WithBatchLinger(TimeSpan.Zero));

        // Go: ch1, err := client.GetSequenceUpdates(ctx, "a") -> assert.Nil(ch1); assert.ErrorIs(err, ErrInvalidOptions)
        AssertErrorIs(TestUtil.Catch(() => client.GetSequenceUpdates(CancellationToken.None, "a")), ClientErrors.InvalidOptions);

        using var cancel1 = new CancellationTokenSource();
        var ch1 = client.GetSequenceUpdates(cancel1.Token, "a", OxiaOptions.PartitionKey("x"));
        Assert.NotNull(ch1);
        cancel1.Cancel();

        var (k1, _) = await client.PutAsync(CancellationToken.None, "a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
        Assert.Equal($"a-{1:D20}", k1);
        var (k2, _) = await client.PutAsync(CancellationToken.None, "a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
        Assert.Equal($"a-{2:D20}", k2);
        Assert.NotEqual(k1, k2);

        using var cancel2 = new CancellationTokenSource();
        var updates2 = client.GetSequenceUpdates(cancel2.Token, "a", OxiaOptions.PartitionKey("x"));

        var recvK2 = await updates2.Recv();
        Assert.Equal(k2, recvK2);

        cancel2.Cancel();

        var (k3, _) = await client.PutAsync(CancellationToken.None, "a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
        Assert.True(updates2.IsEmpty());

        // The channel is closed asynchronously after the cancellation, possibly
        // after delivering a value that was already in flight
        await TestUtil.Eventually(() =>
        {
            var (ready, ok, _) = updates2.TryReceive();
            return ready && !ok;
        }, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));

        using var cancel3 = new CancellationTokenSource();
        var updates3 = client.GetSequenceUpdates(cancel3.Token, "a", OxiaOptions.PartitionKey("x"));

        var recvK3 = await updates3.Recv();
        Assert.Equal(k3, recvK3);

        var (k4, _) = await client.PutAsync(CancellationToken.None, "a", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
        var recvK4 = await updates3.Recv();
        Assert.Equal(k4, recvK4);

        // Stop the subscription first, or it keeps retrying on the closed client
        cancel3.Cancel();
        await client.CloseAsync();

        await standaloneServer.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_GetSequenceUpdates_MultipleShards()
    {
        var config = OxiaServerProfile.WithShards(10);
        await using var standaloneServer = await oxia.StartAsync(config);

        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        // The updates must come from the shard owning the partition key, so pick
        // a key that does not route to shard 0
        var partitionKey = "x";
        Assert.True(Hasher.Xxh332(partitionKey) > Sharding.GenerateShards(0, config.Shards)[0].Max);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestCtx);
        var ctx = cts.Token;
        var updates = client.GetSequenceUpdates(ctx, "a", OxiaOptions.PartitionKey(partitionKey));

        var (key, _) = await client.PutAsync(ctx, "a", B("0"), OxiaOptions.PartitionKey(partitionKey), OxiaOptions.SequenceKeysDeltas(1));

        var (timedOut, _, recvKey) = await updates.ReceiveWithin(TimeSpan.FromSeconds(10));
        if (!timedOut)
        {
            Assert.Equal(key, recvKey);
        }
        else
        {
            Assert.Fail("sequence update not received");
        }

        cts.Cancel();
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestSyncClientImpl_InternalKeys()
    {
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(1));

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        // ////////////////////////////////////////////////////////////////////////

        var ctx = CancellationToken.None;
        await client.PutAsync(ctx, "a", Array.Empty<byte>());
        await client.PutAsync(ctx, "b", Array.Empty<byte>());
        await client.PutAsync(ctx, "c", Array.Empty<byte>());
        await client.PutAsync(ctx, "__oxia/a-test", Array.Empty<byte>());

        // ////////////////////////////////////////////////////////////////////////

        var l = await client.ListAsync(ctx, "a", ""); //nolint:staticcheck
        Assert.Equal(["a", "b", "c"], l);

        l = await client.ListAsync(ctx, "a", "__oxia/a-test---", OxiaOptions.ShowInternalKeys(true)); //nolint:staticcheck
        Assert.Equal(["a", "b", "c", "__oxia/a-test"], l);

        // ////////////////////////////////////////////////////////////////////////

        var resCh = client.RangeScanAsync(ctx, "a", "");
        Assert.Equal("a", (await resCh.Recv()).Key);
        Assert.Equal("b", (await resCh.Recv()).Key);
        Assert.Equal("c", (await resCh.Recv()).Key);
        Assert.True(resCh.IsEmpty());

        resCh = client.RangeScanAsync(ctx, "a", "", OxiaOptions.ShowInternalKeys(true));
        Assert.Equal("a", (await resCh.Recv()).Key);
        Assert.Equal("b", (await resCh.Recv()).Key);
        Assert.Equal("c", (await resCh.Recv()).Key);
        Assert.Equal("__oxia/a-test", (await resCh.Recv()).Key);
        // Verify remaining system-created internal keys are returned
        var systemKeys = new List<string>();
        await foreach (var r in resCh.ReadAllAsync(ctx))
        {
            systemKeys.Add(r.Key);
        }

        Assert.NotEmpty(systemKeys);
        foreach (var k in systemKeys)
        {
            Assert.True(k.StartsWith("__oxia/", StringComparison.Ordinal), $"expected internal key prefix, got: {k}");
        }

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }

    // Ensure the client can handle legitimately large requests without being constrained by gRPC's default configuration.
    [Fact]
    public async Task TestSyncClientImpl_LargeValue()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        var ctx = CancellationToken.None;
        const string key = "/large-value";
        // Larger than the 4 MiB gRPC default receive limit
        var value = new byte[5 * 1024 * 1024];
        Array.Fill(value, (byte)'a');

        await client.PutAsync(ctx, key, value);

        var (_, readValue, _) = await client.GetAsync(ctx, key);
        Assert.NotNull(readValue);
        Assert.True(value.AsSpan().SequenceEqual(readValue), "the value read differs from the value written");

        await client.CloseAsync();
        await standaloneServer.CloseAsync();
    }
}
