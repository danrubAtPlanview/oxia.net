// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : oxia/async_client_impl.go (Get, RangeScan)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The opt-in pooled read API (Oxia.Client.Pooled): GetPooledAsync and RangeScanPooledAsync over the whole client against a
//              REAL gRPC server (PooledApiTestServer), checked against the faithful API: the same values / keys / versions / errors /
//              key order, but the value bytes live in a pooled buffer that Dispose gives back.  Also pins the rules: a double Dispose (also
//              of a copy of the struct) is harmless, a stale copy reads nothing, pooled and classic gets can share one batch, IncludeValue(false)
//              drops the value, an error ends a scan, and the fallbacks (a get that needs several shards) keep the contract.
//   Divergences: n/a (new tests).
// =====================================================================================================================
using Oxia.Client.Pooled;
using Oxia.Common.Constant;
using Xunit;
using Xunit.Sdk;

namespace Oxia.Client.Tests.Alloc;

public class PooledReadApiTests
{
    private static async Task<IAsyncClient> NewClient(PooledApiTestServer server, TimeSpan? linger = null) =>
        await OxiaClient.CreateAsyncClientAsync(server.Address, OxiaOptions.WithBatchLinger(linger ?? TimeSpan.FromMilliseconds(5)),
            OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(30)));

    [Fact]
    public async Task GetPooledAsync_ReturnsTheValueKeyAndVersion_LikeGetAsync()
    {
        await using var server = await PooledApiTestServer.StartAsync();
        var client = await NewClient(server);
        try
        {
            var classic = await client.GetAsync("some-key");
            using var pooled = await client.GetPooledAsync("some-key");

            Assert.Null(classic.Err);
            Assert.Null(pooled.Err);
            Assert.True(pooled.HasValue);
            Assert.Equal(classic.Value, pooled.Value.ToArray());
            Assert.Equal(PooledApiTestServer.ValueOf("some-key"), pooled.Value.ToArray());
            Assert.Equal("some-key", pooled.Key);
            Assert.Equal(classic.Key, pooled.Key);
            Assert.Equal(classic.Version, pooled.Version);
            Assert.Equal(7, pooled.Version.VersionId);
            Assert.Equal(2, pooled.Version.ModificationsCount);
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task GetPooledAsync_KeyNotFound_IsAnErrorWithoutABuffer()
    {
        await using var server = await PooledApiTestServer.StartAsync();
        var client = await NewClient(server);
        try
        {
            using var pooled = await client.GetPooledAsync("missing-key");

            Assert.True(OxiaErrors.Is(pooled.Err, ClientErrors.KeyNotFound));
            Assert.False(pooled.HasValue);
            Assert.True(pooled.Value.IsEmpty);
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task Dispose_IsIdempotent_AlsoForCopies_AndAStaleCopyReadsNothing()
    {
        await using var server = await PooledApiTestServer.StartAsync();
        var client = await NewClient(server);
        try
        {
            var first = await client.GetPooledAsync("k1");
            var copy = first;
            Assert.False(first.Value.IsEmpty);

            first.Dispose();
            first.Dispose();
            copy.Dispose(); // the copy shares the lease: its generation is stale now, nothing is released twice

            Assert.True(copy.Value.IsEmpty);
            Assert.False(copy.HasValue);

            // the pool is intact: the next gets still return their own values (a double release would hand one buffer to two results)
            var results = new List<PooledGetResult>();
            for (int i = 0; i < 20; i++) results.Add(await client.GetPooledAsync("k" + i));
            for (int i = 0; i < 20; i++) Assert.Equal(PooledApiTestServer.ValueOf("k" + i), results[i].Value.ToArray());
            foreach (var r in results) r.Dispose();
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task PooledAndClassicGets_CanShareOneBatch()
    {
        await using var server = await PooledApiTestServer.StartAsync();
        var client = await NewClient(server, TimeSpan.FromMilliseconds(100)); // one batch for all of them

        try
        {
            var classic = new List<ValueTask<GetResult>>();
            var pooled = new List<ValueTask<PooledGetResult>>();
            for (int i = 0; i < 10; i++)
            {
                classic.Add(client.GetAsync("c" + i));
                pooled.Add(client.GetPooledAsync("p" + i));
            }
            var missing = client.GetAsync("missing-1");

            for (int i = 0; i < 10; i++)
            {
                var c = await classic[i];
                Assert.Null(c.Err);
                Assert.Equal("c" + i, c.Key);
                Assert.Equal(PooledApiTestServer.ValueOf("c" + i), c.Value); // the lease was copied into an exact-size array
                using var p = await pooled[i];
                Assert.Null(p.Err);
                Assert.Equal(PooledApiTestServer.ValueOf("p" + i), p.Value.ToArray());
            }
            Assert.True(OxiaErrors.Is((await missing).Err, ClientErrors.KeyNotFound));
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task GetPooledAsync_WithoutTheValue_HasNoValue()
    {
        await using var server = await PooledApiTestServer.StartAsync();
        var client = await NewClient(server);
        try
        {
            using var pooled = await client.GetPooledAsync("k", OxiaOptions.IncludeValue(false));

            Assert.Null(pooled.Err);
            Assert.False(pooled.HasValue);
            Assert.Equal(7, pooled.Version.VersionId);
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task GetPooledAsync_NeedingSeveralShards_FallsBackToACopy()
    {
        await using var server = await PooledApiTestServer.StartAsync(shards: 2);
        var client = await NewClient(server);
        try
        {
            // a floor get without a partition key asks every shard: the classic multi-shard path, copied into a lease
            using var pooled = await client.GetPooledAsync("floor-key", OxiaOptions.ComparisonFloor());

            Assert.Null(pooled.Err);
            Assert.Equal("floor-key", pooled.Key);
            Assert.Equal(PooledApiTestServer.ValueOf("floor-key"), pooled.Value.ToArray());
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    // ----- range scan ---------------------------------------------------------------------------------------------------

    private static async Task<List<(string Key, byte[] Value, long VersionId)>> Scan(IAsyncClient client, params RangeScanOption[] options)
    {
        var records = new List<(string, byte[], long)>();
        var reader = client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0", options);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await foreach (var r in reader.ReadAllAsync(cts.Token))
        {
            using (r)
            {
                if (r.Err is not null) throw r.Err;
                records.Add((System.Text.Encoding.UTF8.GetString(r.KeyUtf8), r.Value.ToArray(), r.Version.VersionId));
            }
        }
        return records;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task RangeScanPooledAsync_MergesTheShardsInKeyOrder_LikeRangeScanAsync(int shards)
    {
        await using var server = await PooledApiTestServer.StartAsync(shards: shards, scanRecords: 25, scanChunk: 4);
        var client = await NewClient(server);
        try
        {
            var pooled = await Scan(client);

            var classic = new List<GetResult>();
            await foreach (var r in client.RangeScanAsync(CancellationToken.None, "/scan/", "/scan0").ReadAllAsync()) classic.Add(r);

            Assert.Equal(25, pooled.Count);
            Assert.Equal(classic.Count, pooled.Count);
            for (int i = 0; i < 25; i++)
            {
                Assert.Equal(PooledApiTestServer.ScanKey(i), pooled[i].Key);
                Assert.Equal(classic[i].Key, pooled[i].Key);
                Assert.Equal(classic[i].Value, pooled[i].Value);
                Assert.Equal(PooledApiTestServer.ValueOf(pooled[i].Key), pooled[i].Value);
                Assert.Equal(i + 1, pooled[i].VersionId);
            }
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task RangeScanPooledAsync_WithAPartitionKey_ScansOneShard()
    {
        await using var server = await PooledApiTestServer.StartAsync(shards: 2, scanRecords: 10);
        var client = await NewClient(server);
        try
        {
            var records = await Scan(client, OxiaOptions.PartitionKey("p"));

            // all the records of ONE shard (every second one)
            Assert.Equal(5, records.Count);
            Assert.True(records.Select(r => r.Key).SequenceEqual(records.Select(r => r.Key).OrderBy(k => k, StringComparer.Ordinal)));
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task RangeScanPooledAsync_AppliesTheRange_AndDropsTheValuesWhenAsked()
    {
        await using var server = await PooledApiTestServer.StartAsync(shards: 2, scanRecords: 20);
        var client = await NewClient(server);
        try
        {
            var reader = client.RangeScanPooledAsync(CancellationToken.None, PooledApiTestServer.ScanKey(5), PooledApiTestServer.ScanKey(9),
                OxiaOptions.IncludeValue(false));
            var keys = new List<string>();
            await foreach (var r in reader.ReadAllAsync())
            {
                using (r)
                {
                    Assert.Null(r.Err);
                    Assert.False(r.HasValue);
                    keys.Add(r.Key); // the string form of the key
                }
            }

            Assert.Equal(Enumerable.Range(5, 4).Select(PooledApiTestServer.ScanKey), keys);
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task RangeScanPooledAsync_AShardThatFails_EndsTheScanWithTheErrorAsTheLastItem()
    {
        await using var server = await PooledApiTestServer.StartAsync(shards: 2, scanRecords: 40, scanChunk: 2, failScanOnShard: 1);
        var client = await NewClient(server);
        try
        {
            var reader = client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0");
            Exception? error = null;
            int records = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await foreach (var r in reader.ReadAllAsync(cts.Token))
            {
                using (r)
                {
                    if (r.Err is not null)
                    {
                        Assert.Null(error); // only one error item, and nothing after it
                        error = r.Err;
                        continue;
                    }
                    Assert.Null(error);
                    records++;
                }
            }

            Assert.NotNull(error);
            Assert.True(records < 40);
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    [Fact]
    public async Task RangeScanPooledAsync_ACancelledToken_CompletesTheReader()
    {
        await using var server = await PooledApiTestServer.StartAsync(scanRecords: 10);
        var client = await NewClient(server);
        try
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            var reader = client.RangeScanPooledAsync(cts.Token, "/scan/", "/scan0");

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            int items = 0;
            await foreach (var r in reader.ReadAllAsync(timeout.Token))
            {
                using (r) items++;
            }
            Assert.True(items <= 10);
        }
        finally
        {
            await client.CloseAsync();
        }
    }
}
