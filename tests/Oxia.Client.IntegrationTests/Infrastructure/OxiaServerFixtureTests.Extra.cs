// =====================================================================================================================
// PORT NOTE
//   Upstream : none -- extra tests (DESIGN.md "Test porting protocol" 6) of the Docker server fixture itself.  They use only raw
//              gRPC (RawOxia / OxiaReadiness), never the client under test, so they can run before the client exists.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Prove the properties the ported Go tests rely on: every StartAsync gives an empty server with exactly the requested
//              profile (shards, key sorting), the announced leader is the address the client dialled, servers are isolated
//              from each other, and closing frees the slot.
//   Divergences: n/a.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.IntegrationTests.Infrastructure;

public class OxiaServerFixtureTests(OxiaServerFixture oxia)
{
    private static void Out(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    [Fact]
    public async Task Fixture_StartsDefaultServer()
    {
        await using var server = await oxia.StartAsync();
        Out($"{server} (container {oxia.ContainerName}, reused={oxia.ContainerWasReused}, external={oxia.IsExternal})");
        foreach (var e in oxia.Events) Out(e);

        var (shards, sorting) = await RawOxia.GetAssignmentsAsync(server.Address);
        Assert.Single(shards);
        Assert.Equal(0, shards[0].Shard);
        // a standalone server announces, as the leader, the authority the client used: the published host port
        Assert.Equal(server.Address, shards[0].Leader);
        Assert.Equal(G.KeySorting.Hierarchical, sorting);
    }

    [Fact]
    public async Task Fixture_AppliesProfile()
    {
        await using var server = await oxia.StartAsync(OxiaServerProfile.WithShards(4, ServerKeySorting.Natural));
        var (shards, sorting) = await RawOxia.GetAssignmentsAsync(server.Address);
        Assert.Equal(4, shards.Count);
        Assert.Equal([0L, 1L, 2L, 3L], shards.Select(s => s.Shard).Order().ToArray());
        Assert.All(shards, s => Assert.Equal(server.Address, s.Leader));
        Assert.Equal(G.KeySorting.Natural, sorting);

        await using var other = await oxia.StartAsync(OxiaServerProfile.Default with
        {
            NotificationsEnabled = false,
            NotificationsRetention = TimeSpan.FromSeconds(1),
        });
        Assert.Single((await RawOxia.GetAssignmentsAsync(other.Address)).Shards);
        Assert.NotEqual(server.Address, other.Address);
    }

    [Fact]
    public async Task Fixture_NewServerIsEmpty_EvenWhenTheSlotIsReused()
    {
        string firstAddress;
        await using (var first = await oxia.StartAsync())
        {
            firstAddress = first.Address;
            // the first record of a fresh shard gets version id 0 (Go: TestAsyncClientImpl asserts it)
            Assert.Equal(0, await RawOxia.PutAsync(first.Address, 0, "/a", [1]));
            Assert.Equal(1, await RawOxia.PutAsync(first.Address, 0, "/b", [2]));
            Assert.Equal([1], (await RawOxia.GetAsync(first.Address, 0, "/a"))!);
        }

        // closing is synchronous: the server is gone
        Assert.False((await OxiaReadiness.ProbeOnceAsync(firstAddress, 1, callTimeout: TimeSpan.FromSeconds(1))).Ready);

        await using var second = await oxia.StartAsync();
        Assert.Null(await RawOxia.GetAsync(second.Address, 0, "/a"));
        Assert.Equal(0, await RawOxia.PutAsync(second.Address, 0, "/a", [3]));
    }

    [Fact]
    public async Task Fixture_ServersAreIsolatedAndCanRunConcurrently()
    {
        var servers = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => oxia.StartAsync()));
        try
        {
            Assert.Equal(4, servers.Select(s => s.Address).Distinct().Count());
            var versions = await Task.WhenAll(servers.Select((s, i) => RawOxia.PutAsync(s.Address, 0, "/k", [(byte)i])));
            Assert.All(versions, v => Assert.Equal(0, v));
            for (var i = 0; i < servers.Length; i++)
            {
                Assert.Equal([(byte)i], (await RawOxia.GetAsync(servers[i].Address, 0, "/k"))!);
            }
        }
        finally
        {
            foreach (var s in servers) await s.CloseAsync();
        }
    }

    [Fact]
    public async Task Fixture_ReportsServerLog()
    {
        await using var server = await oxia.StartAsync();
        var log = await server.GetLogAsync();
        Assert.Contains("Started Grpc server", log);
    }

    /// <summary>
    /// The Go tests pick their keys so that they land on different shards of a standalone server (they `require` it themselves,
    /// computing the shard with xxh3 + GenerateShards).  This checks those preconditions with the .NET hash, so a mismatch of the hash
    /// (component B) shows up here instead of as a puzzling failure in the key sorting tests.
    /// </summary>
    [Fact]
    public void Sharding_PreconditionsOfTheGoTestsHold()
    {
        // TestSyncClientImpl_FloorCeilingGetKeySorting: the two keys of every case are on different shards (4 shards)
        string[][] pairs =
        [
            ["a/x", "b"], ["b/x", "c"], ["a/x", "ab/y"], ["b/x", "a/y/z"],
        ];
        foreach (var pair in pairs)
        {
            Assert.NotEqual(Sharding.ShardOf(pair[0], 4), Sharding.ShardOf(pair[1], 4));
        }

        // TestSyncClientImpl_RangeScanUseIndexKeySorting: the eight records are spread over all 4 shards
        Assert.Equal(4, Enumerable.Range(1, 8).Select(i => Sharding.ShardOf("p" + i, 4)).Distinct().Count());

        // TestSyncClientImplListAndRangeScanEndInclusive: the records of "/t/k-1" are on several shards
        Assert.True(new[] { "p-0", "p-3", "p-4" }.Select(k => Sharding.ShardOf(k, 4)).Distinct().Count() > 1);

        // TestSyncClientImpl_GetSequenceUpdates_MultipleShards: partition key "x" does not route to shard 0 of 10
        Assert.True(Oxia.Common.Hash.Hasher.Xxh332("x") > Sharding.GenerateShards(0, 10)[0].Max);
    }

    [Fact]
    public void Sharding_MatchesGoArithmetic()
    {
        // Go: sharding.GenerateShards(0, 1) wraps bucketSize to 0 and still covers the whole range
        Assert.Equal([new ShardRange(0, 0, uint.MaxValue)], Sharding.GenerateShards(0, 1));
        var four = Sharding.GenerateShards(0, 4);
        Assert.Equal(new ShardRange(0, 0, 1073741823), four[0]);
        Assert.Equal(new ShardRange(3, 3221225472, uint.MaxValue), four[3]);
    }
}
