// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/assignments/leader_hint_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : In a replicated cluster (3 data servers, 1 shard, replication factor 3) a read / list / range-scan request sent to a
//              FOLLOWER is rejected with an error carrying the leader's address (the "leader hint"), and a client that starts from a
//              follower (the DizzyShardManager failure injection makes it use the first server it can reach instead of the shard
//              leader) follows the hint and still succeeds.
//   Go tests: 6   ported: 6   skipped: 0 by default  (all need the opt-in cluster fixture: OXIA_TEST_CLUSTER=1, see README.md)
//   Divergences:
//     * Go starts three in-process `mock.NewServer` data servers and an in-process coordinator (memory metadata providers); here the
//       cluster is OxiaClusterFixture's coordinator + 3 `oxia server` processes in the cluster container, the follower to ask is
//       chosen from the leader the servers announce (`GetLeaderAsync`) instead of the coordinator's `StatusSnapshot`.
//     * `clientPool.GetClientRpc(target)` + `Read/List/RangeScan(...)`+`Recv()` are the same calls on IClientPool / IOxiaClientRpc.
//       NOTE: Go's pool returns the RAW gRPC client, so `constant.FromGrpcError(err)` on the error from `Recv()` yields the metadata
//       with the hint.  The .NET contract (RpcContracts.cs) says IOxiaClientRpc already translates the errors to Oxia sentinels;
//       the test only passes if the translated exception still lets OxiaErrors.FromGrpcError recover the ErrorInfo metadata.
//       If it does not, that is a cross-component finding (component A/B), not a reason to change this test.
//     * `assert.Equal(t, shard.Leader.Public, leader)` inside the polling loop is evaluated exactly like Go (it fails the test at once).
// =====================================================================================================================
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Assignments;

[Collection("oxia-cluster")]
public class LeaderHintTests(OxiaClusterFixture clusters)
{
    /// <summary>The cluster, the shard-0 leader and the follower the Go tests send their requests to (`target`).</summary>
    private async Task<(OxiaCluster Cluster, ClusterServer Leader, string Target)> StartCluster()
    {
        var cluster = await Helpers.NewDefaultCluster(clusters);
        try
        {
            // assert.Eventually(shard 0 is in steady state with a leader, 20 s, 100 ms): the fixture waited for the leader already
            var leader = await cluster.GetLeaderAsync();
            Assert.NotNull(leader);

            var target = cluster.Servers[0].Public;
            if (leader.Name == cluster.Servers[0].Name)
            {
                target = cluster.Servers[1].Public;
            }

            return (cluster, leader, target);
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// The polling loop shared by the "WithoutClient" tests: send the request to the follower until its error carries the leader hint,
    /// which must be the leader's public address.  The assignment dispatcher on the non-leader may not have received the shard
    /// assignments yet, so retry until the leader hint is present.
    /// </summary>
    private static async Task EventuallyLeaderHint<TResponse>(Func<IServerStreamingClient<TResponse>> open, string leaderPublic) where TResponse : class
    {
        await TestUtil.Eventually(async () =>
        {
            IServerStreamingClient<TResponse> stream;
            try
            {
                stream = open();
            }
            catch (Exception)
            {
                return false;
            }

            using (stream)
            {
                Exception err;
                try
                {
                    _ = await stream.RecvAsync(TestCtx);
                    return false; // a response (or EOF) is not the rejection we are waiting for
                }
                catch (Exception e)
                {
                    err = e;
                }

                var (_, metadata) = OxiaErrors.FromGrpcError(err);
                if (metadata is null || !metadata.TryGetLeaderHint(out _, out var leader))
                {
                    return false;
                }

                Assert.Equal(leaderPublic, leader);
                return true;
            }
        }, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(100));
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintWithoutClient()
    {
        var (cluster, leader, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var clientPool = new ClientPool(null, null);
        var clientRpc = clientPool.GetClientRpc(target);
        long shardID = 0;

        await EventuallyLeaderHint(() => clientRpc.Read(new ReadRequest { Shard = shardID }, TestCtx), leader.Public);
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintListWithoutClient()
    {
        var (cluster, leader, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var clientPool = new ClientPool(null, null);
        var clientRpc = clientPool.GetClientRpc(target);
        long shardID = 0;

        await EventuallyLeaderHint(() => clientRpc.List(new ListRequest { Shard = shardID }, TestCtx), leader.Public); //nolint:staticcheck
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintListWithClient()
    {
        var (cluster, _, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var client = await OxiaClient.CreateSyncClientAsync(target,
            OxiaOptions.WithNamespace("default"), OxiaOptions.WithFailureInjection([Failure.DizzyShardManager]));

        await client.PutAsync(TestCtx, "/key1", B("value"));

        await client.PutAsync(TestCtx, "/key2", B("value"));

        var keys = await client.ListAsync(TestCtx, "/key1", "/key3"); //nolint:staticcheck
        Assert.Contains("/key1", keys);
        Assert.Contains("/key2", keys);
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintRangeScanWithoutClient()
    {
        var (cluster, leader, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var clientPool = new ClientPool(null, null);
        var clientRpc = clientPool.GetClientRpc(target);
        long shardID = 0;

        await EventuallyLeaderHint(() => clientRpc.RangeScan(new RangeScanRequest { Shard = shardID }, TestCtx), leader.Public);
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintRangeScanWithClient()
    {
        var (cluster, _, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var client = await OxiaClient.CreateSyncClientAsync(target,
            OxiaOptions.WithNamespace("default"), OxiaOptions.WithFailureInjection([Failure.DizzyShardManager]));

        await client.PutAsync(TestCtx, "/key1", B("value1"));

        await client.PutAsync(TestCtx, "/key2", B("value2"));

        var results = new List<GetResult>(2);
        await foreach (var result in client.RangeScanAsync(TestCtx, "/key1", "/key3").ReadAllAsync(TestCtx))
        {
            results.Add(result);
        }

        Assert.Equal(2, results.Count);
        foreach (var r in results)
        {
            AssertNoError(r.Err);
        }
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestLeaderHintWithClient()
    {
        var (cluster, _, target) = await StartCluster();
        await using var clusterLifetime = cluster;

        await using var client = await OxiaClient.CreateSyncClientAsync(target,
            OxiaOptions.WithNamespace("default"), OxiaOptions.WithFailureInjection([Failure.DizzyShardManager]));

        await client.PutAsync(TestCtx, "/key1", B("value"));

        await client.PutAsync(TestCtx, "/key2", B("value"));

        var (key, value, _) = await client.GetAsync(TestCtx, "/key1");
        Assert.Equal("/key1", key);
        Assert.Equal(B("value"), value);

        (key, value, _) = await client.GetAsync(TestCtx, "/key2");
        Assert.Equal("/key2", key);
        Assert.Equal(B("value"), value);
    }
}
