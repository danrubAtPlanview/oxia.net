// =====================================================================================================================
// PORT NOTE
//   Upstream : none -- extra tests of the opt-in cluster fixture (coordinator + data servers in the cluster container).  Raw gRPC only.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Prove what tests/assignments (leader hints) and tests/resolver (target IP flip) rely on: a 3-server cluster elects a
//              leader whose announced address is the published host address, every server announces the same leader, a read sent to
//              a follower is rejected (the client library must learn the leader from the error's hint), and servers can be killed.
//              Skipped unless OXIA_TEST_CLUSTER=1.
//   Divergences: n/a.  Linux validation: a failed assertion of the leader test now appends the logs of the coordinator and the servers
//              (diagnostics only; the assertions are unchanged) -- it failed once in a native-Linux run (a follower not announcing the leader
//              within 20 s) and the plain message did not say why.
// =====================================================================================================================
using Grpc.Core;
using Grpc.Net.Client;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.IntegrationTests.Infrastructure;

/// <summary>Skip condition of the cluster based tests (<c>[Fact(Skip = "...", SkipUnless = nameof(Enabled), SkipType = typeof(ClusterGate))]</c>).</summary>
public static class ClusterGate
{
    public const string Reason = "needs a coordinator + data servers: opt in with OXIA_TEST_CLUSTER=1 (Docker)";
    public static bool Enabled => OxiaClusterFixture.Enabled;
}

[Collection("oxia-cluster")]
public class OxiaClusterFixtureTests(OxiaClusterFixture clusters)
{
    [Fact(Skip = ClusterGate.Reason, SkipUnless = nameof(ClusterGate.Enabled), SkipType = typeof(ClusterGate))]
    public async Task Cluster_ElectsLeader_FollowersRejectReadsWithLeaderHint()
    {
        await using var cluster = await clusters.StartAsync("itest", serverCount: 3, replicationFactor: 3, initialShardCount: 1);

        try
        {
            var leader = await cluster.GetLeaderAsync();
            Assert.NotNull(leader);
            Assert.Contains(leader, cluster.Servers);

            // every server announces the same leader (the published address of the leader), not the address it was asked on
            foreach (var server in cluster.Servers)
            {
                await TestUtil.Eventually(async () =>
                {
                    try
                    {
                        var (shards, _) = await RawOxia.GetAssignmentsAsync(server.Public);
                        return shards.Count == 1 && shards[0].Leader == leader.Public;
                    }
                    catch (RpcException e) when (e.StatusCode == StatusCode.Unavailable)
                    {
                        return false; // "server not initialized yet": a server that has not received its assignments is simply not ready, keep polling
                    }
                }, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(100), $"{server.Name} (expected leader '{leader.Public}')");
            }

            // a read sent to a follower fails, with the leader address in the status details (the "leader hint")
            var follower = cluster.BootstrapTargetExcluding(leader);
            using var channel = GrpcChannel.ForAddress("http://" + follower.Public);
            var client = new G.OxiaClient.OxiaClientClient(channel);
            var request = new G.ReadRequest { Shard = 0 };
            request.Gets.Add(new G.GetRequest { Key = "/k", IncludeValue = true });

            RpcException? rejected = null;
            await TestUtil.Eventually(async () =>
            {
                try
                {
                    using var call = client.Read(request);
                    await call.ResponseStream.MoveNext(CancellationToken.None);
                    return false;
                }
                catch (RpcException e)
                {
                    rejected = e;
                    return e.Trailers.Any(t => t.Key == "grpc-status-details-bin" && t.ValueBytes is { } b &&
                                               System.Text.Encoding.UTF8.GetString(b).Contains(leader.Public, StringComparison.Ordinal));
                }
            }, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(100), "the follower never answered with the leader hint");
            Assert.NotNull(rejected);
        }
        catch (Xunit.Sdk.XunitException e)
        {
            // diagnostics only (Linux validation): a failed wait is much easier to judge with the logs of the coordinator and the servers
            throw new Xunit.Sdk.XunitException(e.Message + Environment.NewLine + await cluster.GetLogsAsync(), e);
        }
    }

    [Fact(Skip = ClusterGate.Reason, SkipUnless = nameof(ClusterGate.Enabled), SkipType = typeof(ClusterGate))]
    public async Task Cluster_StopAllServers_And_TwoClustersAtOnce()
    {
        await using var first = await clusters.StartAsync("first", serverCount: 2, replicationFactor: 1, initialShardCount: 3, extraAuthority: "first-bootstrap:6648");
        await using var second = await clusters.StartAsync("second", serverCount: 2, replicationFactor: 1, initialShardCount: 3, extraAuthority: "second-bootstrap:6648");

        Assert.Empty(first.Servers.Select(s => s.Public).Intersect(second.Servers.Select(s => s.Public)));
        foreach (var cluster in new[] { first, second })
        {
            var (shards, _) = await RawOxia.GetAssignmentsAsync(cluster.Servers[0].Public);
            Assert.Equal(3, shards.Count);
            Assert.All(shards, s => Assert.Contains(cluster.Servers, srv => srv.Public == s.Leader));
        }

        await second.StopAllServersAsync();
        Assert.False((await OxiaReadiness.ProbeOnceAsync(second.Servers[0].Public, 3, callTimeout: TimeSpan.FromSeconds(1))).Ready);

        // the other cluster is unaffected
        Assert.True((await OxiaReadiness.ProbeOnceAsync(first.Servers[0].Public, 3)).Ready);
    }
}
