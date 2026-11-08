// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/assignments/helpers_test.go   (newCoordinatorInstance, newDefaultClusterConfig)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go builds an in-process coordinator over in-memory metadata providers and describes the cluster with
//              `newDefaultClusterConfig(servers...)` = namespace "default", ReplicationFactor 3, InitialShardCount 1.
//   Go tests: 0 (helpers)
//   Divergences: The coordinator and the three data servers are real `oxia coordinator` / `oxia server` processes started by
//              OxiaClusterFixture (opt-in, OXIA_TEST_CLUSTER=1); `NewDefaultCluster` starts them with the same cluster configuration
//              and waits for shard 0 to have a leader (the Go tests do this wait themselves with assert.Eventually on the
//              coordinator's shard status).
// =====================================================================================================================
using Oxia.TestSupport;

namespace Oxia.Client.IntegrationTests.Assignments;

internal static class Helpers
{
    /// <summary>
    /// Go: <c>newDefaultClusterConfig(sa1, sa2, sa3)</c> + <c>newCoordinatorInstance(...)</c> + the Eventually that waits for shard 0 to be
    /// in steady state with a leader.
    /// </summary>
    public static Task<OxiaCluster> NewDefaultCluster(OxiaClusterFixture clusters, string name = "lh") =>
        clusters.StartAsync(name, serverCount: 3, replicationFactor: 3, initialShardCount: 1);
}
