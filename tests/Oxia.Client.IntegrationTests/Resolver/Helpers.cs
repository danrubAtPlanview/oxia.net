// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/resolver/helpers_test.go   (newCoordinatorInstance)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go builds a coordinator inside the test process: `mock.NewMetadataFromProviders(...)`, `coordruntime.New(...)`,
//              `coordreconciler.New(...)` over in-memory metadata providers and the in-process mock data servers.
//   Go tests: 0 (helper)
//   Divergences: The coordinator is the real `oxia coordinator` process started by OxiaClusterFixture in the opt-in cluster
//              container, together with its data servers (see OxiaClusterFixture); this helper keeps the Go name for the step
//              "create a coordinator for the given servers/namespace and wait until it is serving".  Cleanup that Go registers with
//              t.Cleanup is the cluster's DisposeAsync.
// =====================================================================================================================
using Oxia.TestSupport;

namespace Oxia.Client.IntegrationTests.Resolver;

internal static class Helpers
{
    /// <summary>Go: <c>newCoordinatorInstance(t, metadataProvider, configProvider, rpcProvider)</c> + the data servers + config.</summary>
    public static Task<OxiaCluster> NewCoordinatorInstance(OxiaClusterFixture clusters, string name, int serverCount, int replicationFactor,
        uint initialShardCount, string? extraAuthority = null) =>
        clusters.StartAsync(name, serverCount, replicationFactor, initialShardCount, extraAuthority);
}
