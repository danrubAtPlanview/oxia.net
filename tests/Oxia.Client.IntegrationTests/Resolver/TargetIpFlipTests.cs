// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/resolver/target_ip_flip_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Two coordinator-managed clusters (2 data servers each, 3 shards, replication factor 1); each cluster accepts, besides its
//              servers' own addresses, one extra authority ("clusterN-bootstrap:6648").  A client that dials the authority of cluster 2
//              through a resolver must be rejected when the resolver (the "DNS") points -- before the first connection, or after it, at
//              the moment the cluster-2 servers are gone -- at cluster 1: the cluster-1 servers refuse the unknown authority instead of
//              serving the wrong data.
//   Go tests: 2   ported: 2   skipped: 0 by default  (both need the opt-in cluster fixture: OXIA_TEST_CLUSTER=1, see README.md)
//   Divergences:
//     * Go builds the clusters in-process (mock.NewServer + coordinator runtime); here OxiaClusterFixture runs `oxia coordinator` and
//       `oxia server` processes in a reusable container and publishes the servers' public addresses on the host.
//     * `cluster.leader()` (coordinator metadata) is `cluster.GetLeaderAsync()` (leader announced in the shard assignments).
//     * `assert.ErrorContains(err, text)` checks the text against the whole exception chain (Go's err.Error() contains the wrapped
//       causes' text).
// =====================================================================================================================
using System.Text;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Resolver;

[Collection("oxia-cluster")]
public class TargetIpFlipTests(OxiaClusterFixture clusters)
{
    private static readonly TimeSpan IntegrationRequestTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RejectionRequestTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Go: <c>newCoordinatorCluster(t, prefix, serverCount)</c>.</summary>
    private async Task<OxiaCluster> NewCoordinatorCluster(string prefix, int serverCount)
    {
        // authority: fmt.Sprintf("%s-bootstrap:6648", prefix); ReplicationFactor 1; InitialShardCount 3; AllowExtraAuthorities [authority]
        return await Helpers.NewCoordinatorInstance(clusters, prefix, serverCount, replicationFactor: 1, initialShardCount: 3,
            extraAuthority: $"{prefix}-bootstrap:6648");
    }

    /// <summary>Go: <c>getValue(t, client, key)</c>.</summary>
    private static async Task<(byte[]? Value, Exception? Err)> GetValue(ISyncClient client, string key)
    {
        try
        {
            var (_, value, _) = await client.GetAsync(CancellationToken.None, key);
            return (value, null);
        }
        catch (Exception e)
        {
            return (null, e);
        }
    }

    /// <summary>Go: <c>newClusterClient(t, serviceAddress, opts...)</c> (appends WithRequestTimeout(30s)).</summary>
    private static async Task<ISyncClient> NewClusterClient(string serviceAddress, params ClientOption[] opts)
    {
        ClientOption[] all = [.. opts, OxiaOptions.WithRequestTimeout(IntegrationRequestTimeout)];
        return await OxiaClient.CreateSyncClientAsync(serviceAddress, all);
    }

    /// <summary>Go: <c>waitForClusterReady(t, cluster, name)</c>.</summary>
    private static async Task WaitForClusterReady(OxiaCluster cluster, string name)
    {
        var bootstrapTarget = cluster.BootstrapTargetExcluding(await cluster.GetLeaderAsync()).Public;
        await TestUtil.Eventually(async () =>
        {
            ISyncClient client;
            try
            {
                client = await OxiaClient.CreateSyncClientAsync(bootstrapTarget, OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(2)));
            }
            catch (Exception)
            {
                return false;
            }

            await using (client)
            {
                var key = $"/resolver/{name}/ready/{(DateTime.UtcNow - DateTime.UnixEpoch).Ticks * 100}";
                try
                {
                    await client.PutAsync(CancellationToken.None, key, B(name));
                }
                catch (Exception)
                {
                    return false;
                }

                var (value, err) = await GetValue(client, key);
                return err is null && Encoding.UTF8.GetString(value!) == name;
            }
        }, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestTargetIPFlipBeforeBootstrapRejectsWrongCluster()
    {
        await using var cluster1 = await NewCoordinatorCluster("cluster1", 2);

        await using var cluster2 = await NewCoordinatorCluster("cluster2", 2);

        await WaitForClusterReady(cluster1, "cluster1");
        await WaitForClusterReady(cluster2, "cluster2");

        var resolver = new DynamicResolver($"oxia-bootstrap-{Environment.ProcessId}-before",
            [cluster1.BootstrapTargetExcluding(await cluster1.GetLeaderAsync()).Public]);

        var err = await TestUtil.CatchAsync(async () => await OxiaClient.CreateSyncClientAsync(
            $"{resolver.Scheme}:///{cluster2.Authority}",
            OxiaOptions.WithDialResolver(resolver),
            OxiaOptions.WithRequestTimeout(RejectionRequestTimeout)));
        Assert.NotNull(err);
        Assert.Contains("failed to retrieve the initial list of shard assignments", ErrorText(err));
    }

    [Fact(Skip = Infrastructure.ClusterGate.Reason, SkipUnless = nameof(Infrastructure.ClusterGate.Enabled), SkipType = typeof(Infrastructure.ClusterGate))]
    public async Task TestTargetIPFlipAfterBootstrapRejectsWrongCluster()
    {
        await using var cluster1 = await NewCoordinatorCluster("cluster1", 2);

        await using var cluster2 = await NewCoordinatorCluster("cluster2", 2);

        await WaitForClusterReady(cluster1, "cluster1");
        await WaitForClusterReady(cluster2, "cluster2");

        var cluster2Leader = await cluster2.GetLeaderAsync();
        var cluster2Bootstrap = cluster2.BootstrapTargetExcluding(cluster2Leader);

        var resolver = new DynamicResolver($"oxia-bootstrap-{Environment.ProcessId}-after", [cluster2Bootstrap.Public]);

        await using var client = await NewClusterClient($"{resolver.Scheme}:///{cluster2.Authority}", OxiaOptions.WithDialResolver(resolver));

        await using var cluster1Client = await NewClusterClient(cluster1.Servers[0].Public);

        await using var cluster2Client = await NewClusterClient(cluster2.Servers[0].Public);

        var initialKey = "/resolver/before-flip";
        await client.PutAsync(CancellationToken.None, initialKey, B("cluster-2"));

        var (value, err) = await GetValue(cluster2Client, initialKey);
        AssertNoError(err);
        Assert.Equal(B("cluster-2"), value);

        (_, err) = await GetValue(cluster1Client, initialKey);
        AssertErrorIs(err, ClientErrors.KeyNotFound);

        resolver.UpdateTargets([cluster1.BootstrapTargetExcluding(await cluster1.GetLeaderAsync()).Public]);
        await cluster2.StopAllServersAsync();

        await TestUtil.Eventually(async () =>
        {
            var rejectedKey = $"/resolver/rejected-after-flip-{(DateTime.UtcNow - DateTime.UnixEpoch).Ticks * 100}";
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

            Exception? putErr = await TestUtil.CatchAsync(async () => await client.PutAsync(cts.Token, rejectedKey, B(cluster1.Name)));
            if (putErr is null)
            {
                var (got, getErr) = await GetValue(cluster1Client, rejectedKey);
                if (getErr is null && Encoding.UTF8.GetString(got!) == cluster1.Name)
                {
                    return false;
                }
            }

            return putErr is not null;
        }, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(100));
    }

    /// <summary>Go: <c>err.Error()</c> -- the messages of the whole chain.</summary>
    private static string ErrorText(Exception e)
    {
        var sb = new StringBuilder();
        for (Exception? cur = e; cur is not null; cur = cur.InnerException)
        {
            sb.Append(cur.Message).Append(": ");
            if (cur is AggregateException agg)
            {
                foreach (var inner in agg.InnerExceptions) sb.Append(ErrorText(inner)).Append("; ");
            }
        }

        return sb.ToString();
    }
}
