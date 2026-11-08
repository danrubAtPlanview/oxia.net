// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/resolver/custom_resolver_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The pluggable service discovery (oxia.ServiceResolver / WithDialResolver): a client created with a custom-scheme URI
//              ("scheme:///host:port") asks the resolver for the real addresses, and follows address updates pushed by it.
//   Go tests: 2   ported: 2   skipped: 0
//   Divergences:
//     * `os.Getpid()` is Environment.ProcessId; `newStandaloneServer(t)` (+ t.Cleanup(Close)) is `oxia.StartAsync()` + await using.
//     * The resolvers implement IServiceResolver / AddressUpdater (Client contract), the updater returning an error value like Go.
//     * The standalone server returns, as shard leader, the :authority the client dialled; the URI form
//       "scheme:///<real address>" therefore needs the client to send `<real address>` as authority while connecting to the
//       resolved address -- the behaviour these tests verify.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Resolver;

/// <summary>
/// staticResolver is a simple ServiceResolver that always resolves to a pre-configured address. It demonstrates how users can plug
/// in custom service discovery (e.g. Consul, Kubernetes, DNS SRV) using the Oxia ServiceResolver interface -- no gRPC imports
/// required.
/// </summary>
internal sealed class StaticResolver(string scheme, string target) : IServiceResolver
{
    public string Scheme { get; } = scheme;

    public void Resolve(string endpoint, AddressUpdater updater) => _ = updater([target]);
}

/// <summary>
/// dynamicResolver demonstrates a resolver that can update its target address after creation. This simulates service-discovery
/// systems where the set of backends changes over time (e.g. rolling deploys, failovers). It tracks updaters per endpoint so each
/// connection gets its own updates.
/// </summary>
internal sealed class DynamicResolver(string scheme, IReadOnlyList<string> targets) : IServiceResolver
{
    private readonly object _mu = new();
    private IReadOnlyList<string> _targets = targets;
    private readonly Dictionary<string, AddressUpdater> _updaters = new();

    public string Scheme { get; } = scheme;

    public void Resolve(string endpoint, AddressUpdater updater)
    {
        lock (_mu)
        {
            _updaters[endpoint] = updater;
            _ = updater(_targets);
        }
    }

    /// <summary>UpdateTargets pushes a new set of addresses to all connections.</summary>
    public void UpdateTargets(IReadOnlyList<string> newTargets)
    {
        lock (_mu)
        {
            _targets = newTargets;
            foreach (var updater in _updaters.Values)
            {
                _ = updater(newTargets);
            }
        }
    }
}

public class CustomResolverTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestCustomResolver()
    {
        await using var server = await oxia.StartAsync();
        var actualAddr = server.ServiceAddr;

        // Create a custom resolver that maps the custom scheme to the actual
        // server address. In the URI form used below ("%s:///%s"), the path
        // component after the "///" encodes the real address so the standalone
        // server can return it as the shard leader.
        var sr = new StaticResolver($"oxia-test-{Environment.ProcessId}", actualAddr);

        // Use the custom resolver via WithDialResolver.
        await using var client = await OxiaClient.CreateSyncClientAsync(
            $"{sr.Scheme}:///{actualAddr}",
            OxiaOptions.WithDialResolver(sr),
            OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(10)));

        // Verify the client works end-to-end through the custom resolver.
        var ctx = CancellationToken.None;

        await client.PutAsync(ctx, "/resolver-test", B("hello"));

        var (key, value, _) = await client.GetAsync(ctx, "/resolver-test");
        Assert.Equal("/resolver-test", key);
        Assert.Equal(B("hello"), value);
    }

    [Fact]
    public async Task TestDynamicResolver()
    {
        // Start two servers to simulate a failover scenario.
        await using var server1 = await oxia.StartAsync();
        await using var server2 = await oxia.StartAsync();
        var addr1 = server1.ServiceAddr;
        var addr2 = server2.ServiceAddr;

        var dr = new DynamicResolver($"oxia-dyn-{Environment.ProcessId}", [addr1]);

        // Client initially connects to server1 via the dynamic resolver.
        await using var client = await OxiaClient.CreateSyncClientAsync(
            $"{dr.Scheme}:///{addr1}",
            OxiaOptions.WithDialResolver(dr),
            OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(10)));

        var ctx = CancellationToken.None;

        // Write through server1.
        await client.PutAsync(ctx, "/dyn-test", B("from-server1"));

        var (key, value, _) = await client.GetAsync(ctx, "/dyn-test");
        Assert.Equal("/dyn-test", key);
        Assert.Equal(B("from-server1"), value);

        // Simulate failover: update the resolver to point to server2.
        dr.UpdateTargets([addr2]);

        // Write through server2.
        await client.PutAsync(ctx, "/dyn-test-2", B("from-server2"));

        (key, value, _) = await client.GetAsync(ctx, "/dyn-test-2");
        Assert.Equal("/dyn-test-2", key);
        Assert.Equal(B("from-server2"), value);
    }
}
