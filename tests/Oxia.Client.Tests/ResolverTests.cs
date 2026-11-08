// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/resolver_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0
//   Adaptations: grpc-go's `builder.Build(target, clientConn, opts)` registers the ServiceResolver at channel creation
//                (and the test's fake ClientConn records UpdateState); the Grpc.Net.Client `ResolverFactory.Create` +
//                `Resolver.Start(listener)` pair is the equivalent (Start is where ServiceResolver.Resolve runs), and the
//                listener records the ResolverResults.  `resolver.Address{Addr: "host-1:6648"}` is the BalancerAddress
//                (host, port) of that "host:port".  `r.ResolveNow` is `Refresh`, `r.Close` is `Dispose`.
// =====================================================================================================================
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Oxia.Client.Tests;

public class ResolverTests
{
    [Fact]
    public void TestGrpcResolverResolveNowLeavesUpdatesToServiceResolver()
    {
        var serviceResolver = new TestServiceResolver(["host-1:6648", "host-2:6648"]);
        var states = new List<ResolverResult>();
        var builder = GrpcResolverBuilder.CreateFactory(serviceResolver);

        var r = builder.Create(NewResolverOptions("test:///service"));
        r.Start(states.Add);
        r.Refresh();
        r.Dispose();

        Assert.Equal(["service"], serviceResolver.Endpoints);
        var state = Assert.Single(states);
        Assert.Equal(new[] { ("host-1", 6648), ("host-2", 6648) },
            state.Addresses!.Select(a => (a.EndPoint.Host, a.EndPoint.Port)).ToArray());
    }

    /// <summary>ResolverOptions' constructor is internal in Grpc.Net.Client: the channel builds it, tests go through reflection.</summary>
    internal static ResolverOptions NewResolverOptions(string address) =>
        (ResolverOptions)Activator.CreateInstance(typeof(ResolverOptions),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public,
            null, [new Uri(address), 443, NullLoggerFactory.Instance, new GrpcChannelOptions()], null)!;

    // testServiceResolver records the endpoints it resolves, and resolves each one to addresses.
    private sealed class TestServiceResolver : IServiceResolver
    {
        private readonly string[] _addresses;

        public TestServiceResolver(string[] addresses) => _addresses = addresses;

        public List<string> Endpoints { get; } = new();

        public string Scheme => "test";

        public void Resolve(string endpoint, AddressUpdater updater)
        {
            Endpoints.Add(endpoint);
            _ = updater(_addresses);
        }
    }
}
