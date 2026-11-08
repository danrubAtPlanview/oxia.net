// =====================================================================================================================
// PORT NOTE   (extra tests -- not in Go)
//   Upstream : oxia/resolver.go + the integration test tests/resolver
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The ServiceResolver adapter beyond the single Go unit test: endpoint and address parsing, the updater
//              contract (errors, after dispose), composition with a service provider already on the channel options, and
//              an END-TO-END check that a real GrpcChannel with a custom scheme resolves through the dial option and
//              follows address updates (an in-process Kestrel gRPC health server).
// =====================================================================================================================
using Grpc.Core;
using Grpc.Health.V1;
using Grpc.HealthCheck;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.Tests;

public class ResolverExtraTests
{
    [Theory]
    [InlineData("test:///service", "service")]
    [InlineData("test:service", "service")]
    [InlineData("test://authority/service", "service")]
    [InlineData("test:///a/b", "a/b")]
    [InlineData("test:///", "")]
    [InlineData("test:///with%20space", "with space")]
    public void EndpointOf_StripsExactlyOneLeadingSlash(string address, string expected)
    {
        Assert.Equal(expected, GrpcResolverBuilder.EndpointOf(new Uri(address)));
    }

    [Theory]
    [InlineData("host-1:6648", "host-1", 6648)]
    [InlineData("10.0.0.1:80", "10.0.0.1", 80)]
    [InlineData("[::1]:6648", "::1", 6648)]
    [InlineData("[fe80::1]", "fe80::1", 443)]
    [InlineData("host-only", "host-only", 443)]
    [InlineData("::1", "::1", 443)]            // bare IPv6 literal: more than one colon, no port
    [InlineData("host:notaport", "host:notaport", 443)]
    public void ToBalancerAddress_ParsesHostAndPort(string address, string host, int port)
    {
        var a = GrpcResolverBuilder.ToBalancerAddress(address, 443);
        Assert.Equal(host, a.EndPoint.Host);
        Assert.Equal(port, a.EndPoint.Port);
    }

    [Fact]
    public void Updater_PushesEveryUpdate_AndReportsAnEmptyListAsAnError()
    {
        var results = new List<ResolverResult>();
        AddressUpdater? captured = null;
        var sr = new CapturingResolver("test", u => captured = u);
        var resolver = GrpcResolverBuilder.CreateFactory(sr).Create(ResolverTests.NewResolverOptions("test:///svc"));
        resolver.Start(results.Add);

        Assert.NotNull(captured);
        Assert.Null(captured(["a:1"]));
        Assert.Null(captured(["b:2", "c:3"]));
        Assert.Equal(2, results.Count);
        Assert.Single(results[0].Addresses!);
        Assert.Equal(2, results[1].Addresses!.Count);

        // an empty list is still pushed (the channel reports it) but is returned as an error so that the resolver retries
        Assert.NotNull(captured([]));
        Assert.Equal(3, results.Count);
        Assert.Empty(results[2].Addresses!);
    }

    [Fact]
    public void Updater_AfterDispose_PushesNothingAndReturnsAnError()
    {
        var results = new List<ResolverResult>();
        AddressUpdater? captured = null;
        var resolver = GrpcResolverBuilder.CreateFactory(new CapturingResolver("test", u => captured = u))
            .Create(ResolverTests.NewResolverOptions("test:///svc"));
        resolver.Start(results.Add);
        resolver.Dispose();

        Assert.NotNull(captured!(["a:1"]));
        Assert.Empty(results);
    }

    [Fact]
    public void Factory_NameIsTheSchemeOfTheServiceResolver()
    {
        Assert.Equal("oxia-test", GrpcResolverBuilder.CreateFactory(new CapturingResolver("oxia-test", _ => { })).Name);
    }

    [Fact]
    public void AsDialOption_ComposesWithAServiceProviderAlreadyOnTheOptions()
    {
        var other = GrpcResolverBuilder.CreateFactory(new CapturingResolver("other", _ => { }));
        var options = new GrpcChannelOptions { ServiceProvider = new FactoryProvider(other) };
        var sr = new CapturingResolver("mine", _ => { });

        GrpcResolverBuilder.AsDialOption(sr)(options);

        var factories = ((IEnumerable<ResolverFactory>)options.ServiceProvider!.GetService(typeof(IEnumerable<ResolverFactory>))!).ToList();
        Assert.Equal(new[] { "mine", "other" }, factories.Select(f => f.Name).ToArray());
        Assert.Null(options.ServiceProvider.GetService(typeof(string)));   // unrelated services are delegated to the inner provider
        Assert.Throws<ArgumentNullException>(() => GrpcResolverBuilder.AsDialOption(null!));
    }

    [Fact]
    public async Task EndToEnd_AGrpcChannelWithACustomScheme_ResolvesThroughTheDialOption_AndFollowsAddressUpdates()
    {
        await using var server = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<HealthServiceImpl>(),
            services => services.AddSingleton(NewHealthService()));

        // First resolution points at a dead port; the real address arrives later through the updater
        var sr = new DelayedResolver("oxia-test", firstAddresses: ["127.0.0.1:1"], laterAddresses: [server.Address], delay: TimeSpan.FromMilliseconds(300));
        var options = new GrpcChannelOptions { Credentials = ChannelCredentials.Insecure };
        GrpcResolverBuilder.AsDialOption(sr)(options);

        using var channel = GrpcChannel.ForAddress("oxia-test:///my-service", options);
        var client = new Health.HealthClient(channel);

        var response = await client.CheckAsync(new HealthCheckRequest(),
            new CallOptions(deadline: DateTime.UtcNow.AddSeconds(20)).WithWaitForReady());

        Assert.Equal(HealthCheckResponse.Types.ServingStatus.Serving, response.Status);
        Assert.Equal(new[] { "my-service" }, sr.Endpoints.ToArray());
        await sr.Later;
    }

    [Fact]
    public async Task EndToEnd_AStaticResolution_Works()
    {
        await using var server = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<HealthServiceImpl>(),
            services => services.AddSingleton(NewHealthService()));

        var sr = new DelayedResolver("oxia-test", firstAddresses: [server.Address], laterAddresses: null, delay: TimeSpan.Zero);
        var options = new GrpcChannelOptions { Credentials = ChannelCredentials.Insecure };
        GrpcResolverBuilder.AsDialOption(sr)(options);

        using var channel = GrpcChannel.ForAddress("oxia-test:///svc", options);
        var response = await new Health.HealthClient(channel).CheckAsync(new HealthCheckRequest(),
            new CallOptions(deadline: DateTime.UtcNow.AddSeconds(20)).WithWaitForReady());
        Assert.Equal(HealthCheckResponse.Types.ServingStatus.Serving, response.Status);
    }

    private static HealthServiceImpl NewHealthService()
    {
        var health = new HealthServiceImpl();
        health.SetStatus("", HealthCheckResponse.Types.ServingStatus.Serving);   // the overall server status, asked by an empty service name
        return health;
    }

    private sealed class CapturingResolver : IServiceResolver
    {
        private readonly Action<AddressUpdater> _onResolve;

        public CapturingResolver(string scheme, Action<AddressUpdater> onResolve)
        {
            Scheme = scheme;
            _onResolve = onResolve;
        }

        public string Scheme { get; }

        public void Resolve(string endpoint, AddressUpdater updater) => _onResolve(updater);
    }

    private sealed class DelayedResolver : IServiceResolver
    {
        private readonly string[] _first;
        private readonly string[]? _later;
        private readonly TimeSpan _delay;

        public DelayedResolver(string scheme, string[] firstAddresses, string[]? laterAddresses, TimeSpan delay)
        {
            Scheme = scheme;
            _first = firstAddresses;
            _later = laterAddresses;
            _delay = delay;
            Later = Task.CompletedTask;
        }

        public List<string> Endpoints { get; } = new();

        public Task Later { get; private set; }

        public string Scheme { get; }

        public void Resolve(string endpoint, AddressUpdater updater)
        {
            lock (Endpoints) Endpoints.Add(endpoint);
            _ = updater(_first);
            if (_later is not null)
            {
                Later = Task.Run(async () =>
                {
                    await Task.Delay(_delay);
                    _ = updater(_later);
                });
            }
        }
    }

    private sealed class FactoryProvider : IServiceProvider
    {
        private readonly ResolverFactory _factory;

        public FactoryProvider(ResolverFactory factory) => _factory = factory;

        public object? GetService(Type serviceType) =>
            serviceType == typeof(IEnumerable<ResolverFactory>) ? new[] { _factory } : null;
    }
}
