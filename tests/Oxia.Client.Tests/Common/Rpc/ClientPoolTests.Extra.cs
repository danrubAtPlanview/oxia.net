// =====================================================================================================================
// PORT NOTE
//   Upstream : (none) -- extra tests for common/rpc/client_pool.go + connection.go: lazily created shared connections, channel settings
//              (keepalive, message size, dial options last), per-RPC credentials, the `tls://` prefix, custom resolver schemes through a
//              dial option, large messages, failure callback and re-dial.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Net.Security;
using System.Security.Authentication;
using Grpc.Core;
using Grpc.Health.V1;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;
using static Oxia.Client.Tests.Common.RpcTestSupport;

namespace Oxia.Client.Tests.Common;

public class ClientPoolExtraTests
{
    private sealed class EchoService : G.OxiaClient.OxiaClientBase
    {
        public readonly List<Metadata> Headers = [];
        public readonly List<string> Hosts = [];
        public int ReadSize = 0;

        public override Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
        {
            lock (Headers)
            {
                Headers.Add(context.RequestHeaders);
                Hosts.Add(context.Host);
            }

            return Task.FromResult(new G.KeepAliveResponse());
        }

        public override async Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context)
        {
            var response = new G.ReadResponse();
            response.Gets.Add(new G.GetResponse { Value = Google.Protobuf.ByteString.CopyFrom(new byte[ReadSize]) });
            await responseStream.WriteAsync(response);
        }
    }

    private static async Task<(RunningServer Server, EchoService Service)> StartEcho()
    {
        var service = new EchoService();
        var server = new RunningServer(await TestGrpcServer.StartAsync(app => app.MapGrpcService<EchoService>(), s => s.AddSingleton(service)));
        return (server, service);
    }

    [Fact]
    public async Task ConcurrentGetsOfTheSameTargetShareOneConnection()
    {
        await using var pool = new ClientPool(null, null);

        var rpcs = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(() => pool.GetHealthRpc("localhost:1"))));

        Assert.Equal(1, pool.ConnectionCount);
        Assert.All(rpcs, rpc => Assert.Same(rpcs[0], rpc));
    }

    [Fact]
    public async Task DifferentTargetsGetDifferentConnections_AndTheFacadesAreCachedPerConnection()
    {
        await using var pool = new ClientPool(null, null);

        Assert.Same(pool.GetAdminRpc("localhost:1"), pool.GetAdminRpc("localhost:1"));
        Assert.Same(pool.GetHealthRpc("localhost:1"), pool.GetHealthRpc("localhost:1"));
        Assert.NotSame(pool.GetHealthRpc("localhost:1"), pool.GetHealthRpc("localhost:2"));
        Assert.Equal(2, pool.ConnectionCount);
    }

    // needs OxiaClientRpc (component A) to be constructible
    [Fact]
    public async Task TheClientRpcFacadeIsCachedPerConnection()
    {
        await using var pool = new ClientPool(null, null);

        Assert.Same(pool.GetClientRpc("localhost:1"), pool.GetClientRpc("localhost:1"));
        Assert.NotSame(pool.GetClientRpc("localhost:1"), pool.GetClientRpc("localhost:2"));
        Assert.Same(pool.GetConnectionFromPool("localhost:1").ClientRpc, pool.GetClientRpc("localhost:1"));
    }

    [Fact]
    public async Task TheChannelIsConfiguredLikeTheGoDialOptions()
    {
        GrpcChannelOptions? captured = null;
        await using var pool = new ClientPool(null, null, options => captured = options);

        pool.GetConnectionFromPool("localhost:1");

        Assert.NotNull(captured);
        Assert.Equal(Constants.MaxGrpcFrameSize, captured.MaxReceiveMessageSize);
        var handler = Assert.IsType<SocketsHttpHandler>(captured.HttpHandler);
        Assert.Equal(TimeSpan.FromSeconds(10), handler.KeepAlivePingDelay);
        Assert.Equal(TimeSpan.FromSeconds(3), handler.KeepAlivePingTimeout);
        Assert.Equal(HttpKeepAlivePingPolicy.Always, handler.KeepAlivePingPolicy);
        Assert.True(handler.EnableMultipleHttp2Connections);
        Assert.Equal(Timeout.InfiniteTimeSpan, handler.PooledConnectionIdleTimeout);
        pool.TryGetConnection("localhost:1", out var connection);
        Assert.Equal("localhost:1", connection!.Channel.Target);
    }

    [Fact]
    public async Task DialOptionsAreAppliedLast_AndMayOverrideAnything()
    {
        GrpcChannelOptions? captured = null;
        await using var pool = new ClientPool(null, null,
            options => options.MaxReceiveMessageSize = 1234,
            options => captured = options);

        pool.GetConnectionFromPool("localhost:1");

        Assert.Equal(1234, captured!.MaxReceiveMessageSize);
    }

    private static SslClientAuthenticationOptions SslOf(GrpcChannelOptions options) => ((SocketsHttpHandler)options.HttpHandler!).SslOptions;

    [Fact]
    public async Task APlainTargetIsDialledWithoutTlsAndATlsPrefixOrConfigurationUsesIt()
    {
        GrpcChannelOptions? plain = null, prefixed = null, configured = null;
        await using var plainPool = new ClientPool(null, null, options => plain = options);
        await using var prefixedPool = new ClientPool(null, null, options => prefixed = options);
        var tlsConf = new SslClientAuthenticationOptions { TargetHost = "configured.example" };
        await using var configuredPool = new ClientPool(tlsConf, null, options => configured = options);

        plainPool.GetConnectionFromPool("host.example:6648");
        prefixedPool.GetConnectionFromPool("tls://host.example:6648");
        configuredPool.GetConnectionFromPool("host.example:6648");

        // no TLS: the handler's TLS options are untouched
        Assert.Equal(SslProtocols.None, SslOf(plain!).EnabledSslProtocols);
        // tls:// without configuration: TLS 1.2 minimum (Go: tls.Config{MinVersion: TLS12})
        Assert.Equal(SslProtocols.Tls12 | SslProtocols.Tls13, SslOf(prefixed!).EnabledSslProtocols);
        // a configuration is used as it is
        Assert.Same(tlsConf, SslOf(configured!));

        // the prefix is not part of the dialled name
        prefixedPool.TryGetConnection("tls://host.example:6648", out var connection);
        Assert.Equal("host.example:6648", connection!.Channel.Target);
    }

    [Fact]
    public async Task CredentialsThatRequireTransportSecurityAreAcceptedWithATlsPrefixOrConfiguration()
    {
        var auth = Authentication.NewTokenAuthenticationWithToken("token", true);

        await using var viaPrefix = new ClientPool(null, auth);
        viaPrefix.GetConnectionFromPool("tls://localhost:1");
        Assert.Equal(1, viaPrefix.ConnectionCount);

        await using var viaConf = new ClientPool(new SslClientAuthenticationOptions(), auth);
        viaConf.GetConnectionFromPool("localhost:1");
        Assert.Equal(1, viaConf.ConnectionCount);
    }

    [Fact]
    public async Task PerRpcCredentialsAreSentOnEveryCallOverPlaintextWhenTheyDoNotRequireTransportSecurity()
    {
        var (server, service) = await StartEcho();
        await using var _ = server;
        var n = 0;
        var auth = Authentication.NewTokenAuthenticationWithFunc(() => "tok-" + Interlocked.Increment(ref n), requireTransportSecurity: false);
        await using var pool = new ClientPool(null, auth);

        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Target).CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());
        await client.KeepAliveAsync(new G.SessionHeartbeat());

        lock (service.Headers)
        {
            Assert.Equal(2, service.Headers.Count);
            Assert.Equal("Bearer tok-1", service.Headers[0].GetValue("authorization"));
            Assert.Equal("Bearer tok-2", service.Headers[1].GetValue("authorization"));
        }
    }

    [Fact]
    public async Task NoCredentialsMeansNoAuthorizationHeader()
    {
        var (server, service) = await StartEcho();
        await using var _ = server;
        await using var pool = new ClientPool(null, null);

        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Target).CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());

        lock (service.Headers) Assert.Null(service.Headers.Single().Get("authorization"));
    }

    [Fact]
    public async Task ResponsesLargerThanTheDefault4MBAreReceived()
    {
        var (server, service) = await StartEcho();
        await using var _ = server;
        service.ReadSize = 6 * 1024 * 1024; // the Grpc.Net.Client default limit is 4 MiB; Go's client allows 256 MiB
        await using var pool = new ClientPool(null, null);

        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Target).CallInvoker);
        using var call = client.Read(new G.ReadRequest());
        Assert.True(await call.ResponseStream.MoveNext(CancellationToken.None));
        Assert.Equal(6 * 1024 * 1024, call.ResponseStream.Current.Gets[0].Value.Length);
    }

    [Fact]
    public async Task ACustomResolverSchemeRegisteredThroughADialOptionIsUsed()
    {
        var (server, service) = await StartEcho();
        await using var _s = server;
        var uri = new Uri("http://" + server.Target);
        var factory = new TestResolverFactory("custom", [new BalancerAddress(uri.Host, uri.Port)]);
        var services = new ServiceCollection().AddSingleton<ResolverFactory>(factory).BuildServiceProvider();
        await using var pool = new ClientPool(null, null, options => options.ServiceProvider = services);

        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool("custom:///oxia").CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());

        Assert.True(factory.Created > 0);

        // the connection goes to the resolved address, the HTTP/2 :authority is the endpoint of the target (grpc-go behaviour: the
        // servers check it, see AllowExtraAuthorities in tests/resolver)
        lock (service.Headers) Assert.Equal("oxia", service.Hosts.Single());
    }

    [Fact]
    public async Task TheAuthorityIsTheEndpointOfTheTargetWhateverTheResolverReturns()
    {
        var (server, service) = await StartEcho();
        await using var _s = server;
        var uri = new Uri("http://" + server.Target);
        var factory = new TestResolverFactory("custom", [new BalancerAddress(uri.Host, uri.Port)]);
        var services = new ServiceCollection().AddSingleton<ResolverFactory>(factory).BuildServiceProvider();
        await using var pool = new ClientPool(null, null, options => options.ServiceProvider = services);

        foreach (var target in new[] { "custom:///cluster-bootstrap:6648", "custom:///other-name", "custom://ignored-authority/path.to.endpoint:1" })
        {
            var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(target).CallInvoker);
            await client.KeepAliveAsync(new G.SessionHeartbeat());
        }

        lock (service.Headers) Assert.Equal(["cluster-bootstrap:6648", "other-name", "path.to.endpoint:1"], service.Hosts);
    }

    [Theory]
    [InlineData("custom:///oxia", "oxia")]
    [InlineData("dns:///host.example:6648", "host.example:6648")]
    [InlineData("custom:///a/b", "a/b")]
    [InlineData("custom://authority/endpoint:1", "endpoint:1")]
    [InlineData("custom://authority", "authority")]
    [InlineData("not a uri", "")]
    public void EndpointOfFollowsGrpcGoTargets(string address, string expected)
    {
        Assert.Equal(expected, Connection.EndpointOf(address));
    }

    [Fact]
    public async Task APlainTargetKeepsItsHostAsAuthority()
    {
        var (server, service) = await StartEcho();
        await using var _s = server;
        await using var pool = new ClientPool(null, null);

        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Target).CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());

        lock (service.Headers) Assert.Equal(server.Target, service.Hosts.Single());
    }

    [Fact]
    public async Task ACustomResolverSchemeWorksWithTlsConfigurationToo()
    {
        // only the channel credentials differ: creating the connection must not fail for a resolver scheme
        var factory = new TestResolverFactory("custom", []);
        var services = new ServiceCollection().AddSingleton<ResolverFactory>(factory).BuildServiceProvider();

        await using var plain = new ClientPool(null, Authentication.NewTokenAuthenticationWithToken("t", false), options => options.ServiceProvider = services);
        plain.GetConnectionFromPool("custom:///oxia");
        await using var secure = new ClientPool(new SslClientAuthenticationOptions(), Authentication.NewTokenAuthenticationWithToken("t", true), options => options.ServiceProvider = services);
        secure.GetConnectionFromPool("custom:///oxia");
        Assert.Equal(1, plain.ConnectionCount);
        Assert.Equal(1, secure.ConnectionCount);
    }

    [Fact]
    public async Task AFailedConnectionIsRemovedAndTheNextRequestDialsAgain()
    {
        var (server, _) = await NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus.NotServing);
        await using var _s = server;
        await using var pool = new ClientPool(null, null);

        // the default health cadence is 10s: drive a short one like the Go tests do
        var failing = NewTestConnectionWithHealthConfig(pool, server.Target, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
        pool.SetConnection(server.Target, failing);
        await Eventually(() => !pool.TryGetConnection(server.Target, out _), TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(10));
        Assert.True(failing.Disconnected);

        var next = pool.GetConnectionFromPool(server.Target);
        Assert.NotSame(failing, next);
    }

    [Fact]
    public async Task CloseClosesEveryConnection_AndDisposeNeverThrows()
    {
        var pool = new ClientPool(null, null);
        pool.GetConnectionFromPool("localhost:1");
        pool.GetConnectionFromPool("localhost:2");
        pool.TryGetConnection("localhost:1", out var first);

        await pool.CloseAsync();
        await pool.CloseAsync(); // closing twice is harmless
        await pool.DisposeAsync();

        // the health loop of a closed connection has stopped
        Assert.False(first!.Disconnected);
        await Assert.ThrowsAnyAsync<Exception>(async () => await first.HealthClient.CheckAsync(new HealthCheckRequest()));
    }

    [Fact]
    public async Task TheTargetsRemainReachableAfterTheServerRestartsOnTheSamePort()
    {
        var (server, _) = await StartEcho();
        var port = server.Server.Port;
        await using var pool = new ClientPool(null, null);
        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Target).CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());

        await server.DisposeAsync();
        var (restarted, _) = await StartEchoOn(port);
        await using var _r = restarted;

        await Eventually(async () =>
        {
            try
            {
                await client.KeepAliveAsync(new G.SessionHeartbeat(), deadline: DateTime.UtcNow.AddMilliseconds(500));
                return true;
            }
            catch (RpcException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(50));
    }

    private static async Task<(RunningServer Server, EchoService Service)> StartEchoOn(int port)
    {
        var service = new EchoService();
        var server = new RunningServer(await TestGrpcServer.StartAsync(app => app.MapGrpcService<EchoService>(), s => s.AddSingleton(service), port));
        return (server, service);
    }

    [Fact]
    public async Task TheResolverCanMoveTheTargetToAnotherServerLater()
    {
        var (first, firstService) = await StartEcho();
        await using var _first = first;
        var (second, secondService) = await StartEcho();
        await using var _second = second;
        var firstUri = new Uri("http://" + first.Target);
        var secondUri = new Uri("http://" + second.Target);
        var factory = new TestResolverFactory("dynamic", [new BalancerAddress(firstUri.Host, firstUri.Port)]);
        var services = new ServiceCollection().AddSingleton<ResolverFactory>(factory).BuildServiceProvider();
        await using var pool = new ClientPool(null, null, options => options.ServiceProvider = services);
        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool("dynamic:///cluster-bootstrap:6648").CallInvoker);

        await client.KeepAliveAsync(new G.SessionHeartbeat());
        lock (firstService.Headers) Assert.Single(firstService.Hosts);

        // the target "flips": the old server goes away and the resolver now returns the other one
        await first.DisposeAsync();
        factory.Update([new BalancerAddress(secondUri.Host, secondUri.Port)]);
        await Eventually(async () =>
        {
            try
            {
                await client.KeepAliveAsync(new G.SessionHeartbeat(), deadline: DateTime.UtcNow.AddMilliseconds(500));
                return true;
            }
            catch (RpcException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(50));

        lock (secondService.Headers) Assert.Equal("cluster-bootstrap:6648", secondService.Hosts.Last());
    }

    private sealed class TestResolverFactory(string scheme, IReadOnlyList<BalancerAddress> addresses) : ResolverFactory
    {
        private readonly List<StaticResolver> _resolvers = [];
        private IReadOnlyList<BalancerAddress> _addresses = addresses;

        public int Created;

        public void Update(IReadOnlyList<BalancerAddress> newAddresses)
        {
            _addresses = newAddresses;
            lock (_resolvers) foreach (var resolver in _resolvers) resolver.Push(newAddresses);
        }

        public override string Name => scheme;

        public override Resolver Create(ResolverOptions options)
        {
            Interlocked.Increment(ref Created);
            var resolver = new StaticResolver(_addresses);
            lock (_resolvers) _resolvers.Add(resolver);
            return resolver;
        }
    }

    private sealed class StaticResolver(IReadOnlyList<BalancerAddress> initial) : Resolver
    {
        private IReadOnlyList<BalancerAddress> addresses = initial;
        private Action<ResolverResult>? _listener;

        public void Push(IReadOnlyList<BalancerAddress> newAddresses)
        {
            addresses = newAddresses;
            _listener?.Invoke(ResolverResult.ForResult(newAddresses, serviceConfig: null, serviceConfigStatus: null));
        }

        public override void Start(Action<ResolverResult> listener)
        {
            _listener = listener;
            listener(ResolverResult.ForResult(addresses, serviceConfig: null, serviceConfigStatus: null));
        }

        public override void Refresh()
        {
            _listener?.Invoke(ResolverResult.ForResult(addresses, serviceConfig: null, serviceConfigStatus: null));
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
