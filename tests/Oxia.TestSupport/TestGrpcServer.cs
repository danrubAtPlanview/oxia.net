// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Go equivalent: the tests spin up `grpc.NewServer()` + `net.Listen("tcp", "localhost:0")` and register a mock
//   `proto.OxiaClientServer` (e.g. testOxiaClientServer in oxia/internal/rpc_provider_test.go).  This is the .NET analogue:
//   a Kestrel host listening for cleartext HTTP/2 (h2c) on an ephemeral loopback port that hosts any number of gRPC service
//   classes.  Mock services derive from the protoc-generated bases in namespace Io.Oxia.Proto.V1
//   (OxiaClient.OxiaClientBase, OxiaAdmin.OxiaAdminBase) -- the generated Google.Protobuf types double as the independent
//   wire-format oracle for the hand-written codecs.
// =====================================================================================================================
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Oxia.TestSupport;

public sealed class TestGrpcServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private TestGrpcServer(WebApplication app, string address)
    {
        _app = app;
        Address = address;
    }

    /// <summary>host:port of the listening socket (no scheme), usable as an Oxia service address.</summary>
    public string Address { get; }

    public int Port => int.Parse(Address[(Address.LastIndexOf(':') + 1)..]);

    /// <summary>
    /// Starts a server on 127.0.0.1:<paramref name="port"/> (0 = ephemeral).
    /// <paramref name="mapServices"/> registers the services, e.g. <c>app => app.MapGrpcService&lt;MyMock&gt;()</c>;
    /// <paramref name="configureServices"/> can register singletons the services take through their constructor.
    /// </summary>
    public static async Task<TestGrpcServer> StartAsync(Action<WebApplication> mapServices,
        Action<IServiceCollection>? configureServices = null, int port = 0)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k =>
            k.Listen(System.Net.IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc(o => o.MaxReceiveMessageSize = 256 * 1024 * 1024);
        configureServices?.Invoke(builder.Services);

        var app = builder.Build();
        mapServices(app);
        await app.StartAsync().ConfigureAwait(false);

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses;
        var uri = new Uri(addresses.First().Replace("[::]", "127.0.0.1"));
        return new TestGrpcServer(app, $"{uri.Host}:{uri.Port}");
    }

    /// <summary>Stops listening and aborts open streams (simulates a server going away).</summary>
    public Task StopAsync() => _app.StopAsync();

    public async ValueTask DisposeAsync()
    {
        try { await _app.StopAsync().ConfigureAwait(false); } catch { /* already stopped */ }
        await _app.DisposeAsync().ConfigureAwait(false);
    }
}
