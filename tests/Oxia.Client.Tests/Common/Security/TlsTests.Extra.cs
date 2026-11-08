// =====================================================================================================================
// PORT NOTE
//   Upstream : (none) -- extra tests for common/security/tls.go: the options really produce working client / server TLS configurations
//              (real TLS handshakes over loopback sockets and a TLS gRPC server through ClientPool).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Health.V1;
using Grpc.HealthCheck;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oxia.Common.Rpc;
using Oxia.Common.Security;
using Xunit;
using static Oxia.Client.Tests.Common.TlsTests;

namespace Oxia.Client.Tests.Common;

public class TlsExtraTests
{
    private sealed record HandshakeResult(Exception? ClientError, Exception? ServerError, SslProtocols Protocol, string? ServerThumbprint)
    {
        public bool Succeeded => ClientError is null && ServerError is null;
    }

    private static async Task<HandshakeResult> HandshakeAsync(SslClientAuthenticationOptions client, SslServerAuthenticationOptions server)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var serverTask = Task.Run(async () =>
        {
            try
            {
                using var socket = await listener.AcceptTcpClientAsync();
                using var ssl = new SslStream(socket.GetStream());
                await ssl.AuthenticateAsServerAsync(server);
                var buffer = new byte[1];
                await ssl.ReadExactlyAsync(buffer);
                await ssl.WriteAsync(buffer);
                return (Exception?)null;
            }
            catch (Exception e)
            {
                return e;
            }
        });

        Exception? clientError = null;
        var protocol = SslProtocols.None;
        string? thumbprint = null;
        try
        {
            client.TargetHost ??= "localhost";
            using var tcp = new TcpClient();
            await tcp.ConnectAsync(IPAddress.Loopback, port);
            using var ssl = new SslStream(tcp.GetStream());
            await ssl.AuthenticateAsClientAsync(client);
            protocol = ssl.SslProtocol;
            thumbprint = ssl.RemoteCertificate is null ? null : new X509Certificate2(ssl.RemoteCertificate).Thumbprint;

            // TLS 1.3: a rejected client certificate only shows on the first read
            await ssl.WriteAsync(new byte[] { 1 });
            await ssl.ReadExactlyAsync(new byte[1]);
        }
        catch (Exception e)
        {
            clientError = e;
        }

        var serverError = await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
        return new HandshakeResult(clientError, serverError, protocol, thumbprint);
    }

    private sealed class Pki : IDisposable
    {
        public TempDir Dir { get; } = new();
        public string ServerCert = "", ServerKey = "", ClientCert = "", ClientKey = "", OtherCert = "", OtherKey = "";

        public Pki(string serverDns = "localhost", bool eku = true)
        {
            var (serverCert, serverKey) = GenerateSelfSignedCert("server", eku, serverDns);
            var (clientCert, clientKey) = GenerateSelfSignedCert("client", eku);
            var (otherCert, otherKey) = GenerateSelfSignedCert("other", eku, serverDns);
            ServerCert = Dir.Write("server.pem", serverCert);
            ServerKey = Dir.Write("server-key.pem", serverKey);
            ClientCert = Dir.Write("client.pem", clientCert);
            ClientKey = Dir.Write("client-key.pem", clientKey);
            OtherCert = Dir.Write("other.pem", otherCert);
            OtherKey = Dir.Write("other-key.pem", otherKey);
        }

        public void Dispose() => Dir.Dispose();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AServerCertificateTrustedThroughTheCaFileIsAccepted(bool extendedKeyUsages)
    {
        using var pki = new Pki(eku: extendedKeyUsages);
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf();
        var client = new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert }.TryIntoClientTlsConf();

        var result = await HandshakeAsync(client!, server!);

        Assert.True(result.Succeeded, result.ClientError?.ToString() ?? result.ServerError?.ToString());
        Assert.True((result.Protocol & (SslProtocols.Tls12 | SslProtocols.Tls13)) != 0);
    }

    [Fact]
    public async Task WithoutTheCaFileAnUnknownSelfSignedServerIsRejected()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf();
        var client = new TlsOptions { Enabled = true }.TryIntoClientTlsConf(); // system roots

        var result = await HandshakeAsync(client!, server!);

        Assert.IsAssignableFrom<AuthenticationException>(result.ClientError);
    }

    [Fact]
    public async Task ACaFileThatDoesNotContainTheServerCertificateRejectsIt()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf();
        var client = new TlsOptions { Enabled = true, TrustedCaFile = pki.OtherCert }.TryIntoClientTlsConf();

        var result = await HandshakeAsync(client!, server!);

        Assert.IsAssignableFrom<AuthenticationException>(result.ClientError);
    }

    [Fact]
    public async Task InsecureSkipVerifyAcceptsAnyServerCertificate()
    {
        using var pki = new Pki(serverDns: "somewhere.else");
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf();
        var client = new TlsOptions { Enabled = true, InsecureSkipVerify = true }.TryIntoClientTlsConf();

        var result = await HandshakeAsync(client!, server!);

        Assert.True(result.Succeeded, result.ClientError?.ToString() ?? result.ServerError?.ToString());
    }

    [Fact]
    public async Task TheServerNameIsTheHostNameThatIsVerified()
    {
        using var pki = new Pki(serverDns: "oxia.test");
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf()!;

        // the dialled host name is "localhost": the certificate is for oxia.test
        var mismatch = await HandshakeAsync(new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert }.TryIntoClientTlsConf()!, server);
        Assert.IsAssignableFrom<AuthenticationException>(mismatch.ClientError);

        // ... unless the configured server name says otherwise
        var named = await HandshakeAsync(
            new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert, ServerName = "oxia.test" }.TryIntoClientTlsConf()!, server);
        Assert.True(named.Succeeded, named.ClientError?.ToString() ?? named.ServerError?.ToString());
    }

    [Fact]
    public async Task ClientAuthRequiresACertificateTheCaTrusts()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey, TrustedCaFile = pki.ClientCert, ClientAuth = true }
            .TryIntoServerTlsConf()!;
        var trustServer = new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert };

        // trusted client certificate
        var ok = await HandshakeAsync(
            new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert, CertFile = pki.ClientCert, KeyFile = pki.ClientKey }.TryIntoClientTlsConf()!, server);
        Assert.True(ok.Succeeded, ok.ClientError?.ToString() ?? ok.ServerError?.ToString());

        // no client certificate
        var none = await HandshakeAsync(trustServer.TryIntoClientTlsConf()!, server);
        Assert.False(none.Succeeded);

        // a certificate the CA does not trust
        var untrusted = await HandshakeAsync(
            new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert, CertFile = pki.OtherCert, KeyFile = pki.OtherKey }.TryIntoClientTlsConf()!, server);
        Assert.False(untrusted.Succeeded);
    }

    [Fact]
    public async Task WithACaFileButNoClientAuthTheClientCertificateIsOptionalButVerifiedWhenGiven()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey, TrustedCaFile = pki.ClientCert }.TryIntoServerTlsConf()!;
        TlsOptions Client() => new() { Enabled = true, TrustedCaFile = pki.ServerCert };

        var anonymous = await HandshakeAsync(Client().TryIntoClientTlsConf()!, server);
        Assert.True(anonymous.Succeeded, anonymous.ClientError?.ToString() ?? anonymous.ServerError?.ToString());

        var trusted = Client();
        trusted.CertFile = pki.ClientCert;
        trusted.KeyFile = pki.ClientKey;
        var withCert = await HandshakeAsync(trusted.TryIntoClientTlsConf()!, server);
        Assert.True(withCert.Succeeded, withCert.ClientError?.ToString() ?? withCert.ServerError?.ToString());

        var untrusted = Client();
        untrusted.CertFile = pki.OtherCert;
        untrusted.KeyFile = pki.OtherKey;
        Assert.False((await HandshakeAsync(untrusted.TryIntoClientTlsConf()!, server)).Succeeded);
    }

    [Fact]
    public async Task TheServerWithoutCaAndWithoutClientAuthIgnoresClientCertificates()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf()!;
        Assert.False(server.ClientCertificateRequired);

        var client = new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert, CertFile = pki.ClientCert, KeyFile = pki.ClientKey }.TryIntoClientTlsConf()!;
        var result = await HandshakeAsync(client, server);
        Assert.True(result.Succeeded, result.ClientError?.ToString() ?? result.ServerError?.ToString());
    }

    [Fact]
    public async Task TheVersionRangeIsNegotiated()
    {
        using var pki = new Pki();
        TlsOptions Server(ushort min, ushort max) => new() { CertFile = pki.ServerCert, KeyFile = pki.ServerKey, MinVersion = min, MaxVersion = max };
        TlsOptions Client(ushort min, ushort max) => new() { Enabled = true, TrustedCaFile = pki.ServerCert, MinVersion = min, MaxVersion = max };

        var tls12 = await HandshakeAsync(Client(0, TlsOptions.VersionTls12).TryIntoClientTlsConf()!, Server(0, 0).TryIntoServerTlsConf()!);
        Assert.True(tls12.Succeeded, tls12.ClientError?.ToString() ?? tls12.ServerError?.ToString());
        Assert.Equal(SslProtocols.Tls12, tls12.Protocol);

        var tls13 = await HandshakeAsync(Client(TlsOptions.VersionTls13, 0).TryIntoClientTlsConf()!, Server(0, 0).TryIntoServerTlsConf()!);
        Assert.True(tls13.Succeeded, tls13.ClientError?.ToString() ?? tls13.ServerError?.ToString());
        Assert.Equal(SslProtocols.Tls13, tls13.Protocol);

        var serverOnly13 = await HandshakeAsync(Client(0, TlsOptions.VersionTls12).TryIntoClientTlsConf()!, Server(TlsOptions.VersionTls13, 0).TryIntoServerTlsConf()!);
        Assert.False(serverOnly13.Succeeded);
    }

    [Fact]
    public void TheProtocolsFollowTheConfiguredRange()
    {
        Assert.Equal(SslProtocols.Tls12 | SslProtocols.Tls13, new TlsOptions().MakeCommonConfig().Protocols);
        Assert.Equal(SslProtocols.Tls12, new TlsOptions { MaxVersion = TlsOptions.VersionTls12 }.MakeCommonConfig().Protocols);
        Assert.Equal(SslProtocols.Tls13, new TlsOptions { MinVersion = TlsOptions.VersionTls13 }.MakeCommonConfig().Protocols);
        Assert.Equal(SslProtocols.Tls12 | SslProtocols.Tls13,
            new TlsOptions { MinVersion = TlsOptions.VersionTls12, MaxVersion = TlsOptions.VersionTls13 }.MakeCommonConfig().Protocols);
        Assert.Contains("above TLS 1.3", Assert.Throws<Oxia.Common.Constant.OxiaException>(() => new TlsOptions { MinVersion = 0x0305 }.MakeCommonConfig()).Message);
    }

    [Fact]
    public async Task TheServerCertificateIsReReadFromDiskAtEveryHandshake()
    {
        using var pki = new Pki();
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf()!;
        var client = new TlsOptions { Enabled = true, InsecureSkipVerify = true }.TryIntoClientTlsConf()!;

        var first = await HandshakeAsync(client, server);
        File.Copy(pki.OtherCert, pki.ServerCert, overwrite: true);
        File.Copy(pki.OtherKey, pki.ServerKey, overwrite: true);
        var second = await HandshakeAsync(client, server);

        Assert.True(first.Succeeded && second.Succeeded);
        Assert.NotEqual(first.ServerThumbprint, second.ServerThumbprint);
    }

    [Fact]
    public async Task TheClientCertificateIsReReadFromDiskAtEveryHandshake()
    {
        using var pki = new Pki();
        // the server trusts both client certificates
        var bundle = pki.Dir.Write("clients.pem", File.ReadAllText(pki.ClientCert) + File.ReadAllText(pki.OtherCert));
        var server = new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey, TrustedCaFile = bundle, ClientAuth = true }.TryIntoServerTlsConf()!;
        var client = new TlsOptions { Enabled = true, InsecureSkipVerify = true, CertFile = pki.ClientCert, KeyFile = pki.ClientKey }.TryIntoClientTlsConf()!;

        Assert.True((await HandshakeAsync(client, server)).Succeeded);
        File.Copy(pki.OtherCert, pki.ClientCert, overwrite: true);
        File.Copy(pki.OtherKey, pki.ClientKey, overwrite: true);
        Assert.True((await HandshakeAsync(client, server)).Succeeded);
    }

    [Fact]
    public void ABrokenKeyPairIsReportedWhenTheConfigurationIsBuilt()
    {
        using var pki = new Pki();
        File.WriteAllText(pki.ServerKey, "not a key");

        Assert.ThrowsAny<Exception>(() => new TlsOptions { CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoServerTlsConf());
        Assert.ThrowsAny<Exception>(() => new TlsOptions { Enabled = true, CertFile = pki.ServerCert, KeyFile = pki.ServerKey }.TryIntoClientTlsConf());
    }

    [Fact]
    public void EnabledFollowsTheGoRules()
    {
        Assert.False(new TlsOptions().IsEnabled());
        Assert.True(new TlsOptions { CertFile = "x" }.IsEnabled());
        Assert.False(new TlsOptions { Enabled = false, CertFile = "x" }.IsEnabled());
        Assert.True(new TlsOptions { Enabled = true }.IsEnabled());

        Assert.Null(new TlsOptions().TryIntoClientTlsConf());
        Assert.Null(new TlsOptions { Enabled = false, CertFile = "x" }.TryIntoClientTlsConf());
        Assert.Null(new TlsOptions().TryIntoServerTlsConf());

        var options = new TlsOptions();
        options.WithDefault();
        options.Validate();
    }

    [Fact]
    public void TheClientConfigurationCarriesTheServerNameAndNoCertificateByDefault()
    {
        var conf = new TlsOptions { Enabled = true, ServerName = "oxia.example" }.TryIntoClientTlsConf()!;
        Assert.Equal("oxia.example", conf.TargetHost);
        Assert.Null(conf.LocalCertificateSelectionCallback);
        // SocketsHttpHandler validates the host name against the request host, not TargetHost: a configured server name needs our callback
        Assert.NotNull(conf.RemoteCertificateValidationCallback);
        var defaults = new TlsOptions { Enabled = true }.TryIntoClientTlsConf()!;
        Assert.Null(defaults.TargetHost);
        Assert.Null(defaults.RemoteCertificateValidationCallback); // system roots, standard validation
    }

    [Fact]
    public void CipherSuitesAreAppliedWhereTheRuntimeSupportsThem()
    {
        var options = new TlsOptions { Enabled = true, CipherSuites = [(ushort)TlsCipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256] };
        if (OperatingSystem.IsWindows())
        {
            // fail loudly instead of silently ignoring a security setting
            Assert.Throws<PlatformNotSupportedException>(() => options.TryIntoClientTlsConf());
            return;
        }

        Assert.NotNull(options.TryIntoClientTlsConf()!.CipherSuitesPolicy);
    }

    // ----- ClientPool over TLS ---------------------------------------------------------------------------------------------
    private sealed class TlsHealthServer : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private TlsHealthServer(WebApplication app, int port)
        {
            _app = app;
            Port = port;
        }

        public int Port { get; }

        public static async Task<TlsHealthServer> StartAsync(string certFile, string keyFile)
        {
            var serverCert = new TlsOptions { CertFile = certFile, KeyFile = keyFile }.LoadKeyPair();
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(k => k.Listen(IPAddress.Loopback, 0, o =>
            {
                o.Protocols = HttpProtocols.Http2;
                o.UseHttps(new HttpsConnectionAdapterOptions { ServerCertificate = serverCert });
            }));
            builder.Services.AddGrpc();
            var health = new HealthServiceImpl();
            health.SetStatus(string.Empty, HealthCheckResponse.Types.ServingStatus.Serving);
            builder.Services.AddSingleton(health);
            var app = builder.Build();
            app.MapGrpcService<HealthServiceImpl>();
            await app.StartAsync();
            var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses;
            return new TlsHealthServer(app, new Uri(addresses.First()).Port);
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }

    [Fact]
    public async Task ThePoolReachesATlsServerThroughTheTlsPrefixAndATrustedCa()
    {
        using var pki = new Pki();
        await using var server = await TlsHealthServer.StartAsync(pki.ServerCert, pki.ServerKey);
        var tls = new TlsOptions { Enabled = true, TrustedCaFile = pki.ServerCert, ServerName = "localhost" }.TryIntoClientTlsConf();
        await using var pool = new ClientPool(tls, null);

        var response = await pool.GetHealthRpc($"tls://127.0.0.1:{server.Port}").CheckAsync(new HealthCheckRequest(), deadline: DateTime.UtcNow.AddSeconds(10));

        Assert.Equal(HealthCheckResponse.Types.ServingStatus.Serving, response.Status);
    }

    [Fact]
    public async Task ThePoolDoesNotReachATlsServerItDoesNotTrust()
    {
        using var pki = new Pki();
        await using var server = await TlsHealthServer.StartAsync(pki.ServerCert, pki.ServerKey);
        var tls = new TlsOptions { Enabled = true, TrustedCaFile = pki.OtherCert, ServerName = "localhost" }.TryIntoClientTlsConf();
        await using var pool = new ClientPool(tls, null);

        var error = await Assert.ThrowsAsync<RpcException>(async () =>
            await pool.GetHealthRpc($"127.0.0.1:{server.Port}").CheckAsync(new HealthCheckRequest(), deadline: DateTime.UtcNow.AddSeconds(10)));

        // the handshake failure surfaces as an Internal / Unavailable status carrying the TLS error
        Assert.Contains(error.StatusCode, new[] { StatusCode.Internal, StatusCode.Unavailable });
        Assert.Contains("SSL connection could not be established", error.Status.Detail);
    }

    [Fact]
    public async Task ThePoolReachesATlsServerWithInsecureSkipVerifyAndTokenCredentialsOverTls()
    {
        using var pki = new Pki(serverDns: "not.localhost");
        await using var server = await TlsHealthServer.StartAsync(pki.ServerCert, pki.ServerKey);
        var tls = new TlsOptions { Enabled = true, InsecureSkipVerify = true }.TryIntoClientTlsConf();
        var auth = Oxia.Common.Auth.Authentication.NewTokenAuthenticationWithToken("secret", requireTransportSecurity: true);
        await using var pool = new ClientPool(tls, auth);

        var response = await pool.GetHealthRpc($"127.0.0.1:{server.Port}").CheckAsync(new HealthCheckRequest(), deadline: DateTime.UtcNow.AddSeconds(10));

        Assert.Equal(HealthCheckResponse.Types.ServingStatus.Serving, response.Status);
    }
}
