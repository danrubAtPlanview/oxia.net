// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/security/tls/helpers_test.go + the helpers inside tests/security/tls/tls_encryption_test.go
//              (getPeerTLSOption, getClientTLSOption, newTLSServer, newTLSServerWithInterceptor, newDefaultClusterConfig),
//              tests/security/auth/helpers_test.go and tests/security/auth/auth_oidc_test.go (newOxiaClusterWithAuth, generateRandomStr)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go builds, in every test, three in-process data servers whose `dataserveroption.Options` carry the TLS files / the OIDC
//              provider parameters, an in-memory coordinator over `newDefaultClusterConfig(servers...)` (namespace "default",
//              replication factor 3, one shard) and returns the first server's public address.  Here the same cluster is the
//              OxiaClusterFixture's coordinator + three `oxia server` processes in the (reused) cluster container, configured through
//              an OxiaClusterProfile (extra flags + files copied into the slot's tmpfs).
//   Go tests: 0 (helpers)
//   Divergences:
//     * `oxia standalone` has no TLS / auth flags, so a cluster of three `oxia server` processes is used even where only a client
//       is exercised (as Go does); the servers' public address is the published host address `127.0.0.1:<port>` (certificate SAN
//       127.0.0.1), not `localhost:<port>`.
//     * Go points TLSOptions at the committed `tests/security/certs`; the certificates are generated per run (TestPki) and written
//       to a temporary directory (client side) and to the container's tmpfs (server side); both are deleted afterwards.
//     * `dataServerOption.Server.Internal.TLS.Enabled = &FlagFalse` (TestOnlyEnablePublicTls) is expressed by not passing the
//       `--internal-tls-*` / `--peer-tls-*` flags: TLS is enabled exactly when a certificate file is configured (TLSOptions.IsEnabled).
//     * The fixture checks readiness through its own raw gRPC calls (RawSecurity: the CA is trusted / a valid bearer token is sent).
// =====================================================================================================================
using System.Text.Json;
using Oxia.TestSupport;
using Oxia.TestSupport.Docker;
using Oxia.TestSupport.Security;
using Xunit;

namespace Oxia.Client.IntegrationTests.Security;

/// <summary>The generated certificates and their client-side directory; shared by the tests of one class (xUnit class fixture).</summary>
public sealed class PkiFixture : IDisposable
{
    public PkiFixture()
    {
        Pki = TestPki.Create();
        CertDirectory = Pki.WriteTo(Path.Combine(Path.GetTempPath(), "oxia-net-security-" + Guid.NewGuid().ToString("N")));
    }

    public TestPki Pki { get; }

    /// <summary>Go: <c>filepath.Join(filepath.Dir(pwd), "certs")</c> -- ca.crt, peer.crt/key, client.crt/key.</summary>
    public string CertDirectory { get; }

    /// <summary>Go: <c>getPeerTLSOption()</c>.</summary>
    public Oxia.Common.Security.TlsOptions GetPeerTlsOption() => TestPki.PeerTlsOptions(CertDirectory);

    /// <summary>Go: <c>getClientTLSOption()</c>.</summary>
    public Oxia.Common.Security.TlsOptions GetClientTlsOption() => TestPki.ClientTlsOptions(CertDirectory);

    public void Dispose()
    {
        Pki.Dispose();
        try { Directory.Delete(CertDirectory, recursive: true); } catch (IOException) { /* best effort: it only holds throw-away test certificates */ }
    }
}

internal static class SecurityHelpers
{
    /// <summary>Go: <c>newDefaultClusterConfig</c> (replication factor 3, one shard) + three servers + coordinator.</summary>
    private const int Servers = 3;

    private const int ReplicationFactor = 3;
    private const uint InitialShardCount = 1;

    /// <summary>
    /// Go: three <c>newTLSServer(t)</c> (or <c>newTLSServerWithInterceptor</c> with <paramref name="onlyPublicTls"/>: internal and replication
    /// TLS disabled) and a coordinator whose RPC provider uses the peer certificate as client configuration (<c>nil</c> when only the
    /// public endpoint is secured).
    /// </summary>
    public static async Task<OxiaCluster> NewTlsCluster(OxiaClusterFixture clusters, PkiFixture pki, bool onlyPublicTls = false,
        IReadOnlyList<string>? extraServerArgs = null, bool probeWithClientCertificate = false, string? oidcProviderParams = null,
        string? oidcReadinessToken = null)
    {
        var dir = OxiaClusterProfile.DirToken;
        string[] publicTls =
        [
            "--tls-cert-file", $"{dir}/{TestPki.PeerCertFileName}", "--tls-key-file", $"{dir}/{TestPki.PeerKeyFileName}",
            "--tls-trusted-ca-file", $"{dir}/{TestPki.CaFileName}",
        ];
        string[] internalTls =
        [
            "--internal-tls-cert-file", $"{dir}/{TestPki.PeerCertFileName}", "--internal-tls-key-file", $"{dir}/{TestPki.PeerKeyFileName}",
            "--internal-tls-trusted-ca-file", $"{dir}/{TestPki.CaFileName}",
        ];
        string[] replicationTls =
        [
            "--peer-tls-cert-file", $"{dir}/{TestPki.PeerCertFileName}", "--peer-tls-key-file", $"{dir}/{TestPki.PeerKeyFileName}",
            "--peer-tls-trusted-ca-file", $"{dir}/{TestPki.CaFileName}",
        ];

        var profile = new OxiaClusterProfile
        {
            Files = pki.Pki.PeerFiles(),
            // the Go option struct `*option` (peer certificate + CA) is used for Server.Public, Server.Internal and Replication
            ServerArgs =
            [
                .. onlyPublicTls ? publicTls : [.. publicTls, .. internalTls, .. replicationTls],
                .. extraServerArgs ?? [],
                .. oidcProviderParams is null ? (string[])[] : ["--auth-provider-name", "oidc", "--auth-provider-params", oidcProviderParams],
            ],
            // rpc2.NewControllerRPCProviderFactory(tlsConf): the coordinator dials the data servers' internal endpoint with it
            CoordinatorArgs = onlyPublicTls ? [] : replicationTls,
            Probe = new RawSecurity
            {
                Tls = pki.Pki.RawClientSslOptions(probeWithClientCertificate),
                Token = oidcReadinessToken is null ? null : () => oidcReadinessToken,
            },
        };
        return await StartCluster(clusters, "tls", profile);
    }

    /// <summary>
    /// Go: <c>newOxiaClusterWithAuth</c> / the per-test copies of it: three servers whose public endpoint authenticates with the OIDC
    /// provider configured by <paramref name="providerParamsJson"/> (<c>auth.Options{Provider: ProviderOIDC, ProviderParams}</c>), plus the
    /// coordinator.  <paramref name="readinessToken"/> is any token the servers accept: the fixture needs one to ask for the shard assignments.
    /// </summary>
    public static async Task<OxiaCluster> NewOidcCluster(OxiaClusterFixture clusters, string providerParamsJson, string readinessToken,
        IReadOnlyDictionary<string, string>? files = null)
    {
        var profile = new OxiaClusterProfile
        {
            Files = files ?? new Dictionary<string, string>(),
            ServerArgs = ["--auth-provider-name", "oidc", "--auth-provider-params", providerParamsJson],
            Probe = new RawSecurity { Token = () => readinessToken },
        };
        return await StartCluster(clusters, "oidc", profile);
    }

    private static async Task<OxiaCluster> StartCluster(OxiaClusterFixture clusters, string name, OxiaClusterProfile profile)
    {
        // the first run in a fresh container pulls nothing (the image is already there) but may create the container: allow for it
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(5));
        return await clusters.StartAsync(name, Servers, ReplicationFactor, InitialShardCount, ct: cts.Token, profile: profile);
    }

    /// <summary>
    /// How the data servers (in the container) reach the OIDC provider hosted by this process; skips the test when there is no Docker
    /// fixture (OXIA_TEST_ADDRESS, or no Docker with OXIA_TEST_SKIP_IF_NO_SERVER=1).
    /// </summary>
    public static async Task<string> HostAddressOfTestProcess(OxiaClusterFixture clusters)
    {
        if (IntegrationEnv.ExternalAddress is not null)
        {
            Assert.Skip($"the security tests start their own servers in Docker (unset {IntegrationEnv.AddressVar})");
        }

        try
        {
            return await clusters.GetHostAddressAsync(TestContext.Current.CancellationToken);
        }
        catch (DockerException e) when (IntegrationEnv.SkipIfNoServer)
        {
            Assert.Skip($"Docker not available ({e.Message})");
            throw;
        }
    }

    /// <summary>Go: <c>generateRandomStr(t)</c> (a random UUID).</summary>
    public static string GenerateRandomStr() => Guid.NewGuid().ToString();

    /// <summary>Go: <c>json.Marshal(auth.OIDCOptions{AllowedIssueURLs, AllowedAudiences})</c> (the legacy format; empty fields are omitted).</summary>
    public static string LegacyOidcParams(string allowedIssueUrls, string allowedAudiences) =>
        JsonSerializer.Serialize(new Dictionary<string, object> { ["allowedIssueURLs"] = allowedIssueUrls, ["allowedAudiences"] = allowedAudiences });

    /// <summary>Go: <c>auth.IssuerConfig</c>.</summary>
    public sealed record IssuerConfig(string AllowedAudiences, string UserNameClaim, string? StaticKeyFile = null);

    /// <summary>Go: <c>json.Marshal(auth.OIDCOptions{Issuers: ...})</c> (the per-issuer format).</summary>
    public static string PerIssuerOidcParams(IReadOnlyDictionary<string, IssuerConfig> issuers)
    {
        var map = new Dictionary<string, object>();
        foreach (var (issuer, config) in issuers)
        {
            var entry = new Dictionary<string, object>
            {
                ["allowedAudiences"] = config.AllowedAudiences,
                ["userNameClaim"] = config.UserNameClaim,
            };
            if (config.StaticKeyFile is not null) entry["staticKeyFile"] = config.StaticKeyFile;
            map[issuer] = entry;
        }

        return JsonSerializer.Serialize(new Dictionary<string, object> { ["issuers"] = map });
    }
}
