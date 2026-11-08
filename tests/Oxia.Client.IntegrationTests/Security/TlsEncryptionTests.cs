// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/security/tls/tls_encryption_test.go (+ helpers_test.go, see SecurityHelpers.cs)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client's TLS options against a data server cluster that serves its public endpoint (and, in five of the six tests,
//              the internal and replication endpoints) over TLS with the `peer` certificate: no TLS configuration fails, a CA file that
//              does not contain the server's CA fails, `InsecureSkipVerify` succeeds without the CA, the right CA succeeds, and a
//              cluster with only the public endpoint secured works with a TLS client and fails without one.
//   Go tests: 6   ported: 6   skipped: 0
//   Divergences:
//     * Go starts three in-process `dataserver.New(...)` servers with `security.TLSOptions` in their `Options` and an in-memory
//       coordinator with the peer certificate as client TLS configuration; here they are `oxia server` / `oxia coordinator` processes
//       in the cluster container (SecurityHelpers.NewTlsCluster): `--tls-*` (public), `--internal-tls-*`, `--peer-tls-*` (replication
//       between the servers, and the coordinator's dial to them) with files copied into the container's tmpfs.  The certificates are
//       generated per run with the same SANs as tests/security/certs (DNS localhost, IP 127.0.0.1, IP 192.168.1.2).
//     * Addresses are `127.0.0.1:<published port>` (Go: `localhost:<port>`); both are in the certificate.
//     * `TestClusterHandshakeSuccess` only asserts in Go that the coordinator can be created over TLS-secured servers.  A coordinator
//       that cannot handshake with the servers never elects a leader, so the fixture's wait for the shard-0 leader (and the assertion
//       below that the leader is announced through TLS) is the observable equivalent.
//     * The client is the .NET one: `OxiaOptions.WithTls(TlsOptions.TryIntoClientTlsConf())`; a client that fails to be created throws
//       (Go returns `nil, err`): the tests assert the exception and that no client object came out.
// =====================================================================================================================
using Oxia.Client;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.IntegrationTests.Security;

public class TlsEncryptionTests(OxiaClusterFixture clusters, PkiFixture pki) : IClassFixture<PkiFixture>
{
    [Fact]
    public async Task TestClusterHandshakeSuccess()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);

        // the coordinator reached the three data servers over TLS: a leader was elected and every server announces it through TLS
        var leader = await cluster.GetLeaderAsync(ct: TestContext.Current.CancellationToken);
        Assert.NotNull(leader);
    }

    [Fact]
    public async Task TestClientHandshakeFailByNoTlsConfig()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var sa1 = cluster.Servers[0];

        ISyncClient? client = null;
        var err = await TestUtil.CatchAsync(async () =>
            client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(1))));
        Assert.NotNull(err);
        Assert.Null(client);
    }

    [Fact]
    public async Task TestClientHandshakeByAuthFail()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var sa1 = cluster.Servers[0];

        var tlsOption = pki.GetClientTlsOption();
        // clear the CA file
        tlsOption.TrustedCaFile = "";
        var tlsConf = tlsOption.TryIntoClientTlsConf();
        Assert.NotNull(tlsConf);
        ISyncClient? client = null;
        var err = await TestUtil.CatchAsync(async () =>
            client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithTls(tlsConf), OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(1))));
        Assert.NotNull(err);
        Assert.Null(client);
    }

    [Fact]
    public async Task TestClientHandshakeWithInsecure()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var sa1 = cluster.Servers[0];

        var tlsOption = pki.GetClientTlsOption();
        // clear the CA file
        tlsOption.TrustedCaFile = "";
        tlsOption.InsecureSkipVerify = true;
        var tlsConf = tlsOption.TryIntoClientTlsConf();
        Assert.NotNull(tlsConf);
        var client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithTls(tlsConf));
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestClientHandshakeSuccess()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var sa1 = cluster.Servers[0];

        var tlsOption = pki.GetClientTlsOption();
        var tlsConf = tlsOption.TryIntoClientTlsConf();
        Assert.NotNull(tlsConf);
        var client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithTls(tlsConf));
        await client.CloseAsync();
    }

    [Fact]
    public async Task TestOnlyEnablePublicTls()
    {
        // Go: config.Server.Internal.TLS.Enabled = false, config.Replication.TLS.Enabled = false; the coordinator has no TLS configuration
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki, onlyPublicTls: true);
        var sa1 = cluster.Servers[0];

        // failed without cert
        ISyncClient? client = null;
        var err = await TestUtil.CatchAsync(async () =>
            client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(1))));
        Assert.NotNull(err);
        Assert.Null(client);

        // success with cert
        var tlsOption = pki.GetClientTlsOption();
        var tlsConf = tlsOption.TryIntoClientTlsConf();
        Assert.NotNull(tlsConf);
        client = await OxiaClient.CreateSyncClientAsync(sa1.Public, OxiaOptions.WithTls(tlsConf));
        await client.CloseAsync();
    }
}
