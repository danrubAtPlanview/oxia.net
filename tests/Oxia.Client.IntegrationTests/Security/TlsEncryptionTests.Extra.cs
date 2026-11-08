// =====================================================================================================================
// PORT NOTE
//   Upstream : none (extra tests; the ported ones are in TlsEncryptionTests.cs).  Same cluster as tests/security/tls/tls_encryption_test.go.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : What the Go tests do not look at: the Go tests only create a client (`NewSyncClient(...)`) and close it.  These put and get
//              through the TLS connections (shard leaders are reached with the same TLS configuration), the `tls://` address scheme,
//              the server name check (`TlsOptions.ServerName`) and mutual TLS (`--tls-client-auth`: the client certificate of
//              `TlsOptions.CertFile/KeyFile` is really presented, and a client without one is refused).
//   Divergences: n/a.
// =====================================================================================================================
using Oxia.Client;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.IntegrationTests.Security;

public class TlsEncryptionExtraTests(OxiaClusterFixture clusters, PkiFixture pki) : IClassFixture<PkiFixture>
{
    private static readonly TimeSpan FailFast = TimeSpan.FromSeconds(1);

    private static async Task AssertPutGet(ISyncClient client, string key)
    {
        var ct = TestContext.Current.CancellationToken;
        var payload = "over-tls"u8.ToArray();
        var (_, pVersion) = await client.PutAsync(ct, key, payload);
        var (_, value, gVersion) = await client.GetAsync(ct, key);
        Assert.Equal(pVersion, gVersion);
        Assert.Equal(payload, value);
    }

    private static async Task AssertCreationFails(string address, params ClientOption[] options)
    {
        ISyncClient? client = null;
        var err = await TestUtil.CatchAsync(async () => client = await OxiaClient.CreateSyncClientAsync(address, options));
        try
        {
            Assert.NotNull(err);
            Assert.Null(client);
        }
        finally
        {
            if (client is not null) await client.CloseAsync();
        }
    }

    [Fact]
    public async Task TestTlsClientPutsAndGetsThroughTls()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var tlsConf = pki.GetClientTlsOption().TryIntoClientTlsConf()!;

        var sync = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public, OxiaOptions.WithTls(tlsConf));
        await using (sync)
        {
            await AssertPutGet(sync, "/tls/sync");
        }

        // bootstrapping from a follower works too: the shard leader is dialled with the same TLS configuration
        var other = await OxiaClient.CreateSyncClientAsync(cluster.Servers[2].Public, OxiaOptions.WithTls(tlsConf));
        await using (other)
        {
            var (_, value, _) = await other.GetAsync(TestContext.Current.CancellationToken, "/tls/sync");
            Assert.Equal("over-tls"u8.ToArray(), value);
        }

        var async = await OxiaClient.CreateAsyncClientAsync(cluster.Servers[0].Public, OxiaOptions.WithTls(tlsConf));
        await using (async)
        {
            var put = await async.PutAsync("/tls/async", "async-over-tls"u8.ToArray());
            Assert.Null(put.Err);
        }
    }

    [Fact]
    public async Task TestTlsSchemeAddress()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);
        var tlsConf = pki.GetClientTlsOption().TryIntoClientTlsConf()!;

        // `tls://` selects TLS; the configuration that trusts the CA is the one given with WithTls
        var client = await OxiaClient.CreateSyncClientAsync("tls://" + cluster.Servers[0].Public, OxiaOptions.WithTls(tlsConf));
        await using (client)
        {
            await AssertPutGet(client, "/tls/scheme");
        }

        // without a configuration the system roots are used, which do not trust the test CA
        await AssertCreationFails("tls://" + cluster.Servers[0].Public, OxiaOptions.WithRequestTimeout(FailFast));
    }

    [Fact]
    public async Task TestTlsServerNameIsVerified()
    {
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki);

        // the certificate is valid for `localhost`: the name given in the options is the one checked (the address is 127.0.0.1)
        var good = pki.GetClientTlsOption();
        good.ServerName = "localhost";
        var client = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public, OxiaOptions.WithTls(good.TryIntoClientTlsConf()!));
        await using (client)
        {
            await AssertPutGet(client, "/tls/server-name");
        }

        var bad = pki.GetClientTlsOption();
        bad.ServerName = "not-in-the-certificate.example.com";
        await AssertCreationFails(cluster.Servers[0].Public, OxiaOptions.WithTls(bad.TryIntoClientTlsConf()!), OxiaOptions.WithRequestTimeout(FailFast));
    }

    [Fact]
    public async Task TestMutualTlsPresentsTheClientCertificate()
    {
        // oxia server --tls-client-auth: the public endpoint requires (and verifies against the CA) a client certificate
        await using var cluster = await SecurityHelpers.NewTlsCluster(clusters, pki, extraServerArgs: ["--tls-client-auth"], probeWithClientCertificate: true);

        var withCertificate = pki.GetClientTlsOption();
        var client = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public, OxiaOptions.WithTls(withCertificate.TryIntoClientTlsConf()!));
        await using (client)
        {
            await AssertPutGet(client, "/tls/mutual");
        }

        // trusts the server but has no certificate of its own
        var withoutCertificate = pki.GetClientTlsOption();
        withoutCertificate.CertFile = "";
        withoutCertificate.KeyFile = "";
        await AssertCreationFails(cluster.Servers[0].Public, OxiaOptions.WithTls(withoutCertificate.TryIntoClientTlsConf()!), OxiaOptions.WithRequestTimeout(FailFast));
    }
}
