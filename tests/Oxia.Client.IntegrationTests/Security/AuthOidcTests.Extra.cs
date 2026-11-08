// =====================================================================================================================
// PORT NOTE
//   Upstream : none (extra tests; the ported ones are in AuthOidcTests.cs).  Same servers as tests/security/auth/auth_oidc_test.go.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go tests only use `NewTokenAuthenticationWithToken(token, false)` over a plaintext connection.  These cover
//              `NewTokenAuthenticationWithFunc` (the function is evaluated for every RPC, so a refreshed token is picked up),
//              credentials that require transport security (sent over TLS) and OIDC on top of TLS.
//   Divergences: n/a.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Client;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Oxia.TestSupport.Security;
using Xunit;
using static Oxia.Client.IntegrationTests.Security.SecurityHelpers;

namespace Oxia.Client.IntegrationTests.Security;

public class AuthOidcExtraTests(OxiaClusterFixture clusters, PkiFixture pki) : IClassFixture<PkiFixture>
{
    private static RegisteredClaims ValidClaims(MockOidcProvider oidc, string audience) => new()
    {
        Audience = [audience],
        ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
        Id = GenerateRandomStr(),
        IssuedAt = DateTimeOffset.UtcNow,
        Issuer = oidc.Issuer,
        Subject = GenerateRandomStr(),
    };

    [Fact]
    public async Task TestTokenFuncIsEvaluatedForEveryRpc()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var oidc = await MockOidcProvider.StartAsync(host);
        var audience = GenerateRandomStr();
        var token1 = oidc.Keypair.SignJwt(ValidClaims(oidc, audience));
        var token2 = oidc.Keypair.SignJwt(ValidClaims(oidc, audience)); // another subject, same issuer: also accepted

        await using var cluster = await NewOidcCluster(clusters, LegacyOidcParams(oidc.Issuer, audience), token1);

        var calls = 0;
        var current = token1;
        var auth = Authentication.NewTokenAuthenticationWithFunc(() =>
        {
            Interlocked.Increment(ref calls);
            return Volatile.Read(ref current);
        }, requireTransportSecurity: false);

        var ct = TestContext.Current.CancellationToken;
        var client = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public, OxiaOptions.WithAuthentication(auth));
        await using (client)
        {
            await client.PutAsync(ct, "/auth/func", "v"u8.ToArray());

            var before = Volatile.Read(ref calls);
            Assert.True(before > 0);
            for (var i = 0; i < 10; i++)
            {
                var (_, value, _) = await client.GetAsync(ct, "/auth/func");
                Assert.Equal("v"u8.ToArray(), value);
            }

            // every read is a new RPC: the function is asked again (a refreshed token is used from then on)
            Assert.True(Volatile.Read(ref calls) >= before + 10, $"token function called {Volatile.Read(ref calls) - before} times for 10 reads");

            Volatile.Write(ref current, token2);
            var (_, again, _) = await client.GetAsync(ct, "/auth/func");
            Assert.Equal("v"u8.ToArray(), again);

            // ... and a token the server rejects fails the very next RPC
            Volatile.Write(ref current, "not-a-token");
            var err = await TestUtil.CatchAsync(async () => await client.GetAsync(ct, "/auth/func"));
            Assert.NotNull(err);
            Assert.Equal(StatusCode.Unauthenticated, OxiaErrors.GrpcCode(err));
        }
    }

    [Fact]
    public async Task TestEveryRpcKindOverTlsAndAuthentication()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var oidc = await MockOidcProvider.StartAsync(host);
        var audience = GenerateRandomStr();
        var token = oidc.Keypair.SignJwt(ValidClaims(oidc, audience));
        await using var cluster = await NewTlsCluster(clusters, pki, onlyPublicTls: true,
            oidcProviderParams: LegacyOidcParams(oidc.Issuer, audience), oidcReadinessToken: token);

        var ct = TestContext.Current.CancellationToken;
        var client = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public,
            OxiaOptions.WithTls(pki.GetClientTlsOption().TryIntoClientTlsConf()!),
            OxiaOptions.WithAuthentication(Authentication.NewTokenAuthenticationWithToken(token, requireTransportSecurity: true)));
        await using var _ = client;

        // notifications stream
        var notifications = await client.GetNotificationsAsync();
        await using var __ = notifications;

        // write stream, reads, list, range scan
        await client.PutAsync(ct, "/rpc/a", "a"u8.ToArray());
        await client.PutAsync(ct, "/rpc/b", "b"u8.ToArray());
        Assert.Equal(["/rpc/a", "/rpc/b"], await client.ListAsync(ct, "/rpc/a", "/rpc/c"));
        var scanned = new List<string>();
        await foreach (var r in client.RangeScanAsync(ct, "/rpc/a", "/rpc/c").ReadAllAsync(ct))
        {
            Assert.Null(r.Err);
            scanned.Add(r.Key);
        }

        Assert.Equal(["/rpc/a", "/rpc/b"], scanned);

        // session (create + keep-alive stream): an ephemeral record
        await client.PutAsync(ct, "/rpc/ephemeral", "e"u8.ToArray(), OxiaOptions.Ephemeral());
        Assert.Equal("e"u8.ToArray(), (await client.GetAsync(ct, "/rpc/ephemeral")).Value);

        // the notification of a write made after the subscription
        var seen = false;
        for (var i = 0; i < 20 && !seen; i++)
        {
            var n = await notifications.Ch.Recv(TimeSpan.FromSeconds(10), ct);
            seen = n is not null && n.Key == "/rpc/b";
        }

        Assert.True(seen, "no notification for /rpc/b");

        await client.DeleteRangeAsync(ct, "/rpc/a", "/rpc/c");
        Assert.Empty(await client.ListAsync(ct, "/rpc/a", "/rpc/c"));
    }

    [Fact]
    public async Task TestTokenOverTlsWithRequiredTransportSecurity()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var oidc = await MockOidcProvider.StartAsync(host);
        var audience = GenerateRandomStr();
        var token = oidc.Keypair.SignJwt(ValidClaims(oidc, audience));

        // public endpoint: TLS + OIDC
        await using var cluster = await NewTlsCluster(clusters, pki, onlyPublicTls: true,
            oidcProviderParams: LegacyOidcParams(oidc.Issuer, audience), oidcReadinessToken: token);
        var address = cluster.Servers[0].Public;
        var tlsConf = pki.GetClientTlsOption().TryIntoClientTlsConf()!;
        var secureToken = Authentication.NewTokenAuthenticationWithToken(token, requireTransportSecurity: true);

        // the credentials that require transport security are sent over TLS
        var client = await OxiaClient.CreateSyncClientAsync(address, OxiaOptions.WithTls(tlsConf), OxiaOptions.WithAuthentication(secureToken));
        await using (client)
        {
            var ct = TestContext.Current.CancellationToken;
            var (_, pVersion) = await client.PutAsync(ct, "/auth/tls", "secret"u8.ToArray());
            var (_, value, gVersion) = await client.GetAsync(ct, "/auth/tls");
            Assert.Equal(pVersion, gVersion);
            Assert.Equal("secret"u8.ToArray(), value);
        }

        // TLS alone is not enough: no token is Unauthenticated
        ISyncClient? noToken = null;
        var err = await TestUtil.CatchAsync(async () => noToken = await OxiaClient.CreateSyncClientAsync(address, OxiaOptions.WithTls(tlsConf)));
        Assert.Null(noToken);
        Assert.Equal(StatusCode.Unauthenticated, OxiaErrors.GrpcCode(err));
    }
}
