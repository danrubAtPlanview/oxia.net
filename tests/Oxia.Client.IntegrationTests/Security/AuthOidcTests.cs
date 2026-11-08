// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/security/auth/auth_oidc_test.go (+ helpers_test.go, see SecurityHelpers.cs)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client's token authentication (`OxiaOptions.WithAuthentication(Authentication.NewTokenAuthenticationWithToken(...))`)
//              against data servers whose public endpoint verifies OIDC JWTs: no token, a malformed token, a token of an unknown
//              issuer, a truncated token and an expired token are rejected with gRPC `Unauthenticated`; a correct token can put and
//              get.  Also the per-issuer configuration (two issuers with their own audiences, wrong audience rejected) and the
//              static public key file (no discovery / JWKS fetch needed by the server).
//   Go tests: 3   ported: 3   skipped: 0
//   Divergences:
//     * `mockoidc.Run()` (a provider on 127.0.0.1) is MockOidcProvider, a Kestrel host in the test process; its issuer URL uses the name
//       under which the Docker container reaches this machine (`host.docker.internal` on Docker Desktop, the default gateway on Linux).
//       Tokens are signed by its key pair like `mockOIDC.Keypair.SignJWT(...)`.
//     * The three in-process data servers + coordinator are the cluster fixture's `oxia server` / `oxia coordinator` processes with
//       `--auth-provider-name oidc --auth-provider-params <json>` (the same JSON `json.Marshal(auth.OIDCOptions{...})` produces).
//       The fixture has to ask the servers for the shard assignments to know when the cluster is ready; it does so with a valid
//       token (the one the test would use anyway), which is not part of any assertion.
//     * TestOIDCWithStaticKeyFile: the public key PEM is copied into the container (`{dir}/public-key.pem`, tmpfs) and `staticKeyFile`
//       points there; Go writes it to `t.TempDir()`.
//     * `status.Code(errors.Unwrap(err))` is `OxiaErrors.GrpcCode(err)` (the status code of the first RpcException in the exception chain).
//     * `client.Close()` deferred to the end of the test is `await using`.
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

public class AuthOidcTests(OxiaClusterFixture clusters)
{
    private const string IllegalToken =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6ImRIWFRTQ3lvdXE2RGlXYVF3bFh0TlA1NC1DNzVtdzNJY29Za0VSZmwzZlEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjYxMzM4L29pZGMiLCJzdWIiOiJjZGJiYWE4NC0xODg0LTQ5NTktOTgwNS02NGRiY2NiMzdlZTIiLCJhdWQiOlsiNDc5YzRmYTktNDZjMC00NjY5LTkyZTktY2QxYmM3Mjc2MWNlIl0sImV4cCI6MTcxOTI0MjM4OSwibmJmIjotNjIxMzU1OTY4MDAsImlhdCI6MTcxOTIzODc4OSwianRpIjoiODY1MTlkOGEtMmNiYy00NTY0LTlmZjMtNTUyZTAxYjQzNzc2In0.netDk-UFqBwlxJZlDc3Any2tSqBHXsLxdorM3MrL171Xql6Mms6KCiNabpWbx--xvPtvlzs3v1O1R5LO3bZbI1VO-efumOpvjDBxe6WRqeezGp1spcJ_s0M90MjF7d6uRDxlOfEmPaB1Oryb8kYlyErrdXM3P1jRN_i2HMdju0tKjEVcqIbuzBs5et3RrLHmcP5yMFB9D9xN4zeTd_Rf7Qyl1JdiA2qD-1KDfeVtGAahyuNiR0-VOncY1VU3sqi-h8cviyB7cn2j4Iuo5D-DIuvrbC-jS51NUSLb_nSD8LjuGoc76n3-_zB2svTFVv-1tiLESASqna4HaI_AyRSDNQ";

    private static ClientOption Token(string token) => OxiaOptions.WithAuthentication(Authentication.NewTokenAuthenticationWithToken(token, false));

    /// <summary>Go: <c>assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))</c> after a failed <c>oxia.NewSyncClient(addr, opts...)</c>.</summary>
    private static async Task AssertUnauthenticated(string addr, params ClientOption[] opts)
    {
        ISyncClient? client = null;
        var err = await TestUtil.CatchAsync(async () => client = await OxiaClient.CreateSyncClientAsync(addr, opts));
        try
        {
            Assert.NotNull(err);
            Assert.Equal(StatusCode.Unauthenticated, OxiaErrors.GrpcCode(err));
        }
        finally
        {
            if (client is not null) await client.CloseAsync();
        }
    }

    /// <summary>
    /// Go: <c>newOxiaClusterWithAuth(t, issueURL, audiences)</c> -- the legacy provider parameters
    /// <c>{"allowedIssueURLs": issueURL, "allowedAudiences": audiences}</c>.  <paramref name="readinessToken"/> is only used by the fixture.
    /// </summary>
    private async Task<OxiaCluster> NewOxiaClusterWithAuth(string issueUrl, string audiences, string readinessToken) =>
        await NewOidcCluster(clusters, LegacyOidcParams(issueUrl, audiences), readinessToken);

    [Fact]
    public async Task TestOIDCWithStaticToken()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var mockOIDC = await MockOidcProvider.StartAsync(host);

        var audience = GenerateRandomStr();
        var audience2 = GenerateRandomStr();
        var id = GenerateRandomStr();
        var subject = GenerateRandomStr();
        var registeredClaims = new RegisteredClaims
        {
            Audience = [audience, audience2],
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
            Id = id,
            IssuedAt = DateTimeOffset.UtcNow,
            Issuer = mockOIDC.Issuer,
            NotBefore = default, // jwt.NewNumericDate(time.Time{})
            Subject = subject,
        };
        var signedToken = mockOIDC.Keypair.SignJwt(registeredClaims);

        await using var cluster = await NewOxiaClusterWithAuth(mockOIDC.Issuer, audience, signedToken);
        var addr = cluster.Servers[0].Public;

        // assert connection failed with empty token
        await AssertUnauthenticated(addr);

        // assert connection failed with malformed token
        await AssertUnauthenticated(addr, Token("wrongToken"));

        // assert connection failed with unknown issue
        await AssertUnauthenticated(addr, Token(IllegalToken));

        var cutToken = signedToken[5..];
        await AssertUnauthenticated(addr, Token(cutToken));

        // assert connection failed with expired token
        var expiredToken = mockOIDC.Keypair.SignJwt(new RegisteredClaims
        {
            Audience = [audience],
            ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(1),
            Id = id,
            IssuedAt = DateTimeOffset.UtcNow,
            Issuer = mockOIDC.Issuer,
            NotBefore = default,
            Subject = subject,
        });
        await Task.Delay(TimeSpan.FromSeconds(3));
        await AssertUnauthenticated(addr, Token(expiredToken));

        // assert connection success with correct token
        var client = await OxiaClient.CreateSyncClientAsync(addr, Token(signedToken));
        var ctx = CancellationToken.None;
        var key = "hi";
        var payload = "matt"u8.ToArray();
        var (_, pVersion) = await client.PutAsync(ctx, key, payload);
        var (_, gValue, gVersion) = await client.GetAsync(ctx, key);
        Assert.Equal(pVersion, gVersion);
        Assert.Equal(payload, gValue);
        await client.CloseAsync();
    }

    // TestOIDCWithPerIssuerConfig tests the new per-issuer configuration format.
    [Fact]
    public async Task TestOIDCWithPerIssuerConfig()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var mockOIDC1 = await MockOidcProvider.StartAsync(host);
        await using var mockOIDC2 = await MockOidcProvider.StartAsync(host);

        var audience1 = GenerateRandomStr();
        var audience2 = GenerateRandomStr();
        var subject1 = GenerateRandomStr();
        var subject2 = GenerateRandomStr();

        // Configure cluster with per-issuer settings
        var jsonParams = PerIssuerOidcParams(new Dictionary<string, IssuerConfig>
        {
            [mockOIDC1.Issuer] = new(audience1, "sub"),
            [mockOIDC2.Issuer] = new(audience2, "sub"),
        });

        // Test authentication with issuer 1
        var token1 = mockOIDC1.Keypair.SignJwt(new RegisteredClaims
        {
            Audience = [audience1],
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
            Issuer = mockOIDC1.Issuer,
            Subject = subject1,
            IssuedAt = DateTimeOffset.UtcNow,
        });

        await using var cluster = await NewOidcCluster(clusters, jsonParams, token1);
        var s1Public = cluster.Servers[0].Public;

        var client1 = await OxiaClient.CreateSyncClientAsync(s1Public, Token(token1));
        await using var _1 = client1;

        // Perform operations with client 1
        var ctx = CancellationToken.None;
        await client1.PutAsync(ctx, "key1", "value1"u8.ToArray());

        // Test authentication with issuer 2
        var token2 = mockOIDC2.Keypair.SignJwt(new RegisteredClaims
        {
            Audience = [audience2],
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
            Issuer = mockOIDC2.Issuer,
            Subject = subject2,
            IssuedAt = DateTimeOffset.UtcNow,
        });

        var client2 = await OxiaClient.CreateSyncClientAsync(s1Public, Token(token2));
        await using var _2 = client2;

        // Perform operations with client 2
        await client2.PutAsync(ctx, "key2", "value2"u8.ToArray());

        // Test that issuer 1 token with wrong audience fails
        var wrongAudienceToken = mockOIDC1.Keypair.SignJwt(new RegisteredClaims
        {
            Audience = [audience2], // Wrong audience
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
            Issuer = mockOIDC1.Issuer,
            Subject = subject1,
            IssuedAt = DateTimeOffset.UtcNow,
        });

        await AssertUnauthenticated(s1Public, Token(wrongAudienceToken));
    }

    // TestOIDCWithStaticKeyFile tests static key file support.
    [Fact]
    public async Task TestOIDCWithStaticKeyFile()
    {
        var host = await HostAddressOfTestProcess(clusters);
        await using var mockOIDC = await MockOidcProvider.StartAsync(host);

        // Export public key to a temporary file (here: a file in the container, next to the other secrets of the cluster slot)
        const string keyFileName = "public-key.pem";
        var keyFile = $"{OxiaClusterProfile.DirToken}/{keyFileName}";
        var files = new Dictionary<string, string> { [keyFileName] = mockOIDC.Keypair.PublicKeyPem };

        var audience = GenerateRandomStr();
        var subject = GenerateRandomStr();

        // Configure cluster with static key file
        var jsonParams = PerIssuerOidcParams(new Dictionary<string, IssuerConfig>
        {
            [mockOIDC.Issuer] = new(audience, "sub", keyFile),
        });

        // Create a valid token
        var token = mockOIDC.Keypair.SignJwt(new RegisteredClaims
        {
            Audience = [audience],
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
            Issuer = mockOIDC.Issuer,
            Subject = subject,
            IssuedAt = DateTimeOffset.UtcNow,
        });

        await using var cluster = await NewOidcCluster(clusters, jsonParams, token, files);

        // Test authentication with static key file
        var client = await OxiaClient.CreateSyncClientAsync(cluster.Servers[0].Public, Token(token));
        await using var _ = client;

        // Perform operations
        var ctx = CancellationToken.None;
        var key = "test-key";
        var value = "test-value"u8.ToArray();
        var (_, pVersion) = await client.PutAsync(ctx, key, value);

        var (_, gValue, gVersion) = await client.GetAsync(ctx, key);
        Assert.Equal(pVersion, gVersion);
        Assert.Equal(value, gValue);
    }
}
