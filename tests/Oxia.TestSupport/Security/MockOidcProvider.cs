// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : github.com/oauth2-proxy/mockoidc (mockoidc.Run(), MockOIDC.Issuer(), MockOIDC.Keypair.SignJWT(...), MockOIDC.Shutdown()),
//              golang-jwt/jwt/v5 RegisteredClaims, as used by tests/security/auth/auth_oidc_test.go.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A minimal OpenID Connect provider hosted in the test process (Kestrel on an ephemeral port).  It publishes what the
//              Oxia server's `go-oidc` verifier needs -- the discovery document (`<issuer>/.well-known/openid-configuration`) and the
//              JWKS (`<issuer>/.well-known/jwks.json`) -- and signs RS256 JWTs with its key pair (`Keypair.SignJwt(claims)`).  A token
//              endpoint (`<issuer>/token`) returns a signed token too; the Go tests never call it, it makes the provider usable by
//              hand.  The issuer URL (`http://<host>:<port>/oidc`, same path as mockoidc) is what the server is configured with and
//              what the tokens carry in `iss`, so <host> must be a name under which the Oxia server (in the Docker container) reaches
//              this process: see OxiaClusterFixture.GetHostAddressAsync.
//   Divergences: written with System.Security.Cryptography + System.Text.Json (no JWT package); only RS256, one key, no authorization
//              code flow / userinfo / refresh (mockoidc's browser flow is not used by the tests).  The key pair is 2048 bit RSA like
//              mockoidc's.
// =====================================================================================================================
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Oxia.TestSupport.Security;

/// <summary>Go: <c>jwt.RegisteredClaims</c> (the claims are written in the same order; unset ones are omitted, like <c>omitempty</c>).</summary>
public sealed record RegisteredClaims
{
    public string? Issuer { get; init; }
    public string? Subject { get; init; }

    /// <summary>Go: <c>jwt.ClaimStrings</c>, serialised as an array.</summary>
    public IReadOnlyList<string>? Audience { get; init; }

    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>Go: <c>jwt.NewNumericDate(time.Time{})</c> is <c>default(DateTimeOffset)</c> (year 1, unix -62135596800).</summary>
    public DateTimeOffset? NotBefore { get; init; }

    public DateTimeOffset? IssuedAt { get; init; }
    public string? Id { get; init; }
}

/// <summary>Go: <c>mockoidc.Keypair</c> -- the RSA key pair the provider signs with and publishes.</summary>
public sealed class MockOidcKeypair : IDisposable
{
    private readonly RSA _rsa = RSA.Create(2048);

    public MockOidcKeypair()
    {
        KeyId = Base64Url(RandomNumberGenerator.GetBytes(24));
    }

    /// <summary>The <c>kid</c> of the JWT header and of the JWKS entry.</summary>
    public string KeyId { get; }

    /// <summary>Go: <c>x509.MarshalPKIXPublicKey</c> + <c>pem.EncodeToMemory("PUBLIC KEY")</c> -- the content of a <c>staticKeyFile</c>.</summary>
    public string PublicKeyPem => new(PemEncoding.Write("PUBLIC KEY", _rsa.ExportSubjectPublicKeyInfo()));

    /// <summary>Go: <c>Keypair.SignJWT(claims)</c> -- compact RS256 JWT.</summary>
    public string SignJwt(RegisteredClaims claims)
    {
        var header = Encoding.UTF8.GetBytes($"{{\"alg\":\"RS256\",\"kid\":\"{KeyId}\",\"typ\":\"JWT\"}}");
        var payload = SerializeClaims(claims);
        var signingInput = Base64Url(header) + "." + Base64Url(payload);
        var signature = _rsa.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return signingInput + "." + Base64Url(signature);
    }

    /// <summary>The JWKS document (RFC 7517) with the public key.</summary>
    public string Jwks()
    {
        var p = _rsa.ExportParameters(false);
        return JsonSerializer.Serialize(new
        {
            keys = new[]
            {
                new { kty = "RSA", alg = "RS256", use = "sig", kid = KeyId, n = Base64Url(p.Modulus!), e = Base64Url(p.Exponent!) },
            },
        });
    }

    public void Dispose() => _rsa.Dispose();

    private static byte[] SerializeClaims(RegisteredClaims c)
    {
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            if (c.Issuer is not null) w.WriteString("iss", c.Issuer);
            if (c.Subject is not null) w.WriteString("sub", c.Subject);
            if (c.Audience is { Count: > 0 })
            {
                w.WriteStartArray("aud");
                foreach (var a in c.Audience) w.WriteStringValue(a);
                w.WriteEndArray();
            }

            if (c.ExpiresAt is { } exp) w.WriteNumber("exp", exp.ToUnixTimeSeconds());
            if (c.NotBefore is { } nbf) w.WriteNumber("nbf", nbf.ToUnixTimeSeconds());
            if (c.IssuedAt is { } iat) w.WriteNumber("iat", iat.ToUnixTimeSeconds());
            if (c.Id is not null) w.WriteString("jti", c.Id);
            w.WriteEndObject();
        }

        return stream.ToArray();
    }

    internal static string Base64Url(ReadOnlySpan<byte> data) => Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}

/// <summary>Go: <c>*mockoidc.MockOIDC</c> -- <c>mockoidc.Run()</c> is <see cref="StartAsync"/>, <c>Shutdown()</c> is <see cref="DisposeAsync"/>.</summary>
public sealed class MockOidcProvider : IAsyncDisposable
{
    private const string BasePath = "/oidc";
    private readonly WebApplication _app;

    private MockOidcProvider(WebApplication app, string issuer, MockOidcKeypair keypair)
    {
        _app = app;
        Issuer = issuer;
        Keypair = keypair;
    }

    /// <summary>Go: <c>MockOIDC.Issuer()</c> -- <c>http://&lt;host&gt;:&lt;port&gt;/oidc</c>.</summary>
    public string Issuer { get; }

    /// <summary>Go: <c>MockOIDC.Keypair</c>.</summary>
    public MockOidcKeypair Keypair { get; }

    /// <summary>
    /// Starts the provider on an ephemeral port.  <paramref name="hostName"/> is how the Oxia servers reach this process (it goes into the
    /// issuer URL).  It listens on loopback only on Windows / macOS (Docker Desktop forwards <c>host.docker.internal</c> to the host's
    /// loopback) and on all IPv4 interfaces elsewhere (a Linux container reaches the host through the docker0 bridge).
    /// </summary>
    public static async Task<MockOidcProvider> StartAsync(string hostName)
    {
        var keypair = new MockOidcKeypair();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k => k.Listen(OperatingSystem.IsLinux() ? IPAddress.Any : IPAddress.Loopback, 0));
        var app = builder.Build();

        string issuer = "";
        string Url(string path) => issuer + path;

        app.MapGet(BasePath + "/.well-known/openid-configuration", () => Results.Text(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["issuer"] = issuer,
            ["authorization_endpoint"] = Url("/authorize"),
            ["token_endpoint"] = Url("/token"),
            ["jwks_uri"] = Url("/.well-known/jwks.json"),
            ["response_types_supported"] = new[] { "code" },
            ["subject_types_supported"] = new[] { "public" },
            ["id_token_signing_alg_values_supported"] = new[] { "RS256" },
            ["scopes_supported"] = new[] { "openid" },
            ["claims_supported"] = new[] { "iss", "sub", "aud", "exp", "iat", "jti" },
        }), "application/json"));
        app.MapGet(BasePath + "/.well-known/jwks.json", () => Results.Text(keypair.Jwks(), "application/json"));
        app.MapPost(BasePath + "/token", () =>
        {
            var now = DateTimeOffset.UtcNow;
            var token = keypair.SignJwt(new RegisteredClaims
            {
                Issuer = issuer,
                Subject = "mock-subject",
                Audience = ["mock-client"],
                ExpiresAt = now.AddHours(1),
                IssuedAt = now,
                Id = Guid.NewGuid().ToString(),
            });
            return Results.Text(JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["access_token"] = token,
                ["id_token"] = token,
                ["token_type"] = "bearer",
                ["expires_in"] = 3600,
            }), "application/json");
        });

        await app.StartAsync().ConfigureAwait(false);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        var port = new Uri(address.Replace("[::]", "127.0.0.1").Replace("0.0.0.0", "127.0.0.1")).Port;
        issuer = $"http://{hostName}:{port}{BasePath}";
        return new MockOidcProvider(app, issuer, keypair);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _app.StopAsync().ConfigureAwait(false); } catch { /* already stopped */ }
        await _app.DisposeAsync().ConfigureAwait(false);
        Keypair.Dispose();
    }
}
