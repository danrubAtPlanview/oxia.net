// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : How the fixture's own raw gRPC calls (readiness probe, leader lookup, RawOxia) reach a server that was started with
//              TLS and/or OIDC authentication: a TLS configuration (https + custom validation) and/or a function returning the
//              bearer token sent as `authorization: Bearer <token>`.  Independent of the client under test (no Oxia.Common types).
//   Divergences: n/a (new code).
// =====================================================================================================================
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;

namespace Oxia.TestSupport;

/// <summary>Transport security and credentials for the raw gRPC channels of the test infrastructure.</summary>
public sealed class RawSecurity
{
    /// <summary>TLS configuration; null = plaintext (h2c).</summary>
    public SslClientAuthenticationOptions? Tls { get; init; }

    /// <summary>Bearer token source (evaluated for every call); null = no credentials.</summary>
    public Func<string>? Token { get; init; }

    /// <summary>A channel to <paramref name="address"/> (host:port) with this security.</summary>
    public GrpcChannel CreateChannel(string address)
    {
        var options = new GrpcChannelOptions();
        if (Tls is not null)
        {
            options.HttpHandler = new SocketsHttpHandler { SslOptions = Tls };
        }

        if (Token is { } token)
        {
            var callCredentials = CallCredentials.FromInterceptor((_, metadata) =>
            {
                metadata.Add("authorization", "Bearer " + token());
                return Task.CompletedTask;
            });
            options.Credentials = ChannelCredentials.Create(Tls is not null ? ChannelCredentials.SecureSsl : ChannelCredentials.Insecure, callCredentials);
            options.UnsafeUseInsecureChannelCallCredentials = Tls is null;
        }

        return GrpcChannel.ForAddress((Tls is not null ? "https://" : "http://") + address, options);
    }

    /// <summary>A channel to <paramref name="address"/>, plaintext when <paramref name="security"/> is null.</summary>
    public static GrpcChannel CreateChannel(string address, RawSecurity? security) =>
        security?.CreateChannel(address) ?? GrpcChannel.ForAddress("http://" + address);
}
