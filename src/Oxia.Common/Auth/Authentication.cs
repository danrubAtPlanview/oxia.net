// =====================================================================================================================
// PORT NOTE
//   Upstream : common/auth/authentication.go (interface = grpc credentials.PerRPCCredentials), common/auth/token.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Per-RPC credentials.  The token implementation emits `authorization: Bearer <token>` on every call.
//              `RequireTransportSecurity` mirrors Go: when true the credentials are only sent over TLS (the ClientPool
//              maps it to Grpc.Net.Client's CallCredentials / UnsafeUseInsecureChannelCallCredentials).
//   Divergences: credentials.PerRPCCredentials.GetRequestMetadata(ctx, uri...) (map, error) -> GetRequestMetadataAsync(ct, uri...) returning the
//              map (errors are thrown); the token function is evaluated on every call, like Go.  Tests: Common/Auth/TokenTests.cs (the Go
//              repo has none for this package).
// =====================================================================================================================
namespace Oxia.Common.Auth;

/// <summary>Go: <c>auth.Authentication</c> (embeds credentials.PerRPCCredentials).</summary>
public interface IAuthentication
{
    /// <summary>Go: GetRequestMetadata(ctx, uri...) -- header name/value pairs added to every RPC.</summary>
    ValueTask<IReadOnlyDictionary<string, string>> GetRequestMetadataAsync(CancellationToken ct, params string[] uri);

    bool RequireTransportSecurity { get; }
}

public static class Authentication
{
    public static IAuthentication NewTokenAuthenticationWithFunc(Func<string> tokenGetFunc, bool requireTransportSecurity)
        => new TokenAuthentication(tokenGetFunc, requireTransportSecurity);

    public static IAuthentication NewTokenAuthenticationWithToken(string token, bool requireTransportSecurity)
        => new TokenAuthentication(() => token, requireTransportSecurity);

    private sealed class TokenAuthentication : IAuthentication
    {
        private readonly Func<string> _tokenGetFunc;

        public TokenAuthentication(Func<string> tokenGetFunc, bool requireTransportSecurity)
        {
            _tokenGetFunc = tokenGetFunc;
            RequireTransportSecurity = requireTransportSecurity;
        }

        public bool RequireTransportSecurity { get; }

        public ValueTask<IReadOnlyDictionary<string, string>> GetRequestMetadataAsync(CancellationToken ct, params string[] uri)
        {
            IReadOnlyDictionary<string, string> md = new Dictionary<string, string>(1)
            {
                ["authorization"] = "Bearer " + _tokenGetFunc(),
            };
            return ValueTask.FromResult(md);
        }
    }
}
