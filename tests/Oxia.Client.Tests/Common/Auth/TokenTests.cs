// =====================================================================================================================
// PORT NOTE
//   Upstream : common/auth/token.go, common/auth/authentication.go   (the Go repo has no tests for them)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : New tests: the token authentication yields exactly one header `authorization: Bearer <token>`, evaluates the token
//              function on every call (rotating tokens), and carries the requireTransportSecurity flag.
// =====================================================================================================================
using Oxia.Common.Auth;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class TokenTests
{
    [Fact]
    public async Task WithToken_YieldsABearerAuthorizationHeader()
    {
        var auth = Authentication.NewTokenAuthenticationWithToken("my-token", true);

        var md = await auth.GetRequestMetadataAsync(CancellationToken.None);

        var entry = Assert.Single(md);
        Assert.Equal("authorization", entry.Key);
        Assert.Equal("Bearer my-token", entry.Value);
    }

    [Fact]
    public async Task WithFunc_EvaluatesTheFunctionOnEveryCall()
    {
        var n = 0;
        var auth = Authentication.NewTokenAuthenticationWithFunc(() => $"token-{++n}", false);

        Assert.Equal("Bearer token-1", (await auth.GetRequestMetadataAsync(CancellationToken.None))["authorization"]);
        Assert.Equal("Bearer token-2", (await auth.GetRequestMetadataAsync(CancellationToken.None))["authorization"]);
        Assert.Equal(2, n);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RequireTransportSecurity_IsTheConfiguredFlag(bool require)
    {
        Assert.Equal(require, Authentication.NewTokenAuthenticationWithToken("t", require).RequireTransportSecurity);
        Assert.Equal(require, Authentication.NewTokenAuthenticationWithFunc(() => "t", require).RequireTransportSecurity);
    }

    [Fact]
    public async Task TheUriArgumentsAreIgnored()
    {
        var auth = Authentication.NewTokenAuthenticationWithToken("t", false);

        var md = await auth.GetRequestMetadataAsync(CancellationToken.None, "https://host/service/Method", "other");

        Assert.Equal("Bearer t", md["authorization"]);
    }

    [Fact]
    public async Task AnEmptyTokenStillYieldsTheBearerPrefix()
    {
        // Go: "Bearer" + " " + token
        var md = await Authentication.NewTokenAuthenticationWithToken("", false).GetRequestMetadataAsync(CancellationToken.None);
        Assert.Equal("Bearer ", md["authorization"]);
    }

    [Fact]
    public async Task AFailingTokenFunctionSurfacesToTheCaller()
    {
        var auth = Authentication.NewTokenAuthenticationWithFunc(() => throw new InvalidOperationException("no token"), false);

        // GetRequestMetadataAsync may throw synchronously or return a faulted task; either way awaiting it throws
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await auth.GetRequestMetadataAsync(CancellationToken.None));
    }
}
