// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/admin_errors_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 2, ported: 2, skipped: 0
//   Adapted  : assert.Same -> Assert.Same; errors.Unwrap(err) == nil -> InnerException is null.
// =====================================================================================================================
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.F2;

public class AdminErrorsTests
{
    [Fact]
    public void TestWrapAdminErrorWithoutCause()
    {
        Assert.Same(ClientErrors.Unauthorized, AdminErrors.WrapAdminError(ClientErrors.Unauthorized, null));
    }

    [Fact]
    public void TestAdminErrorWithoutCause()
    {
        var err = new AdminError(ClientErrors.Unauthenticated, null);
        Assert.Equal("unauthenticated", err.Message);
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unauthenticated));
        Assert.Null(err.InnerException);
    }
}
