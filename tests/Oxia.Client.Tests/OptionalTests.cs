// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/optional_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 3, ported: 3, skipped: 0
//   Adaptations: `optionalOf(1)` / `empty[string]()` are Optional.Of / Optional.Empty; `Get()` is TryGet(out); Go's
//                `assert.PanicsWithValue("optional empty on MustGet call", ...)` is an InvalidOperationException with that
//                message.
// =====================================================================================================================
using Xunit;

namespace Oxia.Client.Tests;

public class OptionalTests
{
    [Fact]
    public void TestOptionalPresent()
    {
        var o = Optional.Of(1);
        Assert.True(o.Present);
        Assert.False(o.Empty);
        var ok = o.TryGet(out var v);
        Assert.True(ok);
        Assert.Equal(1, v);
    }

    [Fact]
    public void TestOptionalEmpty()
    {
        var e = Optional.Empty<string>();
        Assert.False(e.Present);
        Assert.True(e.Empty);
        var ok = e.TryGet(out var es);
        Assert.False(ok);
        Assert.Equal("", es ?? "");   // Go's zero value of string is ""; the .NET default is null
    }

    [Fact]
    public void TestOptionalMustGet()
    {
        Assert.Equal(1, Optional.Of(1).MustGet());
        var ex = Assert.Throws<InvalidOperationException>(() => Optional.Empty<int>().MustGet());
        Assert.Equal("optional empty on MustGet call", ex.Message);
    }
}
