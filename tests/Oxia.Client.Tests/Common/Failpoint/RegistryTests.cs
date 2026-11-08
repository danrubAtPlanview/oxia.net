// =====================================================================================================================
// PORT NOTE
//   Upstream : common/failpoint/registry_test.go   (build tag !disable_trap)
//              common/failpoint/registry_disable_test.go   (build tag disable_trap)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: registry_test.go 5, ported: 5; registry_disable_test.go 1, skipped: 1 (no disabled build flavour in the port).
//   Divergences: `Eval[T](name) (T, bool)` -> `Eval<T>(name, out T value)`; time.Duration -> TimeSpan; `t.Cleanup(Disable)` -> try/finally;
//              wg.Go -> Task.Run.  The registry is process-wide: every test of this class uses its own point name pattern "point" like
//              Go and the xUnit runner never runs two methods of one class at once.
// =====================================================================================================================
using Oxia.Common.Failpoint;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class RegistryTests
{
    [Fact]
    public void TestEnableSetsTheValue()
    {
        Failpoints.Enable("point", TimeSpan.FromSeconds(1));
        try
        {
            var ok = Failpoints.Eval<TimeSpan>("point", out var value);
            Assert.True(ok);
            Assert.Equal(TimeSpan.FromSeconds(1), value);
        }
        finally
        {
            Failpoints.Disable("point");
        }
    }

    [Fact]
    public void TestDisableRemovesTheValue()
    {
        Failpoints.Enable("point", TimeSpan.FromSeconds(1));
        Failpoints.Disable("point");

        var ok = Failpoints.Eval<TimeSpan>("point", out var value);
        Assert.False(ok);
        Assert.Equal(default, value);
    }

    [Fact]
    public void TestEvalWithoutValue()
    {
        var ok = Failpoints.Eval<TimeSpan>("other", out var value);
        Assert.False(ok);
        Assert.Equal(default, value);
    }

    [Fact]
    public void TestEvalWithValueOfAnotherType()
    {
        Failpoints.Enable("point", "1s");
        try
        {
            var ok = Failpoints.Eval<TimeSpan>("point", out var value);
            Assert.False(ok);
            Assert.Equal(default, value);
        }
        finally
        {
            Failpoints.Disable("point");
        }
    }

    [Fact]
    public async Task TestEvalWhileEnabling()
    {
        try
        {
            var writer = Task.Run(() =>
            {
                for (var i = 1; i <= 100; i++)
                {
                    Failpoints.Enable("point", i);
                    Failpoints.Disable("point");
                }
            });
            for (var i = 0; i < 100; i++)
            {
                if (Failpoints.Eval<int>("point", out var value))
                {
                    Assert.True(value > 0); // assert.Positive
                }
            }

            await writer;
        }
        finally
        {
            Failpoints.Disable("point");
        }
    }

    [Fact(Skip = "registry_disable_test.go (build tag disable_trap): the port has no disabled build flavour")]
    public void TestEvalDisabled()
    {
    }

    // ----- extra tests (not in Go) --------------------------------------------------------------------------------------
    [Fact]
    public void EnableOverwritesAndDisableOfAnUnknownPointIsHarmless()
    {
        Failpoints.Disable("never-enabled");
        Failpoints.Enable("overwrite", 1);
        Failpoints.Enable("overwrite", 2);
        try
        {
            Assert.True(Failpoints.Eval<int>("overwrite", out var value));
            Assert.Equal(2, value);
        }
        finally
        {
            Failpoints.Disable("overwrite");
        }
    }

    [Fact]
    public void ReferenceValuesAreEvaluatedByType()
    {
        var payload = new List<int> { 1, 2 };
        Failpoints.Enable("ref-point", payload);
        try
        {
            Assert.True(Failpoints.Eval<List<int>>("ref-point", out var list));
            Assert.Same(payload, list);
            Assert.True(Failpoints.Eval<IReadOnlyList<int>>("ref-point", out _)); // assignable
            Assert.False(Failpoints.Eval<string>("ref-point", out var none));
            Assert.Null(none);
        }
        finally
        {
            Failpoints.Disable("ref-point");
        }
    }
}
