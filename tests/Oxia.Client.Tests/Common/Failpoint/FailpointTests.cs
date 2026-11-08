// =====================================================================================================================
// PORT NOTE
//   Upstream : common/failpoint/failpoint_test.go   (build tag !disable_trap)
//              common/failpoint/failpoint_disable_test.go   (build tag disable_trap -- the no-op build)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: failpoint_test.go 6, ported: 6; failpoint_disable_test.go 2, skipped: 2 (the .NET port has no build flavour in
//              which failpoints compile to no-ops; Failpoints.Enabled is the constant true).
//   Divergences: the Go methods on a nil *Failpoints are static methods taking the (nullable) instance; `func() error` hooks are
//              `Func<Exception?>`; `assert.ErrorIs(err, hookErr)` -> reference identity (hook errors are returned as they are).
// =====================================================================================================================
using Oxia.Common.Failpoint;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class FailpointTests
{
    [Fact]
    public void TestFailpointsTriggerRunsHook()
    {
        var hookErr = new Exception("hook");
        var calls = 0;
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["point"] = () =>
            {
                calls++;
                return hookErr;
            },
        });

        Assert.Same(hookErr, Failpoints.Trigger(f, "point"));
        Assert.Equal(1, calls);
    }

    [Fact]
    public void TestFailpointsTriggerWithoutHook()
    {
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["point"] = () => new Exception("hook"),
        });

        Assert.Null(Failpoints.Trigger(f, "other"));
    }

    [Fact]
    public void TestFailpointsTriggerNil()
    {
        Failpoints? f = null;

        Assert.Null(Failpoints.Trigger(f, "point"));
    }

    [Fact]
    public void TestFailpointsFailReplacesResult()
    {
        var hookErr = new Exception("hook");
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["point"] = () => hookErr,
        });

        Assert.Same(hookErr, Failpoints.Fail(f, "point", null));
        Assert.Same(hookErr, Failpoints.Fail(f, "point", new Exception("call")));
    }

    [Fact]
    public void TestFailpointsFailKeepsResult()
    {
        var callErr = new Exception("call");
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["point"] = () => null,
        });

        Assert.Null(Failpoints.Fail(f, "point", null));
        Assert.Same(callErr, Failpoints.Fail(f, "point", callErr));
        Assert.Same(callErr, Failpoints.Fail(f, "other", callErr));
    }

    [Fact]
    public void TestFailpointsFailNil()
    {
        Failpoints? f = null;
        var callErr = new Exception("call");

        Assert.Null(Failpoints.Fail(f, "point", null));
        Assert.Same(callErr, Failpoints.Fail(f, "point", callErr));
    }

    [Fact(Skip = "failpoint_disable_test.go (build tag disable_trap): the port has no disabled build flavour")]
    public void TestFailpointsTriggerDisabled()
    {
    }

    [Fact(Skip = "failpoint_disable_test.go (build tag disable_trap): the port has no disabled build flavour")]
    public void TestFailpointsFailDisabled()
    {
    }

    // ----- extra tests (not in Go) --------------------------------------------------------------------------------------
    [Fact]
    public void Enabled_IsTrue()
    {
        Assert.True(Failpoints.Enabled);
    }

    [Fact]
    public void NullHookTable_HasNoHooks()
    {
        var f = Failpoints.New(null);
        Assert.Null(Failpoints.Trigger(f, "point"));
        var callErr = new Exception("call");
        Assert.Same(callErr, Failpoints.Fail(f, "point", callErr));
    }

    [Fact]
    public void ANullHookEntry_IsTreatedAsNoHook()
    {
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>> { ["point"] = null! });
        Assert.Null(Failpoints.Trigger(f, "point"));
    }

    [Fact]
    public void TheHookRunsOnEveryTrigger()
    {
        var calls = 0;
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["point"] = () => ++calls % 2 == 0 ? new Exception("every other") : null,
        });

        Assert.Null(Failpoints.Trigger(f, "point"));
        Assert.NotNull(Failpoints.Trigger(f, "point"));
        Assert.Null(Failpoints.Trigger(f, "point"));
        Assert.Equal(3, calls);
    }

    [Fact]
    public void TheClientFailpointNamesAreIndependent()
    {
        var closeOldStream = new Exception("close");
        var f = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["getWriteStream.closeOldStream"] = () => closeOldStream,
        });

        Assert.Same(closeOldStream, Failpoints.Trigger(f, "getWriteStream.closeOldStream"));
        Assert.Null(Failpoints.Trigger(f, "newCacheImpl.newCache"));
    }
}
