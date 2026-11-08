// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/model/call_context_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 5 (TestCallContext_WithoutContextIsSent, _SentCallIsNotDropped, _CancelledCallIsDropped,
//              _CallIsDroppedOnceTheContextIsDone, TestNotSent)   ported: 5   skipped: 0
//   Adaptations: context.Context -> CancellationToken; `var callContext *CallContext` (nil receiver) -> the static null-safe
//              CallContext.MarkSent(null); assert.ErrorIs(err, context.Canceled) -> an OperationCanceledException is in the
//              chain of the error (the token cannot tell Canceled from DeadlineExceeded); context.DeadlineExceeded -> a
//              TimeoutException instance matched by reference.  Assertions are unchanged.
// =====================================================================================================================
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Model;

public class CallContextTests
{
    /// <summary>assert.ErrorIs(t, err, context.Canceled).</summary>
    internal static bool IsCanceled(Exception? err)
    {
        for (; err is not null; err = err.InnerException)
        {
            if (err is OperationCanceledException) return true;
        }
        return false;
    }

    [Fact]
    public void TestCallContext_WithoutContextIsSent()
    {
        CallContext? callContext = null;
        Assert.True(CallContext.MarkSent(callContext));
    }

    [Fact]
    public void TestCallContext_SentCallIsNotDropped()
    {
        using var cts = new CancellationTokenSource();
        var callContext = new CallContext(cts.Token);
        Assert.True(callContext.MarkSent());

        cts.Cancel();
        var err = callContext.Cancel();
        Assert.True(IsCanceled(err));
        Assert.False(CallContext.IsNotSent(err));
        Assert.True(callContext.MarkSent());
    }

    [Fact]
    public void TestCallContext_CancelledCallIsDropped()
    {
        using var cts = new CancellationTokenSource();
        var callContext = new CallContext(cts.Token);

        cts.Cancel();
        var err = callContext.Cancel();
        Assert.True(IsCanceled(err));
        Assert.True(CallContext.IsNotSent(err));
        Assert.False(callContext.MarkSent());
    }

    // The call is dropped once the context is done, even if the caller did not
    // cancel it yet.
    [Fact]
    public void TestCallContext_CallIsDroppedOnceTheContextIsDone()
    {
        using var cts = new CancellationTokenSource();
        var callContext = new CallContext(cts.Token);

        cts.Cancel();
        Assert.False(callContext.MarkSent());
        Assert.True(CallContext.IsNotSent(callContext.Err()));
        Assert.True(CallContext.IsNotSent(callContext.Cancel()));
    }

    [Fact]
    public void TestNotSent()
    {
        Assert.Null(CallContext.NotSent(null));

        var deadlineExceeded = new TimeoutException("context deadline exceeded");
        var err = CallContext.NotSent(deadlineExceeded);
        Assert.NotNull(err);
        Assert.True(OxiaErrors.Is(err, deadlineExceeded));
        Assert.Equal(deadlineExceeded.Message, err.Message);
        Assert.True(CallContext.IsNotSent(err));
        Assert.False(CallContext.IsNotSent(deadlineExceeded));
        Assert.False(CallContext.IsNotSent(new Exception("failure")));
    }
}
