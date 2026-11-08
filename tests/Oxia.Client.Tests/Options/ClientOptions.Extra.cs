// =====================================================================================================================
// PORT NOTE   (extra tests -- not in Go)
//   Upstream : oxia/options_client.go behaviour that the Go tests do not pin down
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Client option defaults, application order, error accumulation (multierr semantics), the computed session
//              keep-alive ticker, the default identity, and the WithMaxBatchSize extension that Go lacks.
// =====================================================================================================================
using System.Diagnostics.Metrics;
using Oxia.Common.Constant;
using Xunit;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class ClientOptionsExtraTests
{
    private static Exception? Create(out ClientOptions options, params ClientOption[] opts) =>
        ClientOptions.TryCreate("localhost:6648", opts, out options);

    [Fact]
    public void Defaults_MatchGo()
    {
        Assert.Null(Create(out var o));
        Assert.Equal("localhost:6648", o.ServiceAddress);
        Assert.Equal("default", o.Namespace);
        Assert.Equal(TimeSpan.FromMilliseconds(5), o.BatchLinger);
        Assert.Equal(1000, o.MaxRequestsPerBatch);
        Assert.Equal(128 * 1024, o.MaxBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(30), o.RequestTimeout);
        Assert.Equal(TimeSpan.FromSeconds(15), o.SessionTimeout);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), o.SessionKeepAliveTicker);   // 15s / 10
        Assert.Null(o.MeterFactory);
        Assert.Null(o.Tls);
        Assert.Null(o.Authentication);
        Assert.Null(o.Resolver);
        Assert.Empty(o.FailureInjection);
    }

    [Fact]
    public void DefaultIdentity_IsAFreshLowercaseGuidEachTime()
    {
        Assert.Null(Create(out var a));
        Assert.Null(Create(out var b));
        Assert.True(Guid.TryParseExact(a.Identity, "D", out _), a.Identity);
        Assert.Equal(a.Identity.ToLowerInvariant(), a.Identity);
        Assert.NotEqual(a.Identity, b.Identity);

        Assert.Null(Create(out var c, WithIdentity("me")));
        Assert.Equal("me", c.Identity);
    }

    [Fact]
    public void SessionKeepAliveTicker_IsTheMaxOfTheConfiguredAndATenthOfTheTimeout()
    {
        // smaller than session/10: raised to session/10
        Assert.Null(Create(out var o, WithSessionKeepAliveTicker(TimeSpan.FromMilliseconds(100))));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), o.SessionKeepAliveTicker);

        // larger: kept
        Assert.Null(Create(out o, WithSessionKeepAliveTicker(TimeSpan.FromSeconds(5))));
        Assert.Equal(TimeSpan.FromSeconds(5), o.SessionKeepAliveTicker);

        // option order does not matter: the computation happens after all options were applied
        Assert.Null(Create(out o, WithSessionKeepAliveTicker(TimeSpan.FromSeconds(1)), WithSessionTimeout(TimeSpan.FromSeconds(100))));
        Assert.Equal(TimeSpan.FromSeconds(10), o.SessionKeepAliveTicker);
        Assert.Null(Create(out o, WithSessionTimeout(TimeSpan.FromSeconds(100)), WithSessionKeepAliveTicker(TimeSpan.FromSeconds(1))));
        Assert.Equal(TimeSpan.FromSeconds(10), o.SessionKeepAliveTicker);

        // an invalid session timeout leaves the default, so the ticker is derived from it
        Assert.NotNull(Create(out o, WithSessionTimeout(TimeSpan.FromSeconds(-1))));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), o.SessionKeepAliveTicker);
    }

    [Fact]
    public void LastOptionWins()
    {
        Assert.Null(Create(out var o, WithNamespace("a"), WithNamespace("b"), WithBatchLinger(TimeSpan.FromMilliseconds(1)),
            WithBatchLinger(TimeSpan.FromMilliseconds(2)), WithMaxRequestsPerBatch(5), WithMaxRequestsPerBatch(6)));
        Assert.Equal("b", o.Namespace);
        Assert.Equal(TimeSpan.FromMilliseconds(2), o.BatchLinger);
        Assert.Equal(6, o.MaxRequestsPerBatch);
    }

    [Fact]
    public void InvalidOption_DoesNotUndoTheValueOfAPreviousValidOne()
    {
        var err = Create(out var o, WithNamespace("keep"), WithNamespace(""));
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionNamespace));
        Assert.Equal("keep", o.Namespace);
    }

    [Fact]
    public void SingleError_StaysItself_AndSeveralAreAnAggregateOfAllOfThem()
    {
        var single = Create(out _, WithNamespace(""));
        Assert.Same(ClientErrors.InvalidOptionNamespace, single);

        var several = Create(out _, WithNamespace(""), WithBatchLinger(TimeSpan.FromTicks(-1)), WithNamespace("ok"), WithIdentity(""));
        var agg = Assert.IsAssignableFrom<AggregateException>(several);
        Assert.Equal(3, agg.InnerExceptions.Count);
        Assert.Same(ClientErrors.InvalidOptionNamespace, agg.InnerExceptions[0]);
        Assert.Same(ClientErrors.InvalidOptionBatchLinger, agg.InnerExceptions[1]);
        Assert.Same(ClientErrors.InvalidOptionIdentity, agg.InnerExceptions[2]);
        Assert.Equal($"{ClientErrors.InvalidOptionNamespace.Message}; {ClientErrors.InvalidOptionBatchLinger.Message}; {ClientErrors.InvalidOptionIdentity.Message}", agg.Message);
        Assert.True(OxiaErrors.Is(several, ClientErrors.InvalidOptionBatchLinger));
        Assert.False(OxiaErrors.Is(several, ClientErrors.InvalidOptionTls));
    }

    [Fact]
    public void NullOptionEntries_AreSkipped()
    {
        Assert.Null(Create(out var o, null!, WithNamespace("n")));
        Assert.Equal("n", o.Namespace);
    }

    [Fact]
    public void WithMaxBatchSize_IsAnExtension_ValidatedAgainstTheDeclaredSentinel()
    {
        Assert.Null(Create(out var o, WithMaxBatchSize(1024)));
        Assert.Equal(1024, o.MaxBatchSize);

        foreach (var invalid in new[] { 0, -1 })
        {
            var err = Create(out o, WithMaxBatchSize(invalid));
            Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionMaxBatchSize));
            Assert.Equal(DefaultMaxBatchSize, o.MaxBatchSize);
        }
    }

    [Fact]
    public void WithFailureInjection_ReplacesTheSet()
    {
        Assert.Null(Create(out var o, WithFailureInjection([Failure.DizzyShardManager])));
        Assert.Contains(Failure.DizzyShardManager, o.FailureInjection);

        Assert.Null(Create(out o, WithFailureInjection([Failure.DizzyShardManager]), WithFailureInjection([])));
        Assert.Empty(o.FailureInjection);

        Assert.Null(Create(out o, WithFailureInjection(null!)));
        Assert.Empty(o.FailureInjection);
        Assert.Equal(1, (int)Failure.DizzyShardManager);
    }

    [Fact]
    public void MeterFactory_LastOptionWins_AndGlobalMeansNull()
    {
        using var factory = new SimpleMeterFactory();
        Assert.Null(Create(out var o, WithMeterFactory(factory), WithGlobalMeterProvider()));
        Assert.Null(o.MeterFactory);
        Assert.Null(Create(out o, WithGlobalMeterProvider(), WithMeterFactory(factory)));
        Assert.Same(factory, o.MeterFactory);
    }

    [Fact]
    public void WithDialResolver_ErrorMessagesAreVerbatim()
    {
        Assert.Equal("ServiceResolver must not be nil", Create(out _, WithDialResolver(null!))!.Message);
        Assert.Equal("ServiceResolver.Scheme() must not be empty", Create(out _, WithDialResolver(new OptionsClientTests.MockResolver("")))!.Message);
    }

    [Fact]
    public void SessionTimeoutAndRequestTimeout_RejectNonPositive_BatchLingerAcceptsZero()
    {
        Assert.NotNull(Create(out _, WithSessionTimeout(TimeSpan.Zero)));
        Assert.NotNull(Create(out _, WithRequestTimeout(TimeSpan.Zero)));
        Assert.Null(Create(out var o, WithBatchLinger(TimeSpan.Zero)));
        Assert.Equal(TimeSpan.Zero, o.BatchLinger);
    }

    private sealed class SimpleMeterFactory : IMeterFactory
    {
        public Meter Create(MeterOptions options) => new(options);

        public void Dispose()
        {
        }
    }
}
