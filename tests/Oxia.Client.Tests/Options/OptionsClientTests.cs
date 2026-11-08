// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_client_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 13, ported: 13, skipped: 0
//   Adaptations:
//     * newClientOptions(addr, opts...) is ClientOptions.TryCreate(addr, span, out options); the options are inspected
//       through their internal members (options.sessionKeepAliveTicker -> SessionKeepAliveTicker, ...).
//     * TestWithMeterProvider: an OpenTelemetry SDK MeterProvider is an IMeterFactory here (a tiny test factory); Go's
//       `WithMeterProvider(nil)` yields a noop provider, the .NET equivalent of "no provider" is the default global
//       "oxia_client" Meter, i.e. a null MeterFactory (DESIGN.md).  TestWithGlobalMeterProvider likewise expects the
//       null factory (the process-wide default Meter) instead of otel.GetMeterProvider().
//     * TestWithTLS: *tls.Config is SslClientAuthenticationOptions.  WithXxx(nil) is WithXxx(null!).
//     * mockResolver / mockAuthentication are the small fakes at the bottom of the Go file.
// =====================================================================================================================
using System.Diagnostics.Metrics;
using System.Net.Security;
using System.Security.Authentication;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Xunit;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class OptionsClientTests
{
    [Fact]
    public void TestWithDialResolver()
    {
        var r = new MockResolver("test");

        var err = ClientOptions.TryCreate("localhost:6648", [WithDialResolver(r)], out var options);
        Assert.Null(err);
        Assert.Same(r, options.Resolver);
    }

    [Fact]
    public void TestWithDialResolver_NotSet()
    {
        var err = ClientOptions.TryCreate("localhost:6648", default, out var options);
        Assert.Null(err);
        Assert.Null(options.Resolver);
    }

    [Fact]
    public void TestWithDialResolver_Nil()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithDialResolver(null!)], out _);
        Assert.NotNull(err);
        Assert.Contains("must not be nil", err.Message);
    }

    [Fact]
    public void TestWithDialResolver_EmptyScheme()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithDialResolver(new MockResolver(""))], out _);
        Assert.NotNull(err);
        Assert.Contains("must not be empty", err.Message);
    }

    [Fact]
    public void TestWithNamespace()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithNamespace("ns")], out var options);
        Assert.Null(err);
        Assert.Equal("ns", options.Namespace);

        err = ClientOptions.TryCreate("localhost:6648", [WithNamespace("")], out options);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionNamespace));
        Assert.Equal(DefaultNamespace, options.Namespace);
    }

    [Fact]
    public void TestWithMeterProvider()
    {
        var meterFactory = new TestMeterFactory();
        var err = ClientOptions.TryCreate("localhost:6648", [WithMeterFactory(meterFactory)], out var options);
        Assert.Null(err);
        Assert.Same(meterFactory, options.MeterFactory);

        // Without a meter factory, the client uses the default (global) Meter, which records nothing until a listener subscribes
        err = ClientOptions.TryCreate("localhost:6648", [WithMeterFactory(null)], out options);
        Assert.Null(err);
        Assert.Null(options.MeterFactory);
    }

    [Fact]
    public void TestWithGlobalMeterProvider()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithGlobalMeterProvider()], out var options);
        Assert.Null(err);
        Assert.Null(options.MeterFactory);   // the process-wide default "oxia_client" Meter
    }

    [Fact]
    public void TestWithSessionTimeout()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithSessionTimeout(TimeSpan.FromSeconds(20))], out var options);
        Assert.Null(err);
        Assert.Equal(TimeSpan.FromSeconds(20), options.SessionTimeout);
        // The session keep-alive defaults to a tenth of its timeout
        Assert.Equal(TimeSpan.FromSeconds(2), options.SessionKeepAliveTicker);

        err = ClientOptions.TryCreate("localhost:6648", [WithSessionTimeout(TimeSpan.Zero)], out options);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionSessionTimeout));
        Assert.Equal(DefaultSessionTimeout, options.SessionTimeout);
    }

    [Fact]
    public void TestWithIdentity()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithIdentity("client-1")], out var options);
        Assert.Null(err);
        Assert.Equal("client-1", options.Identity);

        err = ClientOptions.TryCreate("localhost:6648", [WithIdentity("")], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionIdentity));
    }

    [Fact]
    public void TestWithTLS()
    {
        var tlsConf = new SslClientAuthenticationOptions { EnabledSslProtocols = SslProtocols.Tls13 };
        var err = ClientOptions.TryCreate("localhost:6648", [WithTls(tlsConf)], out var options);
        Assert.Null(err);
        Assert.Same(tlsConf, options.Tls);

        err = ClientOptions.TryCreate("localhost:6648", [WithTls(null!)], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionTls));
    }

    [Fact]
    public void TestWithAuthentication()
    {
        var authentication = new MockAuthentication();
        var err = ClientOptions.TryCreate("localhost:6648", [WithAuthentication(authentication)], out var options);
        Assert.Null(err);
        Assert.Same(authentication, options.Authentication);

        err = ClientOptions.TryCreate("localhost:6648", [WithAuthentication(null!)], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionAuthentication));
    }

    [Fact]
    public void TestWithSessionKeepAliveTicker()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithSessionKeepAliveTicker(TimeSpan.FromSeconds(5))], out var options);
        Assert.Null(err);
        Assert.Equal(TimeSpan.FromSeconds(5), options.SessionKeepAliveTicker);
    }

    // The errors of all the invalid options are reported.
    [Fact]
    public void TestNewClientOptionsReportsAllErrors()
    {
        var err = ClientOptions.TryCreate("localhost:6648", [WithIdentity(""), WithTls(null!)], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionIdentity));
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionTls));
    }

    internal sealed class MockResolver : IServiceResolver
    {
        public MockResolver(string scheme) => Scheme = scheme;

        public string Scheme { get; }

        public void Resolve(string endpoint, AddressUpdater updater)
        {
        }
    }

    private sealed class MockAuthentication : IAuthentication
    {
        public ValueTask<IReadOnlyDictionary<string, string>> GetRequestMetadataAsync(CancellationToken ct, params string[] uri) =>
            ValueTask.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());

        public bool RequireTransportSecurity => false;
    }

    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = new();

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }
        }
    }
}
