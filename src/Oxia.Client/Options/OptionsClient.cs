// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_client.go   (clientOptions, ClientOption, newClientOptions, With* options, defaults, sentinels)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the client.  `newClientOptions` applies every option in order, collecting ALL the errors
//              (multierr.Append), and then computes derived values (the session keep-alive ticker defaults to a tenth of
//              the session timeout).  An invalid option leaves the corresponding value at its default.
//   Divergences:
//      * `clientOptions` (a Go value copied through each option) is a class mutated in place by the options; an option
//        validates BEFORE assigning, so a rejected option leaves the previous value untouched exactly like Go.
//      * `newClientOptions(...) (clientOptions, error)` is ClientOptions.TryCreate: the options are always returned (also
//        with errors, tests inspect them) and the error is null / the single error / an AggregateException (multierr;
//        `OxiaErrors.Is` finds each member).  Its message joins the members with "; " like multierr.Error().
//      * WithMeterProvider(otel provider) -> WithMeterFactory(IMeterFactory?): .NET metrics are always created from a
//        Meter; null means the process-wide default "oxia_client" Meter (listeners such as OpenTelemetry's MeterListener
//        attach to it by name), which is also what WithGlobalMeterProvider() selects.  Go's default is a noop provider
//        (no metrics unless asked); a .NET Meter without listeners costs nothing, so the behaviours coincide until a
//        listener subscribes.
//      * WithTLS(*tls.Config) -> WithTls(SslClientAuthenticationOptions).  WithFailureInjection(null) = no failures.
//      * `clientOptions.RequestTimeout()` is the RequestTimeout field.  The default identity is a random GUID (uuid v4,
//        lowercase hyphenated like uuid.NewString()).
//      * EXTENSION: OxiaOptions.WithMaxBatchSize(int).  Go has DefaultMaxBatchSize, the maxBatchSize field (used by the
//        write batchers) and ErrInvalidOptionMaxBatchSize, but NO option function to set it; the .NET client exposes it.
//        Validation follows the declared sentinel (value must be > 0).
//      * Computed option: Go's `defaultTicker := sessionTimeout / 10` is integer nanosecond division; here the same on
//        TimeSpan ticks (100 ns).
// =====================================================================================================================
using System.Diagnostics.Metrics;
using System.Net.Security;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Proto;

namespace Oxia.Client;

/// <summary>Go: ClientOption (apply(clientOptions) (clientOptions, error)).</summary>
public sealed class ClientOption
{
    /// <summary>Applies the option to the options being built; returns the validation error or null.</summary>
    internal readonly Func<ClientOptions, Exception?> Apply;

    internal ClientOption(Func<ClientOptions, Exception?> apply) => Apply = apply;
}

public static partial class OxiaOptions
{
    // Defaults (options_client.go)
    public static readonly TimeSpan DefaultBatchLinger = TimeSpan.FromMilliseconds(5);
    public const int DefaultMaxRequestsPerBatch = 1000;
    public const int DefaultMaxBatchSize = 128 * 1024;
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan DefaultSessionTimeout = TimeSpan.FromSeconds(15);
    public const string DefaultNamespace = Constants.DefaultNamespace;

    /// <summary>Sets the Oxia namespace to be used for this client.  If not set, the client will be using the `default` namespace.</summary>
    public static ClientOption WithNamespace(string @namespace) => new(options =>
    {
        if (string.IsNullOrEmpty(@namespace)) return ClientErrors.InvalidOptionNamespace;
        options.Namespace = @namespace;
        return null;
    });

    /// <summary>
    /// Defines how long the batcher will wait before sending a batched request.  The value must be greater than or equal
    /// to zero.  A value of zero will disable linger, effectively disabling batching.
    /// </summary>
    public static ClientOption WithBatchLinger(TimeSpan batchLinger) => new(options =>
    {
        if (batchLinger < TimeSpan.Zero) return ClientErrors.InvalidOptionBatchLinger;
        options.BatchLinger = batchLinger;
        return null;
    });

    /// <summary>
    /// Defines how many individual requests a batch can contain before the batched request is sent.  The value must be
    /// greater than zero.  A value of one will effectively disable batching.
    /// </summary>
    public static ClientOption WithMaxRequestsPerBatch(int maxRequestsPerBatch) => new(options =>
    {
        if (maxRequestsPerBatch <= 0) return ClientErrors.InvalidOptionMaxRequestsPerBatch;
        options.MaxRequestsPerBatch = maxRequestsPerBatch;
        return null;
    });

    /// <summary>
    /// EXTENSION (not in Go): the maximum size in bytes of a write batch (default <see cref="DefaultMaxBatchSize"/>).
    /// Must be greater than zero (<see cref="ClientErrors.InvalidOptionMaxBatchSize"/>).
    /// </summary>
    public static ClientOption WithMaxBatchSize(int maxBatchSize) => new(options =>
    {
        if (maxBatchSize <= 0) return ClientErrors.InvalidOptionMaxBatchSize;
        options.MaxBatchSize = maxBatchSize;
        return null;
    });

    /// <summary>Defines how long the client will wait for responses before cancelling the request and failing the request.</summary>
    public static ClientOption WithRequestTimeout(TimeSpan requestTimeout) => new(options =>
    {
        if (requestTimeout <= TimeSpan.Zero) return ClientErrors.InvalidOptionRequestTimeout;
        options.RequestTimeout = requestTimeout;
        return null;
    });

    /// <summary>Go: WithMeterProvider(provider). .NET: an IMeterFactory (null => the default "oxia_client" Meter, discoverable by OpenTelemetry/MeterListener).</summary>
    public static ClientOption WithMeterFactory(IMeterFactory? meterFactory) => new(options =>
    {
        options.MeterFactory = meterFactory;
        return null;
    });

    /// <summary>Go: WithGlobalMeterProvider() -- the process-wide provider; .NET: the default global "oxia_client" Meter (MeterFactory = null).</summary>
    public static ClientOption WithGlobalMeterProvider() => WithMeterFactory(null);

    /// <summary>Specifies the session timeout.</summary>
    public static ClientOption WithSessionTimeout(TimeSpan sessionTimeout) => new(options =>
    {
        if (sessionTimeout <= TimeSpan.Zero) return ClientErrors.InvalidOptionSessionTimeout;
        options.SessionTimeout = sessionTimeout;
        return null;
    });

    /// <summary>Sets the identity of the client (default: a random GUID).</summary>
    public static ClientOption WithIdentity(string identity) => new(options =>
    {
        if (string.IsNullOrEmpty(identity)) return ClientErrors.InvalidOptionIdentity;
        options.Identity = identity;
        return null;
    });

    /// <summary>Go: WithTLS(*tls.Config).</summary>
    public static ClientOption WithTls(SslClientAuthenticationOptions tls) => new(options =>
    {
        if (tls is null) return ClientErrors.InvalidOptionTls;
        options.Tls = tls;
        return null;
    });

    public static ClientOption WithAuthentication(IAuthentication authentication) => new(options =>
    {
        if (authentication is null) return ClientErrors.InvalidOptionAuthentication;
        options.Authentication = authentication;
        return null;
    });

    /// <summary>Go: WithFailureInjection(failures) -- replaces the set of injected failures (null/empty = none).</summary>
    public static ClientOption WithFailureInjection(IEnumerable<Failure> failures) => new(options =>
    {
        options.FailureInjection = failures is null ? new HashSet<Failure>() : new HashSet<Failure>(failures);
        return null;
    });

    /// <summary>Internal API used to control the duration of the session keep-alive ticker.  This is for experimental use only.</summary>
    public static ClientOption WithSessionKeepAliveTicker(TimeSpan ticker) => new(options =>
    {
        options.SessionKeepAliveTicker = ticker;
        return null;
    });
}

/// <summary>Go: clientOptions (value type in Go; here a class, mutated only while options are applied).</summary>
internal sealed class ClientOptions
{
    public string ServiceAddress = "";
    public string Namespace = OxiaOptions.DefaultNamespace;
    public TimeSpan BatchLinger = OxiaOptions.DefaultBatchLinger;
    public int MaxRequestsPerBatch = OxiaOptions.DefaultMaxRequestsPerBatch;
    public int MaxBatchSize = OxiaOptions.DefaultMaxBatchSize;
    public TimeSpan RequestTimeout = OxiaOptions.DefaultRequestTimeout;
    public IMeterFactory? MeterFactory;
    public TimeSpan SessionTimeout = OxiaOptions.DefaultSessionTimeout;
    public string Identity = "";
    public SslClientAuthenticationOptions? Tls;
    public IAuthentication? Authentication;
    public TimeSpan SessionKeepAliveTicker;
    public HashSet<Failure> FailureInjection = new();
    public IServiceResolver? Resolver;

    /// <summary>
    /// Go: newClientOptions(serviceAddress, opts...).  All option errors are combined (AggregateException, like multierr)
    /// and returned, not thrown.  Computed option: SessionKeepAliveTicker = max(SessionKeepAliveTicker, SessionTimeout/10).
    /// </summary>
    public static Exception? TryCreate(string serviceAddress, ReadOnlySpan<ClientOption> opts, out ClientOptions result)
    {
        var options = new ClientOptions
        {
            ServiceAddress = serviceAddress,
            Identity = Guid.NewGuid().ToString(),   // defaultIdentity(): uuid.NewString()
        };
        result = options;

        List<Exception>? errs = null;
        foreach (var o in opts)
        {
            var err = o?.Apply(options);
            if (err is not null)
            {
                (errs ??= new List<Exception>(2)).Add(err);
            }
        }

        // ======== computed options =========
        var defaultTicker = TimeSpan.FromTicks(options.SessionTimeout.Ticks / 10);
        if (defaultTicker > options.SessionKeepAliveTicker)
        {
            options.SessionKeepAliveTicker = defaultTicker;
        }

        return Combine(errs);
    }

    /// <summary>multierr.Combine: null for no errors, the error itself for a single one, else an AggregateException.</summary>
    internal static Exception? Combine(List<Exception>? errs)
    {
        if (errs is null || errs.Count == 0) return null;
        if (errs.Count == 1) return errs[0];

        return new MultiError(errs);
    }
}

/// <summary>go.uber.org/multierr's combined error: an AggregateException whose message joins the members with "; ".</summary>
internal sealed class MultiError : AggregateException
{
    private readonly string _message;

    public MultiError(IReadOnlyList<Exception> errors) : base(errors)
    {
        var messages = new string[errors.Count];
        for (int i = 0; i < messages.Length; i++)
        {
            messages[i] = errors[i].Message;
        }

        _message = string.Join("; ", messages);
    }

    public override string Message => _message;
}
