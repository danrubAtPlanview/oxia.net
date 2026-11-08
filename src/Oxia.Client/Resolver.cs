// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/resolver.go   (ServiceResolver, AddressUpdater, WithDialResolver, grpcResolverBuilder, grpcResolver)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Lets applications plug their own service discovery into the gRPC channels of the client.  A
//              ServiceResolver handles a URI scheme (e.g. "dns", "tls"); when the service address is `scheme:///endpoint`
//              gRPC asks the adapter for the addresses of `endpoint`, the ServiceResolver pushes them (at least once, and
//              again whenever they change) through the AddressUpdater, and the adapter forwards them to the channel as
//              resolver results.  The adapter itself never resolves on its own: ResolveNow/Refresh and Close are no-ops.
//   Divergences:
//      * grpc-go's `resolver.Builder` registered with `grpc.WithResolvers(builder)` -> a Grpc.Net.Client
//        `Balancer.ResolverFactory` registered through `GrpcChannelOptions.ServiceProvider` (the only registration point
//        of the .NET channel).  `GrpcResolverBuilder.AsDialOption(sr)` is that `grpc.DialOption`: it wraps any service
//        provider already configured on the options so that other registrations keep working.
//      * grpc-go calls `Build` (and so `ServiceResolver.Resolve`) when the channel is created; the .NET channel calls
//        `Resolver.Start(listener)` when it first connects, which is where `Resolve` is invoked (once per Start).
//      * `target.Endpoint()` (path, or opaque part, without ONE leading "/") is derived from `ResolverOptions.Address`:
//        `test:///service` and `test:service` both give "service".
//      * Addresses "host:port" become BalancerAddress(host, port) ("[::1]:6648" is understood; a missing port falls back
//        to the channel's default port).  grpc-go's UpdateState error becomes the return value of the updater: an empty
//        address list is still pushed (the channel then reports it) but is returned as an error so that the resolver tries
//        again; after the resolver is disposed the updater returns an error and pushes nothing.
//      * WithDialResolver validations are byte-identical: "ServiceResolver must not be nil" and
//        "ServiceResolver.Scheme() must not be empty".
// =====================================================================================================================
using System.Net;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Oxia.Common.Constant;

namespace Oxia.Client;

/// <summary>
/// Resolves a service name to a set of network addresses (Go: ServiceResolver).  Resolve may be called multiple times with
/// different endpoints and concurrently.  Implementations must be safe for concurrent use.  The caller owns the resolver's
/// lifecycle.
/// </summary>
public interface IServiceResolver
{
    /// <summary>Returns the URI scheme handled by this resolver (e.g. "dns", "tls").</summary>
    string Scheme { get; }

    /// <summary>
    /// Resolves the given endpoint and calls <paramref name="updater"/> with the addresses.  The updater must be called at
    /// least once, and again whenever addresses change.  Resolve must return promptly; launch long-running work in a task.
    /// </summary>
    void Resolve(string endpoint, AddressUpdater updater);
}

/// <summary>
/// A callback that the <see cref="IServiceResolver"/> uses to push updated addresses ("host:port") to the underlying
/// transport (Go: AddressUpdater).  If an error is returned, the resolver should try to resolve the target again.  The
/// resolver should use a backoff timer to prevent overloading the server with requests.  If a resolver is certain that
/// reresolving will not change the result, e.g. because it is a watch-based resolver, returned errors can be ignored.
/// </summary>
public delegate Exception? AddressUpdater(IReadOnlyList<string> addresses);

public static partial class OxiaOptions
{
    /// <summary>Configures the client to use a custom <see cref="IServiceResolver"/> for discovering server addresses.</summary>
    public static ClientOption WithDialResolver(IServiceResolver sr) => new(options =>
    {
        if (sr is null)
        {
            return new OxiaException("ServiceResolver must not be nil");
        }

        if (string.IsNullOrEmpty(sr.Scheme))
        {
            return new OxiaException("ServiceResolver.Scheme() must not be empty");
        }

        options.Resolver = sr;
        return null;
    });
}

/// <summary>Go: grpcResolverBuilder -- adapts a ServiceResolver to the gRPC resolver interfaces.</summary>
internal static class GrpcResolverBuilder
{
    /// <summary>Go: grpc.WithResolvers(&amp;grpcResolverBuilder{sr}) as a channel option.</summary>
    public static Action<GrpcChannelOptions> AsDialOption(IServiceResolver sr)
    {
        ArgumentNullException.ThrowIfNull(sr);
        var factory = CreateFactory(sr);
        return options => options.ServiceProvider = new ResolverServiceProvider(factory, options.ServiceProvider);
    }

    /// <summary>The gRPC resolver factory (Go: the grpcResolverBuilder itself, whose Scheme() is the ServiceResolver's).</summary>
    public static ResolverFactory CreateFactory(IServiceResolver sr) => new ServiceResolverFactory(sr);

    private sealed class ServiceResolverFactory : ResolverFactory
    {
        private readonly IServiceResolver _sr;

        public ServiceResolverFactory(IServiceResolver sr) => _sr = sr;

        public override string Name => _sr.Scheme;

        /// <summary>Go: Build.</summary>
        public override Resolver Create(ResolverOptions options) => new ServiceResolverAdapter(_sr, options);
    }

    /// <summary>Go: grpcResolver -- a resolver that never resolves by itself; all updates are pushed by the ServiceResolver.</summary>
    private sealed class ServiceResolverAdapter : Resolver
    {
        private readonly IServiceResolver _sr;
        private readonly string _endpoint;
        private readonly int _defaultPort;
        private volatile bool _disposed;

        public ServiceResolverAdapter(IServiceResolver sr, ResolverOptions options)
        {
            _sr = sr;
            _endpoint = EndpointOf(options.Address);
            _defaultPort = options.DefaultPort;
        }

        public override void Start(Action<ResolverResult> listener)
        {
            _sr.Resolve(_endpoint, addresses => UpdateState(listener, addresses));
        }

        /// <summary>Go: ResolveNow -- left to the ServiceResolver.</summary>
        public override void Refresh()
        {
        }

        protected override void Dispose(bool disposing)
        {
            _disposed = true;
        }

        private Exception? UpdateState(Action<ResolverResult> listener, IReadOnlyList<string> addresses)
        {
            if (_disposed)
            {
                return new OxiaException("resolver is closed");
            }

            var addrs = new List<BalancerAddress>(addresses.Count);
            foreach (var addr in addresses)
            {
                addrs.Add(ToBalancerAddress(addr, _defaultPort));
            }

            listener(ResolverResult.ForResult(addrs));
            return addrs.Count == 0 ? new OxiaException("resolver returned no addresses") : null;
        }
    }

    /// <summary>resolver.Target.Endpoint(): the path (or the opaque part) of the target without ONE leading "/".</summary>
    internal static string EndpointOf(Uri address)
    {
        var endpoint = Uri.UnescapeDataString(address.AbsolutePath);
        return endpoint.StartsWith('/') ? endpoint.Substring(1) : endpoint;
    }

    /// <summary>"host:port", "[ipv6]:port" or "host" (default port) to a BalancerAddress.</summary>
    internal static BalancerAddress ToBalancerAddress(string address, int defaultPort)
    {
        if (address.StartsWith('['))
        {
            int close = address.IndexOf(']');
            if (close > 0)
            {
                var host6 = address.Substring(1, close - 1);
                if (close + 2 <= address.Length && address[close + 1] == ':' &&
                    int.TryParse(address.AsSpan(close + 2), out var port6))
                {
                    return new BalancerAddress(host6, port6);
                }

                return new BalancerAddress(host6, defaultPort);
            }
        }

        int colon = address.LastIndexOf(':');
        if (colon > 0 && address.IndexOf(':') == colon && int.TryParse(address.AsSpan(colon + 1), out var port))
        {
            return new BalancerAddress(address.Substring(0, colon), port);
        }

        return new BalancerAddress(address, defaultPort);
    }

    /// <summary>Exposes the adapter's factory to the channel (the only way to register a ResolverFactory in .NET).</summary>
    private sealed class ResolverServiceProvider : IServiceProvider
    {
        private readonly ResolverFactory _factory;
        private readonly IServiceProvider? _inner;

        public ResolverServiceProvider(ResolverFactory factory, IServiceProvider? inner)
        {
            _factory = factory;
            _inner = inner;
        }

        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(IEnumerable<ResolverFactory>))
            {
                var innerFactories = _inner?.GetService(serviceType) as IEnumerable<ResolverFactory>;
                var all = new List<ResolverFactory> { _factory };
                if (innerFactories is not null)
                {
                    all.AddRange(innerFactories);
                }

                return all;
            }

            return _inner?.GetService(serviceType);
        }
    }
}
