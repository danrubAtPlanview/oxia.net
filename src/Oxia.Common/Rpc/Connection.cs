// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/connection.go   (connection, newConnection, newConnectionWithHealthConfig, healthCheckLoop,
//              notifyDisconnect, Close)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : One gRPC connection to a target, as pooled by ClientPool:
//              * target syntax: an optional `tls://` prefix selects TLS (a TLS configuration always does); a target without a scheme
//                (`host:port`) is dialled directly -- the name is resolved by the connection layer on EVERY (re)connect, never cached,
//                like Go's `passthrough:///` default; a target with a scheme (`dns:///x`, `custom:///x`) is handed to the channel as is
//                (a custom resolver is registered through a dial option);
//              * keepalive pings every 10s with a 3s timeout, also without active calls; unlimited streams per connection;
//                responses up to 256 MiB;
//              * per-RPC credentials from IAuthentication (refused over plaintext when they require transport security);
//              * the application level health loop: grpc.health.v1 Check every 10s with a 10s deadline; a NOT_SERVING answer drops the
//                connection at once, a failed ping only after 3 consecutive failures (a saturated server may answer late), a success
//                resets the count, Unimplemented stops the pings; the failure callback runs on its own task, so Close (which waits for
//                the health loop) can be called from it.
//   Divergences:
//      * grpc.ClientConn -> GrpcChannel (+ a metrics-intercepted CallInvoker; the grpc_client_* counters are on every call,
//        health pings included, like Go's chained interceptors).  grpc.DialOption -> Action<GrpcChannelOptions>, applied last.
//      * credentials.NewTLS(&tls.Config{MinVersion: TLS12}) -> SocketsHttpHandler.SslOptions = TLS 1.2+1.3 (or the configured
//        SslClientAuthenticationOptions).  WithPerRPCCredentials -> CallCredentials (+ UnsafeUseInsecureChannelCallCredentials when the
//        credentials do not require transport security).
//      * context.WithTimeout(ping) -> the call deadline; ticker -> PeriodicTimer; wg.Go -> one Task; sync.Once -> Interlocked.
//      * The Go error "grpc: the credentials require transport level security (use grpc.WithTransportCredentials() to set)" is
//        raised by us (grpc.NewClient does it in Go), wrapped as "error connecting to <target>: ...".
//      * Close cannot fail (GrpcChannel.Dispose does not report errors); ClientPool still guards against it.
//      * The OxiaClientRpc / OxiaAdminClient facades are created once per connection (they are stateless over the CallInvoker).
// =====================================================================================================================
using System.Net.Security;
using System.Security.Authentication;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Health.V1;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Logging;

namespace Oxia.Common.Rpc;

/// <summary>Go: <c>connectionFailureCallback</c> -- invoked (on its own task) once, when the health loop gives up on the connection.</summary>
internal delegate void ConnectionFailureCallback(string target);

internal sealed class Connection : IAsyncCloser
{
    internal static readonly TimeSpan DefaultGrpcClientKeepAliveTime = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan DefaultGrpcClientKeepAliveTimeout = TimeSpan.FromSeconds(3);
    internal const bool DefaultGrpcClientPermitWithoutStream = true;

    /// <summary>grpc-go's <c>MinConnectTimeout</c>: how long a single connection attempt may take.</summary>
    internal static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(20);

    // Cadence and per-ping timeout of the application-level connection health loop. The ping timeout is deliberately generous: it
    // shares the connection with the data traffic, so on a saturated server a slow ping does not mean the connection is dead.
    internal static readonly TimeSpan DefaultConnectionHealthInterval = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan DefaultConnectionHealthPingTimeout = TimeSpan.FromSeconds(10);

    // The number of consecutive failed health pings after which the connection is considered broken. Closing a pooled connection
    // kills every stream multiplexed on it, so a single missed ping on a saturated-but-healthy server must not tear it down.
    internal const int ConnectionHealthFailureThreshold = 3;

    private readonly CancellationTokenSource _cts = new();
    private readonly ConnectionFailureCallback _onFailure;
    private readonly TimeSpan _healthPingTimeout;
    private readonly TimeSpan _healthInterval;
    private readonly ILogger _log;
    private Task _healthLoop = Task.CompletedTask;
    private int _closed;
    private int _disconnected;

    private IOxiaClientRpc? _clientRpc;
    private Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient? _adminRpc;

    private Connection(string target, GrpcChannel channel, ConnectionFailureCallback onFailure, TimeSpan healthInterval,
        TimeSpan healthPingTimeout, ILogger log)
    {
        Target = target;
        Channel = channel;
        CallInvoker = channel.CreateCallInvoker().Intercept(GrpcClientMetricsInterceptor.Default);
        HealthClient = new Health.HealthClient(CallInvoker);
        _onFailure = onFailure;
        _healthInterval = healthInterval;
        _healthPingTimeout = healthPingTimeout;
        _log = log;
    }

    public string Target { get; }

    /// <summary>The underlying channel (Go: the embedded *grpc.ClientConn).</summary>
    public GrpcChannel Channel { get; }

    /// <summary>The calls of this connection, with the grpc_client_* metrics.</summary>
    public CallInvoker CallInvoker { get; }

    /// <summary>Go: connection.healthClient.</summary>
    public Health.HealthClient HealthClient { get; }

    /// <summary>True once the health loop gave up on the connection (Go: connection.disconnected).</summary>
    internal bool Disconnected => Volatile.Read(ref _disconnected) != 0;

    public IOxiaClientRpc ClientRpc => _clientRpc ??= new OxiaClientRpc(CallInvoker);

    public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient AdminRpc => _adminRpc ??= new Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient(CallInvoker);

    /// <summary>Go: newConnection.</summary>
    internal static Connection Create(string target, SslClientAuthenticationOptions? tlsConf, IAuthentication? authentication,
        Action<GrpcChannelOptions>[] dialOptions, ConnectionFailureCallback onFailure)
        => CreateWithHealthConfig(target, tlsConf, authentication, dialOptions, onFailure,
            DefaultConnectionHealthInterval, DefaultConnectionHealthPingTimeout);

    /// <summary>Go: newConnectionWithHealthConfig.</summary>
    internal static Connection CreateWithHealthConfig(string target, SslClientAuthenticationOptions? tlsConf, IAuthentication? authentication,
        Action<GrpcChannelOptions>[] dialOptions, ConnectionFailureCallback onFailure, TimeSpan healthInterval, TimeSpan healthPingTimeout)
    {
        var log = OxiaLog.Create("rpc-connection");
        log.LogDebug("Creating new GRPC connection. target={Target}", target);

        var actualAddress = target;
        var useTls = false;
        if (target.StartsWith(ClientPool.AddressSchemaTls, StringComparison.Ordinal))
        {
            actualAddress = target[ClientPool.AddressSchemaTls.Length..];
            useTls = true;
        }

        if (tlsConf is not null)
        {
            useTls = true;
        }

        // A plain host:port is dialled directly (SocketsHttpHandler resolves the name on every new connection, it never caches it);
        // an address with a scheme is handed to the channel's resolver machinery.
        var hasScheme = actualAddress.Contains("://", StringComparison.Ordinal);
        var channelAddress = hasScheme ? actualAddress : (useTls ? "https://" : "http://") + actualAddress;
        var httpAddress = channelAddress.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
                          channelAddress.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

        var handler = new SocketsHttpHandler
        {
            // The connection stays open while idle; the keepalive pings (also without active calls) detect a dead server.
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = DefaultGrpcClientKeepAliveTime,
            KeepAlivePingTimeout = DefaultGrpcClientKeepAliveTimeout,
            KeepAlivePingPolicy = DefaultGrpcClientPermitWithoutStream ? HttpKeepAlivePingPolicy.Always : HttpKeepAlivePingPolicy.WithActiveRequests,
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = DefaultConnectTimeout, // grpc-go's minimum connect timeout
        };
        if (useTls)
        {
            handler.SslOptions = tlsConf ?? new SslClientAuthenticationOptions { EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13 };
        }

        try
        {
            var options = new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = Constants.MaxGrpcFrameSize,
            };

            // The transport credentials.  For http(s) addresses the channel reads them from the scheme; with a resolver scheme
            // (`custom:///x`) it cannot, and refuses to be created unless the credentials are given.
            var transportCredentials = useTls ? ChannelCredentials.SecureSsl : ChannelCredentials.Insecure;
            if (!httpAddress)
            {
                options.Credentials = transportCredentials;
            }

            if (authentication is not null)
            {
                if (authentication.RequireTransportSecurity && !useTls)
                {
                    throw new OxiaException("grpc: the credentials require transport level security (use grpc.WithTransportCredentials() to set)");
                }

                var callCredentials = CallCredentials.FromInterceptor(async (context, metadata) =>
                {
                    var values = await authentication.GetRequestMetadataAsync(CancellationToken.None, context.ServiceUrl).ConfigureAwait(false);
                    foreach (var (key, value) in values)
                    {
                        metadata.Add(key.ToLowerInvariant(), value);
                    }
                });
                options.Credentials = ChannelCredentials.Create(transportCredentials, callCredentials);
                if (!useTls)
                {
                    options.UnsafeUseInsecureChannelCallCredentials = true;
                }
            }

            if (!httpAddress)
            {
                // grpc-go sends the endpoint of `scheme:///endpoint` as :authority while it connects to the addresses the resolver
                // returns; the balancer would send the resolved address instead
                var endpoint = EndpointOf(channelAddress);
                if (endpoint.Length > 0)
                {
                    options.HttpHandler = new AuthorityHandler(endpoint) { InnerHandler = handler };
                }
            }

            foreach (var dialOption in dialOptions)
            {
                dialOption(options);
            }

            if (options.Credentials is null && !httpAddress)
            {
                // a dial option cleared them
                options.Credentials = transportCredentials;
            }

            var channel = GrpcChannel.ForAddress(channelAddress, options);
            var connection = new Connection(target, channel, onFailure, healthInterval, healthPingTimeout, log);
            using (ExecutionContext.SuppressFlow()) // the loop lives as long as the pooled connection, not as long as the caller that dialled it
            {
                connection._healthLoop = Task.Run(connection.HealthCheckLoopAsync);
            }
            return connection;
        }
        catch (Exception e)
        {
            handler.Dispose();
            throw OxiaErrors.Wrap(e, $"error connecting to {target}");
        }
    }

    /// <summary>
    /// The endpoint of a resolver target, grpc-go's <c>Target.Endpoint()</c>: the path of <c>scheme:///endpoint</c> without the leading
    /// slash (<c>dns:///host:6648</c> -> <c>host:6648</c>); the authority part when there is no path.
    /// </summary>
    internal static string EndpointOf(string address)
    {
        if (!Uri.TryCreate(address, UriKind.Absolute, out var uri))
        {
            return string.Empty;
        }

        var path = uri.AbsolutePath.TrimStart('/');
        return path.Length > 0 ? Uri.UnescapeDataString(path) : uri.Authority;
    }

    /// <summary>Sets the HTTP/2 :authority (Host header) of every request, whatever address the channel connected to.</summary>
    private sealed class AuthorityHandler(string authority) : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.Headers.Host = authority;
            return base.SendAsync(request, cancellationToken);
        }
    }

    /// <summary>Go: healthCheckLoop.</summary>
    private async Task HealthCheckLoopAsync()
    {
        using var ticker = new PeriodicTimer(_healthInterval);
        var consecutiveFailures = 0;
        while (true)
        {
            try
            {
                if (!await ticker.WaitForNextTickAsync(_cts.Token).ConfigureAwait(false)) return;
            }
            catch (OperationCanceledException)
            {
                return;
            }

            HealthCheckResponse? response = null;
            Exception? error = null;
            try
            {
                response = await HealthClient.CheckAsync(new HealthCheckRequest { Service = string.Empty },
                    deadline: DateTime.UtcNow + _healthPingTimeout, cancellationToken: _cts.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                error = e;
            }

            if (error is RpcException { StatusCode: StatusCode.Unimplemented })
            {
                _log.LogDebug("Connection health ping is unsupported. target={Target}", Target);
                return;
            }

            if (_cts.IsCancellationRequested)
            {
                // The connection is being closed
                return;
            }

            var shouldDisconnect = false;
            if (error is null && response!.Status == HealthCheckResponse.Types.ServingStatus.Serving)
            {
                consecutiveFailures = 0;
            }
            else if (error is null)
            {
                // The server deliberately reported it is not serving: give up on the connection right away
                _log.LogWarning("Connection health ping returned unhealthy status. target={Target} status={Status}", Target, response!.Status);
                shouldDisconnect = true;
            }
            else
            {
                // A failed ping can just mean the server is saturated: only give up after several consecutive failures
                consecutiveFailures++;
                _log.LogWarning(error, "Connection health ping failed. target={Target} consecutive-failures={ConsecutiveFailures}",
                    Target, consecutiveFailures);
                shouldDisconnect = consecutiveFailures >= ConnectionHealthFailureThreshold;
            }

            if (shouldDisconnect && Interlocked.CompareExchange(ref _disconnected, 1, 0) == 0)
            {
                NotifyDisconnect();
                return;
            }
        }
    }

    private void NotifyDisconnect()
    {
        // Own task: the callback closes this connection, which waits for the health loop that is calling us
        _ = Task.Run(() =>
        {
            try
            {
                _onFailure(Target);
            }
            catch (Exception e)
            {
                _log.LogWarning(e, "Connection failure callback failed. target={Target}", Target);
            }
        });
    }

    /// <summary>Go: Close() -- stops the health loop (waiting for it) and closes the channel; only the first call does anything.</summary>
    public async ValueTask CloseAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return;
        }

        _cts.Cancel();
        await _healthLoop.ConfigureAwait(false);
        Channel.Dispose();
        _cts.Dispose();
    }
}
