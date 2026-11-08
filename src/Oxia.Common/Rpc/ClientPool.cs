// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/client_pool.go  (clientPool, NewClientPool, getConnectionFromPool, removeConnection, Close);
//              see Connection.cs (connection.go), MetadataInterceptor.cs (metadata.go), GrpcMetrics.cs (grpcmetrics),
//              Security/TlsOptions.cs (security/tls.go)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : One shared connection (GrpcChannel) per target address, created lazily on first use with double-checked locking,
//              and removed from the pool -- and closed -- when its health loop gives up on it, so the next request dials again.
//              The target syntax, TLS, keepalive, per-RPC credentials and the health loop are described in Connection.cs.
//   Mapping  : grpc.DialOption            -> Action<GrpcChannelOptions> (applied last, may override anything)
//              tls.Config                -> SslClientAuthenticationOptions
//              grpc.WithPerRPCCredentials -> CallCredentials (+ UnsafeUseInsecureChannelCallCredentials when
//                                            RequireTransportSecurity is false)
//              GetClientRpc               -> OxiaClientRpc over the connection's (metrics-intercepted) CallInvoker; the
//                                            translation of RpcException => Oxia errors is OxiaClientRpc's.
//   Divergences:
//      * The Go interface also has GetCoordinationRpc / GetReplicationRpc (server-to-server services, not part of the client) and
//        GetHealthRpc; only GetClientRpc and GetAdminRpc are on IClientPool, GetHealthRpc is an extra method of this class.
//      * GetPeer(ctx) (the address of the calling peer of a server context) is server side: not ported.
//      * The Go RWMutex becomes a lock-free ConcurrentDictionary read plus a lock around creation / removal (double-checked).
//      * Connection creation failures are thrown as OxiaException("error connecting to <target>: ...") (Go: returned errors).
//      * The facades returned by GetClientRpc / GetAdminRpc are cached per connection instead of allocated per call.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Net.Security;
using Grpc.Health.V1;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Oxia.Common.Auth;
using Oxia.Common.Logging;

namespace Oxia.Common.Rpc;

public sealed class ClientPool : IClientPool
{
    public const string AddressSchemaTls = "tls://";
    public static readonly TimeSpan DefaultRpcTimeout = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<string, Connection> _connections = new();
    private readonly object _gate = new();
    private readonly SslClientAuthenticationOptions? _tls;
    private readonly IAuthentication? _authentication;
    private readonly Action<GrpcChannelOptions>[] _dialOptions;
    private readonly ILogger _log = OxiaLog.Create("client-pool");

    public ClientPool(SslClientAuthenticationOptions? tls, IAuthentication? authentication,
        params Action<GrpcChannelOptions>[] dialOptions)
    {
        _tls = tls;
        _authentication = authentication;
        _dialOptions = dialOptions ?? [];
    }

    // ----- state the tests (Go: in-package tests) read ---------------------------------------------------------------------
    internal SslClientAuthenticationOptions? Tls => _tls;

    internal IAuthentication? Authentication => _authentication;

    internal Action<GrpcChannelOptions>[] DialOptions => _dialOptions;

    /// <summary>Go: the Lock()ed <c>cp.connections</c> map, as a snapshot.</summary>
    internal IReadOnlyDictionary<string, Connection> Connections => new Dictionary<string, Connection>(_connections);

    internal int ConnectionCount => _connections.Count;

    /// <summary>Go: <c>cp.connections[target] = cnx</c>.</summary>
    internal void SetConnection(string target, Connection connection)
    {
        lock (_gate)
        {
            _connections[target] = connection;
        }
    }

    internal bool TryGetConnection(string target, out Connection? connection) => _connections.TryGetValue(target, out connection);

    /// <summary>The failure callback of pooled connections (Go: <c>cp.removeConnection</c>), fire-and-forget form.</summary>
    internal void OnConnectionFailure(string target) => _ = RemoveConnectionAsync(target);

    // ----- API -------------------------------------------------------------------------------------------------------------
    public IOxiaClientRpc GetClientRpc(string target) => GetConnectionFromPool(target).ClientRpc;

    public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target) => GetConnectionFromPool(target).AdminRpc;

    /// <summary>Go: GetHealthRpc(target) -- the standard grpc.health.v1 client of the pooled connection.</summary>
    public Health.HealthClient GetHealthRpc(string target) => GetConnectionFromPool(target).HealthClient;

    public async ValueTask CloseAsync()
    {
        Connection[] connections;
        lock (_gate)
        {
            connections = [.. _connections.Values];
        }

        foreach (var connection in connections)
        {
            await CloseConnectionAsync(connection).ConfigureAwait(false);
        }
    }

    /// <summary>Close that never throws (Go's Close always returns nil).</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await CloseAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Dispose must not throw
        }
    }

    internal Connection GetConnectionFromPool(string target)
    {
        if (_connections.TryGetValue(target, out var existing))
        {
            return existing;
        }

        lock (_gate)
        {
            if (_connections.TryGetValue(target, out existing))
            {
                return existing;
            }

            var connection = Connection.Create(target, _tls, _authentication, _dialOptions, OnConnectionFailure);
            _connections[target] = connection;
            return connection;
        }
    }

    /// <summary>Go: removeConnection(target) -- drops the pooled connection of the target and closes it.</summary>
    internal async Task RemoveConnectionAsync(string target)
    {
        Connection? connection;
        lock (_gate)
        {
            _connections.TryRemove(target, out connection);
        }

        if (connection is null)
        {
            return;
        }

        await CloseConnectionAsync(connection).ConfigureAwait(false);
    }

    private async ValueTask CloseConnectionAsync(Connection connection)
    {
        try
        {
            await connection.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _log.LogWarning(e, "Failed to close GRPC connection. server_address={ServerAddress}", connection.Target);
        }
    }
}
