// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/client_pool_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pool behaviour around the connection health loop (a NOT_SERVING answer, hanging pings or enough consecutive failures drop
//              the connection from the pool; transient failures, a SERVING answer or an unimplemented health service do not), errors of
//              connection creation, connection reuse across the RPC getters, and Close / removeConnection ignoring close errors.
//   Go tests: 11, ported: 11, skipped: 0.
//   Divergences:
//      * GetClientRpc / GetHealthRpc / GetAminRpc are asserted; GetCoordinationRpc and GetReplicationRpc (server-to-server services, not
//        part of the client) do not exist in the port, so the corresponding lines of TestClientPoolGetRpcReturnsConnectionError /
//        TestClientPoolGetRpcReusesConnection (GetReplicationRpc) are replaced by the remaining getters.
//      * `pool.connections` / `pool.Lock()` -> ClientPool's internal Connections / SetConnection / TryGetConnection;
//        `pool.connections[target].ClientConn.Close()` -> Connection.Channel.Dispose().
//      * `assert.ErrorContains(err, "require transport level security")` -> the thrown OxiaException's message.
//      * Go's `defer pool.Close()` -> `await using`; require.Eventually / require.Never -> RpcTestSupport.Eventually / Never.
//      * Health servers are Kestrel h2c servers (TestGrpcServer) with Grpc.HealthCheck / hand-written services.
// =====================================================================================================================
using Grpc.Health.V1;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Rpc;
using Xunit;
using static Oxia.Client.Tests.Common.RpcTestSupport;

namespace Oxia.Client.Tests.Common;

public class ClientPoolTests
{
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan Ms10 = TimeSpan.FromMilliseconds(10);

    [Fact]
    public async Task TestClientPool_RemovesConnectionAfterHealthFailure()
    {
        var (server, _) = await NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus.NotServing);
        await using var _s = server;
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, Ms10);
        pool.SetConnection(target, conn);

        await Eventually(() => !pool.TryGetConnection(target, out _), TimeSpan.FromSeconds(1), Tick);
    }

    [Fact]
    public async Task TestClientPool_RemovesConnectionWhenHealthPingHangs()
    {
        var (server, _) = await NewTestBlockingHealthServerAsync();
        await using var _s = server;
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, Ms10);
        pool.SetConnection(target, conn);

        await Eventually(() => !pool.TryGetConnection(target, out _), TimeSpan.FromSeconds(1), Tick);
    }

    [Fact]
    public async Task TestClientPool_KeepsConnectionOnTransientHealthFailures()
    {
        await using var server = await NewTestFlakyHealthServerAsync(Connection.ConnectionHealthFailureThreshold - 1);
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, TimeSpan.FromSeconds(1));
        pool.SetConnection(target, conn);

        // Fewer consecutive failures than the threshold must not tear the connection down
        await Never(() => !pool.TryGetConnection(target, out var current) || !ReferenceEquals(current, conn),
            TimeSpan.FromMilliseconds(300), Tick);
    }

    [Fact]
    public async Task TestClientPool_RemovesConnectionAfterConsecutiveHealthFailures()
    {
        await using var server = await NewTestFlakyHealthServerAsync(1000);
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, Ms10);
        pool.SetConnection(target, conn);

        await Eventually(() => !pool.TryGetConnection(target, out _), TimeSpan.FromSeconds(1), Tick);
    }

    [Fact]
    public async Task TestClientPool_KeepsConnectionWhenHealthIsServing()
    {
        var (server, _) = await NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus.Serving);
        await using var _s = server;
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, Ms10);
        pool.SetConnection(target, conn);

        await Never(() => !pool.TryGetConnection(target, out var current) || !ReferenceEquals(current, conn),
            TimeSpan.FromMilliseconds(250), Tick);
    }

    [Fact]
    public async Task TestClientPool_KeepsConnectionWhenHealthCheckIsUnsupported()
    {
        await using var server = await NewTestGrpcServerWithoutHealthAsync();
        var target = server.Target;

        await using var pool = new ClientPool(null, null);

        var conn = NewTestConnectionWithHealthConfig(pool, target, Ms10, Ms10);
        pool.SetConnection(target, conn);

        await Never(() => !pool.TryGetConnection(target, out var current) || !ReferenceEquals(current, conn),
            TimeSpan.FromMilliseconds(100), Tick);
    }

    [Fact]
    public async Task TestClientPoolGetRpcReturnsConnectionError()
    {
        // Credentials that require TLS can't go over the plaintext connection to the target
        await using var pool = new ClientPool(null, Authentication.NewTokenAuthenticationWithToken("token", true));

        const string target = "localhost:1";
        var err = Assert.Throws<OxiaException>(() => pool.GetClientRpc(target));
        Assert.Contains("require transport level security", err.Message);
        err = Assert.Throws<OxiaException>(() => pool.GetHealthRpc(target));
        Assert.Contains("require transport level security", err.Message);
        err = Assert.Throws<OxiaException>(() => pool.GetAdminRpc(target)); // Go: GetAminRpc
        Assert.Contains("require transport level security", err.Message);

        Assert.Equal(0, pool.ConnectionCount);
    }

    [Fact]
    public async Task TestClientPoolGetRpcReusesConnection()
    {
        await using var pool = new ClientPool(null, null);

        const string target = "localhost:1";
        pool.GetAdminRpc(target); // Go: GetAminRpc
        pool.GetHealthRpc(target); // Go: GetReplicationRpc
        pool.GetClientRpc(target);
        Assert.Equal(1, pool.ConnectionCount);
    }

    [Fact]
    public async Task TestClientPoolCloseIgnoresConnectionCloseError()
    {
        var pool = new ClientPool(null, null);

        const string target = "localhost:1";
        pool.GetClientRpc(target);

        // The gRPC connection is already closed, so closing it again fails (in Go)
        pool.TryGetConnection(target, out var connection);
        connection!.Channel.Dispose();
        await pool.CloseAsync();
    }

    [Fact]
    public async Task TestClientPoolRemoveConnectionIgnoresConnectionCloseError()
    {
        await using var pool = new ClientPool(null, null);

        const string target = "localhost:1";
        pool.GetClientRpc(target);

        pool.TryGetConnection(target, out var connection);
        connection!.Channel.Dispose();
        await pool.RemoveConnectionAsync(target);
        Assert.Equal(0, pool.ConnectionCount);

        // The next call opens a new connection
        pool.GetClientRpc(target);
        Assert.Equal(1, pool.ConnectionCount);
    }

    [Fact]
    public async Task TestClientPoolRemoveConnectionKeepsOtherTargets()
    {
        await using var pool = new ClientPool(null, null);

        const string target = "localhost:1";
        pool.GetClientRpc(target);
        pool.TryGetConnection(target, out var cnx);

        await pool.RemoveConnectionAsync("localhost:2");
        pool.TryGetConnection(target, out var current);
        Assert.Same(cnx, current);
    }
}
