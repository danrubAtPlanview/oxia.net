// =====================================================================================================================
// PORT NOTE (review R4 reproduction tests)
//   Upstream : common/rpc/connection.go (Close: c.cancel(); c.wg.Wait(); c.ClientConn.Close()), common/rpc/client_pool.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A closed connection must release its socket.  Go's ClientConn.Close() tears down the transport (TCP connection)
//              and every goroutine of the connection.
// =====================================================================================================================
using System.Net.NetworkInformation;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Tests.Proto;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R4_ConnectionLifetimeTests
{
    private static int EstablishedClientSockets(int serverPort)
    {
        // connections whose REMOTE end is the server port are the client-side sockets of this process
        return IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections()
            .Count(c => c.RemoteEndPoint.Port == serverPort && c.State == TcpState.Established);
    }

    /// <summary>
    /// Connection.CloseAsync disposes the GrpcChannel, but the SocketsHttpHandler is handed to the channel through
    /// GrpcChannelOptions.HttpHandler without DisposeHttpClient = true, so (per Grpc.Net.Client's contract) the channel does NOT
    /// dispose it: the pooled HTTP/2 connection (PooledConnectionIdleTimeout = infinite, keepalive pings every 10 s) outlives the
    /// closed pool.  Go's ClientConn.Close() closes the transport.
    /// </summary>
    [Fact]
    public async Task ClosingTheClientPool_ReleasesTheTcpConnectionOfEveryConnection()
    {
        var state = new MockOxiaState();
        await using var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<MockOxiaClientService>(), s => s.AddSingleton(state));

        var pool = new ClientPool(null, null);
        var rpc = pool.GetClientRpc(server.Address);
        await rpc.KeepAliveAsync(new SessionHeartbeat { Shard = 1, SessionId = 1 }, default);
        Assert.Equal(1, EstablishedClientSockets(server.Port));

        await pool.CloseAsync();

        // give the transport a generous time to close the socket
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (EstablishedClientSockets(server.Port) != 0 && DateTime.UtcNow < deadline) await Task.Delay(50);

        Assert.Equal(0, EstablishedClientSockets(server.Port));
    }

    /// <summary>Control: the same scenario passes when the pool is told to let the channel dispose the handler (the proposed fix).</summary>
    [Fact]
    public async Task Control_WithDisposeHttpClient_TheSocketIsReleased()
    {
        var state = new MockOxiaState();
        await using var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<MockOxiaClientService>(), s => s.AddSingleton(state));

        var pool = new ClientPool(null, null, static o => o.DisposeHttpClient = true);
        var rpc = pool.GetClientRpc(server.Address);
        await rpc.KeepAliveAsync(new SessionHeartbeat { Shard = 1, SessionId = 1 }, default);
        Assert.Equal(1, EstablishedClientSockets(server.Port));

        await pool.CloseAsync();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (EstablishedClientSockets(server.Port) != 0 && DateTime.UtcNow < deadline) await Task.Delay(50);

        Assert.Equal(0, EstablishedClientSockets(server.Port));
    }
}

