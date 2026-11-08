// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/connection_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The connection re-resolves the target name on every dial (a restarted server on a new address is reached again), dials the
//              name of a `tls://` target without the prefix, reports connection creation errors, and closing it stops an in-flight health
//              ping without reporting a failure.
//   Go tests: 4, ported: 4, skipped: 0.
//   Divergences:
//      * `grpc.WithContextDialer(func(ctx, target))` -> a SocketsHttpHandler.ConnectCallback installed by a dial option: it receives the
//        DnsEndPoint (host + port) of the request, i.e. the name that is still to be resolved; the "DNS" is the test's `address` cell.
//        Assertions made inside the callback are collected and checked by the test thread (an exception in the callback would only fail
//        the connection attempt).
//      * `conn.Connect()` (leave idle) has no equivalent: a health Check call, which has to dial first, triggers the connection attempt.
//      * `assert.ErrorContains(err, ...)` -> the message of the thrown OxiaException.
// =====================================================================================================================
using System.Net;
using System.Net.Sockets;
using Grpc.Health.V1;
using Grpc.Net.Client;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Rpc;
using Xunit;
using static Oxia.Client.Tests.Common.RpcTestSupport;

namespace Oxia.Client.Tests.Common;

public class ConnectionTests
{
    private static Action<GrpcChannelOptions> ContextDialer(Func<DnsEndPoint, CancellationToken, ValueTask<Stream>> dialer)
        => options =>
        {
            var handler = (SocketsHttpHandler)options.HttpHandler!;
            handler.ConnectCallback = async (context, ct) => await dialer(context.DnsEndPoint, ct).ConfigureAwait(false);
        };

    private static async ValueTask<Stream> DialTcp(string address, CancellationToken ct)
    {
        var endPoint = IPEndPoint.Parse(address);
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(endPoint, ct).ConfigureAwait(false);
            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    // `deadline` defaults to 100 ms for the polling callers (Eventually retries).  A caller that fires the call ONCE to trigger a dial
    // (Go: conn.Connect(), which has no deadline) passes a generous one: with a 100 ms deadline a busy process can cancel the request
    // before the connection attempt even starts, and then the dialer is never invoked.
    private static async Task<bool> CheckTestConnection(Connection conn, TimeSpan? deadline = null)
    {
        try
        {
            await conn.HealthClient.CheckAsync(new HealthCheckRequest(), deadline: DateTime.UtcNow + (deadline ?? TimeSpan.FromMilliseconds(100)));
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    [Fact]
    public async Task TestConnectionReconnectsToNewAddressOfRestartedServer()
    {
        var (oldServer, _) = await NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus.Serving);
        await using var _old = oldServer;
        var oldAddress = oldServer.Target;

        // Stands in for DNS: the dialer resolves the name of the target on every dial
        var address = oldAddress;
        var dialedNames = new List<string>();
        var dialer = ContextDialer((endPoint, ct) =>
        {
            lock (dialedNames) dialedNames.Add($"{endPoint.Host}:{endPoint.Port}");
            return DialTcp(Volatile.Read(ref address), ct);
        });

        await using var conn = Connection.CreateWithHealthConfig("oxia.test:6649", null, null, [dialer], _ => { },
            TimeSpan.FromHours(1), TimeSpan.FromSeconds(1));
        await Eventually(() => CheckTestConnection(conn), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(10));

        // Restart the server on a new address
        await oldServer.Server.StopAsync();
        var (newServer, _) = await NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus.Serving);
        await using var _new = newServer;
        Volatile.Write(ref address, newServer.Target);

        await Eventually(() => CheckTestConnection(conn), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        lock (dialedNames)
        {
            Assert.NotEmpty(dialedNames);
            Assert.All(dialedNames, name => Assert.Equal("oxia.test:6649", name));
        }
    }

    [Fact]
    public async Task TestConnectionDialsNameOfTLSTarget()
    {
        var dialed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var dialer = ContextDialer((endPoint, _) =>
        {
            dialed.TrySetResult($"{endPoint.Host}:{endPoint.Port}");
            throw new IOException("unreachable");
        });

        await using var conn = Connection.CreateWithHealthConfig("tls://oxia.test:6649", null, null, [dialer], _ => { },
            TimeSpan.FromHours(1), TimeSpan.FromSeconds(1));
        _ = CheckTestConnection(conn, TimeSpan.FromSeconds(30)); // triggers the connection attempt (Go: conn.Connect(), no deadline)

        var target = await dialed.Task.WaitAsync(TimeSpan.FromSeconds(5)); // "the name of the target was never dialed"
        Assert.Equal("oxia.test:6649", target);
    }

    [Fact]
    public void TestConnectionReturnsClientError()
    {
        // Credentials that require TLS can't go over a plaintext connection
        var err = Assert.Throws<OxiaException>(() => Connection.Create("localhost:1", null,
            Authentication.NewTokenAuthenticationWithToken("token", true), [], _ => { }));
        Assert.Contains("error connecting to localhost:1", err.Message);
    }

    // Closing the connection stops the health loop while a ping is in flight, without counting the cancelled ping as a failure.
    [Fact]
    public async Task TestConnectionCloseStopsHealthPingInFlight()
    {
        var (server, service) = await NewTestBlockingHealthServerAsync();
        await using var _s = server;
        var failures = new List<string>();
        var conn = Connection.CreateWithHealthConfig(server.Target, null, null, [],
            target => { lock (failures) failures.Add(target); }, TimeSpan.FromMilliseconds(1), TimeSpan.FromHours(1));

        await service.Checks.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); // "no health ping"
        await conn.CloseAsync();
        Assert.False(conn.Disconnected);

        await Task.Delay(100);
        lock (failures) Assert.Empty(failures); // "the closed connection was reported as failed"
    }
}
