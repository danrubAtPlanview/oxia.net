// =====================================================================================================================
// PORT NOTE (test infrastructure for oxia/internal/rpc_provider_test.go)
//   Upstream : oxia/internal/rpc_provider_test.go  (testOxiaClientServer, notLeaderOxiaClientServer, writeServer,
//              startTestOxiaClientServer, unreachableAddress)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The in-process gRPC mock servers of the Go tests, derived from the protoc-generated OxiaClientBase and hosted
//              by TestGrpcServer (Kestrel, h2c, ephemeral loopback port), plus the helpers that build the dial options and the
//              deadlines the tests need.
//   Divergences: `grpc.WithContextDialer` is a SocketsHttpHandler.ConnectCallback (WithDialer); the unreachable address scan
//                uses TcpListener.  Each server is a singleton service registered in DI.
// =====================================================================================================================
using Xunit;
using System.Net;
using System.Net.Sockets;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using G = Io.Oxia.Proto.V1;

#pragma warning disable CS0672 // the generated base marks List obsolete (Go: //nolint:staticcheck)

namespace Oxia.Client.Tests.Internal.Fakes;

/// <summary>Go: testOxiaClientServer -- answers every data request with an empty success, or runs the hook of a test for the lists and the range scans.</summary>
public class TestOxiaClientServer : G.OxiaClient.OxiaClientBase
{
    public Func<G.ListRequest, IServerStreamWriter<G.ListResponse>, Task>? ListHook { get; set; }
    public Func<G.RangeScanRequest, IServerStreamWriter<G.RangeScanResponse>, Task>? RangeScanHook { get; set; }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream,
        IServerStreamWriter<G.WriteResponse> responseStream, ServerCallContext context)
    {
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            var response = new G.WriteResponse();
            for (int i = 0; i < request.Puts.Count; i++) response.Puts.Add(new G.PutResponse());
            await responseStream.WriteAsync(response, context.CancellationToken).ConfigureAwait(false);
        }
    }

    public override Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context)
    {
        var response = new G.ReadResponse();
        for (int i = 0; i < request.Gets.Count; i++) response.Gets.Add(new G.GetResponse { Status = G.Status.KeyNotFound });
        return responseStream.WriteAsync(response);
    }

    public override Task List(G.ListRequest request, IServerStreamWriter<G.ListResponse> responseStream, ServerCallContext context) =>
        ListHook is { } hook ? hook(request, responseStream) : responseStream.WriteAsync(new G.ListResponse());

    public override Task RangeScan(G.RangeScanRequest request, IServerStreamWriter<G.RangeScanResponse> responseStream, ServerCallContext context) =>
        RangeScanHook is { } hook ? hook(request, responseStream) : responseStream.WriteAsync(new G.RangeScanResponse());
}

/// <summary>Go: notLeaderOxiaClientServer -- a node that no longer leads the shard: it rejects every data and session request with a leader hint.</summary>
public sealed class NotLeaderOxiaClientServer : G.OxiaClient.OxiaClientBase
{
    private readonly string _leaderHint;
    private readonly Action _onRequest;

    public NotLeaderOxiaClientServer(string leaderHint, Action onRequest)
    {
        _leaderHint = leaderHint;
        _onRequest = onRequest;
    }

    private Exception NotLeader()
    {
        _onRequest();
        return OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(0, _leaderHint))!;
    }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream,
        IServerStreamWriter<G.WriteResponse> responseStream, ServerCallContext context)
    {
        if (!await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false)) return;
        throw NotLeader();
    }

    public override Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context) => throw NotLeader();
    public override Task List(G.ListRequest request, IServerStreamWriter<G.ListResponse> responseStream, ServerCallContext context) => throw NotLeader();
    public override Task RangeScan(G.RangeScanRequest request, IServerStreamWriter<G.RangeScanResponse> responseStream, ServerCallContext context) => throw NotLeader();
    public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context) => throw NotLeader();
    public override Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context) => throw NotLeader();
    public override Task<G.CloseSessionResponse> CloseSession(G.CloseSessionRequest request, ServerCallContext context) => throw NotLeader();
}

/// <summary>Go: writeServer -- counts the write requests it receives, and answers them unless it is stuck, like a deposed leader that is still reachable, or a saturated one.</summary>
public sealed class WriteServer : G.OxiaClient.OxiaClientBase
{
    private long _requests;

    public bool Stuck { get; init; }
    public Action? OnRequest { get; init; }
    public long Requests => Interlocked.Read(ref _requests);

    /// <summary>The headers of the last write stream opened (namespace, shard-id).</summary>
    public Metadata? LastHeaders { get; private set; }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream,
        IServerStreamWriter<G.WriteResponse> responseStream, ServerCallContext context)
    {
        LastHeaders = context.RequestHeaders;
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            Interlocked.Increment(ref _requests);
            OnRequest?.Invoke();
            if (Stuck) continue;
            var response = new G.WriteResponse();
            for (int i = 0; i < request.Puts.Count; i++) response.Puts.Add(new G.PutResponse());
            await responseStream.WriteAsync(response, context.CancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>The helpers of the Go rpc provider tests.</summary>
public static class RpcTestHelpers
{
    /// <summary>Go: startTestOxiaClientServer -- returns the running server (its Address is host:port).</summary>
    public static Task<TestGrpcServer> StartServerAsync<T>(T service) where T : G.OxiaClient.OxiaClientBase =>
        TestGrpcServer.StartAsync(static app => app.MapGrpcService<T>(), services => services.AddSingleton(service));

    /// <summary>
    /// Go: unreachableAddress -- returns an address where nothing is listening, so connecting to it fails with "connection refused".
    /// The port is in [10000, 32768): above the fixed ports that tests bind, e.g. Oxia's default 6648, and below the default
    /// ephemeral port ranges (Linux 32768-60999, Windows 49152-65535).  The scan starts at a random port.
    /// </summary>
    public static string UnreachableAddress()
    {
        const int minFixedPort = 10000, ephemeralPortsStart = 32768, ports = ephemeralPortsStart - minFixedPort;
        int start = Random.Shared.Next(ports);
        for (int i = 0; i < ports; i++)
        {
            int port = minFixedPort + (start + i) % ports;
            try
            {
                var listener = new TcpListener(IPAddress.Loopback, port);
                listener.Start();
                listener.Stop();
                return $"127.0.0.1:{port}";
            }
            catch (SocketException)
            {
                // in use: next
            }
        }

        throw new InvalidOperationException("no free port below the ephemeral port range");
    }

    /// <summary>Go: grpc.WithContextDialer(dialer) -- the dialer receives host:port and the connect token.</summary>
    public static Action<GrpcChannelOptions> WithDialer(Func<string, CancellationToken, ValueTask<Stream>> dialer) => options =>
        options.HttpHandler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            ConnectCallback = async (context, ct) =>
                await dialer($"{context.DnsEndPoint.Host}:{context.DnsEndPoint.Port}", ct).ConfigureAwait(false),
        };

    /// <summary>The default dial: a plain TCP connection (Go: `(&amp;net.Dialer{}).DialContext(ctx, "tcp", address)`).</summary>
    public static async ValueTask<Stream> DialTcpAsync(string address, CancellationToken ct)
    {
        int colon = address.LastIndexOf(':');
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(new DnsEndPoint(address[..colon], int.Parse(address[(colon + 1)..])), ct).ConfigureAwait(false);
            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>Go: context.WithTimeout(t.Context(), d).</summary>
    public static CancellationTokenSource WithTimeout(TimeSpan timeout)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(timeout);
        return cts;
    }

    /// <summary>Go: assert.Never(condition, waitFor, tick).</summary>
    public static async Task NeverAsync(Func<bool> condition, TimeSpan waitFor, TimeSpan tick)
    {
        var deadline = DateTime.UtcNow + waitFor;
        while (DateTime.UtcNow < deadline)
        {
            Assert.False(condition(), "the condition became true");
            await Task.Delay(tick, TestContext.Current.CancellationToken);
        }
    }
}
