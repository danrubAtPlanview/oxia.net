// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/connection.go (grpc.WithInitialWindowSize / WithInitialConnWindowSize / WithReadBufferSize /
//              WithWriteBufferSize are NOT set there: grpc-go defaults apply, with its BDP estimator growing the windows) -- no file of
//              its own upstream.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Optional, process-wide transport knobs applied to every connection the client pool dials (Connection.Create).  Everything
//              is "unset = the SocketsHttpHandler / OS default", i.e. the behaviour of the port without this file.  They exist so that
//              docs/performance.md can measure each knob on a real server, and so that a host with an unusual network (large bandwidth-
//              delay product, big values) can set one; the measured outcome is in docs/performance.md ("transport tuning").
//   Divergences: none of the knobs exists in Go (the grpc-go defaults are used there).
// =====================================================================================================================
using System.Net;
using System.Net.Sockets;

namespace Oxia.Common.Rpc;

internal static class TransportTuning
{
    /// <summary>SocketsHttpHandler.InitialHttp2StreamWindowSize (default 64 KiB; the handler grows it by itself when it detects a stall).</summary>
    internal static int? Http2StreamWindow { get; set; }

    /// <summary>SO_SNDBUF of the sockets the client dials (OS default if null).</summary>
    internal static int? SendBuffer { get; set; }

    /// <summary>SO_RCVBUF of the sockets the client dials (OS default if null).</summary>
    internal static int? ReceiveBuffer { get; set; }

    /// <summary>EnableMultipleHttp2Connections (the port sets it, like HTTP/2 clients open more connections when MAX_CONCURRENT_STREAMS is reached).</summary>
    internal static bool MultipleHttp2Connections { get; set; } = true;

    /// <summary>Resolve the proxy once per destination (see TransportProxyCache).  Off only to measure the cost it removes.</summary>
    internal static bool CacheProxyResolution { get; set; } = true;

    internal static void Apply(SocketsHttpHandler handler)
    {
        if (CacheProxyResolution && handler.UseProxy && handler.Proxy is null && HttpClient.DefaultProxy is { } defaultProxy)
        {
            handler.Proxy = new TransportProxyCache(defaultProxy);
        }
        if (Http2StreamWindow is { } window) handler.InitialHttp2StreamWindowSize = window;
        handler.EnableMultipleHttp2Connections = MultipleHttp2Connections;

        if (SendBuffer is not null || ReceiveBuffer is not null)
        {
            // Same as the default connect (TCP_NODELAY on, name resolved on every connect), plus the buffer sizes
            handler.ConnectCallback = static async (context, cancellationToken) =>
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                try
                {
                    if (SendBuffer is { } snd) socket.SendBufferSize = snd;
                    if (ReceiveBuffer is { } rcv) socket.ReceiveBufferSize = rcv;
                    await socket.ConnectAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            };
        }
    }
}
