// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/connection.go (grpc.NewClient: grpc-go resolves the HTTP proxy of a target from the environment ONCE, when it
//              dials -- `http.ProxyFromEnvironment` caches the environment for the life of the process) -- no file of its own upstream.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Per-request proxy resolution costs real time on Windows.  SocketsHttpHandler asks its proxy for EVERY request, and the default
//              proxy there is HttpWindowsProxy, which for a machine with automatic proxy detection (WPAD) calls WinHttpGetProxyForUrl each
//              time: ~65 us of CPU and ~0.1-0.2 ms of latency per request (dotnet-trace: 12 % of the CPU of a linger-0 workload).  The write
//              stream is ONE request, but every Read batch (a gRPC call of its own), List, RangeScan, session and shard-assignment call is a
//              request.  This wrapper asks the system proxy once per destination (scheme, host, port) and remembers the answer -- what
//              grpc-go does with the environment.  The proxy that results is exactly the one the default would have given.
//   Divergences: a change of the system proxy settings while the process runs is not seen for destinations already resolved (Go: same, the
//              environment is read once per process).
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Net;

namespace Oxia.Common.Rpc;

internal sealed class TransportProxyCache(IWebProxy inner) : IWebProxy
{
    private readonly ConcurrentDictionary<(string Scheme, string Host, int Port), Uri?> _proxies = new();
    private readonly ConcurrentDictionary<(string Scheme, string Host, int Port), bool> _bypassed = new();

    public IWebProxy Inner => inner;

    public ICredentials? Credentials
    {
        get => inner.Credentials;
        set => inner.Credentials = value;
    }

    public Uri? GetProxy(Uri destination)
    {
        var key = (destination.Scheme, destination.Host, destination.Port);
        if (_proxies.TryGetValue(key, out var proxy)) return proxy;
        return _proxies.GetOrAdd(key, inner.GetProxy(destination));
    }

    public bool IsBypassed(Uri host)
    {
        var key = (host.Scheme, host.Host, host.Port);
        if (_bypassed.TryGetValue(key, out var bypassed)) return bypassed;
        return _bypassed.GetOrAdd(key, inner.IsBypassed(host));
    }
}
