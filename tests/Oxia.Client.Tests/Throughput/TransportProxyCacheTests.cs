// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : common/rpc/connection.go (grpc-go resolves the environment proxy once per process)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : src/Oxia.Common/Rpc/TransportProxyCache.cs asks the system proxy once per destination instead of once per request (on Windows
//              each ask is a WinHttpGetProxyForUrl call): same answers as the wrapped proxy, one resolution per (scheme, host, port),
//              credentials pass through, and Connection really installs it on the handler.
// =====================================================================================================================
using System.Net;
using Oxia.Common.Rpc;
using Xunit;

namespace Oxia.Client.Tests.Throughput;

public class TransportProxyCacheTests
{
    private sealed class CountingProxy : IWebProxy
    {
        public int GetProxyCalls;
        public int IsBypassedCalls;
        public ICredentials? Credentials { get; set; }

        public Uri? GetProxy(Uri destination)
        {
            Interlocked.Increment(ref GetProxyCalls);
            return destination.Host == "viaproxy" ? new Uri("http://proxy.example:3128") : null;
        }

        public bool IsBypassed(Uri host)
        {
            Interlocked.Increment(ref IsBypassedCalls);
            return host.Host != "viaproxy";
        }
    }

    [Fact]
    public void ResolvesOncePerDestination_AndGivesTheSameAnswers()
    {
        var inner = new CountingProxy();
        var cache = new TransportProxyCache(inner);

        for (int i = 0; i < 1000; i++)
        {
            Assert.Null(cache.GetProxy(new Uri("http://127.0.0.1:6648/io.oxia.proto.v1.OxiaClient/Read")));
            Assert.Equal(new Uri("http://proxy.example:3128"), cache.GetProxy(new Uri("http://viaproxy:6648/io.oxia.proto.v1.OxiaClient/Read")));
            Assert.True(cache.IsBypassed(new Uri("http://127.0.0.1:6648/x")));
            Assert.False(cache.IsBypassed(new Uri("http://viaproxy:6648/x")));
        }

        Assert.Equal(2, inner.GetProxyCalls);
        Assert.Equal(2, inner.IsBypassedCalls);
    }

    [Fact]
    public void ADifferentPortOrSchemeIsADifferentDestination()
    {
        var inner = new CountingProxy();
        var cache = new TransportProxyCache(inner);
        cache.GetProxy(new Uri("http://host:1/a"));
        cache.GetProxy(new Uri("http://host:2/a"));
        cache.GetProxy(new Uri("https://host:1/a"));
        cache.GetProxy(new Uri("http://host:1/b"));
        Assert.Equal(3, inner.GetProxyCalls);
    }

    [Fact]
    public void CredentialsPassThrough()
    {
        var inner = new CountingProxy();
        var cache = new TransportProxyCache(inner);
        var credentials = new NetworkCredential("u", "p");
        cache.Credentials = credentials;
        Assert.Same(credentials, inner.Credentials);
        Assert.Same(credentials, cache.Credentials);
    }

    [Fact]
    public void TheHandlerOfAConnectionResolvesTheProxyThroughTheCache()
    {
        var handler = new SocketsHttpHandler();
        TransportTuning.Apply(handler);
        Assert.IsType<TransportProxyCache>(handler.Proxy);

        // an explicit proxy of the host is left alone (dial options run later and may replace the handler anyway)
        var explicitProxy = new CountingProxy();
        var configured = new SocketsHttpHandler { Proxy = explicitProxy };
        TransportTuning.Apply(configured);
        Assert.Same(explicitProxy, configured.Proxy);

        var noProxy = new SocketsHttpHandler { UseProxy = false };
        TransportTuning.Apply(noProxy);
        Assert.Null(noProxy.Proxy);
    }
}
