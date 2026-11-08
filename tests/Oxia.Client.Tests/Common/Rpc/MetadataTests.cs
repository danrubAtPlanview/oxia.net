// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/metadata_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 2, ported: 2, skipped: 0.
//   Divergences: the outgoing-metadata of a context.Context is the Metadata of CallOptions.Headers (null = no outgoing metadata);
//              "assert.Equal(ctx, result)" is reference equality of the returned Metadata with the input; `md.Get(key)` is
//              Metadata.GetAll(key).  Extra tests drive the interceptor and the dial option against a real server.
// =====================================================================================================================
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Common;

public class MetadataTests
{
    [Fact]
    public void TestWithOutgoingMetadataMergesValues()
    {
        var md = new Metadata { { "existing", "a" }, { "replaced", "old" } };

        var result = RpcMetadata.WithOutgoingMetadata(md, () => new Dictionary<string, string> { ["replaced"] = "new", ["added"] = "b" });

        Assert.NotNull(result);
        Assert.Equal(["a"], result.GetAll("existing").Select(e => e.Value));
        Assert.Equal(["new"], result.GetAll("replaced").Select(e => e.Value));
        Assert.Equal(["b"], result.GetAll("added").Select(e => e.Value));
    }

    [Fact]
    public void TestWithOutgoingMetadataKeepsContextWithoutValues()
    {
        Metadata? ctx = null;

        var result = RpcMetadata.WithOutgoingMetadata(ctx, () => null);

        Assert.Same(ctx, result);
        Assert.Null(result); // metadata.FromOutgoingContext(result) -> ok == false
    }

    // ----- extra tests (not in Go) --------------------------------------------------------------------------------------
    [Fact]
    public void ExistingMetadataIsNotModifiedAndAnEmptySupplyKeepsTheSameInstance()
    {
        var md = new Metadata { { "a", "1" } };

        Assert.Same(md, RpcMetadata.WithOutgoingMetadata(md, () => new Dictionary<string, string>()));
        Assert.Same(md, RpcMetadata.WithOutgoingMetadata(md, () => null));

        var merged = RpcMetadata.WithOutgoingMetadata(md, () => new Dictionary<string, string> { ["b"] = "2" });
        Assert.NotSame(md, merged);
        Assert.Single(md);
        Assert.Equal(2, merged!.Count);
    }

    [Fact]
    public void SuppliedKeysReplaceEveryExistingValueAndAreLowerCased()
    {
        var md = new Metadata { { "dup", "1" }, { "dup", "2" }, { "keep", "k" } };

        var merged = RpcMetadata.WithOutgoingMetadata(md, () => new Dictionary<string, string> { ["DUP"] = "only" })!;

        Assert.Equal(["only"], merged.GetAll("dup").Select(e => e.Value));
        Assert.Equal(["k"], merged.GetAll("keep").Select(e => e.Value));
    }

    [Fact]
    public void TheSupplierIsEvaluatedOnEveryCall()
    {
        var n = 0;
        MetadataValueSupplier supplier = () => new Dictionary<string, string> { ["call"] = (++n).ToString() };

        Assert.Equal("1", RpcMetadata.WithOutgoingMetadata(null, supplier)!.GetValue("call"));
        Assert.Equal("2", RpcMetadata.WithOutgoingMetadata(null, supplier)!.GetValue("call"));
    }

    private sealed class RecordingService : G.OxiaClient.OxiaClientBase
    {
        public readonly List<Metadata> Received = [];

        public override Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
        {
            lock (Received) Received.Add(context.RequestHeaders);
            return Task.FromResult(new G.KeepAliveResponse());
        }

        public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
        {
            lock (Received) Received.Add(context.RequestHeaders);
            await responseStream.WriteAsync(new G.ShardAssignments());
        }
    }

    private static async Task<(TestGrpcServer Server, RecordingService Service)> StartRecordingServer()
    {
        var service = new RecordingService();
        var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<RecordingService>(),
            services => services.AddSingleton(service));
        return (server, service);
    }

    [Fact]
    public async Task TheInterceptorAddsTheValuesToUnaryAndStreamingCalls()
    {
        var (server, service) = await StartRecordingServer();
        await using var _ = server;
        using var channel = GrpcChannel.ForAddress("http://" + server.Address);
        var invoker = channel.CreateCallInvoker().Intercept(
            new MetadataInterceptor(() => new Dictionary<string, string> { ["instance-id"] = "node-1" }));
        var client = new G.OxiaClient.OxiaClientClient(invoker);

        await client.KeepAliveAsync(new G.SessionHeartbeat(), headers: new Metadata { { "existing", "x" } });
        using var stream = client.GetShardAssignments(new G.ShardAssignmentsRequest());
        Assert.True(await stream.ResponseStream.MoveNext(CancellationToken.None));

        lock (service.Received)
        {
            Assert.Equal(2, service.Received.Count);
            Assert.All(service.Received, md => Assert.Equal("node-1", md.GetValue("instance-id")));
            Assert.Equal("x", service.Received[0].GetValue("existing"));
        }
    }

    [Fact]
    public async Task TheDialOptionAddsTheValuesToEveryCallOfAPooledConnection()
    {
        var (server, service) = await StartRecordingServer();
        await using var _ = server;
        var n = 0;
        await using var pool = new ClientPool(null, null,
            RpcMetadata.MetadataInjectionDialOption(() => new Dictionary<string, string> { ["instance-id"] = "node-" + Interlocked.Increment(ref n) }));

        var admin = pool.GetHealthRpc(server.Address); // any client of the pooled connection goes through the same handler chain
        var client = new G.OxiaClient.OxiaClientClient(pool.GetConnectionFromPool(server.Address).CallInvoker);
        await client.KeepAliveAsync(new G.SessionHeartbeat());
        await client.KeepAliveAsync(new G.SessionHeartbeat());
        Assert.NotNull(admin);

        lock (service.Received)
        {
            Assert.Equal(2, service.Received.Count);
            Assert.NotEqual(service.Received[0].GetValue("instance-id"), service.Received[1].GetValue("instance-id"));
            Assert.All(service.Received, md => Assert.StartsWith("node-", md.GetValue("instance-id")));
        }
    }
}
