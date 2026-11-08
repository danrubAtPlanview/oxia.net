// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_reroute_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 4, ported: 4, skipped: 0
//   Real components used: D's BatcherFactory / WriteManager / batchers / Metrics, E's RpcProvider and ShardStrategy,
//   F1's Sessions, C's ClientOptions; the shard manager (SplittableShardManager), the executor (SplitExecutor) and the
//   session server (FrozenShardServer, hosted by TestGrpcServer) are the fakes of the Go test.
//   Adapted  : `newClientOptions(addr)` -> ClientOptions.TryCreate; `t.Context()` -> a token cancelled when the test ends;
//   goroutines -> Task.Run; `chan struct{}` frozenWrite -> Channel.
// =====================================================================================================================
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Internal.Model;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.Tests.F2.TestClients;
using RpcProviderImpl = Oxia.Client.Internal.RpcProvider;

namespace Oxia.Client.Tests.F2;

public class AsyncClientRerouteTests
{
    private static readonly byte[] V1 = "v1"u8.ToArray();
    private static readonly byte[] V2 = "v2"u8.ToArray();

    // A write that is in flight to a shard when the shard splits is rerouted to the child shard. A write to the same key
    // issued after the client received the post-split assignments goes to the child shard directly, and must still be applied
    // after the rerouted one.
    [Fact]
    public async Task TestRerouteKeepsWriteOrderAcrossSplit()
    {
        var shardManager = new SplittableShardManager("");
        var executor = new SplitExecutor { ShardManager = shardManager, FrozenShard = 0 };
        var client = SplitExecutor.NewRerouteTestClient(shardManager, executor);
        try
        {
            var put1 = client.PutAsync("k", V1);
            await F2Util.ReadAsync(executor.FrozenWrite.Reader);

            shardManager.Split(0, 1, 2);
            // The child shard holds put(k, v2) until put(k, v1) is rerouted to it
            var put2 = Task.Run(async () => await client.PutAsync("k", V2));

            // Give put(k, v2) the time to overtake put(k, v1), if the client lets it
            var deadline = DateTime.UtcNow.AddMilliseconds(100);
            while (executor.AppliedWrites().Count == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            executor.Release.SetResult();
            Assert.Null((await F2Util.WithTimeout(put1.AsTask())).Err);
            Assert.Null((await F2Util.WithTimeout(put2)).Err);

            Assert.Equal(["k=v1", "k=v2"], executor.AppliedWrites());
        }
        finally
        {
            client.WriteBatchManager.Dispose();
        }
    }

    [Fact]
    public void TestRerouteSendsDeleteToSuccessorOfPartitionKey()
    {
        var shardManager = new SplittableShardManager("");
        var client = new AsyncClientImpl { ShardManager = shardManager };
        shardManager.Split(0, 1, 2);
        const string partitionKey = "partition";
        var sent = new Dictionary<long, List<object>>();
        void Send(long shardId, object call)
        {
            if (!sent.TryGetValue(shardId, out var calls)) sent[shardId] = calls = [];
            calls.Add(call);
        }

        client.SendToSuccessors(0, new DeleteCall { Key = "key", PartitionKey = partitionKey }, Send);
        client.SendToSuccessors(0, new DeleteRangeCall { MinKeyInclusive = "a", MaxKeyExclusive = "z", PartitionKey = partitionKey }, Send);

        var successor = shardManager.Get(partitionKey);
        Assert.Single(sent);
        Assert.Equal(2, sent[successor].Count);
        Assert.Equal("key", ((DeleteCall)sent[successor][0]).Key);
        Assert.Equal("a", ((DeleteRangeCall)sent[successor][1]).MinKeyInclusive);
    }

    [Fact]
    public void TestRerouteFailsDeleteWithoutSuccessor()
    {
        var client = new AsyncClientImpl { ShardManager = new SplittableShardManager("") };
        var errs = new List<Exception?>();

        client.SendToSuccessors(0, new DeleteCall { Key = "key", Callback = (_, err) => errs.Add(err) },
            (_, _) => Assert.Fail("unexpected send"));

        Assert.Single(errs);
        Assert.True(Oxia.Common.Constant.OxiaErrors.Is(errs[0], Oxia.Common.Constant.OxiaErrors.ShardNotFound));
    }

    // An ephemeral put that waits for its session on a shard when the shard splits gets a session on the child shard, and goes
    // there directly. It must still be applied after a write to the same key that was in flight to the split shard, and is
    // rerouted to the child shard.
    [Fact]
    public async Task TestRerouteKeepsEphemeralWriteOrderAcrossSplit()
    {
        var server = new FrozenShardServer();
        await using var grpcServer = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<FrozenShardServer>(),
            services => services.AddSingleton(server));
        using var testCtx = new CancellationTokenSource();

        Assert.Null(ClientOptions.TryCreate(grpcServer.Address, [], out var options));
        var shardManager = new SplittableShardManager(options.ServiceAddress);
        var executor = new SplitExecutor { ShardManager = shardManager, FrozenShard = 0 };
        var client = SplitExecutor.NewRerouteTestClient(shardManager, executor);
        var rpcProvider = RpcProviderImpl.Create(testCtx.Token, options.Namespace, null, null, options.ServiceAddress, () => shardManager);
        client.Options = options;
        client.Sessions = new Sessions(testCtx.Token, shardManager, rpcProvider, options);
        try
        {
            var put1 = client.PutAsync("k", V1);
            await F2Util.ReadAsync(executor.FrozenWrite.Reader);
            var put2 = Task.Run(async () => await client.PutAsync("k", V2, OxiaOptions.Ephemeral()));
            await F2Util.EventuallyAsync(() => server.Rejected > 0, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));

            shardManager.Split(0, 1, 2);
            // Give put(k, v2) the time to overtake put(k, v1), if the client lets it, once it has a session on the child shard
            await F2Util.EventuallyAsync(() => server.Created.Count > 0, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10),
                "no session created on the child shard");
            var deadline = DateTime.UtcNow.AddMilliseconds(100);
            while (executor.AppliedWrites().Count == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            executor.Release.SetResult();
            Assert.Null((await F2Util.WithTimeout(put1.AsTask())).Err);
            Assert.Null((await F2Util.WithTimeout(put2)).Err);

            Assert.Equal(["k=v1", "k=v2"], executor.AppliedWrites());
        }
        finally
        {
            await client.Sessions.CloseAsync();
            await rpcProvider.CloseAsync();
            client.WriteBatchManager.Dispose();
            testCtx.Cancel();
        }
    }
}
