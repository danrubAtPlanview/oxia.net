// =====================================================================================================================
// REVIEW R3 -- high-iteration safety check of the sync client's cancellation contract (CallContext):
//   * an error that tells the write was NOT SENT (CallContext.IsNotSent) must be true: the server never receives the key;
//   * a write that returns successfully must have been received by the server;
//   * a cancelled write that is neither (a plain cancellation) may or may not have been received.
// Go: oxia/sync_client_impl.go Put/Delete/DeleteRange (select on the result and ctx.Done(), callContext.Cancel()) and
// oxia/internal/model/call_context.go.  C#: SyncClientImpl.cs PutCoreAsync/ErrorCoreAsync + CallContext.cs.
// (This test PASSES: it is a guard for a component found faithful.)
// =====================================================================================================================
using System.Collections.Concurrent;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Internal.Model;
using Oxia.Client.Tests.F2;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Review;

/// <summary>A one-shard leader that records the key of every write it receives and answers OK.</summary>
internal sealed class R3_RecordingLeader : G.OxiaClient.OxiaClientBase, IAsyncDisposable
{
    private TestGrpcServer? _server;
    public ConcurrentDictionary<string, byte> Received { get; } = new();
    public string Address { get; private set; } = "";

    public static async Task<R3_RecordingLeader> StartAsync()
    {
        var s = new R3_RecordingLeader();
        s._server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<R3_RecordingLeader>(), services => services.AddSingleton(s));
        s.Address = s._server.Address;
        return s;
    }

    public ValueTask DisposeAsync() => _server is null ? ValueTask.CompletedTask : _server.DisposeAsync();

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
    {
        var namespaceAssignment = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3 };
        namespaceAssignment.Assignments.Add(new G.ShardAssignment
        {
            Shard = 0, Leader = Address, Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0, MaxHashInclusive = uint.MaxValue },
        });
        var assignments = new G.ShardAssignments();
        assignments.Namespaces[Constants.DefaultNamespace] = namespaceAssignment;
        await responseStream.WriteAsync(assignments);
        try { await Task.Delay(Timeout.Infinite, context.CancellationToken); } catch (OperationCanceledException) { }
    }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream, ServerCallContext context)
    {
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var response = new G.WriteResponse();
            foreach (var put in requestStream.Current.Puts) { Received[put.Key] = 1; response.Puts.Add(new G.PutResponse { Version = new G.Version() }); }
            foreach (var del in requestStream.Current.Deletes) { Received[del.Key] = 1; response.Deletes.Add(new G.DeleteResponse()); }
            await responseStream.WriteAsync(response);
        }
    }
}

public class R3_SyncCancelSafetyTests
{
    [Theory]
    [InlineData("put")]
    [InlineData("delete")]
    public async Task NotSentErrorsAreTrue_AndSuccessesWereReceived(string op)
    {
        await using var leader = await R3_RecordingLeader.StartAsync();
        var client = await OxiaClient.CreateSyncClientAsync(leader.Address);
        try
        {
            const int n = 1500;
            var rnd = new Random(7);
            var outcome = new string[n];     // "ok" | "notsent" | "canceled"
            var tasks = new Task[n];
            var gate = new SemaphoreSlim(3);
            for (int i = 0; i < n; i++)
            {
                int ii = i;
                int delayTicks = rnd.Next(0, 60_000);   // up to 6 ms
                await gate.WaitAsync();
                tasks[i] = Task.Run(async () =>
                {
                    try {
                    using var cts = new CancellationTokenSource();
                    var spin = System.Diagnostics.Stopwatch.StartNew();
                    var timer = Task.Run(() => { while (spin.ElapsedTicks * 10_000_000 / System.Diagnostics.Stopwatch.Frequency < delayTicks) { } cts.Cancel(); });
                    try
                    {
                        if (op == "put") await client.PutAsync(cts.Token, "key" + ii, "v"u8.ToArray());
                        else await client.DeleteAsync(cts.Token, "key" + ii);
                        outcome[ii] = "ok";
                    }
                    catch (Exception e)
                    {
                        Assert.True(OxiaErrors.IsCanceled(e), "unexpected error: " + e);
                        outcome[ii] = CallContext.IsNotSent(e) ? "notsent" : "canceled";
                    }
                    await timer;
                    } finally { gate.Release(); }
                });
            }
            await F2Util.WithTimeout(Task.WhenAll(tasks), TimeSpan.FromSeconds(60));

            // Give the server time to finish what it received
            await Task.Delay(500);
            var received = new HashSet<string>(leader.Received.Keys);

            int ok = 0, notSent = 0, canceled = 0;
            for (int i = 0; i < n; i++)
            {
                bool got = received.Contains("key" + i);
                switch (outcome[i])
                {
                    case "ok": ok++; Assert.True(got, $"key{i}: reported success but the server never received it"); break;
                    case "notsent": notSent++; Assert.False(got, $"key{i}: reported NOT SENT but the server received it"); break;
                    default: canceled++; break;
                }
            }
            Assert.True(ok + notSent + canceled == n);
            if (Environment.GetEnvironmentVariable("R3_PRINT") == "1") Assert.Fail($"{op}: ok={ok} notsent={notSent} canceled={canceled}");
        }
        finally
        {
            await client.CloseAsync();
        }
    }
}
