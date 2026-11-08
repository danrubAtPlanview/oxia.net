// =====================================================================================================================
// PORT NOTE (independent-review regression/differential test; no Go test counterpart)
//   Upstream : oxia/internal/batch/write_manager.go (writeGate), oxia/sessions.go (executeWithSessionId), oxia/async_client_impl.go (Put/Delete)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins review findings R2-D2 (held writes released out of order) and R2-D3 (a plain Put/Delete issued after an ephemeral Put must not reach the batcher before it).
//   See      : docs/review-findings.md
// =====================================================================================================================
// =====================================================================================================================
// R2 review (fidelity) -- the order in which the writes of ONE caller reach the batcher of a shard.
//
// Go (oxia/async_client_impl.go Put/Delete/DeleteRange, oxia/sessions.go executeWithSessionId, oxia/internal/batch/write_manager.go
// writeGate.add): Put() only returns once the call was added to the batcher of its shard: Add BLOCKS while the shard holds its writes
// (after a split) and executeWithSessionId BLOCKS (it holds the sessions mutex) until the session of an ephemeral put exists.  So
// two writes issued one after the other by the same goroutine are added in that order, whatever the state of the shard.
//
// C# (AsyncClientImpl.PutAsync/DeleteAsync, PooledPutCall.StartEphemeral, WriteGate.AddSlowAsync): PutAsync returns a ValueTask before
// the call was added when it has to wait.  Two consequences, both only visible to a caller that does not await the first write
// before issuing the second (the whole point of the async API, `ch1 := Put(); ch2 := Put()` in Go):
//   1. a plain write issued after an ephemeral put overtakes it while the session is being created (the ephemeral put waits in the
//      sessions queue, the plain one goes straight to the batcher);
//   2. several writes waiting on a shard that holds its writes are released by independent thread pool continuations of one
//      TaskCompletionSource, in no particular order.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Client.Tests.F2;
using Oxia.Common.Proto;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;
using F1 = Oxia.Client.Tests.F1Support;
using ShardStrategyImpl = Oxia.Client.Internal.ShardStrategy;

namespace Oxia.Client.Tests.Review;

public class R2_WriteOrderingTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    // ----- 2. writes held by a shard after a split -----------------------------------------------------------------------

    private static string ValueOf(object call) => System.Text.Encoding.UTF8.GetString(((PutCall)call).Value.Span);

    [Fact]
    public async Task WritesHeldByANewShardAreAddedInTheOrderTheyWereIssued()
    {
        const int rounds = 60;
        const int writes = 25;
        int misordered = 0;
        string? example = null;

        for (int round = 0; round < rounds; round++)
        {
            var batchers = new ConcurrentDictionary<long, CapturingWriteBatcher>();
            var client = new AsyncClientImpl();
            var writeManager = new WriteManager(CancellationToken.None, (_, shardId) =>
            {
                var b = new CapturingWriteBatcher();
                batchers[shardId] = b;
                return b;
            }, client.ForwardWriteAsync);
            client.WriteBatchManager = writeManager;
            var sm = new ShardManager(ShardStrategyImpl.New(), null, "service:6648", "default", TimeSpan.FromSeconds(10), writeManager.ShardsReplaced);
            client.ShardManager = sm;
            sm.Update([new Shard(0, "", new HashRange(0, uint.MaxValue))]);

            // shard 0 has a write, then it is split: the children hold their writes until shard 0 handed off its own
            await writeManager.AddAsync(0, new PutCall { Key = "seed", Value = "seed"u8.ToArray() });
            sm.Update(
            [
                new Shard(1, "", new HashRange(0, uint.MaxValue / 2)),
                new Shard(2, "", new HashRange(uint.MaxValue / 2 + 1, uint.MaxValue)),
            ]);

            // one caller, one key, pipelined: `ch1 := Put(k, v1); ch2 := Put(k, v2); ...`
            const string key = "k";
            var pending = new ValueTask<PutResult>[writes];
            for (int i = 0; i < writes; i++)
            {
                pending[i] = client.PutAsync(key, System.Text.Encoding.UTF8.GetBytes(i.ToString("D3")));
            }

            long successor = sm.Get(key);
            // shard 0 completed all its batches: the barrier it was given comes back
            Barrier? barrier = null;
            await F2Util.EventuallyAsync(() =>
            {
                barrier = batchers[0].Calls.OfType<Barrier>().FirstOrDefault();
                return barrier is not null;
            }, Ten, TimeSpan.FromMilliseconds(1), "no barrier");
            barrier!.Done();

            await F2Util.EventuallyAsync(() => batchers[successor].Calls.Count >= writes, Ten, TimeSpan.FromMilliseconds(1),
                $"round {round}: only {batchers[successor].Calls.Count} of {writes} writes reached the successor");
            var values = batchers[successor].Calls.Select(ValueOf).ToArray();
            var expected = Enumerable.Range(0, writes).Select(i => i.ToString("D3")).ToArray();
            if (!expected.SequenceEqual(values))
            {
                misordered++;
                example ??= $"round {round}: [{string.Join(",", values.Take(12))}...]";
            }

            writeManager.Dispose();
        }

        Assert.True(misordered == 0,
            $"{misordered} of {rounds} rounds added the pipelined writes of one caller to the batcher out of order, first: {example}");
    }

    // ----- 1. an ephemeral put followed by plain writes ------------------------------------------------------------------

    private sealed class GatedSessions : F1.UnimplementedRpcProvider
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Open() => _gate.TrySetResult();

        public override async ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
        {
            await _gate.Task.WaitAsync(ct).ConfigureAwait(false);
            return new CreateSessionResponse { SessionId = 42 };
        }

        public override ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request) =>
            ValueTask.FromResult(new KeepAliveResponse());

        public override ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request) =>
            ValueTask.FromResult(new CloseSessionResponse());
    }

    private static (AsyncClientImpl Client, CapturingWriteBatcher Batcher, GatedSessions Rpc, CancellationTokenSource Cts) NewSessionClient()
    {
        var batcher = new CapturingWriteBatcher();
        var cts = new CancellationTokenSource();
        var client = new AsyncClientImpl(new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
        var shardManager = new F1.StaticShardManager { Shards = [0], LeaderAddress = "leader:6648" };
        client.ShardManager = shardManager;
        client.WriteBatchManager = new WriteManager(CancellationToken.None, (_, _) => batcher, client.ForwardWriteAsync);
        var rpc = new GatedSessions();
        client.Sessions = new Sessions(cts.Token, shardManager, rpc, client.Options);
        return (client, batcher, rpc, cts);
    }

    // `ch1 := Put(k, v1, Ephemeral()); ch2 := Put(k, v2)`: Go adds v1 to the batcher before Put returns the first channel, so v2 is
    // applied after v1.  C#: v1 waits in the sessions queue for the session, v2 is added at once: v2 is applied BEFORE v1 and the
    // final value of the key is v1.
    [Fact]
    public async Task APlainPutIssuedAfterAnEphemeralPutIsNotAddedBeforeIt()
    {
        var (client, batcher, rpc, cts) = NewSessionClient();
        try
        {
            var p1 = client.PutAsync("k", "v1"u8.ToArray(), OxiaOptions.Ephemeral());
            var p2 = client.PutAsync("k", "v2"u8.ToArray());

            rpc.Open();
            await F2Util.EventuallyAsync(() => batcher.Calls.Count >= 2, Ten, TimeSpan.FromMilliseconds(5), "the writes never reached the batcher");

            Assert.Equal(["v1", "v2"], batcher.Calls.Select(ValueOf).ToArray());
            GC.KeepAlive(p1);
            GC.KeepAlive(p2);
        }
        finally
        {
            await client.Sessions.CloseAsync();
            cts.Cancel();
        }
    }

    // The same with a delete: Go deletes the ephemeral record, C# leaves it in place (the delete is applied first).
    [Fact]
    public async Task ADeleteIssuedAfterAnEphemeralPutIsNotAddedBeforeIt()
    {
        var (client, batcher, rpc, cts) = NewSessionClient();
        try
        {
            var p1 = client.PutAsync("k", "v1"u8.ToArray(), OxiaOptions.Ephemeral());
            var d = client.DeleteAsync("k");

            rpc.Open();
            await F2Util.EventuallyAsync(() => batcher.Calls.Count >= 2, Ten, TimeSpan.FromMilliseconds(5), "the writes never reached the batcher");

            Assert.Equal(["PutCall", "DeleteCall"], batcher.Calls.Select(c => c is PutCall ? "PutCall" : c is DeleteCall ? "DeleteCall" : c.GetType().Name).ToArray());
            GC.KeepAlive(p1);
            GC.KeepAlive(d);
        }
        finally
        {
            await client.Sessions.CloseAsync();
            cts.Cancel();
        }
    }
}
