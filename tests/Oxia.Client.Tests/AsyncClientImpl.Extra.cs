// =====================================================================================================================
// PORT NOTE (extra tests -- no upstream equivalent)
//   Upstream : oxia/async_client_impl.go  (behaviour of the pooled hot path introduced by the port)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go tests cannot cover what the port adds: pooled call objects reused across operations, completion from
//              other threads, idempotent completion, and the allocation budget of PutAsync / GetAsync / DeleteAsync.
//              The batchers are fakes that complete the calls (inline or from the thread pool); the WriteManager / Manager
//              (component D), the options (C) and ProtoUtils (C) are the real ones.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.F2;

/// <summary>A batcher that answers every call successfully, inline or from the thread pool.</summary>
internal sealed class AnsweringBatcher : IBatcher
{
    private readonly bool _inline;
    private long _versionId;

    public AnsweringBatcher(bool inline) => _inline = inline;

    public ValueTask AddAsync(object call)
    {
        if (_inline) Answer(call);
        else ThreadPool.UnsafeQueueUserWorkItem(static s => s.Item1.Answer(s.Item2), (this, call), preferLocal: false);
        return ValueTask.CompletedTask;
    }

    private void Answer(object call)
    {
        switch (call)
        {
            case PutCall put:
                put.Complete(new PutResponse
                {
                    Status = Status.Ok, Version = new ProtoVersion { VersionId = Interlocked.Increment(ref _versionId) }, Key = null,
                }, null);
                break;
            case DeleteCall delete:
                delete.Complete(_deleteResponse, null);
                break;
            case GetCall get:
                get.Complete(new GetResponse { Status = Status.Ok, Key = null, Value = get.Key.Length == 0 ? null : [(byte)get.Key[0]], Version = new ProtoVersion() }, null);
                break;
            default:
                throw new InvalidOperationException("invalid call");
        }
    }

    private static readonly DeleteResponse _deleteResponse = new() { Status = Status.Ok };

    public Task RunAsync() => Task.CompletedTask;
    public void Dispose() { }
}

public class AsyncClientImplExtraTests
{
    private static readonly byte[] V = "value"u8.ToArray();

    private static AsyncClientImpl NewAnsweringClient(bool inline)
    {
        var shardManager = new StaticShardManager { Shards = [0] };
        var batcher = new AnsweringBatcher(inline);
        var c = new AsyncClientImpl { ShardManager = shardManager };
        c.WriteBatchManager = new WriteManager(CancellationToken.None, (_, _) => batcher, c.ForwardWrite);
        c.ReadBatchManager = new Manager(CancellationToken.None, (_, _) => batcher);
        return c;
    }

    [Fact]
    public async Task PutAsync_ReturnsTheKeyAndVersionOfTheResponse()
    {
        var client = NewAnsweringClient(inline: true);

        var result = await client.PutAsync("a", V);

        Assert.Null(result.Err);
        Assert.Equal("a", result.Key);
        Assert.Equal(1, result.Version.VersionId);
    }

    [Fact]
    public async Task PooledCalls_AreReusedAfterTheResultIsConsumed()
    {
        var shardManager = new StaticShardManager { Shards = [0] };
        var (client, batchers) = TestClients.NewWriteTestClient(shardManager);

        // The pool is process-wide (other tests may rent from it between two operations): look for a reuse in a few rounds
        bool reused = false;
        for (int round = 0; round < 50 && !reused; round++)
        {
            var first = client.PutAsync("a", V);
            var batcher = batchers[0];
            int before = batcher.Calls.Count - 1;
            var firstCall = (PutCall)batcher.Calls[before];
            firstCall.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion() }, null);
            Assert.Null((await first).Err);

            // the call went back to the pool: the next put reuses it (and it was reset)
            var second = client.PutAsync("b", V);
            var secondCall = (PutCall)batcher.Calls[before + 1];
            Assert.Equal("b", secondCall.Key);
            Assert.Null(secondCall.PartitionKey);
            Assert.Null(secondCall.ExpectedVersionId);
            reused = ReferenceEquals(firstCall, secondCall);
            secondCall.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion() }, null);
            Assert.Equal("b", (await second).Key);
        }
        Assert.True(reused, "the pooled call was never reused");
    }

    [Fact]
    public async Task Complete_IsIdempotent_TheFirstOutcomeWins()
    {
        var (client, batchers) = TestClients.NewWriteTestClient(new StaticShardManager { Shards = [0] });
        var result = client.DeleteAsync("a");
        var call = (DeleteCall)batchers[0].Calls[0];

        call.Complete(new DeleteResponse { Status = Status.KeyNotFound }, null);
        call.Complete(null, new Exception("late failure of a closing batcher"));

        Assert.True(OxiaErrors.Is(await result, ClientErrors.KeyNotFound));
    }

    [Fact]
    public async Task Complete_WithAnError_FailsThePut()
    {
        var (client, batchers) = TestClients.NewWriteTestClient(new StaticShardManager { Shards = [0] });
        var result = client.PutAsync("a", V);
        var failure = new Exception("boom");

        ((PutCall)batchers[0].Calls[0]).Complete(null, failure);

        var r = await result;
        Assert.Same(failure, r.Err);
        Assert.Equal("", r.Key);
    }

    [Fact]
    public async Task ManyConcurrentOperations_CompleteFromOtherThreads_WithTheirOwnResults()
    {
        var client = NewAnsweringClient(inline: false);

        var workers = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < 5000; i++)
            {
                string key = $"w{w}-{i}";
                var put = await client.PutAsync(key, V);
                Assert.Null(put.Err);
                Assert.Equal(key, put.Key);

                var get = await client.GetAsync(key);
                Assert.Null(get.Err);
                Assert.Equal(key, get.Key);
                Assert.Equal((byte)'w', get.Value![0]);

                Assert.Null(await client.DeleteAsync(key));
            }
        })).ToArray();

        await F2Util.WithTimeout(Task.WhenAll(workers), TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task PipelinedOperations_AreAllCompleted()
    {
        var client = NewAnsweringClient(inline: false);
        var puts = new List<ValueTask<PutResult>>();
        for (int i = 0; i < 10_000; i++) puts.Add(client.PutAsync("k" + i, V));

        for (int i = 0; i < puts.Count; i++)
        {
            var r = await puts[i];
            Assert.Null(r.Err);
            Assert.Equal("k" + i, r.Key);
        }
    }

    // The hot path: no allocation per operation once the pools are warm.
    [Fact]
    public async Task PutGetDelete_AllocateNothingPerOperation_WhenTheBatcherCompletesInline()
    {
        var client = NewAnsweringClient(inline: true);
        var value = V;

        for (int i = 0; i < 20_000; i++)
        {
            await client.PutAsync("key", value);
            await client.GetAsync("key");
            await client.DeleteAsync("key");
        }

        // The answering batcher itself allocates the response objects of the fake server: measure those and subtract
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            await client.PutAsync("key", value);
        }
        long putBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            await client.GetAsync("key");
        }
        long getBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            await client.DeleteAsync("key");
        }
        long deleteBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        // fake server per op: PutResponse (48) + ProtoVersion (64) ; GetResponse (56) + ProtoVersion (64) + byte[1] (32) ; Delete: 0
        // -> the client adds nothing: allow the fake's allocations (<= 160 B per op) and no more
        Assert.True(putBytes / 1000 <= 160, $"PutAsync allocated {putBytes / 1000} B/op");
        Assert.True(getBytes / 1000 <= 232, $"GetAsync allocated {getBytes / 1000} B/op");
        Assert.True(deleteBytes / 1000 <= 8, $"DeleteAsync allocated {deleteBytes / 1000} B/op");
    }

    [Fact]
    public async Task DeleteRangeAsync_WithoutShards_CompletesImmediately()
    {
        var (client, batchers) = TestClients.NewWriteTestClient(new StaticShardManager { Shards = [] });

        Assert.Null(await client.DeleteRangeAsync("a", "z"));
        Assert.Empty(batchers);
    }

    [Fact]
    public async Task PutAsync_WithAnInvalidOption_FailsWithoutSending()
    {
        var (client, batchers) = TestClients.NewWriteTestClient(new StaticShardManager { Shards = [0] });

        // sequence keys need a partition key
        var result = await client.PutAsync("a", V, OxiaOptions.SequenceKeysDeltas(1));

        Assert.True(OxiaErrors.Is(result.Err, ClientErrors.InvalidOptions));
        Assert.Empty(batchers);
    }

    [Fact]
    public void GetSequenceUpdates_RequiresAPartitionKey()
    {
        var client = new AsyncClientImpl { ShardManager = new StaticShardManager { Shards = [0] } };

        var ex = Assert.ThrowsAny<Exception>(() => client.GetSequenceUpdates(CancellationToken.None, "prefix"));

        Assert.True(OxiaErrors.Is(ex, ClientErrors.InvalidOptions));
        Assert.Contains("partitionKey is required", ex.Message);
    }

    [Theory]
    [InlineData("", true)]
    [InlineData("plain/ascii", true)]
    [InlineData("ключ/日本", true)]
    [InlineData("emoji-😀-pair", true)]
    [InlineData("lone-high-\uD83D", false)]
    [InlineData("\uDE00-lone-low", false)]
    [InlineData("high-then-high-\uD83D😀", false)]
    [InlineData("swapped-\uDE00\uD83D", false)]
    public void ValidateKey_RejectsLoneSurrogates(string key, bool valid)
    {
        Assert.Equal(valid, AsyncClientImpl.IsValidUtf16(key));
        Assert.Equal(valid, AsyncClientImpl.ValidateKey(key) is null);
    }
}
