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
        c.WriteBatchManager = new WriteManager(CancellationToken.None, (_, _) => batcher, c.ForwardWriteAsync);
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
#if DEBUG
        Assert.Skip("allocation budgets only mean something in Release (dotnet test -c Release)");
#endif
        var client = NewAnsweringClient(inline: true);
        var value = V;

        // The tiered JIT needs a few rounds (with pauses: its call counting starts after a quiet period) before the hot
        // methods are optimised; unoptimised code allocates things the optimised one does not. Take the best of the rounds, until the budgets are met (at most 60 rounds).
        long putBytes = long.MaxValue, getBytes = long.MaxValue, deleteBytes = long.MaxValue;
        for (int round = 0; round < 60; round++)
        {
            for (int i = 0; i < 20_000; i++)
            {
                await client.PutAsync("key", value);
                await client.GetAsync("key");
                await client.DeleteAsync("key");
            }
            await Task.Delay(250);

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1_000; i++) await client.PutAsync("key", value);
            putBytes = Math.Min(putBytes, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);

            before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1_000; i++) await client.GetAsync("key");
            getBytes = Math.Min(getBytes, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);

            before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1_000; i++) await client.DeleteAsync("key");
            deleteBytes = Math.Min(deleteBytes, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);

            // (under load the background JIT can take many rounds: stop as soon as the budgets are met)
            if (putBytes <= 128 && getBytes <= 176 && deleteBytes <= 0) break;
        }

        // The answering batcher itself allocates the response objects of the fake server (put: PutResponse + ProtoVersion,
        // 112-128 B; get: GetResponse + ProtoVersion + byte[1], 176 B; delete: nothing): the client adds nothing to that
        Assert.True(putBytes <= 128, $"PutAsync allocated {putBytes} B/op");
        Assert.True(getBytes <= 176, $"GetAsync allocated {getBytes} B/op");
        Assert.True(deleteBytes <= 0, $"DeleteAsync allocated {deleteBytes} B/op");
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

    // (xUnit would replace the lone surrogates of InlineData strings by U+FFFD: the keys are looked up by index)
    private static readonly (string Key, bool Valid)[] ValidateKeyCases =
    [
        ("", true),
        ("plain/ascii", true),
        ("ключ/日本", true),
        ("emoji-😀-pair", true),
        ("lone-high-\uD83D", false),
        ("\uDE00-lone-low", false),
        ("high-then-high-\uD83D😀", false),
        ("swapped-\uDE00\uD83D", false),
    ];

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    public void ValidateKey_RejectsLoneSurrogates(int index)
    {
        var (key, valid) = ValidateKeyCases[index];
        Assert.Equal(valid, AsyncClientImpl.IsValidUtf16(key));
        Assert.Equal(valid, AsyncClientImpl.ValidateKey(key) is null);
    }
}
