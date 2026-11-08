// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sync_client_impl_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 5 top-level (the two TestSyncClient_* tests have 3 subtests each, TestSyncClientListFailsWithTheFirstShardError 2),
//             ported: 5, skipped: 0
//   Real components used: the TestSyncClient_* tests run the real client (OxiaClient.CreateSyncClientAsync) against the mock
//   leader server (LeaderServer, hosted by TestGrpcServer): D's batchers, E's shard manager / RpcProvider, A's codec, B's
//   ClientPool, C's options.  TestCancelContext and the TestSyncClientList* tests use fakes (NeverCompleteAsyncClient,
//   StaticExecutor) like Go.
//   Adapted  : a CancellationToken cannot tell Canceled from DeadlineExceeded: `assert.ErrorIs(err, context.DeadlineExceeded)`
//   -> OxiaErrors.IsCanceled(err); assertGoroutinesEnd -> AsyncClientImpl.RunningStreamTasks.
// =====================================================================================================================
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.F2;

public class SyncClientImplTests
{
    private static readonly byte[] Value = "value"u8.ToArray();

    private static async Task AssertCancellable(Func<CancellationToken, Task> operationFunc)
    {
        using var cts = new CancellationTokenSource();

        var errTask = Task.Run(async () =>
        {
            try
            {
                await operationFunc(cts.Token);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        });

        cts.Cancel();

        Assert.True(OxiaErrors.IsCanceled(await F2Util.WithTimeout(errTask)));
    }

    [Fact]
    public async Task TestCancelContext()
    {
        var asyncClient = new NeverCompleteAsyncClient();
        var syncClient = SyncClientImpl.NewSyncClient(asyncClient);

        await AssertCancellable(async ct => await syncClient.PutAsync(ct, "/a", Array.Empty<byte>()));
        await AssertCancellable(async ct => await syncClient.DeleteAsync(ct, "/a"));
        await AssertCancellable(async ct => await syncClient.DeleteRangeAsync(ct, "/a", "/b"));
        await AssertCancellable(async ct => await syncClient.GetAsync(ct, "/a"));

        await syncClient.CloseAsync();
    }

    /// <summary>
    /// syncWrites are the writes of the key "queued", with every write operation of the SyncClient.
    /// </summary>
    private static Func<CancellationToken, ISyncClient, Task> SyncWrite(string name) => name switch
    {
        "put" => async (ct, client) => await client.PutAsync(ct, "queued", Value),
        "delete" => async (ct, client) => await client.DeleteAsync(ct, "queued"),
        "delete-range" => async (ct, client) => await client.DeleteRangeAsync(ct, "queued", "queued/"),
        _ => throw new ArgumentException(name),
    };

    /// <summary>
    /// assertNotSentWrite writes the key "queued" with a context that ends before the write can be sent: it must fail with an
    /// error telling that it was not sent.
    /// </summary>
    private static async Task AssertNotSentWrite(Func<CancellationToken, ISyncClient, Task> write, ISyncClient client)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var err = await Record.ExceptionAsync(() => write(cts.Token, client));
        Assert.NotNull(err);
        Assert.True(OxiaErrors.IsCanceled(err)); // Go: context.DeadlineExceeded
        Assert.True(CallContext.IsNotSent(err), $"the error must tell that the write was not sent: {err}");
    }

    // With a single write in flight per shard, the writes queued behind one that the leader does not answer, e.g. because it
    // was deposed, can wait longer than the context of their caller. Once the SyncClient reported the failure, the write must
    // never be sent: the application can issue it again.
    [Theory]
    [InlineData("put")]
    [InlineData("delete")]
    [InlineData("delete-range")]
    public async Task TestSyncClient_QueuedWriteIsNotSentAfterItsContextEnds(string name)
    {
        await using var leader = await LeaderServer.StartAsync();
        leader.Elect();
        var client = await OxiaClient.CreateSyncClientAsync(leader.Address);
        try
        {
            var stuck = Task.Run(async () => await client.PutAsync(CancellationToken.None, "stuck", Value));
            Assert.Equal("stuck", await leader.NextReceivedAsync());

            await AssertNotSentWrite(SyncWrite(name), client);

            leader.ReleaseStuck();
            await F2Util.WithTimeout(stuck);

            // The next write is sent after the queued one would have been
            await client.PutAsync(CancellationToken.None, "next", Value);
            Assert.Equal("next", await leader.NextReceivedAsync());
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    // While the shard has no leader, e.g. during an election, the client retries a write without sending it. The write must
    // not be sent once the SyncClient reported the failure, although its request was already being retried.
    [Theory]
    [InlineData("put")]
    [InlineData("delete")]
    [InlineData("delete-range")]
    public async Task TestSyncClient_WriteIsNotSentAfterItsContextEndsWithoutLeader(string name)
    {
        await using var leader = await LeaderServer.StartAsync();
        var client = await OxiaClient.CreateSyncClientAsync(leader.Address);
        try
        {
            await AssertNotSentWrite(SyncWrite(name), client);

            leader.Elect();

            // The next write is sent after the queued one would have been
            await client.PutAsync(CancellationToken.None, "next", Value);
            Assert.Equal("next", await leader.NextReceivedAsync());
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    // The SyncClient fails a list with the error of the first shard that fails, e.g. when no server supports an option of the
    // request. The lists of the other shards end with it, although the context of the caller is not done.
    [Theory]
    [InlineData("every-shard-fails")]
    [InlineData("one-shard-fails")]
    public async Task TestSyncClientListFailsWithTheFirstShardError(string name)
    {
        var failure = new Exception("shard unavailable");
        var executor = name switch
        {
            "every-shard-fails" => new StaticExecutor
            {
                Errs = new Dictionary<long, Exception> { [1] = failure, [2] = failure, [3] = failure, [4] = failure },
            },
            "one-shard-fails" => new StaticExecutor
            {
                Keys = new Dictionary<long, string[]> { [2] = ["a", "b"], [3] = ["c", "d"], [4] = ["e", "f"] },
                Errs = new Dictionary<long, Exception> { [1] = failure },
            },
            _ => throw new ArgumentException(name),
        };
        var asyncClient = StaticExecutor.NewScanTestClient(executor, 1, 2, 3, 4);
        var client = SyncClientImpl.NewSyncClient(asyncClient);

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await client.ListAsync(CancellationToken.None, "a", "z"));
        Assert.True(OxiaErrors.Is(err, failure));
        await F2Util.EventuallyAsync(() => asyncClient.RunningStreamTasks == 0, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10),
            "a task started by (*clientImpl).List did not end");
    }

    // Once the context is done, the lists of the shards drop the keys they have left: the SyncClient fails with the error of the
    // context, instead of returning a part of the keys.
    [Fact]
    public async Task TestSyncClientListFailsOnceItsContextIsDone()
    {
        var asyncClient = StaticExecutor.NewScanTestClient(new StaticExecutor
        {
            Keys = new Dictionary<long, string[]> { [1] = ["a", "b"], [2] = ["c", "d"] },
        }, 1, 2);
        var client = SyncClientImpl.NewSyncClient(asyncClient);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await client.ListAsync(cts.Token, "a", "z"));
        Assert.True(OxiaErrors.IsCanceled(err));
        await F2Util.EventuallyAsync(() => asyncClient.RunningStreamTasks == 0, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10),
            "a task started by (*clientImpl).List did not end");
    }
}
