// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/manager_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 1 (TestManager)   ported: 1   skipped: 0
//   Adaptations: `Close() error` is `Dispose()` that throws the batchers' error (assert.ErrorIs(err, errClose) -> the thrown
//              exception satisfies OxiaErrors.Is); the batcher factory takes (CancellationToken, long).
// =====================================================================================================================
using Oxia.Client.Batch;
using Oxia.Client.Internal.Batch;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Batch;

public class ManagerTests
{
    private static readonly OxiaException ErrClose = new("closed");

    private sealed class TestBatcher : IBatcher
    {
        public bool Closed;

        public void Dispose()
        {
            Closed = true;
            throw ErrClose;
        }

        public ValueTask AddAsync(object call) => default;

        public Task RunAsync() => Task.CompletedTask;
    }

    [Fact]
    public void TestManager()
    {
        var testBatcher = new TestBatcher();

        int newBatcherInvocations = 0;
        IBatcher BatcherFactory(CancellationToken ct, long shardId)
        {
            newBatcherInvocations++;
            return testBatcher;
        }

        var manager = new Manager(CancellationToken.None, BatcherFactory);

        var batcher = manager.Get(UtilsTests.ShardId);
        Assert.Same(testBatcher, batcher);
        Assert.Equal(1, newBatcherInvocations);

        batcher = manager.Get(UtilsTests.ShardId);
        Assert.Same(testBatcher, batcher);
        Assert.Equal(1, newBatcherInvocations);

        var err = Record.Exception(manager.Dispose);
        Assert.True(OxiaErrors.Is(err, ErrClose));

        Assert.True(testBatcher.Closed);
    }
}
