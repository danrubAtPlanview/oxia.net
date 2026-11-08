// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/sequence/sequence_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A sequence-key put batched together with an unrelated put on the same partition still gets the next sequence number.
//   Go tests: 1   ported: 1   skipped: 0
//   Divergences: `_ = client.Put(...)` (result dropped) is `_ = client.PutAsync(...).AsTask()` so that the pooled completion is consumed.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Sequence;

public class SequenceTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestSequence_WithOtherKeyInBatch()
    {
        await using var standaloneServer = await oxia.StartAsync();

        await using var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr);

        for (var i = 1; i <= 3; i++)
        {
            if (i == 2)
            {
                _ = client.PutAsync("/abc", B("0"), OxiaOptions.PartitionKey("x")).AsTask();
            }

            var ch = client.PutAsync("x", B("0"), OxiaOptions.PartitionKey("x"), OxiaOptions.SequenceKeysDeltas(1));
            var pr = await ch;
            AssertNoError(pr.Err);
            Assert.Equal($"x-0000000000000000000{i}", pr.Key);
        }
    }
}
