// =====================================================================================================================
// REVIEW R3 -- reproduction tests (they FAIL until the production code is fixed; they never touch existing tests)
//   Defect   : a null key / range bound passed to the async client makes the returned ValueTask NEVER complete.
//   Go       : strings cannot be nil; every call returns a result (ErrInvalidOptions for an invalid key, validateKey,
//              oxia/async_client_impl.go l.239-244; DeleteRange validates BOTH bounds, l.322-329).
//   C#       : src/Oxia.Client/AsyncClientImpl.cs  DeleteRangeAsync (l.500-546) -- ValidateKey(null) passes (AsSpan() of null is
//              empty), the call is queued, and WriteBatch.Add -> ByteSize(call) -> Encoding.UTF8.GetByteCount(null) throws
//              inside the batcher loop, which logs and drops the call (Batcher.cs "An exception thrown by Add... is logged and
//              does not kill the loop") WITHOUT completing it.  The caller's ValueTask hangs forever.  (Put/Delete/Get with a
//              null key throw synchronously from ShardManager.Get(null) -- acceptable misuse behaviour; the contract in Client.cs
//              says "failures are reported in the result (never thrown), except for argument misuse".)
//   Fix      : ArgumentNullException.ThrowIfNull on the keys / bounds at the public entry points (or validate in ValidateKey and
//              return ErrInvalidOptions like Go does for an invalid key).
// =====================================================================================================================
using Oxia.Client.Tests.F2;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R3_ArgumentMisuseTests
{
    [Theory]
    [InlineData("min")]
    [InlineData("max")]
    public async Task DeleteRange_WithANullBound_DoesNotHang(string which)
    {
        await using var leader = await LeaderServer.StartAsync();
        leader.Elect();
        var client = await OxiaClient.CreateAsyncClientAsync(leader.Address, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        try
        {
            Task<Exception?> pending;
            try
            {
                pending = which == "min"
                    ? client.DeleteRangeAsync(null!, "z").AsTask()
                    : client.DeleteRangeAsync("a", null!).AsTask();
            }
            catch (ArgumentNullException)
            {
                return;     // rejected synchronously: fine
            }

            var finished = await Task.WhenAny(pending, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.True(finished == pending, "DeleteRangeAsync with a null " + which + " bound neither threw nor completed within 5 s: the call is lost in the batcher");
        }
        finally
        {
            await client.CloseAsync();
        }
    }
}
