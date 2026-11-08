// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/batch/utils_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 0 (the file only holds the helpers `shardId`, `one`, `two`, `add` shared by the other tests of the package)
//              ported: the helpers (no test method)   skipped: 0
//   Adaptations: `add(batch, call) (panicked bool)` recovers a panic; the C# batches throw InvalidOperationException("invalid
//              call") for the Go `panic("invalid call")`, so it catches that exception.  `*int64` -> long.
// =====================================================================================================================
using Oxia.Client.Batch;

namespace Oxia.Client.Tests.Batch;

internal static class UtilsTests
{
    public static readonly long ShardId = 1;
    public static readonly long One = 1;
    public static readonly long Two = 2;

    /// <summary>Go: add(b, call) (panicked bool).</summary>
    public static bool Add(IBatch b, object call)
    {
        try
        {
            b.Add(call);
        }
        catch (InvalidOperationException e) when (e.Message == "invalid call")
        {
            return true;
        }

        return false;
    }
}
