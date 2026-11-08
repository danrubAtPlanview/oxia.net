// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/failure_injection.go   (Failure, DizzyShardManager)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The failures a client can be asked to inject on itself (tests only), selected with WithFailureInjection.
//   Divergences: `const DizzyShardManager Failure = 1` -> the enum member Failure.DizzyShardManager (same value).
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>Go: oxia.Failure -- a failure that the client can inject on itself (see WithFailureInjection).</summary>
public enum Failure
{
    /// <summary>Makes the client use the DizzyShardManager: a shard manager whose Leader(shard) is always the service address, so every request goes through the "wrong" server.</summary>
    DizzyShardManager = 1,
}
