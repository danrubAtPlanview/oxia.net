// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/shard_strategy_impl.go  (shardStrategyImpl / NewShardStrategy)
//              oxia/internal/shard_strategy.go       (ShardStrategy, Shard, HashRange -- the interface and the value types live in
//                                                      InternalContracts.cs, owned by the lead)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Decides which shard owns a key: the xxh3-64 hash of the key (truncated to its low 32 bits) must fall in the
//              inclusive hash range of the shard.
//   Divergences:
//      * Go returns a predicate `func(Shard) bool` that closes over the hash code.  The predicate API is kept
//        (<see cref="Get"/>, used by tests and by custom strategies) but ShardManager does NOT use it on its hot path:
//        when the strategy is a <see cref="ShardStrategy"/> it calls <see cref="Hash"/> once and binary-searches the
//        immutable shard snapshot, with no delegate and no closure allocation.
//      * `hashFunc` (a private struct field in Go, replaced by tests) is an optional constructor argument of the internal
//        constructor; the default path calls Hasher.Xxh332 directly (no delegate hop).
// =====================================================================================================================
using Oxia.Common.Hash;

namespace Oxia.Client.Internal;

/// <summary>Go: shardStrategyImpl.</summary>
public sealed class ShardStrategy : IShardStrategy
{
    private readonly Func<string, uint>? _hashFunc;

    /// <summary>Test seam (Go: <c>&amp;shardStrategyImpl{hashFunc: ...}</c>); null selects the default xxh3 hash.</summary>
    internal ShardStrategy(Func<string, uint>? hashFunc) => _hashFunc = hashFunc;

    /// <summary>Go: NewShardStrategy().</summary>
    public static ShardStrategy New() => new(hashFunc: null);

    /// <summary>The 32-bit routing hash of a key (Go: hashFunc(key)).  Allocation free for the default hash function.</summary>
    public uint Hash(string key) => _hashFunc is null ? Hasher.Xxh332(key) : _hashFunc(key);

    /// <summary>Go: Get(key) -- the predicate selecting the shard that owns the key.</summary>
    public Func<Shard, bool> Get(string key)
    {
        uint code = Hash(key);
        return shard =>
        {
            var hashRange = shard.HashRange;
            return hashRange.MinInclusive <= code && code <= hashRange.MaxInclusive;
        };
    }
}
