// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/dizzy_shard_manager.go  (DizzyShardManager, NewDizzyShardManager)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A shard manager for deployments where the clients cannot reach the shard leaders directly (e.g. behind a
//              load balancer): everything is delegated to a regular ShardManager, except that every shard is "led" by the
//              lookup (service) address, so all the requests go there whatever the leader announced by the server.
//   Divergences: Go embeds the ShardManager interface; here every member is forwarded explicitly.  Close() is Dispose().
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client.Internal;

/// <summary>Go: DizzyShardManager.</summary>
public sealed class DizzyShardManager : IShardManager
{
    private readonly IShardManager _inner;
    private readonly string _lookupAddress;

    internal DizzyShardManager(IShardManager inner, string lookupAddress)
    {
        _inner = inner;
        _lookupAddress = lookupAddress;
    }

    /// <summary>Go: NewDizzyShardManager.</summary>
    public static async ValueTask<IShardManager> CreateAsync(IShardStrategy strategy, IRpcProvider rpc, string serviceAddress,
        string ns, TimeSpan requestTimeout, ShardsReplacedListener? onShardsReplaced)
    {
        var manager = await ShardManager.CreateAsync(strategy, rpc, serviceAddress, ns, requestTimeout, onShardsReplaced)
            .ConfigureAwait(false);
        return new DizzyShardManager(manager, serviceAddress);
    }

    /// <summary>Every request goes to the lookup address, whatever the shard leader.</summary>
    public string Leader(long shardId) => _lookupAddress;

    public long Get(string key) => _inner.Get(key);
    public long[] GetAll() => _inner.GetAll();
    public bool Exists(long shardId) => _inner.Exists(shardId);
    public long[]? GetSuccessors(long shardId) => _inner.GetSuccessors(shardId);
    public bool GetSuccessor(long shardId, string key, out long successorShardId) => _inner.GetSuccessor(shardId, key, out successorShardId);
    public Task Changed() => _inner.Changed();
    public KeySorting KeySorting => _inner.KeySorting;
    public void Dispose() => _inner.Dispose();
}
