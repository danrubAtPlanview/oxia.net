// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (Get)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The public entry points of the OPT-IN pooled read API, as extension methods of IAsyncClient so that the existing interface
//              and its signatures do not change:
//                 using PooledGetResult r = await client.GetPooledAsync("key");
//                 ReadOnlySpan<byte> value = r.Value.Span;      // valid until r is disposed
//              Works with the client created by OxiaClient.CreateAsyncClientAsync.  For any other IAsyncClient implementation (a user's
//              mock) and for gets that need several shards (a comparison other than Equal, or an index, without a partition key) the
//              call falls back to GetAsync and COPIES the value into a pooled buffer: same contract, not zero-allocation.
//   Divergences: n/a (an addition; Go has no equivalent).
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client.Pooled;

public static class PooledClientExtensions
{
    /// <summary>
    /// Like <see cref="IAsyncClient.GetAsync"/>, but the value is exposed as a <see cref="ReadOnlyMemory{T}"/> over a pooled buffer instead
    /// of a new <c>byte[]</c>: in steady state a Get allocates nothing.  Dispose the result when you are done with the value.
    /// </summary>
    public static ValueTask<PooledGetResult> GetPooledAsync(this IAsyncClient client, string key, params ReadOnlySpan<GetOption> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (client is AsyncClientImpl impl)
        {
            var opts = GetOptions.Create(options);
            if (opts.PartitionKey is not null ||
                (opts.ComparisonType == KeyComparisonType.Equal && opts.SecondaryIndexName is null))
            {
                return GetPooledSingleShard(impl, key, in opts);
            }
        }

        return GetPooledFallback(client, key, options.ToArray());
    }

    private static ValueTask<PooledGetResult> GetPooledSingleShard(AsyncClientImpl client, string key, in GetOptions opts)
    {
        var call = PooledGetValueCall.Rent();
        call.Key = key;
        call.ComparisonType = opts.ComparisonType;
        call.IncludeValue = opts.IncludeValue;
        call.SecondaryIndexName = opts.SecondaryIndexName;
        call.PartitionKey = opts.PartitionKey;
        var result = call.AsValueTask();

        var add = client.ReadBatchManager.Get(client.GetShardForKey(key, opts.PartitionKey)).AddAsync(call);
        if (add.IsCompletedSuccessfully) return result;
        return AfterAddAsync(add, call, result);
    }

    private static async ValueTask<PooledGetResult> AfterAddAsync(ValueTask add, PooledGetValueCall call, ValueTask<PooledGetResult> result)
    {
        try
        {
            await add.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            call.Complete(null, e);
        }
        return await result.ConfigureAwait(false);
    }

    private static async ValueTask<PooledGetResult> GetPooledFallback(IAsyncClient client, string key, GetOption[] options)
    {
        var r = await client.GetAsync(key, options).ConfigureAwait(false);
        return r.Err is not null ? new PooledGetResult(r.Err) : PooledGetResult.FromArray(r.Key, r.Value, r.Version);
    }
}
