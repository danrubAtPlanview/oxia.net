// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go (NewAsyncClient), oxia/sync_client_impl.go (NewSyncClient),
//              oxia/admin_client_impl.go (NewAdminClient), oxia/cache.go (NewCache)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The public entry points of the library.  Each factory validates its options first (an invalid option set fails
//              the creation with the option error(s), nothing is started) and then connects: the shard manager waits for the
//              first shard assignments, so a server that cannot be reached fails the creation.
//   Divergences: the factories take `params ReadOnlySpan<ClientOption>` and therefore cannot be `async`: they resolve the
//              options synchronously, then await a private async method.  A creation failure surfaces as a FAULTED ValueTask
//              (OxiaErrors.ToThrowable(err); invalid options: the option sentinels are reachable with OxiaErrors.Is,
//              several of them as an AggregateException like multierr).  NewSyncClient prepends WithBatchLinger(0) so that
//              every write is sent as soon as it is made.  NewAdminClient cannot fail.  NewCache forwards to Cache.NewAsync.
// =====================================================================================================================
using System.Net.Security;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Rpc;

namespace Oxia.Client;

public static class OxiaClient
{
    /// <summary>
    /// Creates a new Oxia client with the async interface.  <paramref name="serviceAddress"/> is the target host:port of any
    /// Oxia server to bootstrap the client; it is used for establishing the shard assignments.  Ideally this should be a
    /// load-balanced endpoint.  Example:
    /// <c>await OxiaClient.CreateAsyncClientAsync("my-oxia-service:6648", OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(10)))</c>.
    /// </summary>
    public static ValueTask<IAsyncClient> CreateAsyncClientAsync(string serviceAddress, params ReadOnlySpan<ClientOption> opts)
    {
        var err = ClientOptions.TryCreate(serviceAddress, opts, out var options);
        if (err is not null)
        {
            return ValueTask.FromException<IAsyncClient>(OxiaErrors.ToThrowable(err));
        }
        return CreateAsyncClientCoreAsync(options);
    }

    private static async ValueTask<IAsyncClient> CreateAsyncClientCoreAsync(ClientOptions options)
        => await AsyncClientImpl.NewAsyncClientAsync(options).ConfigureAwait(false);

    /// <summary>
    /// Creates a new Oxia client with the sync interface (every write is sent immediately: BatchLinger 0).
    /// Example: <c>await OxiaClient.CreateSyncClientAsync("my-oxia-service:6648", OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(30)))</c>.
    /// </summary>
    public static ValueTask<ISyncClient> CreateSyncClientAsync(string serviceAddress, params ReadOnlySpan<ClientOption> opts)
    {
        var options = new ClientOption[opts.Length + 1];
        options[0] = OxiaOptions.WithBatchLinger(TimeSpan.Zero);
        opts.CopyTo(options.AsSpan(1));

        var err = ClientOptions.TryCreate(serviceAddress, options, out var resolved);
        if (err is not null)
        {
            return ValueTask.FromException<ISyncClient>(OxiaErrors.ToThrowable(err));
        }
        return CreateSyncClientCoreAsync(resolved);
    }

    private static async ValueTask<ISyncClient> CreateSyncClientCoreAsync(ClientOptions options)
    {
        var asyncClient = await AsyncClientImpl.NewAsyncClientAsync(options).ConfigureAwait(false);
        return SyncClientImpl.NewSyncClient(asyncClient);
    }

    /// <summary>Creates a client of the admin API of the coordinator at <paramref name="adminAddr"/> (host:port).</summary>
    public static IAdminClient CreateAdminClient(string adminAddr, SslClientAuthenticationOptions? tls = null, IAuthentication? authentication = null)
        => new AdminClientImpl(adminAddr, new ClientPool(tls, authentication));

    /// <summary>
    /// Creates a new cache object for a specific type, using <paramref name="serializeFunc"/> and
    /// <paramref name="deserializeFunc"/> for SerDe.  <paramref name="client"/> must be a client made by
    /// <see cref="CreateSyncClientAsync"/>.
    /// </summary>
    public static ValueTask<ICache<T>> CreateCache<T>(ISyncClient client, SerializeFunc serializeFunc, DeserializeFunc deserializeFunc)
        => Cache.NewAsync<T>(client, serializeFunc, deserializeFunc);
}
