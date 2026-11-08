// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/admin_client_impl.go   (adminClientImpl, executeAdminRPCWithRedirect, NewAdminClient -> OxiaClient.CreateAdminClient)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Every admin RPC runs through ExecuteAdminRpcWithRedirectAsync: a 30s (ClientPool.DefaultRpcTimeout) deadline
//              derived from the caller's token, an immediate-retry loop (Go: backoff.Retry with ZeroBackOff bound to the
//              context), and one reason to retry: the server answered NodeIsNotLeader carrying a coordinator-leader hint that
//              differs from the target in use -> repeat the request against the hinted leader.  Any other failure is
//              permanent; the final error is mapped by AdminErrors.MapAdminError (Unauthorized/Unauthenticated/Unknown kinds).
//   Divergences: the retry loop is written inline instead of going through Retry.RetryAsync (identical semantics for
//              ZeroBackOff: no waiting between attempts, the context's error wins once the deadline/token is done); the
//              `result` variable of Go becomes the awaited response.  The generated OxiaAdminClient is called through its
//              virtual CallOptions overloads (that is what the tests' mock overrides).  A failed attempt to obtain the
//              client from the pool (GetAdminRpc throws) is permanent, like Go's `GetAminRpc` error; a null client is
//              the "unable to connect to admin server" error.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Constant;
using Oxia.Common.Rpc;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client;

internal sealed class AdminClientImpl : IAdminClient
{
    internal const string ErrUnableToConnectToAdminServer = "unable to connect to admin server";

    // Settable like the fields of Go's adminClientImpl struct (the tests swap the pool).
    internal string AdminAddr { get; set; }
    internal IClientPool ClientPool { get; set; }

    internal AdminClientImpl(string adminAddr, IClientPool clientPool)
    {
        AdminAddr = adminAddr;
        ClientPool = clientPool;
    }

    public async ValueTask<IReadOnlyList<G.DataServerView>> ListDataServersAsync(CancellationToken ct)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.ListDataServersRequest(),
            static (client, request, options) => client.ListDataServersAsync(request, options)).ConfigureAwait(false);
        return response.DataServers;
    }

    public async ValueTask<G.DataServerView> GetDataServerAsync(CancellationToken ct, string dataServer)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.GetDataServerRequest { DataServer = dataServer },
            static (client, request, options) => client.GetDataServerAsync(request, options)).ConfigureAwait(false);
        return response.DataServer;
    }

    public async ValueTask<G.DataServer> CreateDataServerAsync(CancellationToken ct, G.DataServer dataServer)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.CreateDataServerRequest { DataServer = dataServer },
            static (client, request, options) => client.CreateDataServerAsync(request, options)).ConfigureAwait(false);
        return response.DataServer;
    }

    public async ValueTask<G.DataServer> PatchDataServerAsync(CancellationToken ct, G.DataServer dataServer)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.PatchDataServerRequest { DataServer = dataServer },
            static (client, request, options) => client.PatchDataServerAsync(request, options)).ConfigureAwait(false);
        return response.DataServer;
    }

    public async ValueTask<G.DataServer> DeleteDataServerAsync(CancellationToken ct, string dataServer)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.DeleteDataServerRequest { DataServer = dataServer },
            static (client, request, options) => client.DeleteDataServerAsync(request, options)).ConfigureAwait(false);
        return response.DataServer;
    }

    public async ValueTask DrainedDataServerAsync(CancellationToken ct, string dataServer)
    {
        _ = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.DrainedDataServerRequest { DataServer = dataServer },
            static (client, request, options) => client.DrainedDataServerAsync(request, options)).ConfigureAwait(false);
    }

    public ValueTask CloseAsync() => ClientPool.CloseAsync();

    public async ValueTask<G.Namespace> CreateNamespaceAsync(CancellationToken ct, G.Namespace @namespace)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.CreateNamespaceRequest { Namespace = @namespace },
            static (client, request, options) => client.CreateNamespaceAsync(request, options)).ConfigureAwait(false);
        return response.Namespace;
    }

    public async ValueTask<G.Namespace> PatchNamespaceAsync(CancellationToken ct, G.Namespace @namespace)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.PatchNamespaceRequest { Namespace = @namespace },
            static (client, request, options) => client.PatchNamespaceAsync(request, options)).ConfigureAwait(false);
        return response.Namespace;
    }

    public async ValueTask<G.Namespace> DeleteNamespaceAsync(CancellationToken ct, string @namespace)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.DeleteNamespaceRequest { Namespace = @namespace },
            static (client, request, options) => client.DeleteNamespaceAsync(request, options)).ConfigureAwait(false);
        return response.Namespace;
    }

    public async ValueTask<IReadOnlyList<G.NamespaceView>> ListNamespacesAsync(CancellationToken ct)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.ListNamespacesRequest(),
            static (client, request, options) => client.ListNamespacesAsync(request, options)).ConfigureAwait(false);
        return response.Namespaces;
    }

    public async ValueTask<G.NamespaceView> GetNamespaceAsync(CancellationToken ct, string @namespace)
    {
        var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, new G.GetNamespaceRequest { Namespace = @namespace },
            static (client, request, options) => client.GetNamespaceAsync(request, options)).ConfigureAwait(false);
        return response.Namespace;
    }

    public async ValueTask<SplitShardResult> SplitShardAsync(CancellationToken ct, string @namespace, long shardId, uint? splitPoint)
    {
        var request = new G.SplitShardRequest { Namespace = @namespace, Shard = shardId };
        if (splitPoint.HasValue) request.SplitPoint = splitPoint.Value;

        try
        {
            var response = await ExecuteAdminRpcWithRedirectAsync(this, ct, request,
                static (client, req, options) => client.SplitShardAsync(req, options)).ConfigureAwait(false);
            return new SplitShardResult
            {
                LeftChildShardId = response.LeftChildShard,
                RightChildShardId = response.RightChildShard,
            };
        }
        catch (Exception err)
        {
            return new SplitShardResult { Error = err };
        }
    }

    /// <summary>
    /// Go: executeAdminRPCWithRedirect.  Runs <paramref name="operation"/> against the admin server, following the
    /// coordinator-leader hint of a NodeIsNotLeader failure.  Throws the mapped admin error (AdminErrors.MapAdminError).
    /// </summary>
    internal static async ValueTask<TResponse> ExecuteAdminRpcWithRedirectAsync<TRequest, TResponse>(
        AdminClientImpl admin, CancellationToken ct, TRequest request,
        Func<G.OxiaAdmin.OxiaAdminClient, TRequest, CallOptions, AsyncUnaryCall<TResponse>> operation)
    {
        string target = admin.AdminAddr;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(Oxia.Common.Rpc.ClientPool.DefaultRpcTimeout);
        var token = cts.Token;

        while (true)
        {
            G.OxiaAdmin.OxiaAdminClient? client;
            try
            {
                client = admin.ClientPool.GetAdminRpc(target);
            }
            catch (Exception err)
            {
                throw Mapped(err); // permanent
            }
            if (client is null)
            {
                throw Mapped(new OxiaException(ErrUnableToConnectToAdminServer)); // permanent
            }

            Exception failure;
            try
            {
                var call = operation(client, request, new CallOptions(cancellationToken: token));
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (Exception err)
            {
                failure = err;
            }

            var (oxiaErr, metadata) = OxiaErrors.FromGrpcError(failure);
            if (OxiaErrors.Is(oxiaErr, OxiaErrors.NodeIsNotLeader) &&
                metadata is not null && metadata.TryGetCoordinatorLeaderHint(out var leader) && leader != target)
            {
                target = leader;
                // Retry (ZeroBackOff): no waiting, but the context ends the loop
                if (token.IsCancellationRequested)
                {
                    throw Mapped(new OperationCanceledException(token));
                }
                continue;
            }

            throw Mapped(failure); // permanent
        }
    }

    private static Exception Mapped(Exception err) => OxiaErrors.ToThrowable(AdminErrors.MapAdminError(err)!);
}
