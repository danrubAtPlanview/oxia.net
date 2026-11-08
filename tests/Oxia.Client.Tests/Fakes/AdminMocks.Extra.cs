// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/admin_client_impl_test.go (mockAdminRpcClient, mockAdminClientPool)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go test mocks the generated proto.OxiaAdminClient interface and the rpc.ClientPool; here the mock is a
//              subclass of the generated OxiaAdmin.OxiaAdminClient (a class with virtual methods and a protected ctor for test
//              doubles) overriding the CallOptions overloads that AdminClientImpl calls, plus an IClientPool fake.
//              (The brief of this component mentioned a mock OxiaAdminServer hosted by TestGrpcServer; the Go test has none --
//              it never opens a socket -- and admin.proto is generated client-only, so there is no OxiaAdminBase.)
//   Divergences: `listDataServersCtx` (a context.Context whose Value is inspected) -> the CancellationToken of the call, see
//              OnListDataServers; a Go (nil, err) pair -> a faulted AsyncUnaryCall.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Rpc;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.F2;

internal sealed class MockAdminRpcClient : G.OxiaAdmin.OxiaAdminClient
{
    public G.ListDataServersResponse? ListDataServersResponse { get; init; }
    public CancellationToken ListDataServersCtx { get; private set; }
    /// <summary>Runs inside the ListDataServers call (see TestAdminClientListDataServersPassesContext).</summary>
    public Action<CancellationToken>? OnListDataServers { get; set; }
    public Exception? ListDataServersErr { get; init; }
    public G.GetDataServerResponse? GetDataServerResponse { get; init; }
    public Exception? GetDataServerErr { get; init; }
    public G.CreateDataServerResponse? CreateDataServerResp { get; init; }
    public Exception? CreateDataServerErr { get; init; }
    public G.PatchDataServerResponse? PatchDataServerResp { get; init; }
    public Exception? PatchDataServerErr { get; init; }
    public G.DeleteDataServerResponse? DeleteDataServerResp { get; init; }
    public Exception? DeleteDataServerErr { get; init; }
    public G.DrainedDataServerRequest? DrainedReq { get; private set; }
    public Exception? DrainedErr { get; init; }
    public G.CreateNamespaceResponse? CreateNamespaceResp { get; init; }
    public Exception? CreateNamespaceErr { get; init; }
    public G.PatchNamespaceResponse? PatchNamespaceResp { get; init; }
    public G.PatchNamespaceRequest? PatchNamespaceReq { get; private set; }
    public Exception? PatchNamespaceErr { get; init; }
    public G.DeleteNamespaceResponse? DeleteNamespaceResp { get; init; }
    public Exception? DeleteNamespaceErr { get; init; }
    public G.ListNamespacesResponse? ListNamespacesResp { get; init; }
    public Exception? ListNamespacesErr { get; init; }
    public G.GetNamespaceResponse? GetNamespaceResp { get; init; }
    public Exception? GetNamespaceErr { get; init; }
    public G.SplitShardRequest? SplitShardReq { get; private set; }
    public G.SplitShardResponse? SplitShardResp { get; init; }
    public Exception? SplitShardErr { get; init; }

    private static AsyncUnaryCall<T> Result<T>(T? response, Exception? err) where T : class
        => new(err is not null ? Task.FromException<T>(err) : Task.FromResult(response!),
            Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });

    public override AsyncUnaryCall<G.ListDataServersResponse> ListDataServersAsync(G.ListDataServersRequest request, CallOptions options)
    {
        ListDataServersCtx = options.CancellationToken;
        OnListDataServers?.Invoke(options.CancellationToken);
        return Result(ListDataServersResponse, ListDataServersErr);
    }

    public override AsyncUnaryCall<G.GetDataServerResponse> GetDataServerAsync(G.GetDataServerRequest request, CallOptions options)
        => Result(GetDataServerResponse, GetDataServerErr);

    public override AsyncUnaryCall<G.CreateDataServerResponse> CreateDataServerAsync(G.CreateDataServerRequest request, CallOptions options)
        => Result(CreateDataServerResp, CreateDataServerErr);

    public override AsyncUnaryCall<G.PatchDataServerResponse> PatchDataServerAsync(G.PatchDataServerRequest request, CallOptions options)
        => Result(PatchDataServerResp, PatchDataServerErr);

    public override AsyncUnaryCall<G.DeleteDataServerResponse> DeleteDataServerAsync(G.DeleteDataServerRequest request, CallOptions options)
        => Result(DeleteDataServerResp, DeleteDataServerErr);

    public override AsyncUnaryCall<G.DrainedDataServerResponse> DrainedDataServerAsync(G.DrainedDataServerRequest request, CallOptions options)
    {
        DrainedReq = request;
        return Result(new G.DrainedDataServerResponse(), DrainedErr);
    }

    public override AsyncUnaryCall<G.CreateNamespaceResponse> CreateNamespaceAsync(G.CreateNamespaceRequest request, CallOptions options)
        => Result(CreateNamespaceResp, CreateNamespaceErr);

    public override AsyncUnaryCall<G.PatchNamespaceResponse> PatchNamespaceAsync(G.PatchNamespaceRequest request, CallOptions options)
    {
        PatchNamespaceReq = request;
        return Result(PatchNamespaceResp, PatchNamespaceErr);
    }

    public override AsyncUnaryCall<G.DeleteNamespaceResponse> DeleteNamespaceAsync(G.DeleteNamespaceRequest request, CallOptions options)
        => Result(DeleteNamespaceResp, DeleteNamespaceErr);

    public override AsyncUnaryCall<G.GetNamespaceResponse> GetNamespaceAsync(G.GetNamespaceRequest request, CallOptions options)
        => Result(GetNamespaceResp, GetNamespaceErr);

    public override AsyncUnaryCall<G.ListNamespacesResponse> ListNamespacesAsync(G.ListNamespacesRequest request, CallOptions options)
        => Result(ListNamespacesResp, ListNamespacesErr);

    public override AsyncUnaryCall<G.SplitShardResponse> SplitShardAsync(G.SplitShardRequest request, CallOptions options)
    {
        SplitShardReq = request;
        return Result(SplitShardResp, SplitShardErr);
    }
}

internal sealed class MockAdminClientPool : IClientPool
{
    public G.OxiaAdmin.OxiaAdminClient? AdminClient { get; init; }
    public Dictionary<string, G.OxiaAdmin.OxiaAdminClient>? Clients { get; init; }
    public List<string> Targets { get; } = [];
    public Exception? Err { get; init; }

    public ValueTask CloseAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public IOxiaClientRpc GetClientRpc(string target) => throw new InvalidOperationException("unexpected GetClientRpc call");

    public G.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target)
    {
        Targets.Add(target);
        // Go returns the client together with the error: an error wins
        if (Err is not null) throw Err;
        if (Clients is not null)
        {
            Clients.TryGetValue(target, out var client);
            return client!;
        }
        return AdminClient!;
    }
}
