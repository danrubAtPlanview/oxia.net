// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/admin_client_impl_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 21 (TestAdminClientRequestsFailOnServerError has 11 subtests), ported: 21, skipped: 0
//   Depends on other components: OxiaErrors.FromGrpcError / GrpcCode / IntoGrpcStatusError / WithCoordinatorLeaderHint /
//   TryGetCoordinatorLeaderHint (B).  The pool and the admin client are fakes, exactly like the Go test (no socket).
//   Adapted  :
//     * TestAdminClientListDataServersPassesContext: Go reads a context VALUE (context.WithValue) from the context the mock
//       receives; a CancellationToken carries no values, so the test checks that the token the RPC receives is derived from the
//       caller's token: cancelling the caller's token, from inside the call, cancels the token of the RPC.
//     * `grpcstatus.Error(code, msg)` -> RpcException(new Status(code, msg)); `proto.KeySortingType_NATURAL.String()` -> "NATURAL".
//     * `result == nil` of the table test -> the call threw (the Go helper returns (nil, err)).
//     * GetNameOrDefault()/NotificationsEnabledOrDefault() -> the generated Name / NotificationsEnabled properties.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Constant;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.F2;

public class AdminClientImplTests
{
    private const string NaturalKeySorting = "NATURAL"; // proto.KeySortingType_NATURAL.String()

    private static RpcException GrpcError(StatusCode code, string message) => new(new Status(code, message));

    private static AdminClientImpl NewAdmin(MockAdminRpcClient adminClient) =>
        new("admin-addr", new MockAdminClientPool { AdminClient = adminClient });

    private static G.DataServer DataServer(string name, string pub, string @internal, string? rackLabel = null)
    {
        var dataServer = new G.DataServer
        {
            Identity = new G.DataServerIdentity { Name = name, Public = pub, Internal = @internal },
        };
        if (rackLabel is not null)
        {
            dataServer.Metadata = new G.DataServerMetadata();
            dataServer.Metadata.Labels["rack"] = rackLabel;
        }
        return dataServer;
    }

    [Fact]
    public async Task TestAdminClientListDataServersPassesContext()
    {
        using var cts = new CancellationTokenSource();
        var adminClient = new MockAdminRpcClient { ListDataServersResponse = new G.ListDataServersResponse() };
        var admin = NewAdmin(adminClient);
        bool derived = false;
        adminClient.OnListDataServers = token =>
        {
            Assert.False(token.IsCancellationRequested);
            cts.Cancel(); // the token of the RPC follows the token of the caller
            derived = token.IsCancellationRequested;
        };

        _ = await admin.ListDataServersAsync(cts.Token);

        Assert.True(derived);
    }

    [Fact]
    public async Task TestAdminClientListDataServersMapsGrpcErrors()
    {
        var admin = NewAdmin(new MockAdminRpcClient { ListDataServersErr = GrpcError(StatusCode.NotFound, "data server not found") });

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await admin.ListDataServersAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unknown));
        Assert.Equal(StatusCode.NotFound, OxiaErrors.GrpcCode(err));
    }

    [Fact]
    public async Task TestAdminClientListDataServersMapsUnavailableConnection()
    {
        var admin = new AdminClientImpl("admin-addr", new MockAdminClientPool { Err = GrpcError(StatusCode.Unavailable, "connection refused") });

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await admin.ListDataServersAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unknown));
        Assert.Equal(StatusCode.Unavailable, OxiaErrors.GrpcCode(err));
    }

    [Fact]
    public async Task TestAdminClientListDataServersMapsAuthErrors()
    {
        var admin = NewAdmin(new MockAdminRpcClient { ListDataServersErr = GrpcError(StatusCode.PermissionDenied, "forbidden") });

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await admin.ListDataServersAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unauthorized));
        Assert.Equal(StatusCode.PermissionDenied, OxiaErrors.GrpcCode(err));

        admin.ClientPool = new MockAdminClientPool
        {
            AdminClient = new MockAdminRpcClient { ListDataServersErr = GrpcError(StatusCode.Unauthenticated, "missing token") },
        };

        err = await Assert.ThrowsAnyAsync<Exception>(async () => await admin.ListDataServersAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unauthenticated));
        Assert.Equal(StatusCode.Unauthenticated, OxiaErrors.GrpcCode(err));
    }

    // The admin server address resolves to no client: the request fails without being retried.
    [Fact]
    public async Task TestAdminClientListDataServersFailsWithoutClient()
    {
        var pool = new MockAdminClientPool();
        var admin = new AdminClientImpl("admin-addr", pool);

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await admin.ListDataServersAsync(CancellationToken.None));
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unknown));
        Assert.Contains(AdminClientImpl.ErrUnableToConnectToAdminServer, err.Message);
        Assert.Equal(["admin-addr"], pool.Targets);
    }

    [Theory]
    [InlineData("list-data-servers")]
    [InlineData("get-data-server")]
    [InlineData("create-data-server")]
    [InlineData("patch-data-server")]
    [InlineData("delete-data-server")]
    [InlineData("drained-data-server")]
    [InlineData("create-namespace")]
    [InlineData("patch-namespace")]
    [InlineData("delete-namespace")]
    [InlineData("list-namespaces")]
    [InlineData("get-namespace")]
    public async Task TestAdminClientRequestsFailOnServerError(string name)
    {
        var errServer = GrpcError(StatusCode.Internal, "server error");
        var admin = NewAdmin(new MockAdminRpcClient
        {
            ListDataServersErr = errServer,
            GetDataServerErr = errServer,
            CreateDataServerErr = errServer,
            PatchDataServerErr = errServer,
            DeleteDataServerErr = errServer,
            DrainedErr = errServer,
            CreateNamespaceErr = errServer,
            PatchNamespaceErr = errServer,
            DeleteNamespaceErr = errServer,
            ListNamespacesErr = errServer,
            GetNamespaceErr = errServer,
        });
        var ct = CancellationToken.None;

        Func<Task> request = name switch
        {
            "list-data-servers" => async () => await admin.ListDataServersAsync(ct),
            "get-data-server" => async () => await admin.GetDataServerAsync(ct, "server-1"),
            "create-data-server" => async () => await admin.CreateDataServerAsync(ct, new G.DataServer()),
            "patch-data-server" => async () => await admin.PatchDataServerAsync(ct, new G.DataServer()),
            "delete-data-server" => async () => await admin.DeleteDataServerAsync(ct, "server-1"),
            "drained-data-server" => async () => await admin.DrainedDataServerAsync(ct, "server-1"),
            "create-namespace" => async () => await admin.CreateNamespaceAsync(ct, new G.Namespace { Name = "ns" }),
            "patch-namespace" => async () => await admin.PatchNamespaceAsync(ct, new G.Namespace { Name = "ns" }),
            "delete-namespace" => async () => await admin.DeleteNamespaceAsync(ct, "ns"),
            "list-namespaces" => async () => await admin.ListNamespacesAsync(ct),
            "get-namespace" => async () => await admin.GetNamespaceAsync(ct, "ns"),
            _ => throw new ArgumentException(name),
        };

        var err = await Assert.ThrowsAnyAsync<Exception>(request);
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unknown));
        Assert.Equal(StatusCode.Internal, OxiaErrors.GrpcCode(err));
    }

    [Fact]
    public async Task TestAdminClientListDataServersReturnsResponse()
    {
        const string serverName = "server-1";
        var response = new G.ListDataServersResponse();
        response.DataServers.Add(new G.DataServerView { DataServer = DataServer(serverName, "public-1", "internal-1") });
        var admin = NewAdmin(new MockAdminRpcClient { ListDataServersResponse = response });

        var dataServers = await admin.ListDataServersAsync(CancellationToken.None);
        Assert.Single(dataServers);
        var dataServer = dataServers[0].DataServer;
        Assert.NotNull(dataServer.Identity);
        Assert.True(dataServer.Identity.HasName);
        Assert.Equal(serverName, dataServer.Identity.Name);
        Assert.Equal("public-1", dataServer.Identity.Public);
        Assert.Equal("internal-1", dataServer.Identity.Internal);
    }

    [Fact]
    public async Task TestAdminClientListDataServersRedirectsToCoordinatorLeader()
    {
        const string serverName = "server-1";
        var response = new G.ListDataServersResponse();
        response.DataServers.Add(new G.DataServerView { DataServer = DataServer(serverName, "public-1", "internal-1") });
        var pool = new MockAdminClientPool
        {
            Clients = new Dictionary<string, G.OxiaAdmin.OxiaAdminClient>
            {
                ["coordinator-0:6651"] = new MockAdminRpcClient
                {
                    ListDataServersErr = OxiaErrors.IntoGrpcStatusError(
                        OxiaErrors.NodeIsNotLeader,
                        OxiaErrors.WithCoordinatorLeaderHint("coordinator-1:6651")),
                },
                ["coordinator-1:6651"] = new MockAdminRpcClient { ListDataServersResponse = response },
            },
        };
        var admin = new AdminClientImpl("coordinator-0:6651", pool);

        var dataServers = await admin.ListDataServersAsync(CancellationToken.None);
        Assert.Single(dataServers);
        Assert.Equal(serverName, dataServers[0].DataServer.Identity.Name);
        Assert.Equal(["coordinator-0:6651", "coordinator-1:6651"], pool.Targets);
    }

    [Fact]
    public async Task TestAdminClientGetDataServerReturnsResponse()
    {
        const string serverName = "server-1";
        var admin = NewAdmin(new MockAdminRpcClient
        {
            GetDataServerResponse = new G.GetDataServerResponse
            {
                DataServer = new G.DataServerView { DataServer = DataServer(serverName, "public-1", "internal-1", "rack-1") },
            },
        });

        var dataServer = await admin.GetDataServerAsync(CancellationToken.None, serverName);
        Assert.NotNull(dataServer);
        Assert.NotNull(dataServer.DataServer.Identity);
        Assert.True(dataServer.DataServer.Identity.HasName);
        Assert.Equal(serverName, dataServer.DataServer.Identity.Name);
        Assert.Equal("public-1", dataServer.DataServer.Identity.Public);
        Assert.Equal("internal-1", dataServer.DataServer.Identity.Internal);
        Assert.Equal(new Dictionary<string, string> { ["rack"] = "rack-1" }, dataServer.DataServer.Metadata.Labels);
    }

    [Fact]
    public async Task TestAdminClientCreateDataServerReturnsResponse()
    {
        const string serverName = "server-1";
        var admin = NewAdmin(new MockAdminRpcClient
        {
            CreateDataServerResp = new G.CreateDataServerResponse { DataServer = DataServer(serverName, "public-1", "internal-1", "rack-1") },
        });

        var dataServer = await admin.CreateDataServerAsync(CancellationToken.None, DataServer(serverName, "public-1", "internal-1"));
        Assert.NotNull(dataServer);
        Assert.NotNull(dataServer.Identity);
        Assert.True(dataServer.Identity.HasName);
        Assert.Equal(serverName, dataServer.Identity.Name);
        Assert.Equal("public-1", dataServer.Identity.Public);
        Assert.Equal("internal-1", dataServer.Identity.Internal);
        Assert.Equal(new Dictionary<string, string> { ["rack"] = "rack-1" }, dataServer.Metadata.Labels);
    }

    [Fact]
    public async Task TestAdminClientPatchDataServerReturnsResponse()
    {
        const string serverName = "server-1";
        var admin = NewAdmin(new MockAdminRpcClient
        {
            PatchDataServerResp = new G.PatchDataServerResponse { DataServer = DataServer(serverName, "public-2", "internal-1", "rack-2") },
        });

        var patch = new G.DataServer { Identity = new G.DataServerIdentity { Name = serverName, Public = "public-2" }, Metadata = new G.DataServerMetadata() };
        patch.Metadata.Labels["rack"] = "rack-2";
        var dataServer = await admin.PatchDataServerAsync(CancellationToken.None, patch);
        Assert.NotNull(dataServer);
        Assert.NotNull(dataServer.Identity);
        Assert.True(dataServer.Identity.HasName);
        Assert.Equal(serverName, dataServer.Identity.Name);
        Assert.Equal("public-2", dataServer.Identity.Public);
        Assert.Equal("internal-1", dataServer.Identity.Internal);
        Assert.Equal(new Dictionary<string, string> { ["rack"] = "rack-2" }, dataServer.Metadata.Labels);
    }

    [Fact]
    public async Task TestAdminClientDeleteDataServerReturnsResponse()
    {
        const string serverName = "server-1";
        var admin = NewAdmin(new MockAdminRpcClient
        {
            DeleteDataServerResp = new G.DeleteDataServerResponse { DataServer = DataServer(serverName, "public-1", "internal-1", "rack-1") },
        });

        var dataServer = await admin.DeleteDataServerAsync(CancellationToken.None, serverName);
        Assert.NotNull(dataServer);
        Assert.NotNull(dataServer.Identity);
        Assert.True(dataServer.Identity.HasName);
        Assert.Equal(serverName, dataServer.Identity.Name);
        Assert.Equal("public-1", dataServer.Identity.Public);
        Assert.Equal("internal-1", dataServer.Identity.Internal);
        Assert.Equal(new Dictionary<string, string> { ["rack"] = "rack-1" }, dataServer.Metadata.Labels);
    }

    [Fact]
    public async Task TestAdminClientDrainedDataServerSendsTheName()
    {
        var adminClient = new MockAdminRpcClient();
        var admin = NewAdmin(adminClient);

        await admin.DrainedDataServerAsync(CancellationToken.None, "server-1");
        Assert.NotNull(adminClient.DrainedReq);
        Assert.Equal("server-1", adminClient.DrainedReq.DataServer);
    }

    [Fact]
    public async Task TestAdminClientCreateNamespaceReturnsResponse()
    {
        var admin = NewAdmin(new MockAdminRpcClient
        {
            CreateNamespaceResp = new G.CreateNamespaceResponse
            {
                Namespace = new G.Namespace { Name = "ns-1", InitialShardCount = 4, ReplicationFactor = 3, KeySorting = NaturalKeySorting },
            },
        });

        var @namespace = await admin.CreateNamespaceAsync(CancellationToken.None, new G.Namespace { Name = "ns-1" });
        Assert.NotNull(@namespace);
        Assert.Equal("ns-1", @namespace.Name);
        Assert.Equal(4u, @namespace.InitialShardCount);
        Assert.Equal(3u, @namespace.ReplicationFactor);
        Assert.Equal(NaturalKeySorting, @namespace.KeySorting);
    }

    [Fact]
    public async Task TestAdminClientPatchNamespaceReturnsResponse()
    {
        var adminClient = new MockAdminRpcClient
        {
            PatchNamespaceResp = new G.PatchNamespaceResponse
            {
                Namespace = new G.Namespace
                {
                    Name = "ns-1", InitialShardCount = 4, ReplicationFactor = 3, NotificationsEnabled = false, KeySorting = "natural",
                },
            },
        };
        var admin = NewAdmin(adminClient);

        var @namespace = await admin.PatchNamespaceAsync(CancellationToken.None, new G.Namespace { Name = "ns-1", NotificationsEnabled = false });
        Assert.NotNull(@namespace);
        Assert.Equal("ns-1", @namespace.Name);
        Assert.Equal(4u, @namespace.InitialShardCount);
        Assert.Equal(3u, @namespace.ReplicationFactor);
        Assert.False(@namespace.NotificationsEnabled);
        Assert.Equal("natural", @namespace.KeySorting);
        // the request carried the namespace, with the explicit false
        Assert.NotNull(adminClient.PatchNamespaceReq);
        Assert.True(adminClient.PatchNamespaceReq.Namespace.HasNotificationsEnabled);
    }

    [Fact]
    public async Task TestAdminClientDeleteNamespaceReturnsResponse()
    {
        var admin = NewAdmin(new MockAdminRpcClient
        {
            DeleteNamespaceResp = new G.DeleteNamespaceResponse
            {
                Namespace = new G.Namespace { Name = "ns-1", InitialShardCount = 4, ReplicationFactor = 3, KeySorting = "natural" },
            },
        });

        var @namespace = await admin.DeleteNamespaceAsync(CancellationToken.None, "ns-1");
        Assert.NotNull(@namespace);
        Assert.Equal("ns-1", @namespace.Name);
        Assert.Equal(4u, @namespace.InitialShardCount);
        Assert.Equal(3u, @namespace.ReplicationFactor);
        Assert.Equal("natural", @namespace.KeySorting);
    }

    [Fact]
    public async Task TestAdminClientListNamespacesReturnsResponse()
    {
        var response = new G.ListNamespacesResponse();
        response.Namespaces.Add(new G.NamespaceView
        {
            Namespace = new G.Namespace { Name = "ns-1", InitialShardCount = 4, ReplicationFactor = 3 },
            NamespaceStatus = new G.NamespaceStatus(),
        });
        var admin = NewAdmin(new MockAdminRpcClient { ListNamespacesResp = response });

        var namespaces = await admin.ListNamespacesAsync(CancellationToken.None);
        Assert.Single(namespaces);
        Assert.Equal("ns-1", namespaces[0].Namespace.Name);
        Assert.Equal(4u, namespaces[0].Namespace.InitialShardCount);
        Assert.Equal(3u, namespaces[0].Namespace.ReplicationFactor);
        Assert.NotNull(namespaces[0].NamespaceStatus);
    }

    [Fact]
    public async Task TestAdminClientGetNamespaceReturnsResponse()
    {
        var admin = NewAdmin(new MockAdminRpcClient
        {
            GetNamespaceResp = new G.GetNamespaceResponse
            {
                Namespace = new G.NamespaceView
                {
                    Namespace = new G.Namespace { Name = "ns-1", InitialShardCount = 4, ReplicationFactor = 3, KeySorting = NaturalKeySorting },
                    NamespaceStatus = new G.NamespaceStatus(),
                },
            },
        });

        var @namespace = await admin.GetNamespaceAsync(CancellationToken.None, "ns-1");
        Assert.NotNull(@namespace);
        Assert.Equal("ns-1", @namespace.Namespace.Name);
        Assert.Equal(4u, @namespace.Namespace.InitialShardCount);
        Assert.Equal(3u, @namespace.Namespace.ReplicationFactor);
        Assert.Equal(NaturalKeySorting, @namespace.Namespace.KeySorting);
        Assert.NotNull(@namespace.NamespaceStatus);
    }

    [Fact]
    public async Task TestAdminClientSplitShard()
    {
        var adminClient = new MockAdminRpcClient { SplitShardResp = new G.SplitShardResponse { LeftChildShard = 2, RightChildShard = 3 } };
        var admin = NewAdmin(adminClient);

        var result = await admin.SplitShardAsync(CancellationToken.None, "ns", 1, null);
        Assert.Null(result.Error);
        Assert.Equal(2, result.LeftChildShardId);
        Assert.Equal(3, result.RightChildShardId);
        Assert.Equal("ns", adminClient.SplitShardReq!.Namespace);
        Assert.Equal(1, adminClient.SplitShardReq.Shard);
        Assert.False(adminClient.SplitShardReq.HasSplitPoint);

        uint splitPoint = 1000;
        result = await admin.SplitShardAsync(CancellationToken.None, "ns", 1, splitPoint);
        Assert.Null(result.Error);
        Assert.True(adminClient.SplitShardReq.HasSplitPoint);
        Assert.Equal(splitPoint, adminClient.SplitShardReq.SplitPoint);
    }

    [Fact]
    public async Task TestAdminClientSplitShardFailsOnServerError()
    {
        var admin = NewAdmin(new MockAdminRpcClient { SplitShardErr = GrpcError(StatusCode.PermissionDenied, "forbidden") });

        var result = await admin.SplitShardAsync(CancellationToken.None, "ns", 1, null);
        Assert.True(OxiaErrors.Is(result.Error, ClientErrors.Unauthorized));
        Assert.Equal(0, result.LeftChildShardId);
        Assert.Equal(0, result.RightChildShardId);
    }

    [Fact]
    public void TestWrapAdminErrorPreservesCause()
    {
        var cause = GrpcError(StatusCode.InvalidArgument, "invalid request");

        var err = AdminErrors.MapAdminError(cause);

        Assert.NotNull(err);
        Assert.True(OxiaErrors.Is(err, ClientErrors.Unknown));
        Assert.True(OxiaErrors.Is(err, cause));
    }
}
