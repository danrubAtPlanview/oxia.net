// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : common/rpc/oxia_error_streaming_client.go (error translation of Recv/Send), common/proto/client_grpc.pb.go
//              (the client side of every rpc of the OxiaClient service)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Round trips of every rpc of OxiaClientRpc over a REAL gRPC channel (HTTP/2 over loopback) against a mock server
//              derived from the protoc-generated OxiaClient.OxiaClientBase hosted by TestGrpcServer: unary calls, every
//              server-streaming call, the WriteStream duplex ping-pong, end of stream (null), server errors translated to
//              Oxia sentinels while keeping the original RpcException reachable (leader hints, status code), non-Oxia
//              errors passed through unchanged, cancellation of in-flight calls, Dispose cancelling the call, large payloads.
//   Divergences: n/a (new tests).  The mock server uses OxiaErrors.IntoGrpcStatusError (common-utilities component).
// =====================================================================================================================
using System.Collections.Concurrent;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Proto;

/// <summary>State shared by the per-request instances of <see cref="MockOxiaClientService"/>.</summary>
public sealed class MockOxiaState
{
    public ConcurrentDictionary<string, TaskCompletionSource<bool>> ServerSawCancellation { get; } = new();
    public ConcurrentDictionary<string, (string? Namespace, string? ShardId)> WriteStreamHeaders { get; } = new();

    public TaskCompletionSource<bool> Cancellation(string id) => ServerSawCancellation.GetOrAdd(id, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));

    public static RpcException NotLeader() => OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(3, "leader.example:6648"))!;
}

/// <summary>Mock OxiaClient server.  Behaviour is selected by the request content (keys / shard ids) so tests can share one server.</summary>
public sealed class MockOxiaClientService : G.OxiaClient.OxiaClientBase
{
    private readonly MockOxiaState _state;

    public MockOxiaClientService(MockOxiaState state) => _state = state;

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
    {
        if (request.Namespace == "fail-oxia") throw MockOxiaState.NotLeader();
        if (request.Namespace == "block") await BlockUntilCancelled(request.Namespace, context);
        for (int i = 0; i < 2; i++)
        {
            var sa = new G.ShardAssignments();
            var ns = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3, KeySorting = G.KeySorting.Hierarchical };
            ns.Assignments.Add(new G.ShardAssignment { Shard = 1 + i, Leader = "leader-" + i + ":6648", Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0, MaxHashInclusive = 0x7FFFFFFF } });
            ns.Assignments.Add(new G.ShardAssignment { Shard = 10 + i, Leader = "leader-b:6648", Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0x80000000, MaxHashInclusive = uint.MaxValue } });
            sa.Namespaces[request.Namespace] = ns;
            sa.AllowedAuthorities.Add("authority-" + i);
            await responseStream.WriteAsync(sa);
        }
    }

    public override Task<G.WriteResponse> Write(G.WriteRequest request, ServerCallContext context)
    {
        if (request.Puts.Count > 0 && request.Puts[0].Key == "fail-oxia") throw MockOxiaState.NotLeader();
        return Task.FromResult(Respond(request));
    }

    private static G.WriteResponse Respond(G.WriteRequest request)
    {
        var response = new G.WriteResponse();
        foreach (G.PutRequest p in request.Puts)
        {
            var pr = new G.PutResponse { Status = G.Status.Ok, Version = new G.Version { VersionId = p.OpIndex, ModificationsCount = p.Value.Length, CreatedTimestamp = 111, ModifiedTimestamp = 222 } };
            if (p.HasSessionId) pr.Version.SessionId = p.SessionId;
            if (p.SequenceKeyDelta.Count > 0) pr.Key = p.Key + "-" + string.Join("-", p.SequenceKeyDelta);
            response.Puts.Add(pr);
        }
        foreach (G.DeleteRequest d in request.Deletes) response.Deletes.Add(new G.DeleteResponse { Status = d.Key == "missing" ? G.Status.KeyNotFound : G.Status.Ok });
        foreach (G.DeleteRangeRequest _ in request.DeleteRanges) response.DeleteRanges.Add(new G.DeleteRangeResponse { Status = G.Status.Ok });
        return response;
    }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream, ServerCallContext context)
    {
        string? ns = context.RequestHeaders.GetValue("namespace");
        string? shard = context.RequestHeaders.GetValue("shard-id");
        _state.WriteStreamHeaders[shard ?? "?"] = (ns, shard);
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            G.WriteRequest request = requestStream.Current;
            if (request.Puts.Count > 0 && request.Puts[0].Key == "fail-oxia") throw MockOxiaState.NotLeader();
            if (request.Puts.Count > 0 && request.Puts[0].Key == "fail-other") throw new RpcException(new Grpc.Core.Status(StatusCode.Internal, "boom"));
            if (request.Puts.Count > 0 && request.Puts[0].Key == "block") await BlockUntilCancelled("ws-" + shard, context);
            await responseStream.WriteAsync(Respond(request));
        }
    }

    public override async Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context)
    {
        string first = request.Gets.Count > 0 ? request.Gets[0].Key : "";
        if (first == "fail-oxia") throw MockOxiaState.NotLeader();
        if (first == "fail-other") throw new RpcException(new Grpc.Core.Status(StatusCode.Internal, "boom"));
        if (first.StartsWith("block-", StringComparison.Ordinal)) await BlockUntilCancelled(first, context);
        if (first == "fail-after-first")
        {
            var r = new G.ReadResponse();
            r.Gets.Add(new G.GetResponse { Status = G.Status.Ok, Key = "partial" });
            await responseStream.WriteAsync(r);
            throw MockOxiaState.NotLeader();
        }
        var response = new G.ReadResponse();
        int idx = 0;
        foreach (G.GetRequest get in request.Gets)
        {
            var gr = new G.GetResponse { Status = G.Status.Ok, Version = new G.Version { VersionId = idx, ModificationsCount = 2, CreatedTimestamp = 5, ModifiedTimestamp = 6 } };
            if (get.Key.StartsWith("big-", StringComparison.Ordinal))
            {
                var value = new byte[int.Parse(get.Key[4..])];
                for (int i = 0; i < value.Length; i++) value[i] = (byte)(i * 7);
                gr.Value = ByteString.CopyFrom(value);
            }
            else if (get.IncludeValue)
            {
                gr.Value = ByteString.CopyFromUtf8(get.Key);
            }
            if (get.ComparisonType != G.KeyComparisonType.Equal) gr.Key = get.Key + "!";
            if (get.HasSecondaryIndexName) gr.SecondaryIndexKey = get.SecondaryIndexName;
            response.Gets.Add(gr);
            idx++;
            if (response.Gets.Count == 2)
            {
                await responseStream.WriteAsync(response);
                response = new G.ReadResponse();
            }
        }
        if (response.Gets.Count > 0) await responseStream.WriteAsync(response);
    }

    [Obsolete("List is deprecated upstream; the mock still serves it")]
    public override async Task List(G.ListRequest request, IServerStreamWriter<G.ListResponse> responseStream, ServerCallContext context)
    {
        if (request.StartInclusive == "fail-oxia") throw MockOxiaState.NotLeader();
        for (int i = 0; i < 3; i++)
        {
            var r = new G.ListResponse { EndInclusive = request.EndInclusive };
            r.Keys.Add(request.StartInclusive + "/a" + i);
            r.Keys.Add(request.StartInclusive + "/é€😀" + i);
            await responseStream.WriteAsync(r);
        }
    }

    public override async Task RangeScan(G.RangeScanRequest request, IServerStreamWriter<G.RangeScanResponse> responseStream, ServerCallContext context)
    {
        if (request.StartInclusive == "fail-oxia") throw MockOxiaState.NotLeader();
        for (int i = 0; i < 3; i++)
        {
            var r = new G.RangeScanResponse { EndInclusive = i == 2 && request.EndInclusive };
            for (int k = 0; k < 4; k++)
            {
                var rec = new G.GetResponse { Status = G.Status.Ok, Key = request.StartInclusive + "/" + i + "-" + k, Version = new G.Version { VersionId = k } };
                if (!request.HasIncludeValues || request.IncludeValues) rec.Value = ByteString.CopyFromUtf8("v" + i + k);
                r.Records.Add(rec);
            }
            await responseStream.WriteAsync(r);
        }
    }

    public override async Task GetSequenceUpdates(G.GetSequenceUpdatesRequest request, IServerStreamWriter<G.GetSequenceUpdatesResponse> responseStream, ServerCallContext context)
    {
        if (request.Key.StartsWith("block-", StringComparison.Ordinal)) await BlockUntilCancelled(request.Key, context);
        for (int i = 0; i < 3; i++) await responseStream.WriteAsync(new G.GetSequenceUpdatesResponse { HighestSequenceKey = request.Key + "-" + (i + 1).ToString("D4") });
    }

    public override async Task GetNotifications(G.NotificationsRequest request, IServerStreamWriter<G.NotificationBatch> responseStream, ServerCallContext context)
    {
        if (request.Shard == -1) await BlockUntilCancelled("notif-block", context);
        long offset = request.HasStartOffsetExclusive ? request.StartOffsetExclusive : -1;
        for (int i = 0; i < 3; i++)
        {
            var b = new G.NotificationBatch { Shard = request.Shard, Offset = ++offset, Timestamp = 1_700_000_000_000UL + (ulong)i };
            b.Notifications.Add(new G.NotificationEntry { Key = "/k" + i, Value = new G.Notification { Type = G.NotificationType.KeyCreated, VersionId = i } });
            b.Notifications.Add(new G.NotificationEntry { Key = "", Value = new G.Notification { Type = G.NotificationType.KeyRangeDeleted, KeyRangeLast = "/z" } });
            await responseStream.WriteAsync(b);
        }
    }

    public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
    {
        if (request.ClientIdentity == "fail-oxia") throw MockOxiaState.NotLeader();
        if (request.ClientIdentity == "fail-other") throw new RpcException(new Grpc.Core.Status(StatusCode.Internal, "boom"));
        if (request.ClientIdentity == "slow") return SlowAsync(context);
        return Task.FromResult(new G.CreateSessionResponse { SessionId = 1000 + request.Shard * 10 + request.SessionTimeoutMs });
    }

    private async Task<G.CreateSessionResponse> SlowAsync(ServerCallContext context)
    {
        await BlockUntilCancelled("slow-unary", context);
        return new G.CreateSessionResponse();
    }

    public override Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
    {
        if (request.SessionId == -1) throw MockOxiaState.NotLeader();
        return Task.FromResult(new G.KeepAliveResponse());
    }

    public override Task<G.CloseSessionResponse> CloseSession(G.CloseSessionRequest request, ServerCallContext context)
    {
        if (request.SessionId == -1) throw MockOxiaState.NotLeader();
        return Task.FromResult(new G.CloseSessionResponse());
    }

    private async Task BlockUntilCancelled(string id, ServerCallContext context)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            _state.Cancellation(id).TrySetResult(true);
            throw;
        }
    }
}

public sealed class OxiaClientRpcFixture : IAsyncLifetime
{
    public MockOxiaState State { get; } = new();
    public TestGrpcServer Server { get; private set; } = null!;
    public GrpcChannel Channel { get; private set; } = null!;
    public OxiaClientRpc Rpc { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<MockOxiaClientService>(), services => services.AddSingleton(State));
        Channel = GrpcChannel.ForAddress("http://" + Server.Address, new GrpcChannelOptions { MaxReceiveMessageSize = null, MaxSendMessageSize = null });
        Rpc = new OxiaClientRpc(Channel.CreateCallInvoker());
    }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await Server.DisposeAsync();
    }
}

public class OxiaClientRpcTests : IClassFixture<OxiaClientRpcFixture>
{
    private static readonly TimeSpan Timeout5s = TimeSpan.FromSeconds(10);
    private readonly OxiaClientRpcFixture _f;

    public OxiaClientRpcTests(OxiaClientRpcFixture fixture) => _f = fixture;

    private static async Task<List<T>> RecvAll<T>(IServerStreamingClient<T> stream) where T : class
    {
        var result = new List<T>();
        while (await stream.RecvAsync().AsTask().WaitAsync(Timeout5s) is { } next) result.Add(next);
        return result;
    }

    // ---- unary -------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task CreateSession_KeepAlive_CloseSession_RoundTrip()
    {
        var created = await _f.Rpc.CreateSessionAsync(new CreateSessionRequest { Shard = 7, SessionTimeoutMs = 3, ClientIdentity = "client-é€😀" }, default);
        Assert.Equal(1073L, created.SessionId);
        Assert.NotNull(await _f.Rpc.KeepAliveAsync(new SessionHeartbeat { Shard = 7, SessionId = created.SessionId }, default));
        Assert.NotNull(await _f.Rpc.CloseSessionAsync(new CloseSessionRequest { Shard = 7, SessionId = created.SessionId }, default));
    }

    [Fact]
    public async Task DeprecatedUnaryWrite_RoundTrips()
    {
        var req = new WriteRequest { Shard = 1 };
        req.Puts.Add(new PutRequest { Key = "a", Value = new byte[] { 1, 2, 3 }, SessionId = 5, OpIndex = 9 });
        req.Puts.Add(new PutRequest { Key = "seq-", SequenceKeyDelta = new ulong[] { 1, 2 }, OpIndex = 10 });
        req.Deletes.Add(new DeleteRequest { Key = "missing" });
        req.DeleteRanges.Add(new DeleteRangeRequest { StartInclusive = "a", EndExclusive = "b" });
        WriteResponse resp = await _f.Rpc.WriteAsync(req, default);
        Assert.Equal(2, resp.Puts.Count);
        Assert.Equal(9L, resp.Puts[0].Version!.VersionId);
        Assert.Equal(3L, resp.Puts[0].Version!.ModificationsCount);
        Assert.Equal(5L, resp.Puts[0].Version!.SessionId);
        Assert.Equal(111UL, resp.Puts[0].Version!.CreatedTimestamp);
        Assert.Null(resp.Puts[0].Key);
        Assert.Equal("seq--1-2", resp.Puts[1].Key);
        Assert.Equal(Oxia.Common.Proto.Status.KeyNotFound, resp.Deletes[0].Status);
        Assert.Single(resp.DeleteRanges);
    }

    [Fact]
    public async Task UnaryOxiaError_IsTranslatedAndKeepsTheOriginalGrpcFailure()
    {
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await _f.Rpc.CreateSessionAsync(new CreateSessionRequest { ClientIdentity = "fail-oxia" }, default));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader), ex.ToString());
        AssertLeaderHintReachable(ex);
        Assert.Equal(OxiaErrors.NodeIsNotLeader.Message, ex.Message);

        ex = await Assert.ThrowsAnyAsync<Exception>(async () => await _f.Rpc.KeepAliveAsync(new SessionHeartbeat { SessionId = -1 }, default));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader));
        ex = await Assert.ThrowsAnyAsync<Exception>(async () => await _f.Rpc.CloseSessionAsync(new CloseSessionRequest { SessionId = -1 }, default));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader));
        ex = await Assert.ThrowsAnyAsync<Exception>(async () => await _f.Rpc.WriteAsync(new WriteRequest { Puts = { new PutRequest { Key = "fail-oxia" } } }, default));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader));
    }

    private static void AssertLeaderHintReachable(Exception thrown)
    {
        // what RpcProvider does with every failure: FromGrpcError(thrown) must still see the leader hint ...
        var (translated, metadata) = OxiaErrors.FromGrpcError(thrown);
        Assert.True(OxiaErrors.Is(translated, OxiaErrors.NodeIsNotLeader));
        Assert.NotNull(metadata);
        Assert.True(metadata!.TryGetLeaderHint(out long shard, out string leader));
        Assert.Equal(3L, shard);
        Assert.Equal("leader.example:6648", leader);
        // ... and the gRPC status code of the original failure
        Assert.NotEqual(StatusCode.OK, OxiaErrors.GrpcCode(thrown));
        Assert.Equal(MockOxiaState.NotLeader().StatusCode, OxiaErrors.GrpcCode(thrown));
        var translatedException = Assert.IsType<TranslatedGrpcException>(thrown);
        Assert.Same(OxiaErrors.NodeIsNotLeader, translatedException.Sentinel);
        Assert.IsType<RpcException>(translatedException.InnerException);
        Assert.Same(translatedException.InnerException, translatedException.GrpcException);
        Assert.Same(translatedException.InnerException, translatedException.Original);
        Assert.True(translatedException.GrpcException!.Trailers.Count > 0, "the original trailers (grpc-status-details-bin) must still be there");
    }

    [Fact]
    public async Task UnaryNonOxiaError_IsRethrownUnchanged()
    {
        var ex = await Assert.ThrowsAsync<RpcException>(async () => await _f.Rpc.CreateSessionAsync(new CreateSessionRequest { ClientIdentity = "fail-other" }, default));
        Assert.Equal(StatusCode.Internal, ex.StatusCode);
        Assert.Equal("boom", ex.Status.Detail);
        Assert.Equal(StatusCode.Internal, OxiaErrors.GrpcCode(ex));
    }

    [Fact]
    public async Task UnaryCancellation_CancelsTheCall()
    {
        using var cts = new CancellationTokenSource();
        Task<CreateSessionResponse> call = _f.Rpc.CreateSessionAsync(new CreateSessionRequest { ClientIdentity = "slow" }, cts.Token).AsTask();
        await Task.Delay(100);
        Assert.False(call.IsCompleted);
        cts.Cancel();
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => call.WaitAsync(Timeout5s));
        Assert.True(OxiaErrors.IsCanceled(ex), ex.ToString());
        await _f.State.Cancellation("slow-unary").Task.WaitAsync(Timeout5s);
    }

    // ---- server streaming ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task GetShardAssignments_StreamsEveryUpdate_ThenNull()
    {
        using var stream = _f.Rpc.GetShardAssignments(new ShardAssignmentsRequest { Namespace = "default" }, default);
        List<ShardAssignments> all = await RecvAll(stream);
        Assert.Equal(2, all.Count);
        NamespaceShardsAssignment ns = all[1].Namespaces["default"];
        Assert.Equal(ShardKeyRouter.Xxhash3, ns.ShardKeyRouter);
        Assert.Equal(KeySorting.Hierarchical, ns.KeySorting);
        Assert.Equal(2, ns.Assignments.Count);
        Assert.Equal(2L, ns.Assignments[0].Shard);
        Assert.Equal("leader-1:6648", ns.Assignments[0].Leader);
        Assert.Equal(0x7FFFFFFFu, ns.Assignments[0].Int32HashRange!.MaxHashInclusive);
        Assert.Equal(0x80000000u, ns.Assignments[1].Int32HashRange!.MinHashInclusive);
        Assert.Equal(new[] { "authority-1" }, all[1].AllowedAuthorities);
        // after the end the stream keeps answering null
        Assert.Null(await stream.RecvAsync());
    }

    [Fact]
    public async Task Read_StreamsResponses_InOrder()
    {
        var req = new ReadRequest { Shard = 4 };
        for (int i = 0; i < 5; i++) req.Gets.Add(new GetRequest { Key = "key-" + i, IncludeValue = i % 2 == 0, ComparisonType = i == 3 ? KeyComparisonType.Floor : KeyComparisonType.Equal, SecondaryIndexName = i == 4 ? "idx" : null });
        using var stream = _f.Rpc.Read(req, default);
        List<ReadResponse> all = await RecvAll(stream);
        Assert.Equal(new[] { 2, 2, 1 }, all.Select(r => r.Gets.Count));
        GetResponse g0 = all[0].Gets[0];
        Assert.Equal(Oxia.Common.Proto.Status.Ok, g0.Status);
        Assert.Equal("key-0"u8.ToArray(), g0.Value);
        Assert.Null(all[0].Gets[1].Value);                          // include_value = false -> field absent
        Assert.Equal(2L, g0.Version!.ModificationsCount);
        Assert.Equal(5UL, g0.Version.CreatedTimestamp);
        Assert.Equal("key-3!", all[1].Gets[1].Key);
        Assert.Equal("idx", all[2].Gets[0].SecondaryIndexKey);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100_000)]
    [InlineData(3_000_000)]
    public async Task Read_LargeValues_TravelThroughRealSegments(int size)
    {
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "big-" + size, IncludeValue = true });
        req.Gets.Add(new GetRequest { Key = "small", IncludeValue = true });
        using var stream = _f.Rpc.Read(req, default);
        List<ReadResponse> all = await RecvAll(stream);
        GetResponse big = all.Single().Gets[0];
        Assert.NotNull(big.Value);
        Assert.Equal(size, big.Value!.Length);
        for (int i = 0; i < size; i += 4099) Assert.Equal((byte)(i * 7), big.Value[i]);
        Assert.Equal("small"u8.ToArray(), all.Single().Gets[1].Value);
    }

    [Fact]
    public async Task Read_ManyKeys_FromParallelCalls()
    {
        async Task<int> One(int id)
        {
            var req = new ReadRequest { Shard = id };
            for (int i = 0; i < 200; i++) req.Gets.Add(new GetRequest { Key = "k" + id + "-" + i, IncludeValue = true });
            using var s = _f.Rpc.Read(req, default);
            var all = await RecvAll(s);
            int n = 0;
            foreach (var r in all) foreach (var g in r.Gets) { Assert.Equal("k" + id + "-" + n, System.Text.Encoding.UTF8.GetString(g.Value!)); n++; }
            return n;
        }
        int[] counts = await Task.WhenAll(Enumerable.Range(0, 16).Select(One));
        Assert.All(counts, c => Assert.Equal(200, c));
    }

    [Fact]
    public async Task List_StreamsKeys_WithMultiByteUtf8()
    {
        using var stream = _f.Rpc.List(new ListRequest { StartInclusive = "pre", EndExclusive = "z", EndInclusive = true }, default);
        List<ListResponse> all = await RecvAll(stream);
        Assert.Equal(3, all.Count);
        Assert.All(all, r => Assert.True(r.EndInclusive));
        Assert.Equal(new[] { "pre/a2", "pre/é€😀2" }, all[2].Keys);
    }

    [Fact]
    public async Task RangeScan_StreamsRecords_AndHonoursIncludeValues()
    {
        using (var stream = _f.Rpc.RangeScan(new RangeScanRequest { StartInclusive = "s", EndExclusive = "t", EndInclusive = true }, default))
        {
            List<RangeScanResponse> all = await RecvAll(stream);
            Assert.Equal(3, all.Count);
            Assert.False(all[0].EndInclusive);
            Assert.True(all[2].EndInclusive);
            Assert.Equal("s/1-2", all[1].Records[2].Key);
            Assert.Equal("v12"u8.ToArray(), all[1].Records[2].Value);
            Assert.Equal(2L, all[1].Records[2].Version!.VersionId);
        }
        using (var stream = _f.Rpc.RangeScan(new RangeScanRequest { StartInclusive = "s", IncludeValues = false }, default))
        {
            List<RangeScanResponse> all = await RecvAll(stream);
            Assert.All(all.SelectMany(r => r.Records), rec => Assert.Null(rec.Value));
        }
    }

    [Fact]
    public async Task GetSequenceUpdates_StreamsHighestKeys()
    {
        using var stream = _f.Rpc.GetSequenceUpdates(new GetSequenceUpdatesRequest { Shard = 2, Key = "/seq/" }, default);
        List<GetSequenceUpdatesResponse> all = await RecvAll(stream);
        Assert.Equal(new[] { "/seq/-0001", "/seq/-0002", "/seq/-0003" }, all.Select(r => r.HighestSequenceKey));
    }

    [Fact]
    public async Task GetNotifications_StreamsBatches()
    {
        using var stream = _f.Rpc.GetNotifications(new NotificationsRequest { Shard = 9, StartOffsetExclusive = 41 }, default);
        List<NotificationBatch> all = await RecvAll(stream);
        Assert.Equal(3, all.Count);
        Assert.Equal(new long[] { 42, 43, 44 }, all.Select(b => b.Offset));
        Assert.Equal(9L, all[0].Shard);
        Assert.Equal(1_700_000_000_002UL, all[2].Timestamp);
        NotificationEntry e0 = all[1].Notifications[0];
        Assert.Equal("/k1", e0.Key);
        Assert.Equal(Oxia.Common.Proto.NotificationType.KeyCreated, e0.Value!.Type);
        Assert.Equal(1L, e0.Value.VersionId);
        NotificationEntry e1 = all[1].Notifications[1];
        Assert.Equal("", e1.Key);
        Assert.Equal(Oxia.Common.Proto.NotificationType.KeyRangeDeleted, e1.Value!.Type);
        Assert.Equal("/z", e1.Value.KeyRangeLast);
    }

    // ---- errors ------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task StreamOxiaError_IsTranslated_OnTheFirstRecv()
    {
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "fail-oxia" });
        using var stream = _f.Rpc.Read(req, default);
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader), ex.ToString());
        Assert.False(OxiaErrors.Is(ex, OxiaErrors.ShardNotFound));
        AssertLeaderHintReachable(ex);
        Assert.True(OxiaErrors.IsRetryable(ex));
    }

    [Fact]
    public async Task StreamErrorAfterMessages_DeliversTheMessagesThenTheError()
    {
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "fail-after-first" });
        using var stream = _f.Rpc.Read(req, default);
        ReadResponse? first = await stream.RecvAsync();
        Assert.Equal("partial", first!.Gets.Single().Key);
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader));
    }

    [Fact]
    public async Task StreamNonOxiaError_IsRethrownUnchanged()
    {
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "fail-other" });
        using var stream = _f.Rpc.Read(req, default);
        var ex = await Assert.ThrowsAsync<RpcException>(async () => await stream.RecvAsync());
        Assert.Equal(StatusCode.Internal, ex.StatusCode);
    }

    [Theory]
    [InlineData("shard-assignments")]
    [InlineData("list")]
    [InlineData("range-scan")]
    public async Task EveryStreamingCall_TranslatesOxiaErrors(string which)
    {
        IServerStreamingClient<object> Wrap<T>(IServerStreamingClient<T> s) where T : class => new Boxed<T>(s);
        using IServerStreamingClient<object> stream = which switch
        {
            "shard-assignments" => Wrap(_f.Rpc.GetShardAssignments(new ShardAssignmentsRequest { Namespace = "fail-oxia" }, default)),
            "list" => Wrap(_f.Rpc.List(new ListRequest { StartInclusive = "fail-oxia" }, default)),
            _ => Wrap(_f.Rpc.RangeScan(new RangeScanRequest { StartInclusive = "fail-oxia" }, default)),
        };
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader));
    }

    private sealed class Boxed<T> : IServerStreamingClient<object> where T : class
    {
        private readonly IServerStreamingClient<T> _inner;
        public Boxed(IServerStreamingClient<T> inner) => _inner = inner;
        public async ValueTask<object?> RecvAsync(CancellationToken ct = default) => await _inner.RecvAsync(ct);
        public void Dispose() => _inner.Dispose();
    }

    // ---- cancellation ------------------------------------------------------------------------------------------------

    [Fact]
    public async Task CancellingTheToken_CancelsAPendingStreamingCall()
    {
        using var cts = new CancellationTokenSource();
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "block-token" });
        using var stream = _f.Rpc.Read(req, cts.Token);
        Task<ReadResponse?> pending = stream.RecvAsync().AsTask();
        await Task.Delay(150);
        Assert.False(pending.IsCompleted);
        cts.Cancel();
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(Timeout5s));
        Assert.True(OxiaErrors.IsCanceled(ex), ex.ToString());
        await _f.State.Cancellation("block-token").Task.WaitAsync(Timeout5s);   // the server saw the call being cancelled
    }

    [Fact]
    public async Task AlreadyCancelledToken_FailsTheCall()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var req = new ReadRequest();
        req.Gets.Add(new GetRequest { Key = "k" });
        using var stream = _f.Rpc.Read(req, cts.Token);
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.True(OxiaErrors.IsCanceled(ex), ex.ToString());
    }

    [Fact]
    public async Task Dispose_CancelsTheCallOnTheServer()
    {
        var stream = _f.Rpc.GetSequenceUpdates(new GetSequenceUpdatesRequest { Key = "block-dispose" }, default);
        Task<GetSequenceUpdatesResponse?> pending = stream.RecvAsync().AsTask();
        await Task.Delay(150);
        Assert.False(pending.IsCompleted);
        stream.Dispose();
        await _f.State.Cancellation("block-dispose").Task.WaitAsync(Timeout5s);
        await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(Timeout5s));
    }

    [Fact]
    public async Task RecvToken_CancelsAPendingRecv()
    {
        using var cts = new CancellationTokenSource();
        using var stream = _f.Rpc.GetNotifications(new NotificationsRequest { Shard = -1 }, default);
        Task<NotificationBatch?> pending = stream.RecvAsync(cts.Token).AsTask();
        await Task.Delay(150);
        cts.Cancel();
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(Timeout5s));
        Assert.True(OxiaErrors.IsCanceled(ex), ex.ToString());
    }

    // ---- write stream ------------------------------------------------------------------------------------------------

    private static Metadata Headers(string ns, long shard) => new() { { "namespace", ns }, { "shard-id", shard.ToString() } };

    [Fact]
    public async Task WriteStream_PingPong_ManyBatches()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns-a", 31), default);
        for (int round = 0; round < 200; round++)
        {
            var req = new WriteRequest { Shard = 31 };
            int puts = 1 + round % 7;
            for (int i = 0; i < puts; i++) req.Puts.Add(new PutRequest { Key = "r" + round + "-" + i, Value = new byte[round % 300], OpIndex = (uint)i, SequenceKeyDelta = i == 2 ? new ulong[] { 5 } : null });
            req.Deletes.Add(new DeleteRequest { Key = round % 2 == 0 ? "missing" : "present" });
            req.DeleteRanges.Add(new DeleteRangeRequest { StartInclusive = "a", EndExclusive = "z" });
            await call.SendAsync(req).AsTask().WaitAsync(Timeout5s);
            WriteResponse? resp = await call.RecvAsync().AsTask().WaitAsync(Timeout5s);
            Assert.NotNull(resp);
            Assert.Equal(puts, resp!.Puts.Count);
            for (int i = 0; i < puts; i++)
            {
                Assert.Equal(i, (int)resp.Puts[i].Version!.VersionId);
                Assert.Equal(round % 300, (int)resp.Puts[i].Version!.ModificationsCount);
            }
            if (puts > 2) Assert.Equal("r" + round + "-2-5", resp.Puts[2].Key);
            Assert.Null(resp.Puts[0].Key);
            Assert.Equal(round % 2 == 0 ? Oxia.Common.Proto.Status.KeyNotFound : Oxia.Common.Proto.Status.Ok, resp.Deletes.Single().Status);
            Assert.Single(resp.DeleteRanges);
        }
        Assert.Equal(("ns-a", "31"), _f.State.WriteStreamHeaders["31"]);
    }

    [Fact]
    public async Task WriteStream_CloseSend_EndsTheStreamWithNull_AndCancelsContext()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns-b", 32), default);
        CancellationToken context = call.Context;
        Assert.False(context.IsCancellationRequested);
        var req = new WriteRequest();
        req.Puts.Add(new PutRequest { Key = "x" });
        await call.SendAsync(req);
        Assert.NotNull(await call.RecvAsync().AsTask().WaitAsync(Timeout5s));
        await call.CloseSendAsync();
        Assert.Null(await call.RecvAsync().AsTask().WaitAsync(Timeout5s));
        Assert.True(context.IsCancellationRequested, "Context must be cancelled once the stream ended");
        Assert.True(call.Context.IsCancellationRequested);
        Assert.Equal(("ns-b", "32"), _f.State.WriteStreamHeaders["32"]);
    }

    [Fact]
    public async Task WriteStream_ServerOxiaError_IsTranslatedOnRecv()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 33), default);
        var req = new WriteRequest();
        req.Puts.Add(new PutRequest { Key = "fail-oxia" });
        await call.SendAsync(req);
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await call.RecvAsync().AsTask().WaitAsync(Timeout5s));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NodeIsNotLeader), ex.ToString());
        AssertLeaderHintReachable(ex);
        Assert.True(call.Context.IsCancellationRequested);
    }

    [Fact]
    public async Task WriteStream_ServerNonOxiaError_IsRethrownUnchanged()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 34), default);
        var req = new WriteRequest();
        req.Puts.Add(new PutRequest { Key = "fail-other" });
        await call.SendAsync(req);
        var ex = await Assert.ThrowsAsync<RpcException>(async () => await call.RecvAsync().AsTask().WaitAsync(Timeout5s));
        Assert.Equal(StatusCode.Internal, ex.StatusCode);
    }

    [Fact]
    public async Task WriteStream_SendAfterTheServerFailed_ThrowsATranslatedOrGrpcError()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 35), default);
        var bad = new WriteRequest();
        bad.Puts.Add(new PutRequest { Key = "fail-oxia" });
        await call.SendAsync(bad);
        await Assert.ThrowsAnyAsync<Exception>(async () => await call.RecvAsync().AsTask().WaitAsync(Timeout5s));
        // the call is over: further sends fail (Go: Send returns io.EOF / the status error)
        var next = new WriteRequest();
        next.Puts.Add(new PutRequest { Key = "after" });
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            for (int i = 0; i < 50; i++)
            {
                await call.SendAsync(next);
                await Task.Delay(20);
            }
        });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task WriteStream_TokenCancellation_CancelsTheCallAndTheContext()
    {
        using var cts = new CancellationTokenSource();
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 36), cts.Token);
        CancellationToken context = call.Context;
        var req = new WriteRequest();
        req.Puts.Add(new PutRequest { Key = "block" });
        await call.SendAsync(req);
        Task<WriteResponse?> pending = call.RecvAsync().AsTask();
        await Task.Delay(150);
        Assert.False(context.IsCancellationRequested);
        cts.Cancel();
        Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(Timeout5s));
        Assert.True(OxiaErrors.IsCanceled(ex), ex.ToString());
        Assert.True(context.IsCancellationRequested);
        await _f.State.Cancellation("ws-36").Task.WaitAsync(Timeout5s);
    }

    [Fact]
    public async Task WriteStream_Dispose_CancelsTheContextAndTheServerCall()
    {
        IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 37), default);
        CancellationToken context = call.Context;
        var req = new WriteRequest();
        req.Puts.Add(new PutRequest { Key = "block" });
        await call.SendAsync(req);
        Task<WriteResponse?> pending = call.RecvAsync().AsTask();
        await Task.Delay(150);
        call.Dispose();
        Assert.True(context.IsCancellationRequested);
        Assert.True(call.Context.IsCancellationRequested);   // still usable after Dispose
        await _f.State.Cancellation("ws-37").Task.WaitAsync(Timeout5s);
        await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(Timeout5s));
    }

    [Fact]
    public async Task WriteStream_LargeBatch_RoundTrips()
    {
        using IWriteStreamCall call = _f.Rpc.WriteStream(Headers("ns", 38), default);
        var req = new WriteRequest { Shard = 38 };
        for (int i = 0; i < 1000; i++) req.Puts.Add(new PutRequest { Key = "/bulk/" + i, Value = new byte[1024], OpIndex = (uint)i });
        req.Puts.Add(new PutRequest { Key = "/huge", Value = new byte[2_000_000], OpIndex = 1000 });
        await call.SendAsync(req).AsTask().WaitAsync(Timeout5s);
        WriteResponse resp = (await call.RecvAsync().AsTask().WaitAsync(Timeout5s))!;
        Assert.Equal(1001, resp.Puts.Count);
        Assert.Equal(2_000_000L, resp.Puts[1000].Version!.ModificationsCount);
        Assert.Equal(1000L, resp.Puts[1000].Version!.VersionId);
        await call.CloseSendAsync();
        Assert.Null(await call.RecvAsync());
    }
}
