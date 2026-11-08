// =====================================================================================================================
// PORT NOTE (benchmark)
//   Upstream : oxia/internal/rpc_provider.go (ExecuteWrite / ExecuteRead), oxia/internal/write_stream.go (streamWrapper.Send)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The cost of one write / read request through the rpc provider on the success path, with the network replaced by an
//              in-memory stream that answers synchronously: what is left is the provider's own overhead (leader lookup, stream
//              cache lookup, the pending-request queue, the future, the async state machines).  Allocation budget per request:
//              see the table printed by BenchmarkDotNet (kept in benchmarks/README.md by the lead).
// =====================================================================================================================
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;

namespace Oxia.Client.Benchmarks.Internal;

[MemoryDiagnoser]
public class RpcProviderBenchmarks
{
    private RpcProvider _provider = null!;
    private Func<WriteRequest> _prepare = null!;
    private ReadRequest _read = null!;
    private WriteRequest _write = null!;
    private CancellationTokenSource _cts = null!;

    [GlobalSetup]
    public void Setup()
    {
        _cts = new CancellationTokenSource();
        var sm = new ShardManager(ShardStrategy.New(), null, "service:6648", "default", TimeSpan.FromSeconds(1), null);
        sm.Update([new Shard(0, "leader:6648", new HashRange(0, uint.MaxValue))]);
        _provider = new RpcProvider(_cts.Token, "default", new Pool(), "service:6648", () => sm);

        _write = new WriteRequest { Shard = 0 };
        _write.Puts.Add(new PutRequest { Key = "key", Value = new byte[16] });
        _prepare = () => _write;
        _read = new ReadRequest { Shard = 0 };
        _read.Gets.Add(new GetRequest { Key = "key" });
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _provider.CloseAsync();
        _cts.Cancel();
    }

    [Benchmark]
    public ValueTask<WriteResponse> ExecuteWrite() => _provider.ExecuteWrite(CancellationToken.None, 0, _prepare);

    [Benchmark]
    public ValueTask<ReadResponse> ExecuteRead() => _provider.ExecuteRead(CancellationToken.None, _read);

    private sealed class Pool : IClientPool
    {
        private readonly Rpc _rpc = new();
        public IOxiaClientRpc GetClientRpc(string target) => _rpc;
        public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target) => throw new NotSupportedException();
        public ValueTask CloseAsync() => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Rpc : IOxiaClientRpc
    {
        public IWriteStreamCall WriteStream(Metadata? headers, CancellationToken ct) => new EchoWriteStream(ct);
        public IServerStreamingClient<ReadResponse> Read(ReadRequest request, CancellationToken ct) => new ReadStream();
        public IServerStreamingClient<ListResponse> List(ListRequest request, CancellationToken ct) => throw new NotSupportedException();
        public IServerStreamingClient<RangeScanResponse> RangeScan(RangeScanRequest request, CancellationToken ct) => throw new NotSupportedException();
        public IServerStreamingClient<ShardAssignments> GetShardAssignments(ShardAssignmentsRequest request, CancellationToken ct) => throw new NotSupportedException();
        public IServerStreamingClient<GetSequenceUpdatesResponse> GetSequenceUpdates(GetSequenceUpdatesRequest request, CancellationToken ct) => throw new NotSupportedException();
        public IServerStreamingClient<NotificationBatch> GetNotifications(NotificationsRequest request, CancellationToken ct) => throw new NotSupportedException();
        public ValueTask<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken ct) => throw new NotSupportedException();
        public ValueTask<KeepAliveResponse> KeepAliveAsync(SessionHeartbeat request, CancellationToken ct) => throw new NotSupportedException();
        public ValueTask<CloseSessionResponse> CloseSessionAsync(CloseSessionRequest request, CancellationToken ct) => throw new NotSupportedException();
    }

    /// <summary>Answers each request with a pre-built response, through a channel-less hand-off (the receive loop is real).</summary>
    private sealed class EchoWriteStream(CancellationToken context) : IWriteStreamCall
    {
        private readonly System.Threading.Channels.Channel<WriteResponse> _responses =
            System.Threading.Channels.Channel.CreateUnbounded<WriteResponse>(new System.Threading.Channels.UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = true,
            });

        private readonly WriteResponse _response = new();

        public CancellationToken Context => context;

        public ValueTask SendAsync(WriteRequest request, CancellationToken ct = default)
        {
            _responses.Writer.TryWrite(_response);
            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResponse?> RecvAsync(CancellationToken ct = default) => RecvCore(ct);

        private async ValueTask<WriteResponse?> RecvCore(CancellationToken ct) => await _responses.Reader.ReadAsync(ct);

        public ValueTask CloseSendAsync() => ValueTask.CompletedTask;
        public void Dispose() { }
    }

    private sealed class ReadStream : IServerStreamingClient<ReadResponse>
    {
        private static readonly ReadResponse Response = new();
        private bool _sent;

        public ValueTask<ReadResponse?> RecvAsync(CancellationToken ct = default)
        {
            if (_sent) return ValueTask.FromResult<ReadResponse?>(null);
            _sent = true;
            return ValueTask.FromResult<ReadResponse?>(Response);
        }

        public void Dispose() { }
    }
}
