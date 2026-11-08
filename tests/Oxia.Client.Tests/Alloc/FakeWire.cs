// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A CallInvoker that plays the part of the network for the END-TO-END ALLOCATION TESTS of OUR side of the client: it runs
//              the real hand-written serializers on every request (into a reused buffer), answers from canned bytes through the real
//              deserializers (so the responses are the pooled messages the transport really produces), and itself allocates (almost)
//              nothing per operation.  The real Grpc.Net.Client + Kestrel cost ~2-3 KB per request and is not ours: it is measured by
//              the benchmark `alloc` profile (benchmarks/.../Alloc) and documented in docs/performance-allocation.md.
//              What the fake does allocate: one TaskCompletionSource per write-stream message that has to wait (the real transport
//              allocates its own Task there too).
//              FakeWireClient assembles a real AsyncClientImpl (batchers, rpc provider, executor) over it.
// =====================================================================================================================
using System.Buffers;
using System.Collections.Concurrent;
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Tests.F2;
using Oxia.Client.Tests.Proto;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using Oxia.Common.Rpc;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;
using Status = Oxia.Common.Proto.Status;

namespace Oxia.Client.Tests.Alloc;

internal sealed class FakeWireInvoker : CallInvoker
{
    private static readonly Task<bool> True = Task.FromResult(true);
    private static readonly Task<bool> False = Task.FromResult(false);
    private static readonly Metadata EmptyMetadata = new();
    private static readonly Task<Metadata> EmptyHeaders = Task.FromResult(EmptyMetadata);

    [ThreadStatic] private static FakeSerializationContext? t_serialization;
    [ThreadStatic] private static ReusableDeserializationContext? t_deserialization;

    private readonly ConcurrentDictionary<(int, int, int), byte[]> _writeResponses = new();
    private readonly ConcurrentDictionary<int, byte[][]> _readResponses = new();
    private byte[][]? _scanResponses;
    private byte[][]? _notificationBatches;

    private long _pendingReads;

    private static readonly long PendingReadBytes = MeasurePendingReadBytes();

    private static long MeasurePendingReadBytes()
    {
        long size = 0;
        for (int i = 0; i < 3; i++) // the first round also pays for the JIT / the type loading
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var task = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously).Task;
            size = GC.GetAllocatedBytesForCurrentThread() - before;
            GC.KeepAlive(task);
        }
        return size;
    }

    /// <summary>
    /// Bytes the FAKE itself allocated (the Task + TaskCompletionSource of every write-stream read that had to wait, as the real transport
    /// also allocates its own there): the tests subtract it, so the budgets are those of the code under test.
    /// </summary>
    public long FakeAllocatedBytes => Interlocked.Read(ref _pendingReads) * PendingReadBytes;

    /// <summary>Reads answer KeyNotFound (set before the first read: the canned responses are built once per size).</summary>
    public bool GetsNotFound { get; set; }

    /// <summary>Records of a RangeScan, and records per message.</summary>
    public int ScanRecords { get; set; } = 1000;
    public int ScanChunk { get; set; } = 250;

    /// <summary>Notifications per batch of the (endless) notification stream.</summary>
    public int NotificationsPerBatch { get; set; } = 20;

    public static T Deserialize<T>(Marshaller<T> marshaller, byte[] bytes)
    {
        var ctx = t_deserialization ??= new ReusableDeserializationContext();
        ctx.Set(new ReadOnlySequence<byte>(bytes));
        return marshaller.ContextualDeserializer(ctx);
    }

    private static void SerializeRequest<T>(Marshaller<T> marshaller, T request)
    {
        var ctx = t_serialization ??= new FakeSerializationContext(new ArrayBufferWriter<byte>(64 * 1024));
        ctx.Reset();
        marshaller.ContextualSerializer(request, ctx);
    }

    // ----- canned responses ---------------------------------------------------------------------------------------------

    private byte[] WriteResponseBytes(int puts, int deletes, int ranges) => _writeResponses.GetOrAdd((puts, deletes, ranges), static k =>
    {
        var response = new WriteResponse();
        for (int i = 0; i < k.Item1; i++) response.Puts.Add(new PutResponse { Status = Status.Ok, Version = new ProtoVersion { VersionId = 42, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL } });
        for (int i = 0; i < k.Item2; i++) response.Deletes.Add(new DeleteResponse { Status = Status.Ok });
        for (int i = 0; i < k.Item3; i++) response.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
        return MessageCodec.ToArray<WriteResponseCodec, WriteResponse>(response);
    });

    private byte[][] ReadResponseBytes(int gets) => _readResponses.TryGetValue(gets, out var cached) ? cached : BuildReadResponse(gets);

    private byte[][] BuildReadResponse(int gets)
    {
        var response = new ReadResponse();
        for (int i = 0; i < gets; i++)
        {
            response.Gets.Add(GetsNotFound
                ? new GetResponse { Status = Status.KeyNotFound }
                : new GetResponse { Status = Status.Ok, Value = new byte[100], Version = new ProtoVersion { VersionId = 42, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL } });
        }
        return _readResponses[gets] = [MessageCodec.ToArray<ReadResponseCodec, ReadResponse>(response)];
    }

    private byte[][] ScanResponseBytes()
    {
        if (_scanResponses is { } cached) return cached;
        var chunks = new List<byte[]>();
        for (int start = 0; start < ScanRecords; start += ScanChunk)
        {
            var response = new RangeScanResponse();
            for (int i = start; i < Math.Min(ScanRecords, start + ScanChunk); i++)
            {
                response.Records.Add(new GetResponse
                {
                    Status = Status.Ok,
                    Key = "/scan/key-" + i.ToString("D8"),
                    Value = new byte[100],
                    Version = new ProtoVersion { VersionId = i + 1, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL },
                });
            }
            chunks.Add(MessageCodec.ToArray<RangeScanResponseCodec, RangeScanResponse>(response));
        }
        return _scanResponses = chunks.ToArray();
    }

    private byte[][] NotificationBatchBytes()
    {
        if (_notificationBatches is { } cached) return cached;
        // batch 0 is the empty one that confirms the subscription; the stream then repeats batches 1..n
        var batches = new List<byte[]> { MessageCodec.ToArray<NotificationBatchCodec, NotificationBatch>(new NotificationBatch { Shard = 0, Offset = 0 }) };
        for (int b = 0; b < 8; b++)
        {
            var batch = new NotificationBatch { Shard = 0, Offset = b + 1, Timestamp = 1_700_000_000_000UL };
            for (int i = 0; i < NotificationsPerBatch; i++)
            {
                batch.Notifications.Add(new NotificationEntry
                {
                    Key = "/notifications/key-" + (b * NotificationsPerBatch + i).ToString("D6"),
                    Value = new Oxia.Common.Proto.Notification { Type = Oxia.Common.Proto.NotificationType.KeyModified, VersionId = 7 },
                });
            }
            batches.Add(MessageCodec.ToArray<NotificationBatchCodec, NotificationBatch>(batch));
        }
        return _notificationBatches = batches.ToArray();
    }

    // ----- the CallInvoker ----------------------------------------------------------------------------------------------

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        SerializeRequest(method.RequestMarshaller, request);
        return (AsyncUnaryCall<TResponse>)UnaryCalls.GetOrAdd(method.Name, static name => name switch
        {
            "KeepAlive" => NewUnary(Deserialize(OxiaClientMethods.KeepAliveResponseMarshaller, [])),
            "CloseSession" => NewUnary(Deserialize(OxiaClientMethods.CloseSessionResponseMarshaller, [])),
            _ => throw new NotSupportedException(name),
        });
    }

    private static readonly ConcurrentDictionary<string, object> UnaryCalls = new();

    private static AsyncUnaryCall<T> NewUnary<T>(T response) where T : class =>
        new(Task.FromResult(response), EmptyHeaders, static () => Grpc.Core.Status.DefaultSuccess, static () => EmptyMetadata, static () => { });

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        SerializeRequest(method.RequestMarshaller, request);
        switch (method.Name)
        {
            case "Read":
            {
                var read = (ReadRequest)(object)request;
                var stream = CannedStream<TResponse>.Rent(method.ResponseMarshaller, ReadResponseBytes(read.Gets.Count), loopFrom: -1);
                return stream.Call;
            }
            case "RangeScan":
                return CannedStream<TResponse>.Rent(method.ResponseMarshaller, ScanResponseBytes(), loopFrom: -1).Call;
            case "GetNotifications":
                return CannedStream<TResponse>.Rent(method.ResponseMarshaller, NotificationBatchBytes(), loopFrom: 1).Call;
            default:
                throw new NotSupportedException(method.Name);
        }
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options)
    {
        if (method.Name != "WriteStream") throw new NotSupportedException(method.Name);
        var stream = new FakeWriteStream(this);
        return (AsyncDuplexStreamingCall<TRequest, TResponse>)(object)new AsyncDuplexStreamingCall<WriteRequest, WriteResponse>(
            stream, stream, EmptyHeaders, static () => Grpc.Core.Status.DefaultSuccess, static () => EmptyMetadata, static () => { });
    }

    // ----- streams ------------------------------------------------------------------------------------------------------

    /// <summary>A response stream that plays canned messages through the real deserializer; pooled together with its call object.</summary>
    private sealed class CannedStream<TResponse> : IAsyncStreamReader<TResponse> where TResponse : class
    {
        private static readonly ConcurrentStack<CannedStream<TResponse>> Pool = new();

        private Marshaller<TResponse> _marshaller = null!;
        private byte[][] _messages = null!;
        private int _index;
        private int _loopFrom;

        public AsyncServerStreamingCall<TResponse> Call { get; }

        public TResponse Current { get; private set; } = null!;

        private CannedStream()
        {
            Call = new AsyncServerStreamingCall<TResponse>(this, EmptyHeaders, static () => Grpc.Core.Status.DefaultSuccess, static () => EmptyMetadata, Return);
        }

        public static CannedStream<TResponse> Rent(Marshaller<TResponse> marshaller, byte[][] messages, int loopFrom)
        {
            if (!Pool.TryPop(out var s)) s = new CannedStream<TResponse>();
            s._marshaller = marshaller;
            s._messages = messages;
            s._index = 0;
            s._loopFrom = loopFrom;
            return s;
        }

        // The Call's dispose action must reach THIS stream: it is a static-looking delegate per stream, bound once in the constructor
        private void Return() => Pool.Push(this);

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (_index >= _messages.Length)
            {
                if (_loopFrom < 0) return False;
                _index = _loopFrom;
            }
            Current = Deserialize(_marshaller, _messages[_index++]);
            return True;
        }
    }

    /// <summary>The write stream: every request is serialized by the real serializer and answered with the matching canned response.</summary>
    private sealed class FakeWriteStream(FakeWireInvoker owner) : IClientStreamWriter<WriteRequest>, IAsyncStreamReader<WriteResponse>
    {
        private readonly object _gate = new();
        private readonly Queue<WriteResponse> _ready = new();
        private TaskCompletionSource<bool>? _waiter;
        private bool _completed;

        public WriteResponse Current { get; private set; } = null!;
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(WriteRequest message, CancellationToken cancellationToken) => WriteAsync(message);

        public Task WriteAsync(WriteRequest message)
        {
            SerializeRequest(OxiaClientMethods.WriteRequestMarshaller, message);
            var response = Deserialize(OxiaClientMethods.WriteResponseMarshaller,
                owner.WriteResponseBytes(message.Puts.Count, message.Deletes.Count, message.DeleteRanges.Count));

            TaskCompletionSource<bool>? waiter;
            lock (_gate)
            {
                waiter = _waiter;
                _waiter = null;
                if (waiter is null) _ready.Enqueue(response);
                else Current = response;
            }
            waiter?.SetResult(true);
            return Task.CompletedTask;
        }

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (_ready.Count > 0)
                {
                    Current = _ready.Dequeue();
                    return True;
                }
                if (_completed) return False;
                Interlocked.Increment(ref owner._pendingReads);
                return (_waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            }
        }

        public Task CompleteAsync()
        {
            TaskCompletionSource<bool>? waiter;
            lock (_gate)
            {
                _completed = true;
                waiter = _waiter;
                _waiter = null;
            }
            waiter?.SetResult(false);
            return Task.CompletedTask;
        }
    }
}

internal sealed class ReusableDeserializationContext : DeserializationContext
{
    private ReadOnlySequence<byte> _payload;

    public void Set(ReadOnlySequence<byte> payload) => _payload = payload;

    public override int PayloadLength => checked((int)_payload.Length);
    public override byte[] PayloadAsNewBuffer() => _payload.ToArray();
    public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _payload;
}

internal sealed class FakeWirePool(IOxiaClientRpc rpc) : IClientPool
{
    public IOxiaClientRpc GetClientRpc(string target) => rpc;
    public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target) => throw new NotSupportedException();
    public ValueTask CloseAsync() => ValueTask.CompletedTask;
}

/// <summary>A real AsyncClientImpl (batchers, rpc provider, executor) over a <see cref="FakeWireInvoker"/>.</summary>
internal sealed class FakeWireClient
{
    public AsyncClientImpl Client { get; }
    public FakeWireInvoker Invoker { get; }
    public RpcProvider Provider { get; }
    public StaticShardManager ShardManager { get; }

    /// <param name="window">operations per batch: the batch is completed when it holds that many calls (the linger is long)</param>
    /// <param name="intercepted">route the calls through the metrics interceptor, like a real connection</param>
    public FakeWireClient(int window, bool intercepted = false)
    {
        Invoker = new FakeWireInvoker();
        CallInvoker invoker = intercepted ? Grpc.Core.Interceptors.CallInvokerExtensions.Intercept(Invoker, GrpcClientMetricsInterceptor.Default) : Invoker;
        ShardManager = new StaticShardManager { Shards = [0], LeaderAddress = "fake:1" };
        Provider = new RpcProvider(CancellationToken.None, Constants.DefaultNamespace, new FakeWirePool(new OxiaClientRpc(invoker)), "fake:1", () => ShardManager);

        var options = new ClientOptions { BatchLinger = TimeSpan.FromSeconds(5), MaxRequestsPerBatch = window, RequestTimeout = TimeSpan.FromSeconds(30) };
        var metrics = new OxiaMetrics(null);
        var batchers = new BatcherFactory(Provider, options.Namespace, options.BatchLinger, window, metrics, options.RequestTimeout);
        Client = new AsyncClientImpl(options) { ShardManager = ShardManager, RpcProvider = Provider, Executor = Provider, Metrics = metrics };
        batchers.WriteRerouter = Client.RerouteWrites;
        batchers.ReadRerouter = Client.RerouteReads;
        Client.WriteBatchManager = new WriteManager(Client.Ctx, (ct, shard) => batchers.NewWriteBatcher(ct, shard, options.MaxBatchSize), Client.ForwardWriteAsync);
        Client.ReadBatchManager = new Manager(Client.Ctx, batchers.NewReadBatcher);
    }

    public ValueTask CloseAsync()
    {
        Client.WriteBatchManager.Dispose();
        Client.ReadBatchManager.Dispose();
        return Provider.CloseAsync();
    }
}
