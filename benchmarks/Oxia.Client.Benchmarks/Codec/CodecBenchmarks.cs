// =====================================================================================================================
// PORT NOTE (benchmarks)
//   Upstream : (none -- compares the hand-written codecs with the Google.Protobuf generated classes, which stand in for the
//              reflection-free vtproto codecs of the Go client)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Micro-benchmarks of the hot wire paths: encoding a WriteRequest with 1 / 100 / 1000 puts, parsing a WriteResponse
//              with 1 / 100 / 1000 put responses (contiguous and as a 4 KiB-segmented ReadOnlySequence, which is what gRPC.NET
//              delivers), and parsing a GetResponse / ReadResponse.  "Hand" = Oxia.Common.Proto.Codec, "Google" = protoc
//              generated Io.Oxia.Proto.V1 classes (the baseline).  Hand parsing reuses one message instance, Google parsing
//              allocates a fresh message each time (that is its API; reuse via MergeFrom after Clear is also measured).
//   Divergences: n/a (benchmark code).
//   Run      : dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Oxia.Client.Benchmarks.Codec*" [--job short]
//              (the project's Program.cs must dispatch to BenchmarkSwitcher.FromAssembly(...).Run(args))
// =====================================================================================================================
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Benchmarks.Codec;

[MemoryDiagnoser]
public class WriteRequestEncodeBenchmarks
{
    [Params(1, 100, 1000)]
    public int Puts;

    private WriteRequest _hand = null!;
    private G.WriteRequest _google = null!;
    private ArrayBufferWriter<byte> _output = null!;

    [GlobalSetup]
    public void Setup()
    {
        var value = new byte[100];
        new Random(1).NextBytes(value);
        _hand = new WriteRequest { Shard = 7 };
        _google = new G.WriteRequest { Shard = 7 };
        for (int i = 0; i < Puts; i++)
        {
            string key = "/benchmark/user-" + i.ToString("D10");
            _hand.Puts.Add(new PutRequest { Key = key, Value = value, ExpectedVersionId = i % 4 == 0 ? i : null, OpIndex = (uint)i });
            var g = new G.PutRequest { Key = key, Value = ByteString.CopyFrom(value), OpIndex = (uint)i };
            if (i % 4 == 0) g.ExpectedVersionId = i;
            _google.Puts.Add(g);
        }
        _output = new ArrayBufferWriter<byte>(4 * 1024 * 1024);
    }

    [Benchmark(Baseline = true, Description = "Google WriteTo(IBufferWriter)")]
    public int Google()
    {
        _output.Clear();
        _google.WriteTo(_output);
        return _output.WrittenCount;
    }

    [Benchmark(Description = "Hand Write(IBufferWriter)")]
    public int Hand()
    {
        _output.Clear();
        WriteRequestCodec.Write(_hand, _output);
        return _output.WrittenCount;
    }

    [Benchmark(Description = "Hand Size() only")]
    public int HandSizeOnly() => WriteRequestCodec.Size(_hand);

    [Benchmark(Description = "Google CalculateSize() only")]
    public int GoogleSizeOnly() => _google.CalculateSize();
}

[MemoryDiagnoser]
public class WriteResponseParseBenchmarks
{
    [Params(1, 100, 1000)]
    public int Puts;

    private byte[] _bytes = null!;
    private ReadOnlySequence<byte> _segmented;
    private WriteResponse _reused = null!;
    private G.WriteResponse _googleReused = null!;

    [GlobalSetup]
    public void Setup()
    {
        var g = new G.WriteResponse();
        for (int i = 0; i < Puts; i++)
        {
            var version = new G.Version { VersionId = 1_000_000 + i, ModificationsCount = i % 3, CreatedTimestamp = 1_700_000_000_000UL + (ulong)i, ModifiedTimestamp = 1_700_000_000_100UL + (ulong)i };
            if (i % 2 == 0) version.SessionId = 99;
            g.Puts.Add(new G.PutResponse { Status = G.Status.Ok, Version = version });
        }
        _bytes = g.ToByteArray();
        _segmented = BuildSegmented(_bytes, 4096);
        _reused = new WriteResponse();
        _googleReused = new G.WriteResponse();
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> m) => Memory = m;
        public Segment Append(ReadOnlyMemory<byte> m)
        {
            var next = new Segment(m) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }

    internal static ReadOnlySequence<byte> BuildSegmented(byte[] data, int size)
    {
        Segment? first = null, last = null;
        for (int i = 0; i < data.Length; i += size)
        {
            var mem = new ReadOnlyMemory<byte>(data, i, Math.Min(size, data.Length - i));
            if (first is null) first = last = new Segment(mem);
            else last = last!.Append(mem);
        }
        return first is null ? ReadOnlySequence<byte>.Empty : new ReadOnlySequence<byte>(first, 0, last!, last!.Memory.Length);
    }

    [Benchmark(Baseline = true, Description = "Google Parser.ParseFrom(span) -> new message")]
    public G.WriteResponse Google() => G.WriteResponse.Parser.ParseFrom(_bytes);

    [Benchmark(Description = "Google Parser.ParseFrom(sequence 4KiB segments) -> new message")]
    public G.WriteResponse GoogleSegmented() => G.WriteResponse.Parser.ParseFrom(_segmented);

    [Benchmark(Description = "Google clear lists + MergeFrom (reused message)")]
    public G.WriteResponse GoogleReused()
    {
        _googleReused.Puts.Clear();
        _googleReused.Deletes.Clear();
        _googleReused.DeleteRanges.Clear();
        _googleReused.MergeFrom(_bytes);
        return _googleReused;
    }

    [Benchmark(Description = "Hand Parse(span) -> reused message")]
    public WriteResponse Hand()
    {
        WriteResponseCodec.Parse(_bytes, _reused);
        return _reused;
    }

    [Benchmark(Description = "Hand Parse(sequence 4KiB segments) -> reused message")]
    public WriteResponse HandSegmented()
    {
        WriteResponseCodec.Parse(in _segmented, _reused);
        return _reused;
    }

    [Benchmark(Description = "Hand Parse(span) -> new message")]
    public WriteResponse HandNewMessage()
    {
        var m = new WriteResponse();
        WriteResponseCodec.Parse(_bytes, m);
        return m;
    }
}

[MemoryDiagnoser]
public class GetResponseParseBenchmarks
{
    [Params(1, 100)]
    public int Gets;

    private byte[] _single = null!;
    private byte[] _read = null!;
    private GetResponse _reusedGet = null!;
    private ReadResponse _reusedRead = null!;

    [GlobalSetup]
    public void Setup()
    {
        var value = new byte[100];
        new Random(2).NextBytes(value);
        var one = new G.GetResponse
        {
            Status = G.Status.Ok,
            Version = new G.Version { VersionId = 42, ModificationsCount = 3, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_500UL },
            Value = ByteString.CopyFrom(value),
        };
        _single = one.ToByteArray();
        var read = new G.ReadResponse();
        for (int i = 0; i < Gets; i++) read.Gets.Add(one.Clone());
        _read = read.ToByteArray();
        _reusedGet = new GetResponse();
        _reusedRead = new ReadResponse();
    }

    [Benchmark(Baseline = true, Description = "Google GetResponse.ParseFrom")]
    public G.GetResponse GoogleGet() => G.GetResponse.Parser.ParseFrom(_single);

    [Benchmark(Description = "Hand GetResponse Parse (reused)")]
    public GetResponse HandGet()
    {
        GetResponseCodec.Parse(_single, _reusedGet);
        return _reusedGet;
    }

    [Benchmark(Description = "Google ReadResponse.ParseFrom")]
    public G.ReadResponse GoogleRead() => G.ReadResponse.Parser.ParseFrom(_read);

    [Benchmark(Description = "Hand ReadResponse Parse (reused)")]
    public ReadResponse HandRead()
    {
        ReadResponseCodec.Parse(_read, _reusedRead);
        return _reusedRead;
    }
}
