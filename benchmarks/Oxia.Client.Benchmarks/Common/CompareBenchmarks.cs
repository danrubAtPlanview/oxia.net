// =====================================================================================================================
// PORT NOTE
//   Upstream : common/compare/compare_with_slash_benchmark_test.go, and BenchmarkEncoderNaturalDecode of common/compare/encode_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The key comparison / encoding micro benchmarks of the key-ordering code (hot path of the multi-shard merge and of the
//              shard routing).  Names follow the Go benchmarks (Benchmark_CompareWithSlash -> CompareWithSlash, ...), `[Benchmark]` methods
//              are grouped in one class so that the report puts them side by side.
//   Divergences:
//      * Go's `[]byte(benchKeyA)` per iteration (Benchmark_BytesCompareCopy) is Encoding.UTF8.GetBytes; Benchmark_BytesCompareUnsafe
//        (string memory viewed as bytes, free in Go) has no .NET equivalent -- a .NET string is UTF-16 -- and is replaced by a stackalloc
//        transcoding (BytesCompareStackalloc); strings.Compare is string.CompareOrdinal.
//      * Added: EncodeTo* (the allocation-free span API), Decode of the pooled / stack paths.
//      * Run: dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*CompareBenchmarks*" (the entry point is
//        the lead's Program.cs).
// =====================================================================================================================
using System.Text;
using BenchmarkDotNet.Attributes;
using Oxia.Common.Compare;

namespace Oxia.Client.Benchmarks.Common;

[MemoryDiagnoser]
public class CompareBenchmarks
{
    private const string BenchKeyA = "/test/aaaaaaaaaaa/bbbbbbbbbbb/cccccccccccc/dddddddddddddd";
    private const string BenchKeyB = "/test/aaaaaaaaaaa/bbbbbbbbbbb/ccccccccccccddddddddddddddd";

    private const string BenchKeyAns = "test-aaaaaaaaaaa-bbbbbbbbbbb-cccccccccccc-dddddddddddddd";
    private const string BenchKeyBns = "test-aaaaaaaaaaa-bbbbbbbbbbb-ccccccccccccddddddddddddddd";

    private readonly byte[] _ka = Encoding.UTF8.GetBytes(BenchKeyA);
    private readonly byte[] _kb = Encoding.UTF8.GetBytes(BenchKeyB);
    private readonly byte[] _kans = Encoding.UTF8.GetBytes(BenchKeyAns);
    private readonly byte[] _kbns = Encoding.UTF8.GetBytes(BenchKeyBns);

    private readonly byte[] _hierarchicalA = Encoders.Hierarchical.Encode(BenchKeyA);
    private readonly byte[] _hierarchicalAns = Encoders.Hierarchical.Encode(BenchKeyAns);
    private readonly byte[] _naturalInternal = [0xff, 0xff, .. "oxia/session/0000000000123456"u8];
    private readonly byte[] _naturalPlain = "/some/regular/application/key-123456"u8.ToArray();

    // ----- the byte comparison baselines ---------------------------------------------------------------------------------
    [Benchmark]
    public int BytesCompareCopy() => Encoding.UTF8.GetBytes(BenchKeyA).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(BenchKeyB));

    [Benchmark]
    public int BytesCompareStackalloc()
    {
        Span<byte> a = stackalloc byte[128];
        Span<byte> b = stackalloc byte[128];
        var la = Encoding.UTF8.GetBytes(BenchKeyA, a);
        var lb = Encoding.UTF8.GetBytes(BenchKeyB, b);
        return a[..la].SequenceCompareTo(b[..lb]);
    }

    [Benchmark]
    public int BytesCompareNoCopy() => _ka.AsSpan().SequenceCompareTo(_kb);

    [Benchmark]
    public int StringsCompare() => string.CompareOrdinal(BenchKeyA, BenchKeyB);

    // ----- CompareWithSlash ----------------------------------------------------------------------------------------------
    [Benchmark]
    public int CompareWithSlash() => KeyCompare.CompareWithSlash(_ka, _kb);

    [Benchmark]
    public int CompareWithSlashButNoSlashes() => KeyCompare.CompareWithSlash(_kans, _kbns);

    // ----- encoders ------------------------------------------------------------------------------------------------------
    [Benchmark]
    public byte[] EncodeHierarchical() => Encoders.Hierarchical.Encode(BenchKeyA);

    [Benchmark]
    public byte[] EncodeHierarchicalButNoSlashes() => Encoders.Hierarchical.Encode(BenchKeyAns);

    [Benchmark]
    public int EncodeToHierarchical()
    {
        Span<byte> buffer = stackalloc byte[128];
        return Encoders.Hierarchical.EncodeTo(BenchKeyA, buffer);
    }

    [Benchmark]
    public int EncodeToHierarchicalButNoSlashes()
    {
        Span<byte> buffer = stackalloc byte[128];
        return Encoders.Hierarchical.EncodeTo(BenchKeyAns, buffer);
    }

    [Benchmark]
    public string DecodeHierarchical() => Encoders.Hierarchical.Decode(_hierarchicalA);

    [Benchmark]
    public string DecodeHierarchicalButNoSlashes() => Encoders.Hierarchical.Decode(_hierarchicalAns);

    [Benchmark]
    public byte[] EncodeNatural() => Encoders.Natural.Encode(BenchKeyA);

    [Benchmark]
    public int EncodeToNatural()
    {
        Span<byte> buffer = stackalloc byte[128];
        return Encoders.Natural.EncodeTo(BenchKeyA, buffer);
    }

    [Benchmark]
    public string DecodeNatural() => Encoders.Natural.Decode(_hierarchicalA);

    // BenchmarkEncoderNaturalDecode/internal and /plain (encode_test.go)
    [Benchmark]
    public string EncoderNaturalDecode_Internal() => Encoders.Natural.Decode(_naturalInternal);

    [Benchmark]
    public string EncoderNaturalDecode_Plain() => Encoders.Natural.Decode(_naturalPlain);
}
