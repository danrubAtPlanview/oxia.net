// =====================================================================================================================
// PORT NOTE (benchmarks; no upstream counterpart other than the Go *_benchmark_test.go / Benchmark* functions)
//   Entry point of the BenchmarkDotNet suite.  Run a subset with a filter, e.g.
//     dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Codec*" --job short
//     dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*ClientOperation*" --memory
//   Always run in Release: allocation numbers and tiered-JIT behaviour are meaningless in Debug.
// =====================================================================================================================
using BenchmarkDotNet.Running;

namespace Oxia.Client.Benchmarks;

public static class Program
{
    public static int Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        return 0;
    }
}
