// =====================================================================================================================
// PORT NOTE (benchmarks; no upstream counterpart other than the Go *_benchmark_test.go / Benchmark* functions)
//   Entry point of the BenchmarkDotNet suite.  Run a subset with a filter, e.g.
//     dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Codec*" --job short
//     dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*ClientOperation*" --memory
//   Always run in Release: allocation numbers and tiered-JIT behaviour are meaningless in Debug.
//
//   The first argument `load` switches to the end-to-end LOAD GENERATOR (Load/): throughput, latency percentiles, CPU, GC and allocations
//   of the client against a real Oxia server (or the in-process echo server), optionally head to head with the Go client:
//     dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load --help
// =====================================================================================================================
using BenchmarkDotNet.Running;
using Oxia.Client.Benchmarks.Load;

namespace Oxia.Client.Benchmarks;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "load")
        {
            return await LoadProgram.RunAsync(args[1..]);
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        return 0;
    }
}
