// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : cmd/perf (the Go load tool).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Entry point of `dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load [options]`: starts a fresh server per
//              run (docker slot / echo / external), runs the .NET client, optionally the Go client (--go oxia-load.exe) on the SAME kind of
//              fresh server with the same parameters, and prints both side by side plus one machine readable RESULT line each.
// =====================================================================================================================
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Oxia.Client.Benchmarks.Load;

public static class LoadProgram
{
    public static async Task<int> RunAsync(string[] args)
    {
        LoadOptions options;
        try
        {
            options = LoadOptions.Parse(args);
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine("error: " + e.Message);
            Console.Error.WriteLine(LoadOptions.Usage);
            return 2;
        }
        if (options.Help)
        {
            Console.WriteLine(LoadOptions.Usage);
            return 0;
        }

        PrintEnvironment(options);

        var dotnetRuns = new List<LoadResult>();
        var goRuns = new List<LoadResult>();
        var runner = new LoadRunner(options);
        for (int run = 1; run <= options.Runs; run++)
        {
            if (!options.GoOnly)
            {
                Console.WriteLine($"-- .NET run {run}/{options.Runs}");
                await using var server = await LoadServer.StartAsync(options);
                Console.WriteLine($"  server: {server.Description}  [{server.Address}]");
                var result = await runner.RunAsync(server);
                result.Print(Console.Out);
                Console.WriteLine(result.ToResultLine());
                dotnetRuns.Add(result);
            }

            if (options.GoExe is not null)
            {
                Console.WriteLine($"-- Go run {run}/{options.Runs}");
                await using var server = await LoadServer.StartAsync(options);
                Console.WriteLine($"  server: {server.Description}  [{server.Address}]");
                var result = await RunGoAsync(options, server);
                if (result is null)
                {
                    Console.WriteLine("  the Go run produced no RESULT line");
                    return 1;
                }
                result.Print(Console.Out);
                Console.WriteLine(result.ToResultLine());
                goRuns.Add(result);
            }
        }

        if (options.Runs > 1 && dotnetRuns.Count > 0)
        {
            Console.WriteLine($"-- .NET ops/s of {options.Runs} runs: " + string.Join(", ", dotnetRuns.Select(r => r.D("ops_per_s").ToString("N0"))));
        }
        if (options.Runs > 1 && goRuns.Count > 0)
        {
            Console.WriteLine($"-- Go ops/s of {options.Runs} runs: " + string.Join(", ", goRuns.Select(r => r.D("ops_per_s").ToString("N0"))));
        }
        if (dotnetRuns.Count > 0 && goRuns.Count > 0)
        {
            Console.WriteLine($"== comparison (median run of {options.Runs}): " + options.Describe());
            LoadResult.PrintComparison(Console.Out, LoadResult.Median(dotnetRuns), LoadResult.Median(goRuns));
        }
        return 0;
    }

    private static async Task<LoadResult?> RunGoAsync(LoadOptions options, LoadServer server)
    {
        var psi = new ProcessStartInfo(options.GoExe!, options.ToGoArguments(server.Address))
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        using var p = Process.Start(psi)!;
        // The server container CPU of the Go run: sampled around the child's whole life (warm-up included, so only indicative)
        var stderr = p.StandardError.ReadToEndAsync();
        string stdout = await p.StandardOutput.ReadToEndAsync();
        await p.WaitForExitAsync();
        string err = await stderr;
        if (err.Length > 0) Console.Error.Write(err);
        return LoadResult.ParseResultLine(stdout);
    }

    private static void PrintEnvironment(LoadOptions o)
    {
        Console.WriteLine($"== {o.Describe()}");
        Console.WriteLine($"   duration {LoadOptions.FormatDuration(o.Duration)} after {LoadOptions.FormatDuration(o.Warmup)} warm-up, {o.Runs} run(s)");
        Console.WriteLine($"   {RuntimeInformation.OSDescription}, {RuntimeInformation.FrameworkDescription}, {Environment.ProcessorCount} logical processors, " +
                          $"{(System.Runtime.GCSettings.IsServerGC ? "server" : "workstation")} GC, concurrent GC {(IsConcurrentGc() ? "on" : "off")}, " +
                          $"TieredPGO {(AppContext.TryGetSwitch("System.Runtime.TieredPGO", out var pgo) ? pgo : true)}");
        if (OperatingSystem.IsWindows() && TimerResolution.TryQuery(out var min, out var max, out var current))
        {
            Console.WriteLine($"   system timer resolution: current {current / 10000.0:0.###} ms (finest {max / 10000.0:0.###} ms, coarsest {min / 10000.0:0.###} ms)");
        }
        ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
        Console.WriteLine($"   thread pool min threads {minWorkers}/{minIo}{(o.MinThreads > 0 ? $" -> {o.MinThreads}" : "")}");
        if (o.Tune.Length > 0) Console.WriteLine($"   tune: {o.Tune}");
    }

    private static bool IsConcurrentGc() => AppContext.TryGetSwitch("System.GC.Concurrent", out var on) && on;
}

/// <summary>ntdll!NtQueryTimerResolution: the Windows system timer resolution (100 ns units), which bounds the precision of every .NET timer wait.</summary>
internal static class TimerResolution
{
    [DllImport("ntdll.dll")]
    private static extern int NtQueryTimerResolution(out uint minimum, out uint maximum, out uint current);

    public static bool TryQuery(out uint coarsest, out uint finest, out uint current)
    {
        try
        {
            return NtQueryTimerResolution(out coarsest, out finest, out current) == 0;
        }
        catch (Exception)
        {
            coarsest = finest = current = 0;
            return false;
        }
    }
}
