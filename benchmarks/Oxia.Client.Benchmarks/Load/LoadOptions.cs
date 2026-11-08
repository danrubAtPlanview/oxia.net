// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : cmd/perf/cmd.go (flags: service address, namespace, request rate, read percentage, keys cardinality, value size,
//              random payload, batch linger, max requests per batch, request timeout).  The Go tool is open loop and prints a
//              periodic summary; this tool adds a closed-loop mode, a warm-up, an HDR histogram and the process figures.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Command line of the `load` mode of the benchmarks exe.  Durations use the Go syntax (`5ms`, `1.5s`, `0`).
// =====================================================================================================================
using System.Globalization;

namespace Oxia.Client.Benchmarks.Load;

public sealed class LoadOptions
{
    /// <summary>docker (the reusable oxia-net-tests container, fresh server per run) | echo (in-process RawEchoServer) | host:port.</summary>
    public string Server { get; set; } = "";
    public int Shards { get; set; } = 1;
    public bool NoNotifications { get; set; }

    /// <summary>async | sync.</summary>
    public string Client { get; set; } = "async";
    public bool Put { get; set; } = true;
    public bool Get { get; set; } = true;
    public bool Delete { get; set; }
    public int ReadPercent { get; set; } = 50;
    public int Keys { get; set; } = 100_000;
    public int ValueSize { get; set; } = 128;
    public bool RandomPayload { get; set; }
    public bool Preload { get; set; } = true;

    public int Producers { get; set; } = 64;
    public TimeSpan Duration { get; set; } = TimeSpan.FromSeconds(20);
    public TimeSpan Warmup { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Open loop target rate in operations per second; 0 = closed loop (every producer keeps one operation in flight).</summary>
    public double Rate { get; set; }
    public int MaxInflight { get; set; } = 200_000;

    public TimeSpan Linger { get; set; } = OxiaOptions.DefaultBatchLinger;
    public int MaxRequestsPerBatch { get; set; } = OxiaOptions.DefaultMaxRequestsPerBatch;
    public int MaxBatchSize { get; set; } = OxiaOptions.DefaultMaxBatchSize;
    public TimeSpan RequestTimeout { get; set; } = OxiaOptions.DefaultRequestTimeout;

    /// <summary>ThreadPool.SetMinThreads(n, n) before the run (0 = leave the default).</summary>
    public int MinThreads { get; set; }

    /// <summary>Extra client options, "name=value,name=value" (the tuning knobs of docs/performance.md).</summary>
    public string Tune { get; set; } = "";

    /// <summary>How many times the run is repeated (a fresh server each); the median is summarised.</summary>
    public int Runs { get; set; } = 1;

    /// <summary>Path of oxia-load.exe (Load/go/build.ps1): after every .NET run the Go client runs the same workload on a fresh server.</summary>
    public string? GoExe { get; set; }
    public bool GoOnly { get; set; }

    public bool Help { get; set; }

    public static LoadOptions Parse(string[] args)
    {
        var o = new LoadOptions();
        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            if (!a.StartsWith("--", StringComparison.Ordinal)) throw new ArgumentException($"unexpected argument '{a}'");
            string name = a[2..];
            string? value = null;
            int eq = name.IndexOf('=');
            if (eq >= 0)
            {
                value = name[(eq + 1)..];
                name = name[..eq];
            }

            string Next()
            {
                if (value is not null) return value;
                if (i + 1 >= args.Length) throw new ArgumentException($"--{name} needs a value");
                return args[++i];
            }

            bool Flag()
            {
                if (value is not null) return bool.Parse(value);
                if (i + 1 < args.Length && (args[i + 1] is "true" or "false")) return bool.Parse(args[++i]);
                return true;
            }

            switch (name)
            {
                case "help" or "h": o.Help = true; break;
                case "server": o.Server = Next(); break;
                case "shards": o.Shards = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "no-notifications": o.NoNotifications = Flag(); break;
                case "client": o.Client = Next(); break;
                case "ops": o.ParseOps(Next()); break;
                case "read-percent": o.ReadPercent = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "keys": o.Keys = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "value-size": o.ValueSize = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "random-payload": o.RandomPayload = Flag(); break;
                case "preload": o.Preload = Flag(); break;
                case "producers": o.Producers = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "duration": o.Duration = ParseDuration(Next()); break;
                case "warmup": o.Warmup = ParseDuration(Next()); break;
                case "rate": o.Rate = double.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "max-inflight": o.MaxInflight = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "linger": o.Linger = ParseDuration(Next()); break;
                case "max-requests-per-batch": o.MaxRequestsPerBatch = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "max-batch-size": o.MaxBatchSize = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "request-timeout": o.RequestTimeout = ParseDuration(Next()); break;
                case "min-threads": o.MinThreads = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "tune": o.Tune = Next(); break;
                case "runs": o.Runs = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "go": o.GoExe = Next(); break;
                case "go-only": o.GoOnly = Flag(); break;
                default: throw new ArgumentException($"unknown option --{name}");
            }
        }

        if (o.Server.Length == 0)
        {
            o.Server = Environment.GetEnvironmentVariable("OXIA_TEST_ADDRESS") is { Length: > 0 } env ? env : "docker";
        }
        if (o.Client is not ("async" or "sync")) throw new ArgumentException("--client must be async or sync");
        if (!o.Put && !o.Get && !o.Delete) throw new ArgumentException("--ops selects nothing");
        if (o.Producers < 1 || o.Keys < 1 || o.ValueSize < 0) throw new ArgumentException("--producers/--keys/--value-size out of range");
        if (o.Rate > 0 && o.Client != "async") throw new ArgumentException("--rate (open loop) needs --client async");
        return o;
    }

    private void ParseOps(string list)
    {
        Put = Get = Delete = false;
        foreach (var op in list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            switch (op)
            {
                case "put": Put = true; break;
                case "get": Get = true; break;
                case "delete": Delete = true; break;
                default: throw new ArgumentException($"unknown operation '{op}'");
            }
        }
    }

    /// <summary>Go's time.ParseDuration subset: a number followed by ns, us, ms, s, m; "0" is zero.</summary>
    public static TimeSpan ParseDuration(string s)
    {
        s = s.Trim();
        if (s == "0") return TimeSpan.Zero;
        // the two-letter suffixes first: "ms" also ends with "s"
        (string Suffix, double Ticks)[] units = [("ns", 0.01), ("us", 10), ("µs", 10), ("ms", 10_000), ("s", 10_000_000), ("m", 600_000_000)];
        foreach (var (suffix, ticks) in units)
        {
            if (s.EndsWith(suffix, StringComparison.Ordinal))
            {
                double n = double.Parse(s[..^suffix.Length], CultureInfo.InvariantCulture);
                return TimeSpan.FromTicks((long)(n * ticks));
            }
        }
        throw new ArgumentException($"bad duration '{s}'");
    }

    public static string FormatDuration(TimeSpan t) => t == TimeSpan.Zero ? "0" : t.TotalMilliseconds >= 1000 && t.Ticks % 10_000_000 == 0
        ? $"{t.TotalSeconds.ToString(CultureInfo.InvariantCulture)}s"
        : $"{t.TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture)}ms";

    public string Describe()
    {
        var ops = string.Join('+', new[] { Put ? "put" : null, Get ? "get" : null, Delete ? "delete" : null }.Where(x => x is not null));
        string mode = Rate > 0 ? $"open loop {Rate:0} ops/s" : $"closed loop, {Producers} producers";
        return $"{Client} client, {ops}{(Put && Get ? $" ({ReadPercent}% reads)" : "")}, {Keys} keys x {ValueSize} B, {mode}, linger {FormatDuration(Linger)}, " +
               $"max {MaxRequestsPerBatch} requests/batch";
    }

    /// <summary>The arguments the Go twin needs for the same workload.</summary>
    public string ToGoArguments(string address)
    {
        var ops = string.Join(',', new[] { Put ? "put" : null, Get ? "get" : null, Delete ? "delete" : null }.Where(x => x is not null));
        return $"-addr {address} -client {Client} -ops {ops} -read-percent {ReadPercent} -keys {Keys} -value-size {ValueSize} " +
               $"-producers {Producers} -duration {Duration.TotalSeconds.ToString(CultureInfo.InvariantCulture)}s " +
               $"-warmup {Warmup.TotalSeconds.ToString(CultureInfo.InvariantCulture)}s -linger {FormatDuration(Linger)} " +
               $"-max-requests-per-batch {MaxRequestsPerBatch} -request-timeout {FormatDuration(RequestTimeout)} -rate {Rate.ToString(CultureInfo.InvariantCulture)}" +
               (RandomPayload ? " -random-payload" : "") + (Preload ? "" : " -preload=false");
    }

    public const string Usage = """
        dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load [options]

        server     --server docker|echo|host:port   docker = reusable oxia-net-tests container (a fresh server per run; default, or OXIA_TEST_ADDRESS)
                                                    echo   = in-process zero-allocation RawEchoServer (CPU-bound analysis of the client)
                                                    echo-proc = the same in a child process (client-only CPU figures; works with --go)
                   --shards N  --no-notifications   profile of the docker server
        workload   --client async|sync  --ops put,get,delete  --read-percent 50  --keys 100000  --value-size 128  --random-payload
                   --producers 64      closed loop: this many operations in flight (each producer awaits its operation, then issues the next)
                   --rate R            open loop at R ops/s instead (latency measured from the intended start; needs --client async)
                   --duration 20s  --warmup 3s  --preload true|false  --runs 1  --max-inflight 200000
        client     --linger 5ms  --max-requests-per-batch 1000  --max-batch-size 131072  --request-timeout 30s
                   --min-threads N     ThreadPool.SetMinThreads(N, N)
                   --tune a=b,c=d      experimental knobs (docs/performance.md)
        go         --go path\to\oxia-load.exe   run the Go client on the same workload after every run (build it with Load/go/build.ps1)
                   --go-only                    run only the Go client
        """;
}
