// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The outcome of one load run as an ordered list of key=value pairs: printed for people (Print) and as ONE machine readable
//              `RESULT k=v k=v ...` line that both this tool and the Go twin (Load/go/main.go) emit, so that a result can be parsed back
//              (the Go run is a child process) and several runs can be summarised/compared (Compare, Median).
// =====================================================================================================================
using System.Globalization;

namespace Oxia.Client.Benchmarks.Load;

internal sealed class LoadResult
{
    private readonly List<KeyValuePair<string, string>> _values = new();

    public void Add(string key, string value) => _values.Add(new(key, value));

    public void Add(string key, double value, string format = "0.##") => Add(key, value.ToString(format, CultureInfo.InvariantCulture));

    public void Add(string key, long value) => Add(key, value.ToString(CultureInfo.InvariantCulture));

    public bool TryGet(string key, out string value)
    {
        foreach (var kv in _values)
        {
            if (kv.Key == key)
            {
                value = kv.Value;
                return true;
            }
        }
        value = "";
        return false;
    }

    public double D(string key) => TryGet(key, out var v) && double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) ? d : double.NaN;

    public string S(string key) => TryGet(key, out var v) ? v : "";

    public string ToResultLine() => "RESULT " + string.Join(' ', _values.Select(kv => $"{kv.Key}={kv.Value}"));

    public static LoadResult? ParseResultLine(string text)
    {
        foreach (var raw in text.Split('\n'))
        {
            var line = raw.Trim();
            if (!line.StartsWith("RESULT ", StringComparison.Ordinal)) continue;
            var r = new LoadResult();
            foreach (var token in line[7..].Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                int eq = token.IndexOf('=');
                if (eq > 0) r.Add(token[..eq], token[(eq + 1)..]);
            }
            return r;
        }
        return null;
    }

    private static string Ms(double us) => double.IsNaN(us) ? "-" : (us / 1000).ToString(us < 100_000 ? "0.000" : "0.0", CultureInfo.InvariantCulture);

    public void Print(TextWriter w)
    {
        w.WriteLine($"  impl {S("impl")}, {S("client")} client: {D("ops_per_s"):N0} ops/s over {D("seconds"):0.0} s  (put {S("put")}, get {S("get")}, delete {S("delete")}, errors {S("errors")})");
        w.WriteLine($"  latency ms : p50 {Ms(D("p50_us"))}  p90 {Ms(D("p90_us"))}  p99 {Ms(D("p99_us"))}  p99.9 {Ms(D("p999_us"))}  max {Ms(D("max_us"))}  mean {Ms(D("mean_us"))}");
        foreach (var op in new[] { "put", "get", "delete" })
        {
            if (TryGet($"{op}_p50_us", out _))
            {
                w.WriteLine($"     {op,-6}  : p50 {Ms(D($"{op}_p50_us"))}  p99 {Ms(D($"{op}_p99_us"))}");
            }
        }
        w.WriteLine($"  client cpu : {D("cpu_s"):0.00} s = {D("cpu_us_per_op"):0.00} us/op" +
                    (TryGet("server_cpu_s", out _) ? $"   |   server (container) cpu: {D("server_cpu_s"):0.00} s = {D("server_cpu_us_per_op"):0.00} us/op" : ""));
        w.WriteLine($"  gc         : gen0 {S("gc0")}  gen1 {S("gc1")}  gen2 {S("gc2")}   allocated {D("alloc_bytes_per_op"):N0} B/op" +
                    (TryGet("workitems_per_op", out _) ? $"   threadpool work items/op {D("workitems_per_op"):0.00}, lock contentions {S("lock_contentions")}, threads {S("threads")}" : ""));
    }

    public static void PrintComparison(TextWriter w, LoadResult dotnet, LoadResult go)
    {
        w.WriteLine();
        w.WriteLine($"  {"",-24}{".NET",14}{"Go",14}{".NET/Go",10}");
        void Row(string label, string key, string format, double scale = 1)
        {
            double a = dotnet.D(key) * scale, b = go.D(key) * scale;
            string ratio = double.IsNaN(a) || double.IsNaN(b) || b == 0 ? "-" : (a / b).ToString("0.00", CultureInfo.InvariantCulture);
            string sa = double.IsNaN(a) ? "-" : a.ToString(format, CultureInfo.InvariantCulture);
            string sb = double.IsNaN(b) ? "-" : b.ToString(format, CultureInfo.InvariantCulture);
            w.WriteLine($"  {label,-24}{sa,14}{sb,14}{ratio,10}");
        }
        Row("ops/s", "ops_per_s", "N0");
        Row("p50 (ms)", "p50_us", "0.000", 0.001);
        Row("p90 (ms)", "p90_us", "0.000", 0.001);
        Row("p99 (ms)", "p99_us", "0.000", 0.001);
        Row("p99.9 (ms)", "p999_us", "0.000", 0.001);
        Row("max (ms)", "max_us", "0.000", 0.001);
        Row("client cpu (s)", "cpu_s", "0.00");
        Row("client cpu (us/op)", "cpu_us_per_op", "0.00");
        Row("allocated (B/op)", "alloc_bytes_per_op", "N0");
        Row("GCs", "gc0", "N0");
        w.WriteLine("  (the ratio is .NET divided by Go; ops/s: higher is better, everything else lower)");
    }

    /// <summary>The run whose ops/s is the median of the runs.</summary>
    public static LoadResult Median(IReadOnlyList<LoadResult> runs)
    {
        var sorted = runs.OrderBy(r => r.D("ops_per_s")).ToList();
        return sorted[sorted.Count / 2];
    }
}
