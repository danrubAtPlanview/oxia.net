// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (Go tests embed the servers in-process).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The lifecycle shared by the two Docker fixtures (standalone servers, 3-node clusters): look a container up by a
//              fixed name derived from a hash of its settings, reuse it when running, `docker start` it when stopped, otherwise
//              create it (pulling the image only if it is missing).  The container idles on `tail -f /dev/null` (with an init
//              process, so killed children are reaped); the fixtures start/stop Oxia processes inside it with `docker exec`.
//              Ports are published on 127.0.0.1 with EPHEMERAL host ports (collision free); `docker port` is read after every
//              start because Docker may re-assign them.  Only containers labelled oxia.net.tests=1 are ever touched.
//   Divergences: n/a (new code).
// =====================================================================================================================
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Oxia.TestSupport.Docker;

internal sealed class ReusableContainer
{
    public const string Label = "oxia.net.tests";
    private static readonly Regex PortLine = new(@"^(\d+)/tcp -> .*:(\d+)\s*$", RegexOptions.Compiled | RegexOptions.Multiline);

    private ReusableContainer(string name, string kind, IReadOnlyDictionary<int, int> hostPorts, bool reused)
    {
        Name = name;
        Kind = kind;
        HostPorts = hostPorts;
        Reused = reused;
    }

    public string Name { get; }

    /// <summary>"tests" (standalone servers) or "cluster".</summary>
    public string Kind { get; }

    /// <summary>Container port -> host port (on 127.0.0.1).</summary>
    public IReadOnlyDictionary<int, int> HostPorts { get; }

    /// <summary>False when this call created the container.</summary>
    public bool Reused { get; }

    /// <summary><c>oxia-net-&lt;kind&gt;-&lt;hash&gt;[-&lt;OXIA_TEST_INSTANCE&gt;]</c>.</summary>
    public static string ComputeName(string kind, string hashMaterial)
    {
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(hashMaterial)))[..10].ToLowerInvariant();
        return $"oxia-net-{kind}-{hash}" + (IntegrationEnv.Instance is { } i ? "-" + Regex.Replace(i, "[^A-Za-z0-9_.-]", "-") : "");
    }

    public static async Task<ReusableContainer> EnsureAsync(string kind, string hashMaterial, IReadOnlyList<int> containerPorts,
        Action<string> log, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var (available, detail) = await DockerCli.ProbeAsync(ct).ConfigureAwait(false);
        if (!available)
        {
            throw new DockerException($"Docker is not available ({detail}). Install/start Docker, or point {IntegrationEnv.AddressVar} " +
                                      $"at an externally started `oxia standalone`. Set {IntegrationEnv.SkipIfNoServerVar}=1 to skip instead of fail.");
        }

        var name = ComputeName(kind, hashMaterial);
        var image = IntegrationEnv.Image;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var state = await InspectAsync(name, ct).ConfigureAwait(false);
            bool reused;
            switch (state)
            {
                case null:
                    await EnsureImageAsync(image, ct).ConfigureAwait(false);
                    var run = await DockerCli.RunAsync(BuildRunArgs(name, kind, image, containerPorts), null, TimeSpan.FromMinutes(2), ct).ConfigureAwait(false);
                    if (!run.Ok)
                    {
                        if (run.Combined.Contains("is already in use", StringComparison.OrdinalIgnoreCase)) continue; // another run created it
                        throw new DockerException($"docker run failed: {run.Combined}");
                    }

                    reused = false;
                    log($"created container {name} from {image}");
                    var version = await DockerCli.RunAsync(["exec", name, "oxia", "--version"], null, TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                    log("server: " + version.StdOut.Replace("\r", "").Replace("\n", " ").Trim());
                    break;
                case "running":
                    reused = true;
                    log($"reusing running container {name}");
                    break;
                case "paused":
                    await DockerCli.RunCheckedAsync(["unpause", name], null, null, ct).ConfigureAwait(false);
                    reused = true;
                    log($"unpaused container {name}");
                    break;
                case "exited" or "created":
                    await DockerCli.RunCheckedAsync(["start", name], null, null, ct).ConfigureAwait(false);
                    reused = true;
                    log($"started stopped container {name}");
                    break;
                default: // dead, removing, restarting...: start over
                    log($"container {name} is '{state}': recreating");
                    await DockerCli.RunAsync(["rm", "-f", name], null, null, ct).ConfigureAwait(false);
                    continue;
            }

            var ports = await ReadPortsAsync(name, ct).ConfigureAwait(false);
            var missing = containerPorts.Where(p => !ports.ContainsKey(p)).ToList();
            if (missing.Count > 0)
            {
                throw new DockerException($"container {name} does not publish the expected ports (missing: {string.Join(',', missing)}); " +
                                          "remove it (scripts/clean-oxia-containers.ps1) and retry");
            }

            log($"container {name} ready in {sw.ElapsedMilliseconds} ms (reused={reused}); host ports {ports.Values.Min()}..{ports.Values.Max()}");
            return new ReusableContainer(name, kind, ports, reused);
        }

        throw new DockerException($"could not create or reuse container {name}");
    }

    /// <summary>Runs a shell script (LF line endings) in the container with <c>sh -s -- args</c> (busybox ash).</summary>
    public Task<DockerResult> ExecScriptAsync(string script, IEnumerable<string> args, TimeSpan timeout, CancellationToken ct = default)
    {
        var command = new List<string> { "exec", "-i", Name, "sh", "-s", "--" };
        command.AddRange(args);
        return DockerCli.RunAsync(command, script, timeout, ct);
    }

    public Task<DockerResult> ExecAsync(IEnumerable<string> command, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var args = new List<string> { "exec", Name };
        args.AddRange(command);
        return DockerCli.RunAsync(args, null, timeout ?? TimeSpan.FromSeconds(30), ct);
    }

    public Task<DockerResult> RemoveAsync() => DockerCli.RunAsync(["rm", "-f", Name]);

    /// <summary>State of the container (<c>running</c>, <c>exited</c>, ...) or null if it does not exist; refuses foreign containers.</summary>
    private static async Task<string?> InspectAsync(string name, CancellationToken ct)
    {
        var r = await DockerCli.RunAsync(["inspect", "-f", "{{.State.Status}}|{{index .Config.Labels \"" + Label + "\"}}", name],
            null, null, ct).ConfigureAwait(false);
        if (!r.Ok) return null;
        var parts = r.StdOut.Trim().Split('|');
        if (parts.Length < 2 || parts[1] != "1")
        {
            throw new DockerException($"a container named {name} exists but is not labelled {Label}=1; refusing to touch it");
        }

        return parts[0];
    }

    private static async Task EnsureImageAsync(string image, CancellationToken ct)
    {
        var r = await DockerCli.RunAsync(["image", "inspect", image], null, null, ct).ConfigureAwait(false);
        if (r.Ok) return;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await DockerCli.RunCheckedAsync(["pull", image], null, TimeSpan.FromMinutes(10), ct).ConfigureAwait(false);
        Console.Error.WriteLine($"[oxia-fixture] pulled {image} in {sw.Elapsed.TotalSeconds:0.#} s");
    }

    private static List<string> BuildRunArgs(string name, string kind, string image, IReadOnlyList<int> containerPorts)
    {
        var args = new List<string>
        {
            "run", "-d", "--init", "--name", name,
            "--label", Label + "=1",
            "--label", "oxia.net.tests.kind=" + kind,
            "--label", "oxia.net.tests.image=" + image,
            "--label", "oxia.net.tests.created=" + DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            "--memory", IntegrationEnv.Memory,
            "--tmpfs", "/data:rw,size=1g",
        };
        if (IntegrationEnv.Cpus is { } cpus)
        {
            args.Add("--cpus");
            args.Add(cpus);
        }

        foreach (var port in containerPorts)
        {
            // ephemeral host port on loopback only: collision free, and the server echoes whatever authority the client used
            args.Add("-p");
            args.Add($"127.0.0.1::{port}");
        }

        args.AddRange(["--entrypoint", "tail", image, "-f", "/dev/null"]);
        return args;
    }

    private static async Task<Dictionary<int, int>> ReadPortsAsync(string name, CancellationToken ct)
    {
        var map = new Dictionary<int, int>();
        // `docker port` can briefly return nothing right after start on Docker Desktop: retry a few times
        for (var i = 0; i < 20; i++)
        {
            var output = await DockerCli.RunCheckedAsync(["port", name], null, null, ct).ConfigureAwait(false);
            foreach (Match m in PortLine.Matches(output))
            {
                map[int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture)] = int.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
            }

            if (map.Count > 0) return map;
            await Task.Delay(100, ct).ConfigureAwait(false);
        }

        return map;
    }
}
