// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : tests/client/*_test.go, tests/sequence, tests/resolver: every Go integration test starts its own in-process
//              server with  `standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))`,
//              uses `standaloneServer.ServiceAddr()` and finally `standaloneServer.Close()`.
//              Server side: cmd/standalone/cmd.go (flags), oxiad/dataserver/standalone.go, oxiad/dataserver/assignment/
//              assignment_dispatcher.go (standalone mode answers shard assignments with the :authority the client dialled).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The .NET replacement for "start an in-process server": ONE reusable Docker container (named after a hash of
//              image+settings, labelled oxia.net.tests=1, created lazily, kept after the run so the next run reuses it) that
//              hosts a pool of "slots".  A slot is a port pair plus a directory on the container's tmpfs; StartAsync(profile)
//              leases a slot and launches `oxia standalone <profile flags>` in it (docker exec), so every test gets a FRESH,
//              EMPTY server with exactly the configuration the Go test sets (shards, key sorting, notifications, retention)
//              -- the same isolation as Go -- without creating a container per test.  Closing the server kills the process
//              and wipes its directory.  Slots are guarded by lock directories inside the container, so several test runs
//              (different worktrees / IDE + CLI) can share the container safely.
//              Because a standalone server answers shard assignments with the :authority the client used, a published
//              ephemeral host port (docker `-p 127.0.0.1::<port>`) is all that is needed: the leader address the client
//              receives is the address it already reaches.  OXIA_TEST_ADDRESS bypasses Docker for an external server.
//   Divergences: see tests/Oxia.Client.IntegrationTests/README.md ("Why not one container per profile").
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Oxia.TestSupport.Docker;
using Xunit;

namespace Oxia.TestSupport;

/// <summary>One running standalone server (Go: <c>*dataserver.Standalone</c>).  Created by <see cref="OxiaServerFixture.StartAsync"/>.</summary>
public sealed class OxiaServer : IAsyncDisposable
{
    private readonly OxiaServerFixture? _owner;
    private readonly int _slot;
    private int _closed;

    internal OxiaServer(OxiaServerFixture? owner, OxiaServerProfile profile, string address, int slot)
    {
        _owner = owner;
        Profile = profile;
        Address = address;
        _slot = slot;
    }

    /// <summary>host:port the clients connect to (Go: <c>ServiceAddr()</c>).</summary>
    public string Address { get; }

    /// <summary>Go: <c>standaloneServer.ServiceAddr()</c>.</summary>
    public string ServiceAddr => Address;

    public OxiaServerProfile Profile { get; }

    /// <summary>Go: <c>config.NumShards</c>.</summary>
    public uint NumShards => Profile.Shards;

    /// <summary>The slot inside the container (-1 for an external server).</summary>
    public int Slot => _slot;

    /// <summary>Last lines of the server's log (diagnostics for failing tests).</summary>
    public Task<string> GetLogAsync() => _owner is null || _slot < 0 ? Task.FromResult("(external server: no log)") : _owner.GetSlotLogAsync(_slot);

    /// <summary>Go: <c>standaloneServer.Close()</c>.  Kills the server process, wipes its data and frees the slot.  Idempotent.</summary>
    public async Task CloseAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;
        if (_owner is not null && _slot >= 0) await _owner.ReleaseSlotAsync(_slot).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try { await CloseAsync().ConfigureAwait(false); }
        catch { /* dispose must not throw; use CloseAsync to observe failures */ }
    }

    public override string ToString() => $"oxia standalone @ {Address} [{Profile}]";
}

/// <summary>
/// Reusable Docker based Oxia server provider; register it as an xUnit v3 assembly fixture
/// (<c>[assembly: AssemblyFixture(typeof(OxiaServerFixture))]</c>) and take it as a constructor argument of the test classes.
/// Nothing happens until the first <see cref="StartAsync"/>.
/// </summary>
public sealed class OxiaServerFixture : IAsyncLifetime
{
    public const string ContainerLabel = "oxia.net.tests";
    private const string ScriptVersion = "v1";
    private const int BaseContainerPort = 6648;
    private const int BaseMetricsPort = 7648;
    private static readonly Regex AnsiColors = new("\u001b\\[[0-9;]*m", RegexOptions.Compiled);
    private static readonly Regex PortLine = new(@"^(\d+)/tcp -> .*:(\d+)\s*$", RegexOptions.Compiled | RegexOptions.Multiline);

    private readonly int _slots = IntegrationEnv.Slots;
    private readonly SemaphoreSlim _gate;
    private readonly bool[] _busy;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentQueue<string> _events = new();
    private readonly string _owner = $"{Environment.ProcessId}:{Environment.MachineName}:{Process.GetCurrentProcess().StartTime.Ticks}";
    private ContainerInfo? _container;
    private bool _externalVerified;

    public OxiaServerFixture()
    {
        _gate = new SemaphoreSlim(_slots, _slots);
        _busy = new bool[_slots];
    }

    /// <summary>Everything the fixture did (container reuse/creation, leases, timings); useful as test output.</summary>
    public IReadOnlyCollection<string> Events => _events;

    /// <summary>True when OXIA_TEST_ADDRESS points the tests at an externally started server.</summary>
    public bool IsExternal => IntegrationEnv.ExternalAddress is not null;

    /// <summary>The name of the Docker container (null before the first server was started or in external mode).</summary>
    public string? ContainerName => _container?.Name;

    /// <summary>True when this run found the container already running / existing (false = created by this run).</summary>
    public bool? ContainerWasReused => _container?.Reused;

    /// <summary>The <c>oxia --version</c> text of the server in the container (e.g. "oxia version 8456f61 commit: 8456f61e..."); null for an external server.</summary>
    public async Task<string?> GetServerVersionAsync(CancellationToken ct = default)
    {
        if (IsExternal) return null;
        var c = await EnsureContainerAsync(ct).ConfigureAwait(false);
        var r = await DockerCli.RunAsync(["exec", c.Name, "oxia", "--version"], null, TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
        return r.Ok ? r.StdOut.Replace("\r", "").Trim() : r.Combined;
    }

    public ValueTask InitializeAsync() => default;

    public async ValueTask DisposeAsync()
    {
        if (_container is { } c && IntegrationEnv.Cleanup)
        {
            Log($"OXIA_TEST_CLEANUP: removing container {c.Name}");
            var r = await DockerCli.RunAsync(["rm", "-f", c.Name]).ConfigureAwait(false);
            if (!r.Ok) Log("docker rm failed: " + r.Combined);
        }
    }

    /// <summary>
    /// Starts a fresh, empty standalone server with the configuration of <paramref name="profile"/> (default
    /// <see cref="OxiaServerProfile.Default"/>) and waits until every shard has a leader.
    /// </summary>
    public async Task<OxiaServer> StartAsync(OxiaServerProfile? profile = null, CancellationToken ct = default)
    {
        profile ??= OxiaServerProfile.Default;
        if (IntegrationEnv.ExternalAddress is { } external) return await StartExternalAsync(external, profile, ct).ConfigureAwait(false);

        ContainerInfo container;
        try
        {
            container = await EnsureContainerAsync(ct).ConfigureAwait(false);
        }
        catch (DockerException e) when (IntegrationEnv.SkipIfNoServer)
        {
            Assert.Skip($"no Oxia server available ({e.Message}); set {IntegrationEnv.AddressVar} or start Docker");
            throw;
        }

        var sw = Stopwatch.StartNew();
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        var slot = -1;
        try
        {
            slot = await LockAndStartAsync(container, profile, ct).ConfigureAwait(false);
            var address = $"127.0.0.1:{container.HostPorts[BaseContainerPort + slot]}";
            var healthSlot = slot;
            await OxiaReadiness.WaitUntilReadyAsync(address, profile.Shards, TimeSpan.FromSeconds(60), "default",
                () => HealthCheckAsync(container, healthSlot), () => GetSlotLogAsync(healthSlot), ct).ConfigureAwait(false);
            Log($"slot {slot} ready at {address} in {sw.ElapsedMilliseconds} ms [{profile}]");
            var server = new OxiaServer(this, profile, address, slot);
            slot = -1; // ownership moved to the OxiaServer
            return server;
        }
        catch
        {
            if (slot >= 0) await ReleaseSlotAsync(slot).ConfigureAwait(false);
            else _gate.Release();
            throw;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // External server (OXIA_TEST_ADDRESS)
    // ------------------------------------------------------------------------------------------------------------------

    private async Task<OxiaServer> StartExternalAsync(string address, OxiaServerProfile profile, CancellationToken ct)
    {
        if (!profile.IsDefault)
        {
            Assert.Skip($"needs a server configured as [{profile}] but {IntegrationEnv.AddressVar}={address} provides only the default " +
                        "profile (1 shard, hierarchical sorting, notifications on); unset it to use the Docker fixture");
        }

        if (!_externalVerified)
        {
            await OxiaReadiness.WaitUntilReadyAsync(address, 1, TimeSpan.FromSeconds(30), ct: ct).ConfigureAwait(false);
            _externalVerified = true;
            Log($"using external server {address}");
        }

        return new OxiaServer(null, profile, address, -1);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Container management
    // ------------------------------------------------------------------------------------------------------------------

    private sealed record ContainerInfo(string Name, IReadOnlyDictionary<int, int> HostPorts, bool Reused);

    /// <summary>The container name: <c>oxia-net-tests-&lt;hash of image+settings&gt;[-&lt;OXIA_TEST_INSTANCE&gt;]</c>.</summary>
    public static string ComputeContainerName()
    {
        var material = string.Join('|', ScriptVersion, IntegrationEnv.Image, IntegrationEnv.Slots, IntegrationEnv.Memory, IntegrationEnv.Cpus ?? "-");
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(material)))[..10].ToLowerInvariant();
        return "oxia-net-tests-" + hash + (IntegrationEnv.Instance is { } i ? "-" + SanitizeName(i) : "");
    }

    private static string SanitizeName(string s) => Regex.Replace(s, "[^A-Za-z0-9_.-]", "-");

    private async Task<ContainerInfo> EnsureContainerAsync(CancellationToken ct)
    {
        if (_container is not null) return _container;
        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _container ??= await EnsureContainerCoreAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task<ContainerInfo> EnsureContainerCoreAsync(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var (available, detail) = await DockerCli.ProbeAsync(ct).ConfigureAwait(false);
        if (!available)
        {
            throw new DockerException($"Docker is not available ({detail}). Install/start Docker, or point {IntegrationEnv.AddressVar} " +
                                      $"at an externally started `oxia standalone`. Set {IntegrationEnv.SkipIfNoServerVar}=1 to skip instead of fail.");
        }

        var name = ComputeContainerName();
        var image = IntegrationEnv.Image;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var state = await InspectAsync(name, ct).ConfigureAwait(false);
            bool reused;
            switch (state)
            {
                case null:
                    await EnsureImageAsync(image, ct).ConfigureAwait(false);
                    var run = await DockerCli.RunAsync(BuildRunArgs(name, image), null, TimeSpan.FromMinutes(2), ct).ConfigureAwait(false);
                    if (!run.Ok)
                    {
                        if (run.Combined.Contains("is already in use", StringComparison.OrdinalIgnoreCase)) continue; // another run created it
                        throw new DockerException($"docker run failed: {run.Combined}");
                    }

                    reused = false;
                    Log($"created container {name} from {image}");
                    var version = await DockerCli.RunAsync(["exec", name, "oxia", "--version"], null, TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                    Log("server: " + version.StdOut.Replace(Environment.NewLine, " ").Replace("\n", " ").Trim());
                    break;
                case "running":
                    reused = true;
                    Log($"reusing running container {name}");
                    break;
                case "paused":
                    await DockerCli.RunCheckedAsync(["unpause", name], null, null, ct).ConfigureAwait(false);
                    reused = true;
                    Log($"unpaused container {name}");
                    break;
                case "exited" or "created":
                    await DockerCli.RunCheckedAsync(["start", name], null, null, ct).ConfigureAwait(false);
                    reused = true;
                    Log($"started stopped container {name}");
                    break;
                default: // dead, removing, restarting...: start over
                    Log($"container {name} is '{state}': recreating");
                    await DockerCli.RunAsync(["rm", "-f", name], null, null, ct).ConfigureAwait(false);
                    continue;
            }

            var ports = await ReadPortsAsync(name, ct).ConfigureAwait(false);
            if (Enumerable.Range(0, _slots).Any(i => !ports.ContainsKey(BaseContainerPort + i)))
            {
                throw new DockerException($"container {name} does not publish the expected ports (found: {string.Join(',', ports.Keys)}); " +
                                          $"remove it (scripts/clean-oxia-containers.ps1) and retry");
            }

            Log($"container {name} ready in {sw.ElapsedMilliseconds} ms (reused={reused}); host ports {ports.Values.Min()}..{ports.Values.Max()}");
            return new ContainerInfo(name, ports, reused);
        }

        throw new DockerException($"could not create or reuse container {name}");
    }

    /// <summary>State of the container (<c>running</c>, <c>exited</c>, ...) or null if it does not exist; refuses foreign containers.</summary>
    private static async Task<string?> InspectAsync(string name, CancellationToken ct)
    {
        var r = await DockerCli.RunAsync(["inspect", "-f", "{{.State.Status}}|{{index .Config.Labels \"" + ContainerLabel + "\"}}", name],
            null, null, ct).ConfigureAwait(false);
        if (!r.Ok) return null;
        var parts = r.StdOut.Trim().Split('|');
        if (parts.Length < 2 || parts[1] != "1")
        {
            throw new DockerException($"a container named {name} exists but is not labelled {ContainerLabel}=1; refusing to touch it");
        }

        return parts[0];
    }

    private static async Task EnsureImageAsync(string image, CancellationToken ct)
    {
        var r = await DockerCli.RunAsync(["image", "inspect", image], null, null, ct).ConfigureAwait(false);
        if (r.Ok) return;
        var sw = Stopwatch.StartNew();
        await DockerCli.RunCheckedAsync(["pull", image], null, TimeSpan.FromMinutes(10), ct).ConfigureAwait(false);
        Console.Error.WriteLine($"[oxia-fixture] pulled {image} in {sw.Elapsed.TotalSeconds:0.#} s");
    }

    private List<string> BuildRunArgs(string name, string image)
    {
        var args = new List<string>
        {
            "run", "-d", "--init", "--name", name,
            "--label", ContainerLabel + "=1",
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

        for (var i = 0; i < _slots; i++)
        {
            // ephemeral host port on loopback only: collision free, and the server echoes whatever authority the client used
            args.Add("-p");
            args.Add($"127.0.0.1::{BaseContainerPort + i}");
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

    // ------------------------------------------------------------------------------------------------------------------
    // Slots
    // ------------------------------------------------------------------------------------------------------------------

    private async Task<int> LockAndStartAsync(ContainerInfo container, OxiaServerProfile profile, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(5);
        var first = Random.Shared.Next(_slots);
        while (true)
        {
            for (var i = 0; i < _slots; i++)
            {
                var slot = (first + i) % _slots;
                lock (_busy)
                {
                    if (_busy[slot]) continue;
                    _busy[slot] = true;
                }

                var started = false;
                try
                {
                    var args = new List<string>
                    {
                        "exec", "-i", container.Name, "sh", "-s", "--",
                        slot.ToString(CultureInfo.InvariantCulture), _owner,
                        (BaseContainerPort + slot).ToString(CultureInfo.InvariantCulture),
                        (BaseMetricsPort + slot).ToString(CultureInfo.InvariantCulture),
                    };
                    args.AddRange(profile.ToFlags());
                    var r = await DockerCli.RunAsync(args, SlotScripts.Acquire, TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
                    if (r.ExitCode == 0 && r.StdOut.StartsWith("OK", StringComparison.Ordinal))
                    {
                        started = true;
                        return slot;
                    }

                    if (r.ExitCode == 3)
                    {
                        await ReclaimIfStaleAsync(container, slot, r.StdOut, ct).ConfigureAwait(false);
                        continue; // held by another test run: try the next slot
                    }

                    throw new DockerException($"could not start a server in slot {slot}: exit {r.ExitCode}: {r.Combined}");
                }
                finally
                {
                    if (!started) lock (_busy) _busy[slot] = false;
                }
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"all {_slots} server slots of container {container.Name} are held by other test runs for over 5 minutes");
            }

            await Task.Delay(250, ct).ConfigureAwait(false);
        }
    }

    /// <summary>If the lock is held by a test process on this machine that no longer exists, removes it.</summary>
    private async Task ReclaimIfStaleAsync(ContainerInfo container, int slot, string lockedOutput, CancellationToken ct)
    {
        // "LOCKED <pid>:<machine>:<starttime ticks>"
        var owner = lockedOutput.Trim();
        if (owner.StartsWith("LOCKED", StringComparison.Ordinal)) owner = owner[6..].Trim();
        var parts = owner.Split(':');
        if (parts.Length != 3 || parts[1] != Environment.MachineName || !int.TryParse(parts[0], out var pid)) return;
        var alive = false;
        try
        {
            using var p = Process.GetProcessById(pid);
            alive = !p.HasExited && p.StartTime.Ticks.ToString(CultureInfo.InvariantCulture) == parts[2];
        }
        catch (ArgumentException)
        {
            // no such process
        }
        catch (InvalidOperationException)
        {
            // exited meanwhile
        }

        if (alive) return;
        Log($"slot {slot}: removing stale lock of dead test process {pid}");
        await DockerCli.RunAsync(["exec", "-i", container.Name, "sh", "-s", "--", slot.ToString(CultureInfo.InvariantCulture)],
            SlotScripts.Release, TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
    }

    private async Task<string?> HealthCheckAsync(ContainerInfo container, int slot)
    {
        var r = await DockerCli.RunAsync(["exec", container.Name, "sh", "-c", $"kill -0 \"$(cat /data/slot{slot}/pid)\""], null, TimeSpan.FromSeconds(20))
            .ConfigureAwait(false);
        return r.Ok ? null : "the oxia process exited";
    }

    internal async Task<string> GetSlotLogAsync(int slot)
    {
        if (_container is not { } c) return "(no container)";
        var r = await DockerCli.RunAsync(["exec", c.Name, "tail", "-n", "40", $"/data/slot{slot}/log"], null, TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        return $"--- last log lines of {c.Name} slot {slot} ---{Environment.NewLine}{AnsiColors.Replace(r.Combined, "")}";
    }

    internal async Task ReleaseSlotAsync(int slot)
    {
        try
        {
            if (_container is { } c)
            {
                var r = await DockerCli.RunAsync(["exec", "-i", c.Name, "sh", "-s", "--", slot.ToString(CultureInfo.InvariantCulture)],
                    SlotScripts.Release, TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                if (!r.Ok) Log($"slot {slot}: release failed: {r.Combined}");
            }
        }
        finally
        {
            lock (_busy) _busy[slot] = false;
            _gate.Release();
        }
    }

    private void Log(string message)
    {
        var line = $"{DateTime.Now:HH:mm:ss.fff} {message}";
        _events.Enqueue(line);
        if (IntegrationEnv.Verbose) Console.Error.WriteLine("[oxia-fixture] " + line);
    }

    /// <summary>Shell scripts executed inside the container with <c>docker exec -i &lt;name&gt; sh -s -- args</c> (busybox ash).</summary>
    private static class SlotScripts
    {
        /// <summary>args: slot owner port metricsPort [oxia standalone flags...]; prints OK, or "LOCKED owner" (exit 3).</summary>
        public const string Acquire = """
            slot="$1"; owner="$2"; port="$3"; mport="$4"; shift 4
            base="/data/slot$slot"; lock="/data/locks/$slot"
            mkdir -p /data/locks
            if ! mkdir "$lock" 2>/dev/null; then
              echo "LOCKED $(cat "$lock/owner" 2>/dev/null)"
              exit 3
            fi
            echo "$owner" > "$lock/owner"
            if [ -f "$base/pid" ]; then kill -9 "$(cat "$base/pid")" 2>/dev/null; fi
            rm -rf "$base"
            mkdir -p "$base"
            nohup sh -c "echo \$\$ > $base/pid; exec oxia standalone -p 0.0.0.0:$port -m 127.0.0.1:$mport --data-dir $base/db --wal-dir $base/wal --wal-sync-data=false --db-cache-size-mb 8 $*" > "$base/log" 2>&1 < /dev/null &
            echo OK

            """;

        /// <summary>args: slot.  Kills the process, wipes the slot directory and drops the lock.</summary>
        public const string Release = """
            slot="$1"; base="/data/slot$slot"; lock="/data/locks/$slot"
            if [ -f "$base/pid" ]; then kill -9 "$(cat "$base/pid")" 2>/dev/null; fi
            rm -rf "$base" "$lock"
            echo OK

            """;
    }
}
