// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : tests/mock/server.go (NewServer), tests/mock/coordinator.go + oxiad/coordinator runtime (the in-process coordinator
//              the Go tests/assignments and tests/resolver tests start with `memory.NewProvider(...)`, `coordruntime.New(...)`),
//              cmd/server/cmd.go and cmd/coordinator/cmd.go (the real processes), common/proto/metadata.proto (ClusterConfiguration).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Opt-in (OXIA_TEST_CLUSTER=1) replacement for the Go "mock cluster": a SECOND reusable container
//              (oxia-net-cluster-<hash>, same lifecycle as the standalone one) in which a "cluster slot" runs one `oxia coordinator`
//              plus N `oxia server` data servers (replication factor / shards / extra authorities as configured), all started with
//              `docker exec`.  The servers advertise to the coordinator -- hence to clients as shard leaders -- their PUBLISHED host
//              address (127.0.0.1:<ephemeral host port>), while replication and coordinator traffic stay on in-container loopback
//              ports.  Servers can be killed individually or all together (Go: server.Close()).
//   Divergences: The Go tests read the leader from the coordinator's in-process metadata (`mock.StatusSnapshot`); here it is read
//              from the shard assignments any live data server streams (the same `Leader` the clients get).  The coordinator uses
//              the `file` metadata provider on the container tmpfs instead of the in-memory one.
//              Wave 2 (T2, security tests): StartAsync takes an optional OxiaClusterProfile (extra `oxia server` / `oxia coordinator` flags
//              and files written to /data/cluster<slot>/files, e.g. TLS certificates and OIDC key files); a cluster started with a profile is
//              NOT opt-in (OXIA_TEST_CLUSTER), because `oxia standalone` has no TLS / auth flags and this is the only way to test them.
//              GetHostAddressAsync tells how the container reaches the test host (for an OIDC provider hosted by the test process).
//              Linux validation: the servers' public addresses are `{OXIA_TEST_HOST}:{published port}` (default 127.0.0.1) and
//              OXIA_TEST_CONTAINER_HOST overrides / forces ("gateway") the way GetHostAddressAsync reaches the test host -- docs/linux-validation.md.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Oxia.TestSupport.Docker;
using Xunit;

namespace Oxia.TestSupport;

/// <summary>A data server of an <see cref="OxiaCluster"/> (Go: <c>*proto.DataServerIdentity</c>).</summary>
public sealed record ClusterServer(string Name, int Index, string Public, string Internal);

/// <summary>A coordinator + data servers running in a cluster slot of the cluster container.</summary>
public sealed class OxiaCluster : IAsyncDisposable
{
    private readonly OxiaClusterFixture _owner;
    private readonly int _slot;
    private int _closed;

    internal OxiaCluster(OxiaClusterFixture owner, int slot, string name, string? authority, IReadOnlyList<ClusterServer> servers, uint shards,
        RawSecurity? probe = null)
    {
        _owner = owner;
        _slot = slot;
        Name = name;
        Authority = authority;
        Servers = servers;
        InitialShardCount = shards;
        Probe = probe;
    }

    /// <summary>The TLS / credentials the raw gRPC calls of the test infrastructure use against this cluster (null = plaintext, no auth).</summary>
    public RawSecurity? Probe { get; }

    public string Name { get; }

    /// <summary>The extra authority the cluster accepts (Go: <c>AllowExtraAuthorities</c>), if any.</summary>
    public string? Authority { get; }

    public IReadOnlyList<ClusterServer> Servers { get; }

    public uint InitialShardCount { get; }

    /// <summary>
    /// The leader of the shard as announced by the live data servers (Go: <c>metadata.GetShardStatus(ns, shard).Leader</c>);
    /// null when none answers or the shard has no leader yet.
    /// </summary>
    public async Task<ClusterServer?> GetLeaderAsync(long shard = 0, CancellationToken ct = default)
    {
        foreach (var server in Servers)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(3));
                var (shards, _) = await RawOxia.GetAssignmentsAsync(server.Public, ct: cts.Token, security: Probe).ConfigureAwait(false);
                var leader = shards.FirstOrDefault(s => s.Shard == shard).Leader;
                if (!string.IsNullOrEmpty(leader)) return Servers.FirstOrDefault(s => s.Public == leader);
            }
            catch (Exception e) when (e is Grpc.Core.RpcException or HttpRequestException or OperationCanceledException or InvalidOperationException)
            {
                // this server is down or not initialised: ask the next one
            }
        }

        return null;
    }

    /// <summary>Go: <c>cluster.bootstrapTargetExcluding(excluded)</c>: a server that is not <paramref name="excluded"/>.</summary>
    public ClusterServer BootstrapTargetExcluding(ClusterServer? excluded)
    {
        foreach (var server in Servers)
        {
            if (excluded is null || server.Name != excluded.Name) return server;
        }

        throw new InvalidOperationException("no bootstrap target available");
    }

    /// <summary>Go: <c>server.Close()</c> on every data server (the coordinator keeps running).</summary>
    public Task StopAllServersAsync() => _owner.StopServersAsync(_slot);

    /// <summary>Last log lines of the coordinator and the servers.</summary>
    public Task<string> GetLogsAsync() => _owner.GetClusterLogsAsync(_slot);

    /// <summary>Stops everything and frees the cluster slot.  Idempotent.</summary>
    public async Task CloseAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;
        await _owner.ReleaseClusterAsync(_slot).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try { await CloseAsync().ConfigureAwait(false); }
        catch { /* dispose must not throw */ }
    }
}

/// <summary>
/// Opt-in provider of Oxia clusters (coordinator + data servers); an xUnit v3 assembly fixture that does nothing until
/// <see cref="StartAsync"/> is called.  Requires Docker and OXIA_TEST_CLUSTER=1.
/// </summary>
public sealed class OxiaClusterFixture : IAsyncLifetime
{
    private const string ScriptVersion = "v1";
    private const int MaxServers = 4;
    private static readonly Regex AnsiColors = new("\u001b\\[[0-9;]*m", RegexOptions.Compiled);

    private readonly int _clusters = IntegrationEnv.ClusterSlots;
    private readonly SemaphoreSlim _gate;
    private readonly bool[] _busy;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentQueue<string> _events = new();
    private ReusableContainer? _container;

    public OxiaClusterFixture()
    {
        _gate = new SemaphoreSlim(_clusters, _clusters);
        _busy = new bool[_clusters];
    }

    /// <summary>True when the cluster based tests are enabled (OXIA_TEST_CLUSTER=1) and Docker is the server provider.</summary>
    public static bool Enabled => IntegrationEnv.ClusterEnabled && IntegrationEnv.ExternalAddress is null;

    public IReadOnlyCollection<string> Events => _events;

    public string? ContainerName => _container?.Name;

    public ValueTask InitializeAsync() => default;

    public async ValueTask DisposeAsync()
    {
        if (_container is { } c && IntegrationEnv.Cleanup)
        {
            Log($"OXIA_TEST_CLEANUP: removing container {c.Name}");
            await c.RemoveAsync().ConfigureAwait(false);
        }
    }

    private static int PublicPort(int cluster, int server) => 7000 + cluster * MaxServers + server;
    private static int InternalPort(int cluster, int server) => 7100 + cluster * MaxServers + server;
    private static int MetricsPort(int cluster, int server) => 7200 + cluster * MaxServers + server;
    private static int CoordInternalPort(int cluster) => 7300 + cluster;
    private static int CoordPublicPort(int cluster) => 7400 + cluster;
    private static int CoordMetricsPort(int cluster) => 7500 + cluster;

    private string HashMaterial() => string.Join('|', ScriptVersion, IntegrationEnv.Image, _clusters, MaxServers, IntegrationEnv.Memory);

    private async Task<ReusableContainer> EnsureContainerAsync(CancellationToken ct)
    {
        if (_container is not null) return _container;
        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var ports = new List<int>();
            for (var c = 0; c < _clusters; c++)
            {
                for (var s = 0; s < MaxServers; s++) ports.Add(PublicPort(c, s));
            }

            return _container ??= await ReusableContainer.EnsureAsync("cluster", HashMaterial(), ports, Log, ct).ConfigureAwait(false);
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Starts a cluster of <paramref name="serverCount"/> data servers named <c>{name}-s1..</c> with a coordinator that creates the
    /// <c>default</c> namespace (Go: <c>ClusterConfiguration{Namespaces: [{default, replicationFactor, initialShardCount}], Servers,
    /// AllowExtraAuthorities}</c>) and waits until every shard has a leader.
    /// </summary>
    public async Task<OxiaCluster> StartAsync(string name, int serverCount, int replicationFactor, uint initialShardCount,
        string? extraAuthority = null, CancellationToken ct = default, OxiaClusterProfile? profile = null)
    {
        if (serverCount is < 1 or > MaxServers) throw new ArgumentOutOfRangeException(nameof(serverCount));
        // the security tests (profile != null) are the only way to run TLS / OIDC against a real server (`oxia standalone` has no such
        // flags), so they are not opt-in
        if (profile is null && !IntegrationEnv.ClusterEnabled) Assert.Skip($"cluster tests are opt-in: set {IntegrationEnv.ClusterVar}=1");
        if (IntegrationEnv.ExternalAddress is not null) Assert.Skip($"cluster tests need the Docker fixture (unset {IntegrationEnv.AddressVar})");

        ReusableContainer container;
        try
        {
            container = await EnsureContainerAsync(ct).ConfigureAwait(false);
        }
        catch (DockerException e) when (IntegrationEnv.SkipIfNoServer)
        {
            Assert.Skip($"Docker not available ({e.Message})");
            throw;
        }

        var sw = Stopwatch.StartNew();
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        var slot = -1;
        try
        {
            slot = await LockAndStartAsync(container, name, serverCount, replicationFactor, initialShardCount, extraAuthority, ct, profile).ConfigureAwait(false);
            var servers = Enumerable.Range(0, serverCount)
                .Select(i => new ClusterServer($"{name}-s{i + 1}", i, $"{IntegrationEnv.Host}:{container.HostPorts[PublicPort(slot, i)]}", $"127.0.0.1:{InternalPort(slot, i)}"))
                .ToList();
            var cluster = new OxiaCluster(this, slot, name, extraAuthority, servers, initialShardCount, profile?.Probe);
            var healthSlot = slot;
            await OxiaReadiness.WaitUntilReadyAsync(servers[0].Public, initialShardCount, TimeSpan.FromSeconds(90), "default",
                () => HealthCheckAsync(container, healthSlot, serverCount), () => GetClusterLogsAsync(healthSlot), ct, profile?.Probe).ConfigureAwait(false);
            // the first assignments can come before every shard has an elected leader: wait for shard 0's leader like the Go tests
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
            while (await cluster.GetLeaderAsync(0, ct).ConfigureAwait(false) is null)
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException("cluster has no leader for shard 0 after 60 s" + Environment.NewLine + await GetClusterLogsAsync(slot).ConfigureAwait(false));
                await Task.Delay(100, ct).ConfigureAwait(false);
            }

            Log($"cluster '{name}' ({serverCount} servers, rf={replicationFactor}, shards={initialShardCount}) ready in cluster slot {slot} in {sw.ElapsedMilliseconds} ms");
            slot = -1; // ownership moved to the OxiaCluster
            return cluster;
        }
        catch
        {
            if (slot >= 0) await ReleaseClusterAsync(slot).ConfigureAwait(false);
            else _gate.Release();
            throw;
        }
    }

    private async Task<int> LockAndStartAsync(ReusableContainer container, string name, int serverCount, int replicationFactor, uint shards,
        string? extraAuthority, CancellationToken ct, OxiaClusterProfile? profile = null)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(5);
        var first = Random.Shared.Next(_clusters);
        while (true)
        {
            for (var i = 0; i < _clusters; i++)
            {
                var slot = (first + i) % _clusters;
                lock (_busy)
                {
                    if (_busy[slot]) continue;
                    _busy[slot] = true;
                }

                var started = false;
                try
                {
                    var script = BuildAcquireScript(container, slot, name, serverCount, replicationFactor, shards, extraAuthority, profile);
                    var r = await container.ExecScriptAsync(script, [], TimeSpan.FromSeconds(90), ct).ConfigureAwait(false);
                    if (r.ExitCode == 0 && r.StdOut.StartsWith("OK", StringComparison.Ordinal))
                    {
                        started = true;
                        return slot;
                    }

                    if (r.ExitCode == 3)
                    {
                        if (!ProcessOwner.IsAlive(r.StdOut))
                        {
                            Log($"cluster slot {slot}: removing stale lock ({r.StdOut.Trim()})");
                            await container.ExecScriptAsync(BuildReleaseScript(slot), [], TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                        }

                        continue;
                    }

                    throw new DockerException($"could not start cluster '{name}' in slot {slot}: exit {r.ExitCode}: {r.Combined}");
                }
                finally
                {
                    if (!started) lock (_busy) _busy[slot] = false;
                }
            }

            if (DateTime.UtcNow > deadline) throw new TimeoutException($"all {_clusters} cluster slots of {container.Name} are held by other test runs");
            await Task.Delay(250, ct).ConfigureAwait(false);
        }
    }

    private static string BuildAcquireScript(ReusableContainer container, int c, string name, int serverCount, int rf, uint shards, string? extraAuthority,
        OxiaClusterProfile? profile = null)
    {
        var yaml = new StringBuilder();
        yaml.Append("namespaces:\n  - name: default\n");
        yaml.Append(CultureInfo.InvariantCulture, $"    replicationFactor: {rf}\n    initialShardCount: {shards}\n");
        yaml.Append("servers:\n");
        for (var s = 0; s < serverCount; s++)
        {
            yaml.Append(CultureInfo.InvariantCulture, $"  - name: {name}-s{s + 1}\n    public: {IntegrationEnv.Host}:{container.HostPorts[PublicPort(c, s)]}\n    internal: 127.0.0.1:{InternalPort(c, s)}\n");
        }

        if (extraAuthority is not null) yaml.Append(CultureInfo.InvariantCulture, $"allowExtraAuthorities:\n  - {extraAuthority}\n");

        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture, $"base=/data/cluster{c}\nlock=/data/locks/cluster{c}\n");
        sb.Append("mkdir -p /data/locks\n");
        sb.Append("if ! mkdir \"$lock\" 2>/dev/null; then echo \"LOCKED $(cat \"$lock/owner\" 2>/dev/null)\"; exit 3; fi\n");
        sb.Append(CultureInfo.InvariantCulture, $"echo '{ProcessOwner.Id}' > \"$lock/owner\"\n");
        sb.Append("for f in \"$base\"/*/pid; do if [ -f \"$f\" ]; then kill -9 \"$(cat \"$f\")\" 2>/dev/null; fi; done\n");
        sb.Append("rm -rf \"$base\"; mkdir -p \"$base/coord\"\n");
        if (profile is not null) AppendProfileFiles(sb, c, profile);
        for (var s = 0; s < serverCount; s++)
        {
            sb.Append(CultureInfo.InvariantCulture, $"mkdir -p \"$base/s{s}\"\n");
            if (profile is null)
            {
                sb.Append(CultureInfo.InvariantCulture,
                    $"nohup sh -c \"echo \\$\\$ > $base/s{s}/pid; exec oxia server -p 0.0.0.0:{PublicPort(c, s)} -i 127.0.0.1:{InternalPort(c, s)} -m 127.0.0.1:{MetricsPort(c, s)} " +
                    $"--data-dir $base/s{s}/db --wal-dir $base/s{s}/wal --wal-sync-data=false --db-cache-size-mb 8\" > \"$base/s{s}/log\" 2>&1 < /dev/null &\n");
            }
            else
            {
                AppendProfiledProcess(sb, c, $"s{s}",
                    $"oxia server -p 0.0.0.0:{PublicPort(c, s)} -i 127.0.0.1:{InternalPort(c, s)} -m 127.0.0.1:{MetricsPort(c, s)} " +
                    $"--data-dir /data/cluster{c}/s{s}/db --wal-dir /data/cluster{c}/s{s}/wal --wal-sync-data=false --db-cache-size-mb 8",
                    profile.ServerArgs);
            }
        }

        sb.Append("cat > \"$base/coord/cluster.yaml\" <<'OXIA_CLUSTER_YAML'\n").Append(yaml).Append("OXIA_CLUSTER_YAML\n");
        if (profile is null)
        {
            sb.Append(CultureInfo.InvariantCulture,
                $"nohup sh -c \"echo \\$\\$ > $base/coord/pid; exec oxia coordinator -i 127.0.0.1:{CoordInternalPort(c)} -p 127.0.0.1:{CoordPublicPort(c)} -m 127.0.0.1:{CoordMetricsPort(c)} ");
            sb.Append("--metadata file --file-clusters-status-path $base/coord/status.json --cconfig $base/coord/cluster.yaml\" > \"$base/coord/log\" 2>&1 < /dev/null &\n");
        }
        else
        {
            AppendProfiledProcess(sb, c, "coord",
                $"oxia coordinator -i 127.0.0.1:{CoordInternalPort(c)} -p 127.0.0.1:{CoordPublicPort(c)} -m 127.0.0.1:{CoordMetricsPort(c)} " +
                $"--metadata file --file-clusters-status-path /data/cluster{c}/coord/status.json --cconfig /data/cluster{c}/coord/cluster.yaml",
                profile.CoordinatorArgs);
        }

        sb.Append("echo OK\n");
        return sb.ToString();
    }

    // ---- profile support (security tests): extra files in /data/cluster<c>/files and extra flags, via a generated run.sh ----

    private static string FilesDir(int c) => $"/data/cluster{c}/files";

    /// <summary>Writes the profile's files (certificates, keys, ...) into the slot's tmpfs directory with quoted here-documents.</summary>
    private static void AppendProfileFiles(StringBuilder sb, int c, OxiaClusterProfile profile)
    {
        if (profile.Files.Count == 0) return;
        sb.Append(CultureInfo.InvariantCulture, $"mkdir -p {FilesDir(c)}\n");
        var index = 0;
        foreach (var (fileName, content) in profile.Files)
        {
            if (fileName.Length == 0 || !fileName.All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '.' or '-' or '_'))
            {
                throw new ArgumentException($"invalid profile file name '{fileName}'", nameof(profile));
            }

            var terminator = $"OXIA_FILE_{index++}";
            if (content.Contains(terminator, StringComparison.Ordinal)) throw new ArgumentException($"profile file '{fileName}' contains '{terminator}'", nameof(profile));
            sb.Append(CultureInfo.InvariantCulture, $"cat > {FilesDir(c)}/{fileName} <<'{terminator}'\n").Append(content.Replace("\r\n", "\n", StringComparison.Ordinal));
            if (!content.EndsWith('\n')) sb.Append('\n');
            sb.Append(terminator).Append('\n');
        }

        sb.Append(CultureInfo.InvariantCulture, $"chmod 600 {FilesDir(c)}/*\n");
    }

    /// <summary>
    /// Starts <c>exec &lt;command&gt; &lt;args...&gt;</c> in the background through a generated <c>run.sh</c> (so that an argument can hold
    /// JSON with quotes): the script records its pid (as the plain variant does: the exec'd process keeps it) and the output goes to <c>log</c>.
    /// </summary>
    private static void AppendProfiledProcess(StringBuilder sb, int c, string dir, string command, IReadOnlyList<string> extraArgs)
    {
        var run = $"/data/cluster{c}/{dir}/run.sh";
        sb.Append(CultureInfo.InvariantCulture, $"cat > {run} <<'OXIA_RUN'\n");
        sb.Append(CultureInfo.InvariantCulture, $"echo $$ > /data/cluster{c}/{dir}/pid\nexec {command}");
        foreach (var arg in extraArgs)
        {
            sb.Append(' ').Append(ShellQuote(arg.Replace(OxiaClusterProfile.DirToken, FilesDir(c), StringComparison.Ordinal)));
        }

        sb.Append("\nOXIA_RUN\n");
        sb.Append(CultureInfo.InvariantCulture, $"nohup sh {run} > /data/cluster{c}/{dir}/log 2>&1 < /dev/null &\n");
    }

    private static string ShellQuote(string value) => "'" + value.Replace("'", "'\\''", StringComparison.Ordinal) + "'";

    /// <summary>
    /// The name under which processes INSIDE the cluster container reach a service listening on the machine running the tests:
    /// <c>host.docker.internal</c> when it resolves (Docker Desktop, or a container created with <c>--add-host</c>), otherwise the
    /// container's default gateway (the docker0 bridge on Linux; the service must then listen on all interfaces).
    /// </summary>
    public async Task<string> GetHostAddressAsync(CancellationToken ct = default)
    {
        // OXIA_TEST_CONTAINER_HOST: an explicit address, or "gateway" to skip the host.docker.internal lookup (see IntegrationEnv.ContainerHost)
        if (IntegrationEnv.ContainerHost is { } configured && !configured.Equals("gateway", StringComparison.OrdinalIgnoreCase)) return configured;

        var container = await EnsureContainerAsync(ct).ConfigureAwait(false);
        var lookup = IntegrationEnv.ContainerHost is null
            ? "if getent hosts host.docker.internal >/dev/null 2>&1; then echo host.docker.internal; else ip route | awk '/^default/ {print $3; exit}'; fi"
            : "ip route | awk '/^default/ {print $3; exit}'";
        var r = await container.ExecAsync(["sh", "-c", lookup], TimeSpan.FromSeconds(20), ct).ConfigureAwait(false);
        var host = r.StdOut.Trim();
        if (!r.Ok || host.Length == 0) throw new DockerException("could not determine how the cluster container reaches the host: " + r.Combined);
        return host;
    }

    private static string BuildReleaseScript(int c) =>
        $"base=/data/cluster{c}\nlock=/data/locks/cluster{c}\n" +
        "for f in \"$base\"/*/pid; do if [ -f \"$f\" ]; then kill -9 \"$(cat \"$f\")\" 2>/dev/null; fi; done\n" +
        "rm -rf \"$base\" \"$lock\"\necho OK\n";

    private static string BuildStopServersScript(int c) =>
        $"base=/data/cluster{c}\n" +
        "for f in \"$base\"/s*/pid; do if [ -f \"$f\" ]; then kill -9 \"$(cat \"$f\")\" 2>/dev/null; fi; done\necho OK\n";

    private static async Task<string?> HealthCheckAsync(ReusableContainer container, int c, int serverCount)
    {
        var checks = string.Join("; ", Enumerable.Range(0, serverCount).Select(s => $"kill -0 \"$(cat /data/cluster{c}/s{s}/pid)\" || exit 1")) +
                     $"; kill -0 \"$(cat /data/cluster{c}/coord/pid)\"";
        var r = await container.ExecAsync(["sh", "-c", checks], TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        return r.Ok ? null : "an oxia process of the cluster exited";
    }

    internal async Task StopServersAsync(int c)
    {
        if (_container is not { } container) return;
        var r = await container.ExecScriptAsync(BuildStopServersScript(c), [], TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        if (!r.Ok) throw new DockerException("could not stop the cluster servers: " + r.Combined);
    }

    internal async Task<string> GetClusterLogsAsync(int c)
    {
        if (_container is not { } container) return "(no container)";
        var r = await container.ExecAsync(["sh", "-c",
            $"for f in /data/cluster{c}/coord/log /data/cluster{c}/s*/log; do echo \"--- $f\"; tail -n 25 \"$f\"; done"], TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        return AnsiColors.Replace(r.Combined, "");
    }

    internal async Task ReleaseClusterAsync(int c)
    {
        try
        {
            if (_container is { } container)
            {
                var r = await container.ExecScriptAsync(BuildReleaseScript(c), [], TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                if (!r.Ok) Log($"cluster slot {c}: release failed: {r.Combined}");
            }
        }
        finally
        {
            lock (_busy) _busy[c] = false;
            _gate.Release();
        }
    }

    private void Log(string message)
    {
        var line = $"{DateTime.Now:HH:mm:ss.fff} {message}";
        _events.Enqueue(line);
        if (IntegrationEnv.Verbose) Console.Error.WriteLine("[oxia-cluster] " + line);
    }
}
