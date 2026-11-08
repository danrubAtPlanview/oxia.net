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
//              hosts a pool of "slots".  A slot is a port plus a directory on the container's tmpfs; StartAsync(profile)
//              leases a slot and launches `oxia standalone <profile flags>` in it (docker exec), so every test gets a FRESH,
//              EMPTY server with exactly the configuration the Go test sets (shards, key sorting, notifications, retention)
//              -- the same isolation as Go -- without creating a container per test.  Closing the server kills the process
//              and wipes its directory.  Slots are guarded by lock directories inside the container, so several test runs
//              (different worktrees / IDE + CLI) can share the container safely.
//              Because a standalone server answers shard assignments with the :authority the client used, a published
//              ephemeral host port (docker `-p 127.0.0.1::<port>`) is all that is needed: the leader address the client
//              receives is the address it already reaches.  OXIA_TEST_ADDRESS bypasses Docker for an external server.
//   Divergences: see tests/Oxia.Client.IntegrationTests/README.md ("Why one container with slots instead of a container per profile").
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
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
    private const string ScriptVersion = "v1";
    private const int BaseContainerPort = 6648;
    private const int BaseMetricsPort = 7648;
    private static readonly Regex AnsiColors = new("\u001b\\[[0-9;]*m", RegexOptions.Compiled);

    private readonly int _slots = IntegrationEnv.Slots;
    private readonly SemaphoreSlim _gate;
    private readonly bool[] _busy;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentQueue<string> _events = new();
    private readonly string _owner = ProcessOwner.Id;
    private ReusableContainer? _container;
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
        var r = await c.ExecAsync(["oxia", "--version"], null, ct).ConfigureAwait(false);
        return r.Ok ? r.StdOut.Replace("\r", "").Trim() : r.Combined;
    }

    public ValueTask InitializeAsync() => default;

    public async ValueTask DisposeAsync()
    {
        if (_container is { } c && IntegrationEnv.Cleanup)
        {
            Log($"OXIA_TEST_CLEANUP: removing container {c.Name}");
            var r = await c.RemoveAsync().ConfigureAwait(false);
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

        ReusableContainer container;
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

    /// <summary>The container name: <c>oxia-net-tests-&lt;hash of image+settings&gt;[-&lt;OXIA_TEST_INSTANCE&gt;]</c>.</summary>
    public static string ComputeContainerName() => ReusableContainer.ComputeName("tests", HashMaterial());

    private static string HashMaterial() =>
        string.Join('|', ScriptVersion, IntegrationEnv.Image, IntegrationEnv.Slots, IntegrationEnv.Memory, IntegrationEnv.Cpus ?? "-");

    private async Task<ReusableContainer> EnsureContainerAsync(CancellationToken ct)
    {
        if (_container is not null) return _container;
        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _container ??= await ReusableContainer.EnsureAsync("tests", HashMaterial(),
                Enumerable.Range(0, _slots).Select(i => BaseContainerPort + i).ToList(), Log, ct).ConfigureAwait(false);
        }
        finally
        {
            _initLock.Release();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Slots
    // ------------------------------------------------------------------------------------------------------------------

    private async Task<int> LockAndStartAsync(ReusableContainer container, OxiaServerProfile profile, CancellationToken ct)
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
                        slot.ToString(CultureInfo.InvariantCulture), _owner,
                        (BaseContainerPort + slot).ToString(CultureInfo.InvariantCulture),
                        (BaseMetricsPort + slot).ToString(CultureInfo.InvariantCulture),
                    };
                    args.AddRange(profile.ToFlags());
                    var r = await container.ExecScriptAsync(SlotScripts.Acquire, args, TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
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
    private async Task ReclaimIfStaleAsync(ReusableContainer container, int slot, string lockedOutput, CancellationToken ct)
    {
        if (ProcessOwner.IsAlive(lockedOutput)) return;
        Log($"slot {slot}: removing stale lock ({lockedOutput.Trim()})");
        await container.ExecScriptAsync(SlotScripts.Release, [slot.ToString(CultureInfo.InvariantCulture)], TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
    }

    private static async Task<string?> HealthCheckAsync(ReusableContainer container, int slot)
    {
        var r = await container.ExecAsync(["sh", "-c", $"kill -0 \"$(cat /data/slot{slot}/pid)\""], TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        return r.Ok ? null : "the oxia process exited";
    }

    internal async Task<string> GetSlotLogAsync(int slot)
    {
        if (_container is not { } c) return "(no container)";
        var r = await c.ExecAsync(["tail", "-n", "40", $"/data/slot{slot}/log"], TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        return $"--- last log lines of {c.Name} slot {slot} ---{Environment.NewLine}{AnsiColors.Replace(r.Combined, "")}";
    }

    internal async Task ReleaseSlotAsync(int slot)
    {
        try
        {
            if (_container is { } c)
            {
                var r = await c.ExecScriptAsync(SlotScripts.Release, [slot.ToString(CultureInfo.InvariantCulture)], TimeSpan.FromSeconds(30)).ConfigureAwait(false);
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

/// <summary>Identity of this test process, written into the lock directories of the container so stale locks can be detected.</summary>
internal static class ProcessOwner
{
    /// <summary><c>pid:machine:startTicks</c>.</summary>
    public static readonly string Id = $"{Environment.ProcessId}:{Environment.MachineName}:{Process.GetCurrentProcess().StartTime.Ticks}";

    /// <summary>
    /// Whether the owner recorded in a "LOCKED &lt;pid:machine:ticks&gt;" answer may still be running.  Owners on other machines or
    /// with an unparsable id are assumed alive.
    /// </summary>
    public static bool IsAlive(string lockedOutput)
    {
        var owner = lockedOutput.Trim();
        if (owner.StartsWith("LOCKED", StringComparison.Ordinal)) owner = owner[6..].Trim();
        var parts = owner.Split(':');
        if (parts.Length != 3 || parts[1] != Environment.MachineName || !int.TryParse(parts[0], out var pid)) return true;
        try
        {
            using var p = Process.GetProcessById(pid);
            return !p.HasExited && p.StartTime.Ticks.ToString(CultureInfo.InvariantCulture) == parts[2];
        }
        catch (ArgumentException)
        {
            return false; // no such process
        }
        catch (InvalidOperationException)
        {
            return false; // exited meanwhile
        }
    }
}
