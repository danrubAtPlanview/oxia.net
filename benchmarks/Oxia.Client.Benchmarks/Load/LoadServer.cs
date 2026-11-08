// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Where the load goes.  Three providers behind one type:
//                * docker  -- a FRESH `oxia standalone` in a slot of the reusable oxia-net-tests-* container (Oxia.TestSupport.OxiaServerFixture:
//                             lock safe, shared with the other agents' test runs; the container is never created or removed here other than
//                             through the fixture).  Fresh per run so that every run starts from an empty database.
//                * host:port (or OXIA_TEST_ADDRESS) -- an externally started server, used as is.
//                * echo    -- the in-process, zero-allocation RawEchoServer of the micro-benchmarks, for CPU-bound analysis of the client
//                             (no Docker Desktop loopback latency, no server work).  One shard, every put succeeds, every get finds 100 bytes.
//              For the docker provider the CPU time the container consumed during the measured window is read from its cgroup
//              (`docker exec ... cat /sys/fs/cgroup/cpu.stat`), which covers the server and nothing else of ours.
// =====================================================================================================================
using System.Diagnostics;
using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Benchmarks.Client;
using Oxia.TestSupport;

namespace Oxia.Client.Benchmarks.Load;

internal sealed class LoadServer : IAsyncDisposable
{
    private static OxiaServerFixture? _fixture;

    private readonly OxiaServer? _docker;
    private readonly TestGrpcServer? _echo;
    private readonly string? _container;
    private readonly Process? _child;

    private LoadServer(string address, string description, OxiaServer? docker, TestGrpcServer? echo, string? container, Process? child = null)
    {
        Address = address;
        Description = description;
        _docker = docker;
        _echo = echo;
        _container = container;
        _child = child;
    }

    public string Address { get; }

    public string Description { get; }

    /// <summary>True when the server shares this process (CPU and allocation figures include it).</summary>
    public bool InProcess => _echo is not null;

    public static async Task<LoadServer> StartAsync(LoadOptions options)
    {
        switch (options.Server)
        {
            case "echo":
            {
                var echo = new RawEchoServer();
                var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<RawEchoServer>(), services => services.AddSingleton(echo));
                echo.Address = server.Address;
                return new LoadServer(server.Address, "in-process RawEchoServer (zero allocation)", null, server, null);
            }
            case "echo-proc":
            {
                // The same echo server in a CHILD process: the CPU time and the allocations of this process are the client's alone, and a Go
                // client can be pointed at it too (CPU-bound head to head without a real server).
                var psi = new ProcessStartInfo(Environment.ProcessPath!, [Environment.GetCommandLineArgs()[0], "load", "--serve-echo"])
                {
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardInput = true,
                };
                if (!Path.GetFileNameWithoutExtension(Environment.ProcessPath!).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
                {
                    psi.ArgumentList.Clear();
                    psi.ArgumentList.Add("load");
                    psi.ArgumentList.Add("--serve-echo");
                }
                var child = Process.Start(psi)!;
                string? line = await child.StandardOutput.ReadLineAsync();
                if (line is null || !line.StartsWith("ECHO ", StringComparison.Ordinal)) throw new InvalidOperationException("the echo server did not start: " + line);
                return new LoadServer(line[5..].Trim(), "RawEchoServer in a child process (zero allocation, no real work)", null, null, null, child);
            }
            case "docker":
            {
                _fixture ??= new OxiaServerFixture();
                var profile = OxiaServerProfile.Default with { Shards = (uint)options.Shards, NotificationsEnabled = !options.NoNotifications };
                var docker = await _fixture.StartAsync(profile);
                return new LoadServer(docker.Address,
                    $"oxia standalone in {_fixture.ContainerName} slot {docker.Slot} ({options.Shards} shard(s), notifications {(options.NoNotifications ? "off" : "on")})",
                    docker, null, _fixture.ContainerName);
            }
            default:
                return new LoadServer(options.Server, $"external server {options.Server}", null, null, null);
        }
    }

    /// <summary>CPU seconds the server container consumed so far (all its processes), or null when unknown.</summary>
    public async Task<double?> ReadServerCpuSecondsAsync()
    {
        if (_container is null) return null;
        try
        {
            var psi = new ProcessStartInfo("docker", ["exec", _container, "cat", "/sys/fs/cgroup/cpu.stat"])
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            using var p = Process.Start(psi)!;
            string text = await p.StandardOutput.ReadToEndAsync();
            await p.WaitForExitAsync();
            foreach (var line in text.Split('\n'))
            {
                if (line.StartsWith("usage_usec ", StringComparison.Ordinal))
                {
                    return long.Parse(line.AsSpan(11).Trim(), CultureInfo.InvariantCulture) / 1e6;
                }
            }
        }
        catch (Exception)
        {
            // best effort
        }
        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_docker is not null) await _docker.DisposeAsync();
        if (_echo is not null) await _echo.DisposeAsync();
        if (_child is not null)
        {
            try { _child.Kill(true); } catch (InvalidOperationException) { }
            _child.Dispose();
        }
    }

    /// <summary>`load --serve-echo`: runs the echo server until the parent kills this process; prints "ECHO host:port" once it listens.</summary>
    public static async Task ServeEchoAsync()
    {
        var echo = new RawEchoServer();
        await using var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<RawEchoServer>(), services => services.AddSingleton(echo));
        echo.Address = server.Address;
        Console.Out.WriteLine("ECHO " + server.Address);
        Console.Out.Flush();
        await Task.Delay(Timeout.Infinite);
    }
}
