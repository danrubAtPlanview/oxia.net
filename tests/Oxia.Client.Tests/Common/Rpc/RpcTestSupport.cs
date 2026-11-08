// =====================================================================================================================
// PORT NOTE
//   Upstream : test helpers of common/rpc/client_pool_test.go (newTestHealthServer, flakyHealthServer, blockingHealthServer,
//              newTestGrpcServerWithoutHealth, newTestConnectionWithHealthConfig) and testify's require.Eventually / require.Never.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : grpc.health.v1 servers hosted by Oxia.TestSupport.TestGrpcServer (Kestrel, h2c, loopback) that answer SERVING /
//              NOT_SERVING, fail the first N checks, never answer, or do not implement the health service at all.
//   Divergences: `grpc.NewServer + net.Listen` -> TestGrpcServer; grpc health.NewServer() -> Grpc.HealthCheck.HealthServiceImpl;
//              t.Cleanup(server.Stop) -> IAsyncDisposable.
// =====================================================================================================================
using Grpc.Core;
using Grpc.Health.V1;
using Grpc.HealthCheck;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using System.Diagnostics;
using System.Threading.Channels;
using Xunit;

namespace Oxia.Client.Tests.Common;

/// <summary>A started test server and the address (host:port) it listens on.</summary>
internal sealed class RunningServer : IAsyncDisposable
{
    public RunningServer(TestGrpcServer server) => Server = server;

    public TestGrpcServer Server { get; }

    public string Target => Server.Address;

    public ValueTask DisposeAsync() => Server.DisposeAsync();
}

/// <summary>Go: flakyHealthServer -- fails the first remainingFailures Check calls and then answers SERVING.</summary>
internal sealed class FlakyHealthService : Health.HealthBase
{
    private int _remainingFailures;

    public FlakyHealthService(int failures) => _remainingFailures = failures;

    public override Task<HealthCheckResponse> Check(HealthCheckRequest request, ServerCallContext context)
    {
        if (Interlocked.Decrement(ref _remainingFailures) >= 0)
        {
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "health check too slow"));
        }

        return Task.FromResult(new HealthCheckResponse { Status = HealthCheckResponse.Types.ServingStatus.Serving });
    }
}

/// <summary>Go: blockingHealthServer -- answers no Check call, and signals each one on Checks when it has room.</summary>
internal sealed class BlockingHealthService : Health.HealthBase
{
    private readonly Channel<bool> _checks = Channel.CreateBounded<bool>(1);

    public ChannelReader<bool> Checks => _checks.Reader;

    public override async Task<HealthCheckResponse> Check(HealthCheckRequest request, ServerCallContext context)
    {
        _checks.Writer.TryWrite(true);
        await Task.Delay(Timeout.Infinite, context.CancellationToken).ConfigureAwait(false);
        throw new InvalidOperationException("unreachable");
    }
}

internal static class RpcTestSupport
{
    /// <summary>Go: newTestHealthServer -- a health server that answers <paramref name="status"/> for the "" service, ready to be used.</summary>
    public static async Task<(RunningServer Server, HealthServiceImpl Health)> NewTestHealthServerAsync(HealthCheckResponse.Types.ServingStatus status)
    {
        var health = new HealthServiceImpl();
        health.SetStatus(string.Empty, status);
        var server = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<HealthServiceImpl>(), services => services.AddSingleton(health));
        var running = new RunningServer(server);

        // wait for the server to answer (Go: require.Eventually on a throw-away client)
        using var channel = Grpc.Net.Client.GrpcChannel.ForAddress("http://" + running.Target);
        var client = new Health.HealthClient(channel);
        await Eventually(async () =>
        {
            try
            {
                await client.CheckAsync(new HealthCheckRequest(), deadline: DateTime.UtcNow.AddMilliseconds(100));
                return true;
            }
            catch (RpcException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(10));

        return (running, health);
    }

    public static async Task<RunningServer> NewTestFlakyHealthServerAsync(int failures)
    {
        var service = new FlakyHealthService(failures);
        return new RunningServer(await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<FlakyHealthService>(), services => services.AddSingleton(service)));
    }

    public static async Task<(RunningServer Server, BlockingHealthService Service)> NewTestBlockingHealthServerAsync()
    {
        var service = new BlockingHealthService();
        var server = new RunningServer(await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<BlockingHealthService>(), services => services.AddSingleton(service)));
        return (server, service);
    }

    public static async Task<RunningServer> NewTestGrpcServerWithoutHealthAsync()
        => new(await TestGrpcServer.StartAsync(static _ => { }));

    /// <summary>Go: newTestConnectionWithHealthConfig.</summary>
    internal static Connection NewTestConnectionWithHealthConfig(ClientPool pool, string target, TimeSpan healthInterval, TimeSpan healthPingTimeout)
        => Connection.CreateWithHealthConfig(target, pool.Tls, pool.Authentication, pool.DialOptions, pool.OnConnectionFailure,
            healthInterval, healthPingTimeout);

    /// <summary>testify's <c>require.Eventually(condition, waitFor, tick)</c>.</summary>
    public static async Task Eventually(Func<bool> condition, TimeSpan waitFor, TimeSpan tick)
        => await Eventually(() => Task.FromResult(condition()), waitFor, tick);

    public static async Task Eventually(Func<Task<bool>> condition, TimeSpan waitFor, TimeSpan tick)
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            if (await condition()) return;
            Assert.True(sw.Elapsed < waitFor, $"Condition never satisfied within {waitFor}");
            await Task.Delay(tick);
        }
    }

    /// <summary>testify's <c>require.Never(condition, waitFor, tick)</c>.</summary>
    public static async Task Never(Func<bool> condition, TimeSpan waitFor, TimeSpan tick)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < waitFor)
        {
            Assert.False(condition(), "Condition satisfied");
            await Task.Delay(tick);
        }
    }
}
