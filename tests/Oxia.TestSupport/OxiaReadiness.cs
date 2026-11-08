// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (Go tests call `dataserver.NewStandalone`, which returns only once the shards have a leader).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Readiness probe for a freshly started `oxia` process: dials GetShardAssignments with the protoc-generated
//              OxiaClientClient (raw gRPC, NO dependency on the client under test) until the server answers with an
//              assignment for every expected shard and each shard has a leader address.
//   Divergences: n/a (new code; wave 2 added the optional RawSecurity argument: TLS / bearer token for secured servers).
// =====================================================================================================================
using Grpc.Core;
using Grpc.Net.Client;
using G = Io.Oxia.Proto.V1;

namespace Oxia.TestSupport;

public static class OxiaReadiness
{
    /// <summary>Whether <paramref name="address"/> (host:port) answers GetShardAssignments with <paramref name="expectedShards"/> led shards.</summary>
    public static async Task<(bool Ready, string Detail)> ProbeOnceAsync(string address, uint expectedShards, string @namespace = "default",
        TimeSpan? callTimeout = null, CancellationToken ct = default, RawSecurity? security = null)
    {
        try
        {
            using var channel = RawSecurity.CreateChannel(address, security);
            var client = new G.OxiaClient.OxiaClientClient(channel);
            using var call = client.GetShardAssignments(new G.ShardAssignmentsRequest { Namespace = @namespace },
                deadline: DateTime.UtcNow + (callTimeout ?? TimeSpan.FromSeconds(3)), cancellationToken: ct);
            if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
            {
                return (false, "the assignments stream ended without a message");
            }

            var assignments = call.ResponseStream.Current;
            if (!assignments.Namespaces.TryGetValue(@namespace, out var ns))
            {
                return (false, $"no assignments for namespace '{@namespace}' yet");
            }

            if (ns.Assignments.Count != expectedShards)
            {
                return (false, $"{ns.Assignments.Count} shard assignments, expected {expectedShards}");
            }

            foreach (var a in ns.Assignments)
            {
                if (string.IsNullOrEmpty(a.Leader)) return (false, $"shard {a.Shard} has no leader yet");
            }

            return (true, $"{ns.Assignments.Count} shard(s) led by {ns.Assignments[0].Leader}");
        }
        catch (RpcException e)
        {
            return (false, $"{e.StatusCode}: {e.Status.Detail}");
        }
        catch (Exception e) when (e is HttpRequestException or IOException or InvalidOperationException)
        {
            return (false, e.GetType().Name + ": " + e.Message);
        }
    }

    /// <summary>
    /// Polls <see cref="ProbeOnceAsync"/> every 50 ms until ready or <paramref name="timeout"/> elapses.
    /// <paramref name="healthCheck"/> (about once a second) returns a failure message when the process is known to be dead,
    /// so that a server that crashed at start-up fails fast; <paramref name="diagnostics"/> adds context (e.g. the server log)
    /// to the exception.
    /// </summary>
    public static async Task WaitUntilReadyAsync(string address, uint expectedShards, TimeSpan timeout, string @namespace = "default",
        Func<Task<string?>>? healthCheck = null, Func<Task<string>>? diagnostics = null, CancellationToken ct = default, RawSecurity? security = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        var nextHealthCheck = DateTime.UtcNow + TimeSpan.FromSeconds(1);
        string last = "not probed";
        while (true)
        {
            var (ready, detail) = await ProbeOnceAsync(address, expectedShards, @namespace, null, ct, security).ConfigureAwait(false);
            if (ready) return;
            last = detail;

            string? failure = null;
            if (healthCheck is not null && DateTime.UtcNow >= nextHealthCheck)
            {
                failure = await healthCheck().ConfigureAwait(false);
                nextHealthCheck = DateTime.UtcNow + TimeSpan.FromSeconds(1);
            }

            if (failure is not null || DateTime.UtcNow >= deadline)
            {
                var extra = diagnostics is null ? "" : Environment.NewLine + await diagnostics().ConfigureAwait(false);
                throw new TimeoutException(failure is not null
                    ? $"Oxia server at {address} died during start-up: {failure}{extra}"
                    : $"Oxia server at {address} was not ready after {timeout.TotalSeconds:0} s: {last}{extra}");
            }

            await Task.Delay(50, ct).ConfigureAwait(false);
        }
    }
}
