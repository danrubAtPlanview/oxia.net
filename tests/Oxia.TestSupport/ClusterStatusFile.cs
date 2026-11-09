// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : tests/assignments/leader_hint_test.go (and every Go test that starts the in-process coordinator): the test waits with
//              `assert.Eventually(... shard.GetStatusOrDefault() == proto.ShardStatusSteadyState && shard.Leader != nil, 20s, 100ms)` on the
//              coordinator's `StatusSnapshot()` before it sends its first request.  common/proto/metadata.proto (ShardMetadata, ShardStatus,
//              `ensemble_caught_up`), oxiad/coordinator (the `file` metadata provider writes ClusterStatus as YAML).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The real coordinator process of OxiaClusterFixture persists its ClusterStatus to `--file-clusters-status-path`.  This reads
//              that YAML (no YAML dependency: the three facts needed are found by indentation) and answers the Go tests' question "is every
//              shard in steady state with a leader" -- plus `ensembleCaughtUp`, which a Go test never has to wait for because its data
//              servers are initialised before the in-process coordinator starts, whereas here the coordinator and the three `oxia server`
//              processes are started at the same instant and a data server answers "server not initialized yet" (its instance id is not
//              assigned) until the coordinator reached it: a shard can have a leader (a quorum) while the third server is still being
//              added, and the leader can change when that takes long (seen on a loaded native-Linux Docker host: a follower that did not
//              announce the leader for 20 s, a leader hint naming another server than the leader the fixture had returned).
//   Divergences: n/a (new code).
// =====================================================================================================================
using System.Globalization;
using System.Text.RegularExpressions;

namespace Oxia.TestSupport;

/// <summary>Reads the coordinator's persisted cluster status (<c>status.json</c>, YAML despite the name).</summary>
public static class ClusterStatusFile
{
    private const int SteadyState = 1; // proto.ShardStatusSteadyState

    private static readonly Regex Indented = new(@"^(?<indent> *)(?<text>\S.*)$", RegexOptions.Compiled);

    /// <summary>
    /// True when the document describes exactly <paramref name="expectedShards"/> shards and each one is in steady state, has a leader
    /// and has its whole ensemble caught up.  <paramref name="detail"/> says what is missing otherwise (for the timeout message).
    /// </summary>
    public static bool IsSteady(string statusYaml, uint expectedShards, out string detail)
    {
        var shards = new List<(string Id, int Status, bool HasLeader, bool CaughtUp)>();
        var shardsIndent = -1;   // indentation of the "shards:" line, -1 outside a shards section
        var shardIndent = -1;    // indentation of the "<id>:" headers
        var propIndent = -1;     // indentation of the properties of the current shard
        string? id = null;
        int status = 0;
        bool hasLeader = false, caughtUp = false;

        void Flush()
        {
            if (id is not null) shards.Add((id, status, hasLeader, caughtUp));
            id = null;
            propIndent = -1;
            status = 0;
            hasLeader = false;
            caughtUp = false;
        }

        foreach (var raw in statusYaml.Split('\n'))
        {
            var line = raw.TrimEnd('\r');
            var m = Indented.Match(line);
            if (!m.Success) continue;
            var indent = m.Groups["indent"].Length;
            var text = m.Groups["text"].Value;

            if (shardsIndent >= 0 && indent <= shardsIndent)
            {
                Flush(); // left the shards section
                shardsIndent = -1;
                shardIndent = -1;
            }

            if (shardsIndent < 0)
            {
                if (text == "shards:") shardsIndent = indent;
                continue;
            }

            if (shardIndent < 0 && indent > shardsIndent) shardIndent = indent; // the first child fixes the indentation of the shard ids
            if (indent == shardIndent && text.EndsWith(':') && long.TryParse(text.AsSpan(0, text.Length - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                Flush();
                id = text[..^1];
                continue;
            }

            if (id is null) continue;
            if (propIndent < 0) propIndent = indent; // the first child fixes the indentation of the shard's properties
            if (indent != propIndent) continue;      // only the direct properties of the shard
            if (text.StartsWith("status:", StringComparison.Ordinal) && int.TryParse(text.AsSpan(7).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s)) status = s;
            else if (text == "leader:") hasLeader = true;
            else if (text == "ensembleCaughtUp: true") caughtUp = true;
        }

        Flush();

        if (shards.Count != expectedShards)
        {
            detail = $"{shards.Count} shard(s) in the coordinator status, expected {expectedShards}";
            return false;
        }

        foreach (var shard in shards)
        {
            if (shard.Status != SteadyState) { detail = $"shard {shard.Id} has status {shard.Status} (steady state is {SteadyState})"; return false; }
            if (!shard.HasLeader) { detail = $"shard {shard.Id} has no leader"; return false; }
            if (!shard.CaughtUp) { detail = $"shard {shard.Id}: the ensemble is not caught up yet"; return false; }
        }

        detail = $"{shards.Count} shard(s) in steady state";
        return true;
    }
}
