// =====================================================================================================================
// PORT NOTE
//   Upstream : none -- extra tests (DESIGN.md "Test porting protocol" 6) of tests/Oxia.TestSupport/ClusterStatusFile.cs, which turns the
//              coordinator's persisted ClusterStatus into the Go precondition `shard.Status == SteadyState && shard.Leader != nil`.
//              The documents are copies of what the real `oxia coordinator` (file metadata provider) wrote in the cluster container.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pure parsing tests (no Docker): steady / election / not caught up / no leader / wrong shard count / CRLF.
//   Divergences: n/a.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.IntegrationTests.Infrastructure;

public class ClusterStatusFileTests
{
    private const string ThreeShards = """
        namespaces:
            default:
                replicationFactor: 1
                shards:
                    0:
                        status: 1
                        term: 0
                        leader:
                            name: first-s1
                            public: 127.0.0.1:32776
                            internal: 127.0.0.1:7100
                        ensemble:
                            - name: first-s1
                              public: 127.0.0.1:32776
                              internal: 127.0.0.1:7100
                        int32HashRange:
                            min: 0
                            max: 1431655765
                        ensembleCaughtUp: true
                    1:
                        status: 1
                        term: 1
                        leader:
                            name: first-s2
                            public: 127.0.0.1:32777
                            internal: 127.0.0.1:7101
                        ensemble:
                            - name: first-s2
                              public: 127.0.0.1:32777
                              internal: 127.0.0.1:7101
                        int32HashRange:
                            min: 1431655766
                            max: 2863311531
                        ensembleCaughtUp: true
                    2:
                        status: 1
                        term: 1
                        leader:
                            name: first-s1
                            public: 127.0.0.1:32776
                            internal: 127.0.0.1:7100
                        ensemble:
                            - name: first-s1
                              public: 127.0.0.1:32776
                              internal: 127.0.0.1:7100
                        int32HashRange:
                            min: 2863311532
                            max: 4294967295
                        ensembleCaughtUp: true
        shardIdGenerator: 3
        instanceId: f711cec3-3ed9-47c1-837d-5191a7784e8f
        dataServers:
            first-s1:
                state: 1
                metadata: {}
            first-s2:
                state: 1
                metadata: {}
        """;

    [Fact]
    public void AllShardsSteadyWithLeaderAndCaughtUp_IsSteady()
    {
        Assert.True(ClusterStatusFile.IsSteady(ThreeShards, 3, out var detail), detail);
        Assert.Equal("3 shard(s) in steady state", detail);
    }

    [Fact]
    public void CrLfLineEndings_AreAccepted()
    {
        Assert.True(ClusterStatusFile.IsSteady(ThreeShards.Replace("\n", "\r\n"), 3, out var detail), detail);
    }

    [Fact]
    public void AShardInElection_IsNotSteady()
    {
        // what the coordinator wrote while shard 1 was being (re-)elected: status 2, no leader, no caught-up flag
        var doc = ThreeShards.Replace("""
                    1:
                        status: 1
                        term: 1
                        leader:
                            name: first-s2
                            public: 127.0.0.1:32777
                            internal: 127.0.0.1:7101
        """.ReplaceLineEndings("\n"), "            1:\n                status: 2\n                term: 1");
        Assert.NotEqual(ThreeShards, doc);
        Assert.False(ClusterStatusFile.IsSteady(doc, 3, out var detail));
        Assert.Contains("shard 1", detail);
    }

    [Fact]
    public void ALeaderWhoseEnsembleIsNotCaughtUp_IsNotSteady()
    {
        // a quorum elected a leader but the third server has not been reached yet ("server not initialized yet"): the shard is in steady state
        // with a leader and the Go test would go on, yet the leader can still change
        var doc = ThreeShards.Replace("ensembleCaughtUp: true", "ensembleCaughtUp: false");
        Assert.False(ClusterStatusFile.IsSteady(doc, 3, out var detail));
        Assert.Contains("not caught up", detail);

        // omitted (omitempty) is false too
        var omitted = string.Join('\n', ThreeShards.Split('\n').Where(l => !l.Contains("ensembleCaughtUp")));
        Assert.False(ClusterStatusFile.IsSteady(omitted, 3, out _));
    }

    [Fact]
    public void AShardWithoutALeader_IsNotSteady()
    {
        var doc = ThreeShards.Replace("""
                        leader:
                            name: first-s2
                            public: 127.0.0.1:32777
                            internal: 127.0.0.1:7101
        """.ReplaceLineEndings("\n"), "");
        Assert.NotEqual(ThreeShards, doc);
        Assert.False(ClusterStatusFile.IsSteady(doc, 3, out var detail));
        Assert.Contains("shard 1 has no leader", detail);
    }

    [Theory]
    [InlineData(2u)]
    [InlineData(4u)]
    public void AWrongShardCount_IsNotSteady(uint expected)
    {
        Assert.False(ClusterStatusFile.IsSteady(ThreeShards, expected, out var detail));
        Assert.Contains("3 shard(s)", detail);
    }

    [Theory]
    [InlineData("")]
    [InlineData("namespaces:\n")]
    [InlineData("namespaces:\n    default:\n        replicationFactor: 3\n        shards:\n")]
    public void AnEmptyOrPartialDocument_IsNotSteady(string doc)
    {
        Assert.False(ClusterStatusFile.IsSteady(doc, 1, out var detail));
        Assert.Contains("0 shard(s)", detail);
    }
}
