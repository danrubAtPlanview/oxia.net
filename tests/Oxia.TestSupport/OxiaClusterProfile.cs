// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : tests/security/tls/tls_encryption_test.go (newTLSServerWithInterceptor: dataserveroption.Options.Server.Public/Internal.TLS,
//              Replication.TLS), tests/security/auth/auth_oidc_test.go (newOxiaClusterWithAuth: Server.Public.Auth),
//              cmd/server/cmd.go and cmd/coordinator/cmd.go (the flags that carry those options in the real processes).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go security tests build `dataserveroption.Options` in code (TLS files, OIDC provider params) for three in-process data
//              servers.  The `oxia standalone` command has NO TLS / auth flags (only `oxia server` does), so the security tests run on
//              the cluster fixture (coordinator + 3 `oxia server` processes).  A profile is the same knobs as raw command line flags plus
//              the files they refer to; the fixture writes the files into the cluster slot's tmpfs directory (never to disk on the host
//              Docker VM), appends the flags to every data server / the coordinator, and uses `Probe` for its own raw readiness calls.
//   Divergences: n/a (new code).
// =====================================================================================================================
namespace Oxia.TestSupport;

/// <summary>Extra configuration of the processes of an <see cref="OxiaCluster"/>: command line flags and the files they use.</summary>
public sealed record OxiaClusterProfile
{
    /// <summary>
    /// In <see cref="ServerArgs"/> / <see cref="CoordinatorArgs"/> (also inside JSON values) this token is replaced by the directory
    /// the <see cref="Files"/> are written to (<c>/data/cluster&lt;slot&gt;/files</c>, a tmpfs wiped when the cluster is closed).
    /// </summary>
    public const string DirToken = "{dir}";

    /// <summary>File name (no directory) -> text content, written to <see cref="DirToken"/> before the processes start.</summary>
    public IReadOnlyDictionary<string, string> Files { get; init; } = new Dictionary<string, string>();

    /// <summary>Appended to the command line of every <c>oxia server</c>.</summary>
    public IReadOnlyList<string> ServerArgs { get; init; } = [];

    /// <summary>Appended to the command line of the <c>oxia coordinator</c>.</summary>
    public IReadOnlyList<string> CoordinatorArgs { get; init; } = [];

    /// <summary>The TLS / credentials the fixture's own raw gRPC calls (readiness, leader lookup) need to talk to the servers.</summary>
    public RawSecurity? Probe { get; init; }

    public override string ToString() =>
        $"files=[{string.Join(',', Files.Keys)}] server=[{string.Join(' ', ServerArgs.Where(a => a.StartsWith("--", StringComparison.Ordinal)))}] " +
        $"coordinator=[{string.Join(' ', CoordinatorArgs.Where(a => a.StartsWith("--", StringComparison.Ordinal)))}]";
}
