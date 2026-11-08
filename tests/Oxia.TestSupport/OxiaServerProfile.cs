// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : oxiad/dataserver/standalone.go  (StandaloneConfig, NewTestConfig),  cmd/standalone/cmd.go (flags)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go tests configure an in-process server with
//                  config := dataserver.NewTestConfig(t.TempDir())
//                  config.NumShards = 10 / config.KeySorting = proto.KeySortingType_NATURAL /
//                  config.NotificationsEnabled = false / config.DataServerOptions.Storage.Notification.Retention = 1s
//              A profile is the same set of knobs expressed as `oxia standalone` command line flags, so that one reusable
//              Docker container can run any number of differently configured servers (see OxiaServerFixture).
//   Divergences: The data/WAL directories, the public bind address and the metrics endpoint are managed by the fixture
//              (tmpfs inside the container, unique port per slot); `--wal-sync-data=false` is always passed (tests do not
//              need durability, the Go test config keeps the default but the server is thrown away after each test anyway).
// =====================================================================================================================
using System.Globalization;

namespace Oxia.TestSupport;

/// <summary>Go: <c>proto.KeySortingType</c> (the values accepted by <c>oxia standalone --key-sorting</c>).</summary>
public enum ServerKeySorting
{
    /// <summary>File-system like: keys with fewer '/' sort first, '/' sorts after any other byte (the server default).</summary>
    Hierarchical = 0,

    /// <summary>Byte-wise ordering.</summary>
    Natural = 1,
}

/// <summary>Go: <c>proto.KeySortingType.String()</c> is "NATURAL"/"HIERARCHICAL" (used as Go sub-test names).</summary>
public static class ServerKeySortingExtensions
{
    public static string GoString(this ServerKeySorting s) => s == ServerKeySorting.Natural ? "NATURAL" : "HIERARCHICAL";
    public static string Flag(this ServerKeySorting s) => s == ServerKeySorting.Natural ? "natural" : "hierarchical";
}

/// <summary>
/// The configuration of one standalone Oxia server (Go: <c>dataserver.StandaloneConfig</c> as built by
/// <c>dataserver.NewTestConfig</c>).  Use <c>with</c> expressions to derive variants:
/// <c>OxiaServerProfile.Default with { Shards = 10 }</c>.
/// </summary>
public sealed record OxiaServerProfile
{
    /// <summary>Go: NewTestConfig(dir): 1 shard, hierarchical key sorting, notifications enabled.</summary>
    public static readonly OxiaServerProfile Default = new();

    /// <summary>Several shards, as the Go tests that "ensure correctness across shards" (10), or the key sorting tests (4).</summary>
    public static OxiaServerProfile WithShards(uint shards, ServerKeySorting keySorting = ServerKeySorting.Hierarchical) =>
        Default with { Shards = shards, KeySorting = keySorting };

    /// <summary>The 4-shard profile of the issue statement ("MultiShard").</summary>
    public static readonly OxiaServerProfile MultiShard = WithShards(4);

    /// <summary>Go: config.NumShards.</summary>
    public uint Shards { get; init; } = 1;

    /// <summary>Go: config.KeySorting.</summary>
    public ServerKeySorting KeySorting { get; init; } = ServerKeySorting.Hierarchical;

    /// <summary>Go: config.NotificationsEnabled.</summary>
    public bool NotificationsEnabled { get; init; } = true;

    /// <summary>Go: config.DataServerOptions.Storage.Notification.Retention (null = the server default, 1h).</summary>
    public TimeSpan? NotificationsRetention { get; init; }

    /// <summary>Extra raw `oxia standalone` flags (escape hatch; must not contain spaces or shell metacharacters).</summary>
    public IReadOnlyList<string> ExtraArgs { get; init; } = [];

    /// <summary>True when the profile is the plain <see cref="Default"/> (the only one an external OXIA_TEST_ADDRESS server can satisfy).</summary>
    public bool IsDefault => Shards == 1 && KeySorting == ServerKeySorting.Hierarchical && NotificationsEnabled &&
                             NotificationsRetention is null && ExtraArgs.Count == 0;

    /// <summary>The `oxia standalone` flags that realise this profile (excluding address / directory flags).</summary>
    public IEnumerable<string> ToFlags()
    {
        yield return "--shards";
        yield return Shards.ToString(CultureInfo.InvariantCulture);
        yield return "--key-sorting";
        yield return KeySorting.Flag();
        yield return $"--notifications-enabled={(NotificationsEnabled ? "true" : "false")}";
        if (NotificationsRetention is { } r)
        {
            yield return "--notifications-retention-time";
            yield return Math.Max(1, (long)r.TotalMilliseconds).ToString(CultureInfo.InvariantCulture) + "ms";
        }

        foreach (var a in ExtraArgs) yield return a;
    }

    public override string ToString() =>
        $"shards={Shards} sorting={KeySorting.Flag()} notifications={(NotificationsEnabled ? "on" : "off")}" +
        (NotificationsRetention is { } r ? $" retention={r.TotalSeconds:0.###}s" : "") +
        (ExtraArgs.Count > 0 ? " +" + string.Join(' ', ExtraArgs) : "");
}
