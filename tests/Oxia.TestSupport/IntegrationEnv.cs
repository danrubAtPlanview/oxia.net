// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none -- the Go integration tests start an in-process server (`dataserver.NewStandalone`), so they have no
//              configuration surface.  This file is the single place that reads the environment variables which steer the
//              Docker based replacement (see tests/Oxia.Client.IntegrationTests/README.md).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Central, documented list of every OXIA_TEST_* knob.
//   Divergences: n/a (new code).
// =====================================================================================================================
namespace Oxia.TestSupport;

/// <summary>The <c>OXIA_TEST_*</c> environment variables understood by the test infrastructure.</summary>
public static class IntegrationEnv
{
    public const string AddressVar = "OXIA_TEST_ADDRESS";
    public const string ImageVar = "OXIA_TEST_IMAGE";
    public const string CleanupVar = "OXIA_TEST_CLEANUP";
    public const string ClusterVar = "OXIA_TEST_CLUSTER";
    public const string SlotsVar = "OXIA_TEST_SLOTS";
    public const string ClusterSlotsVar = "OXIA_TEST_CLUSTER_SLOTS";
    public const string InstanceVar = "OXIA_TEST_INSTANCE";
    public const string MemoryVar = "OXIA_TEST_MEMORY";
    public const string CpusVar = "OXIA_TEST_CPUS";
    public const string SkipIfNoServerVar = "OXIA_TEST_SKIP_IF_NO_SERVER";
    public const string VerboseVar = "OXIA_TEST_VERBOSE";
    public const string HostVar = "OXIA_TEST_HOST";
    public const string ContainerHostVar = "OXIA_TEST_CONTAINER_HOST";

    public const string DefaultImage = "oxia/oxia:nightly-beta";
    public const int DefaultSlots = 8;
    public const string DefaultHost = "127.0.0.1";

    /// <summary>OXIA_TEST_ADDRESS: an externally started standalone server; the Docker logic is skipped entirely.</summary>
    public static string? ExternalAddress => Get(AddressVar);

    /// <summary>OXIA_TEST_IMAGE (default <c>oxia/oxia:nightly-beta</c>: built nightly from upstream main; the build of 2026-11-07 is exactly the pinned commit 8456f61. The <c>main</c> tag is stale).</summary>
    public static string Image => Get(ImageVar) ?? DefaultImage;

    /// <summary>OXIA_TEST_CLEANUP=1: remove the containers when the test run ends (default: keep them for the next run).</summary>
    public static bool Cleanup => IsTrue(CleanupVar);

    /// <summary>OXIA_TEST_CLUSTER=1: also run the tests that need a coordinator + 3 data servers (opt-in).</summary>
    public static bool ClusterEnabled => IsTrue(ClusterVar);

    /// <summary>OXIA_TEST_SLOTS: how many standalone servers may run concurrently inside the container (default 8).</summary>
    public static int Slots => int.TryParse(Get(SlotsVar), out var n) && n is > 0 and <= 64 ? n : DefaultSlots;

    /// <summary>OXIA_TEST_CLUSTER_SLOTS: how many clusters (coordinator + up to 3 servers each) may run concurrently (default 2).</summary>
    public static int ClusterSlots => int.TryParse(Get(ClusterSlotsVar), out var n) && n is > 0 and <= 16 ? n : 2;

    /// <summary>OXIA_TEST_INSTANCE: suffix of the container name, to run several isolated fixtures side by side.</summary>
    public static string? Instance => Get(InstanceVar);

    /// <summary>OXIA_TEST_MEMORY (docker --memory, default 3g) of the container.</summary>
    public static string Memory => Get(MemoryVar) ?? "3g";

    /// <summary>OXIA_TEST_CPUS (docker --cpus) of the container; unset = no CPU limit (the servers are idle most of the time).</summary>
    public static string? Cpus => Get(CpusVar);

    /// <summary>OXIA_TEST_SKIP_IF_NO_SERVER=1: skip (instead of fail) the tests when neither Docker nor OXIA_TEST_ADDRESS is available.</summary>
    public static bool SkipIfNoServer => IsTrue(SkipIfNoServerVar);

    /// <summary>OXIA_TEST_VERBOSE=1: the fixture prints what it does (docker commands, timings) to stderr.</summary>
    public static bool Verbose => IsTrue(VerboseVar);

    /// <summary>
    /// OXIA_TEST_HOST (default <c>127.0.0.1</c>): the host name/address under which the test process reaches the PUBLISHED container ports
    /// (<c>docker port</c> prints only the port).  The default is right whenever the test process runs on the machine that runs the Docker
    /// daemon (Windows, macOS, native Linux).  When the test process itself runs inside a Linux container that talks to the host's
    /// daemon through its socket (Docker Desktop; a CI job container), 127.0.0.1 is the container's own loopback: set it to
    /// <c>host.docker.internal</c> (Docker Desktop) or to the host's address.  The Oxia servers echo the authority a client dialled as the
    /// leader address, so nothing else needs to know about it (docs/linux-validation.md).
    /// </summary>
    public static string Host => Get(HostVar) ?? DefaultHost;

    /// <summary>
    /// OXIA_TEST_CONTAINER_HOST: how the processes INSIDE the containers reach the test process (the OIDC provider the security tests host).
    /// Unset = detect (<c>host.docker.internal</c> when it resolves in the container, else the container's default gateway); the value
    /// <c>gateway</c> forces the default gateway (needed when the test process runs in a host-network container of Docker Desktop's VM, where
    /// <c>host.docker.internal</c> names the Windows/macOS host, not the VM); any other value is used as is.
    /// </summary>
    public static string? ContainerHost => Get(ContainerHostVar);

    private static string? Get(string name)
    {
        var v = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(v) ? null : v.Trim();
    }

    private static bool IsTrue(string name) => Get(name) is { } v &&
        (v == "1" || v.Equals("true", StringComparison.OrdinalIgnoreCase) || v.Equals("yes", StringComparison.OrdinalIgnoreCase));
}
