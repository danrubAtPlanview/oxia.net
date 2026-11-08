// =====================================================================================================================
// PORT NOTE
//   Upstream : common/constant/constants.go, common/constant/keys.go, common/constant/flag.go (client-relevant subset)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Protocol-level constants shared by client and server: gRPC metadata header names, default ports,
//              session-timeout bounds, max gRPC frame size and the prefix of Oxia's internal keys.
//   Divergence: none (values are identical).  Go's `time.Duration` constants map to `TimeSpan`.
// =====================================================================================================================
namespace Oxia.Common.Constant;

public static class Constants
{
    public const long I64NegativeOne = -1;
    public const long I64Zero = 0;
    public const uint U32Zero = 0;

    public const string MetadataTerm = "term";
    public const string MetadataNamespace = "namespace";
    public const string MetadataShardId = "shard-id";
    public const string MetadataSplitHashRangeMin = "split-hash-range-min";
    public const string MetadataSplitHashRangeMax = "split-hash-range-max";
    /// <summary>The hash range of the parent of a split child.</summary>
    public const string MetadataSplitParentHashRangeMin = "split-parent-hash-range-min";
    public const string MetadataSplitParentHashRangeMax = "split-parent-hash-range-max";
    public const string MetadataInstanceId = "instance-id";
    public const string DefaultNamespace = "default";

    public const int DefaultPublicPort = 6648;
    public const int DefaultInternalPort = 6649;
    public const int DefaultAdminPort = 6651;

    public static readonly TimeSpan MaxSessionTimeout = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan MinSessionTimeout = TimeSpan.FromSeconds(2);

    /// <summary>MaxGrpcFrameSize is the largest gRPC message size allowed by both the client and the server.</summary>
    public const int MaxGrpcFrameSize = 256 * 1024 * 1024;

    /// <summary>InternalKeyPrefix is the prefix of keys used by oxia (keys.go).</summary>
    public const string InternalKeyPrefix = "__oxia/";
}
