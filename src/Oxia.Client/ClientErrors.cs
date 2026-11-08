// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/client.go (var block), oxia/options_client.go (ErrInvalidOption*), oxia/admin_errors.go (sentinels only),
//              oxia/internal/batch/write_batch.go (ErrRequestTooLarge), oxia/batch/batcher.go (ErrShuttingDown)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client package's sentinel errors (see OxiaErrors in Oxia.Common for the matching rules).  Messages are
//              byte-identical to Go.
// =====================================================================================================================
using Oxia.Common.Constant;

namespace Oxia.Client;

public static class ClientErrors
{
    /// <summary>ErrKeyNotFound: A record associated with the specified key was not found.</summary>
    public static readonly OxiaException KeyNotFound = new("key not found");

    /// <summary>ErrUnexpectedVersionId: The expected version id passed as a condition does not match the current version id of the stored record.</summary>
    public static readonly OxiaException UnexpectedVersionId = new("unexpected version id");

    public static readonly OxiaException InvalidOptions = new("invalid options");

    /// <summary>ErrRequestTooLarge: returned when a request is larger than the maximum batch size.</summary>
    public static readonly OxiaException RequestTooLarge = new("put request is too large");

    /// <summary>ErrUnknownStatus: Unknown error.</summary>
    public static readonly OxiaException UnknownStatus = new("unknown status");

    /// <summary>ErrEndInclusiveUnsupported: a List/RangeScan with EndInclusive went to a shard whose leader excluded the end of the range.</summary>
    public static readonly OxiaException EndInclusiveUnsupported = new("the shard does not support an inclusive end of range");

    /// <summary>batch.ErrShuttingDown.</summary>
    public static readonly OxiaException ShuttingDown = new("shutting down");

    // options_client.go
    public static readonly OxiaException InvalidOptionBatchLinger = new("BatchLinger must be greater than or equal to zero");
    public static readonly OxiaException InvalidOptionMaxRequestsPerBatch = new("MaxRequestsPerBatch must be greater than zero");
    public static readonly OxiaException InvalidOptionMaxBatchSize = new("MaxBatchSize must be greater than zero");
    /// <summary>EXTENSION (not in Go): see OxiaOptions.WithBatchQueueCapacity.</summary>
    public static readonly OxiaException InvalidOptionBatchQueueCapacity = new("BatchQueueCapacity must be greater than zero");
    public static readonly OxiaException InvalidOptionRequestTimeout = new("RequestTimeout must be greater than zero");
    public static readonly OxiaException InvalidOptionSessionTimeout = new("SessionTimeout must be greater than zero");
    public static readonly OxiaException InvalidOptionIdentity = new("Identity must be non-empty");
    public static readonly OxiaException InvalidOptionNamespace = new("Namespace cannot be empty");
    public static readonly OxiaException InvalidOptionTls = new("Tls cannot be empty");
    public static readonly OxiaException InvalidOptionAuthentication = new("Authentication cannot be empty");

    // admin_errors.go
    public static readonly OxiaException Unauthorized = new("unauthorized");
    public static readonly OxiaException Unauthenticated = new("unauthenticated");
    public static readonly OxiaException Unknown = new("unknown");
}
