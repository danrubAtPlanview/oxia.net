// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/proto_utils.go   (toPutResult, toDeleteResult, toDeleteRangeResult, toGetResult, toVersion, toError,
//              toSecondaryIndexes)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Conversions from the wire messages (Oxia.Common.Proto) to the public result types of the client.
//   Divergences:
//      * Static and allocation-free (the sentinel errors are shared instances; results are structs).  The only
//        allocations are ToSecondaryIndexes' list and its SecondaryIndex messages, which exist only for puts that
//        carry secondary indexes (null otherwise, like Go's nil slice).
//      * `error` -> `Exception?`; `proto.Version` -> ProtoVersion; `oxia.Version` -> RecordVersion.
//      * toVersion dereferences a nil version in Go (panic); ToVersion(null) returns the default RecordVersion instead.
//      * `toGetResult(r, key, err)` keeps Go's parameter order and its `err != nil` shortcut (r may be null then).
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client;

internal static class ProtoUtils
{
    public static PutResult ToPutResult(string originalKey, PutResponse r)
    {
        var err = ToError(r.Status);
        if (err is not null)
        {
            return new PutResult("", default, err);
        }

        return new PutResult(r.Key ?? originalKey, ToVersion(r.Version), null);
    }

    public static Exception? ToDeleteResult(DeleteResponse r) => ToError(r.Status);

    public static Exception? ToDeleteRangeResult(DeleteRangeResponse r) => ToError(r.Status);

    public static GetResult ToGetResult(GetResponse? r, string originalKey, Exception? err)
    {
        if (err is not null)
        {
            return new GetResult("", null, default, err);
        }

        err = ToError(r!.Status);
        if (err is not null)
        {
            return new GetResult("", null, default, err);
        }

        return new GetResult(r.Key ?? originalKey, r.Value, ToVersion(r.Version), null);
    }

    public static RecordVersion ToVersion(ProtoVersion? version)
    {
        if (version is null)
        {
            return default;
        }

        return new RecordVersion
        {
            VersionId = version.VersionId,
            ModificationsCount = version.ModificationsCount,
            CreatedTimestamp = version.CreatedTimestamp,
            ModifiedTimestamp = version.ModifiedTimestamp,
            Ephemeral = version.SessionId is not null,
            ClientIdentity = version.ClientIdentity ?? "",
            SessionId = version.SessionId ?? 0,
        };
    }

    public static Exception? ToError(Status status) => status switch
    {
        Status.Ok => null,
        Status.UnexpectedVersionId => ClientErrors.UnexpectedVersionId,
        Status.KeyNotFound => ClientErrors.KeyNotFound,
        Status.InvalidArgument => ClientErrors.InvalidOptions,
        _ => ClientErrors.UnknownStatus,
    };

    /// <summary>The wire form of the secondary indexes of a put; null when there are none.</summary>
    public static List<SecondaryIndex>? ToSecondaryIndexes(List<SecondaryIdxOption>? secondaryIndexes)
    {
        if (secondaryIndexes is null || secondaryIndexes.Count == 0)
        {
            return null;
        }

        var res = new List<SecondaryIndex>(secondaryIndexes.Count);
        foreach (var si in secondaryIndexes)
        {
            res.Add(new SecondaryIndex { IndexName = si.IndexName, SecondaryKey = si.SecondaryKey });
        }

        return res;
    }
}
