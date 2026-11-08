// =====================================================================================================================
// PORT NOTE   (extra tests -- not in Go)
//   Upstream : oxia/proto_utils.go (the Go tests only cover the INVALID_ARGUMENT status, in options_put_test.go)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The conversions from wire messages to public results: status -> sentinel error mapping, key fallback to the
//              original key, version mapping (ephemeral <=> session id), secondary index conversion; plus the public
//              value types of Client.cs (RecordVersion equality, NotificationType string forms) and the optional.
// =====================================================================================================================
using Oxia.Common.Constant;
using Xunit;
using P = Oxia.Common.Proto;

namespace Oxia.Client.Tests;

public class ProtoUtilsExtraTests
{
    [Theory]
    [InlineData(P.Status.Ok, null)]
    [InlineData(P.Status.KeyNotFound, "KeyNotFound")]
    [InlineData(P.Status.UnexpectedVersionId, "UnexpectedVersionId")]
    [InlineData(P.Status.InvalidArgument, "InvalidOptions")]
    [InlineData(P.Status.SessionDoesNotExist, "UnknownStatus")]
    [InlineData((P.Status)42, "UnknownStatus")]
    public void ToError_MapsStatusToTheSentinels(P.Status status, string? sentinel)
    {
        var expected = sentinel switch
        {
            null => null,
            "KeyNotFound" => ClientErrors.KeyNotFound,
            "UnexpectedVersionId" => ClientErrors.UnexpectedVersionId,
            "InvalidOptions" => ClientErrors.InvalidOptions,
            _ => ClientErrors.UnknownStatus,
        };
        Assert.Same(expected, ProtoUtils.ToError(status));
        Assert.Same(expected, ProtoUtils.ToDeleteResult(new P.DeleteResponse { Status = status }));
        Assert.Same(expected, ProtoUtils.ToDeleteRangeResult(new P.DeleteRangeResponse { Status = status }));
    }

    [Fact]
    public void ToPutResult_UsesTheServerKeyWhenPresent_ElseTheOriginalKey()
    {
        var version = new P.ProtoVersion { VersionId = 5, ModificationsCount = 2, CreatedTimestamp = 10, ModifiedTimestamp = 20 };

        var plain = ProtoUtils.ToPutResult("orig", new P.PutResponse { Status = P.Status.Ok, Version = version });
        Assert.Null(plain.Err);
        Assert.Equal("orig", plain.Key);
        Assert.Equal(5L, plain.Version.VersionId);

        var sequenced = ProtoUtils.ToPutResult("orig", new P.PutResponse { Status = P.Status.Ok, Version = version, Key = "orig-0001" });
        Assert.Equal("orig-0001", sequenced.Key);

        // an empty (but present) server key is used as is
        Assert.Equal("", ProtoUtils.ToPutResult("orig", new P.PutResponse { Status = P.Status.Ok, Version = version, Key = "" }).Key);
    }

    [Fact]
    public void ToPutResult_OnError_HasNoKeyNorVersion()
    {
        var r = ProtoUtils.ToPutResult("orig", new P.PutResponse { Status = P.Status.UnexpectedVersionId, Version = new P.ProtoVersion { VersionId = 9 }, Key = "x" });
        Assert.Same(ClientErrors.UnexpectedVersionId, r.Err);
        Assert.Equal("", r.Key);
        Assert.Equal(default, r.Version);
    }

    [Fact]
    public void ToGetResult_CoversTheErrorShortcutTheStatusAndTheKeyFallback()
    {
        var boom = new OxiaException("boom");
        var viaErr = ProtoUtils.ToGetResult(null, "k", boom);
        Assert.Same(boom, viaErr.Err);
        Assert.Null(viaErr.Value);

        var notFound = ProtoUtils.ToGetResult(new P.GetResponse { Status = P.Status.KeyNotFound }, "k", null);
        Assert.Same(ClientErrors.KeyNotFound, notFound.Err);
        Assert.Equal("", notFound.Key);

        var value = new byte[] { 1, 2, 3 };
        var ok = ProtoUtils.ToGetResult(new P.GetResponse { Value = value, Version = new P.ProtoVersion { VersionId = 7 } }, "k", null);
        Assert.Null(ok.Err);
        Assert.Equal("k", ok.Key);
        Assert.Same(value, ok.Value);
        Assert.Equal(7L, ok.Version.VersionId);

        var floor = ProtoUtils.ToGetResult(new P.GetResponse { Key = "found", Version = new P.ProtoVersion() }, "asked", null);
        Assert.Equal("found", floor.Key);
        Assert.Null(floor.Value);
    }

    [Fact]
    public void ToVersion_MapsEveryFieldAndEphemeralFollowsTheSessionId()
    {
        var persistent = ProtoUtils.ToVersion(new P.ProtoVersion
        {
            VersionId = 1, ModificationsCount = 2, CreatedTimestamp = 3, ModifiedTimestamp = 4,
        });
        Assert.Equal(new RecordVersion { VersionId = 1, ModificationsCount = 2, CreatedTimestamp = 3, ModifiedTimestamp = 4 }, persistent);
        Assert.False(persistent.Ephemeral);
        Assert.Equal(0L, persistent.SessionId);
        Assert.Equal("", persistent.ClientIdentity);

        var ephemeral = ProtoUtils.ToVersion(new P.ProtoVersion
        {
            VersionId = 1, ModificationsCount = 2, CreatedTimestamp = 3, ModifiedTimestamp = 4, SessionId = 99, ClientIdentity = "me",
        });
        Assert.True(ephemeral.Ephemeral);
        Assert.Equal(99L, ephemeral.SessionId);
        Assert.Equal("me", ephemeral.ClientIdentity);

        // session id 0 is still "has a session" (Go: SessionId != nil)
        Assert.True(ProtoUtils.ToVersion(new P.ProtoVersion { SessionId = 0 }).Ephemeral);

        Assert.Equal(default, ProtoUtils.ToVersion(null));
    }

    [Fact]
    public void ToSecondaryIndexes_IsNullWithoutIndexes_ElseKeepsTheOrder()
    {
        Assert.Null(ProtoUtils.ToSecondaryIndexes(null));
        Assert.Null(ProtoUtils.ToSecondaryIndexes(new List<SecondaryIdxOption>()));

        var res = ProtoUtils.ToSecondaryIndexes([new SecondaryIdxOption("i1", "k1"), new SecondaryIdxOption("i1", "k2"), new SecondaryIdxOption("i2", "k3")]);
        Assert.NotNull(res);
        Assert.Equal(new[] { ("i1", "k1"), ("i1", "k2"), ("i2", "k3") }, res.Select(s => (s.IndexName, s.SecondaryKey)).ToArray());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Public types of Client.cs
    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public void RecordVersion_EqualityIsByValue_AndCoversEveryField()
    {
        var a = new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" };
        var same = new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" };
        Assert.Equal(a, same);
        Assert.True(a == same);
        Assert.False(a != same);
        Assert.Equal(a.GetHashCode(), same.GetHashCode());
        Assert.True(a.Equals((object)same));
        Assert.False(a.Equals((object?)null));
        Assert.False(a.Equals("x"));
        Assert.Equal(a, a with { });   // init-only properties support `with` copies
        Assert.NotEqual(a, a with { VersionId = 2 });
    }

    [Fact]
    public void RecordVersion_EachFieldMatters()
    {
        var a = new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" };
        var variants = new[]
        {
            new RecordVersion { VersionId = 9, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 9, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 9, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 9, Ephemeral = true, SessionId = 5, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = false, SessionId = 5, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 9, ClientIdentity = "x" },
            new RecordVersion { VersionId = 1, CreatedTimestamp = 2, ModifiedTimestamp = 3, ModificationsCount = 4, Ephemeral = true, SessionId = 5, ClientIdentity = "y" },
        };
        foreach (var v in variants)
        {
            Assert.NotEqual(a, v);
            Assert.True(a != v);
        }

        // a null identity reads as "" (Go's zero value)
        Assert.Equal(new RecordVersion(), new RecordVersion { ClientIdentity = "" });
        Assert.Equal("", default(RecordVersion).ClientIdentity);
        Assert.Equal(-1L, RecordVersion.VersionIdNotExists);
    }

    [Fact]
    public void NotificationType_StringFormsAreTheContract()
    {
        Assert.Equal(0, (int)NotificationType.KeyCreated);
        Assert.Equal(1, (int)NotificationType.KeyModified);
        Assert.Equal(2, (int)NotificationType.KeyDeleted);
        Assert.Equal(3, (int)NotificationType.KeyRangeRangeDeleted);
        Assert.Equal(4, (int)NotificationType.NotificationsMissed);
        Assert.Equal("KeyCreated", NotificationType.KeyCreated.GoString());
        Assert.Equal("KeyModified", NotificationType.KeyModified.GoString());
        Assert.Equal("KeyDeleted", NotificationType.KeyDeleted.GoString());
        Assert.Equal("KeyRangeRangeDeleted", NotificationType.KeyRangeRangeDeleted.GoString());
        Assert.Equal("NotificationsMissed", NotificationType.NotificationsMissed.GoString());
        Assert.Equal("Unknown", ((NotificationType)5).GoString());
        Assert.Equal("Unknown", ((NotificationType)int.MinValue).GoString());
    }

    [Fact]
    public void Optional_EmptyIsSharedAndImmutable_PresentHoldsNullAndDefaultValues()
    {
        Assert.Same(Optional.Empty<int>(), Optional.Empty<int>());
        Assert.NotSame(Optional.Empty<int>(), (object)Optional.Empty<string>());

        var presentNull = Optional.Of<string?>(null);
        Assert.True(presentNull.Present);
        Assert.True(presentNull.TryGet(out var s));
        Assert.Null(s);
        Assert.Null(presentNull.MustGet());

        var presentZero = Optional.Of(0);
        Assert.True(presentZero.Present);
        Assert.False(presentZero.Empty);
        Assert.Equal(0, presentZero.MustGet());

        Assert.False(Optional.Empty<int>().TryGet(out var v));
        Assert.Equal(0, v);
    }
}
