// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- ValueLease is the pooled value buffer of the opt-in pooled read API, an addition of wave 2 / Z1)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The lease holds a record's value and key bytes in one buffer, with explicit presence; releasing it is guarded by a
//              generation (a second release of the same lease, e.g. a double Dispose of a copy of PooledGetResult, is ignored); growing the
//              buffer keeps the bytes already written; a lease that is reused starts clean.
//   Divergences: n/a (new tests).
// =====================================================================================================================
using System.Text;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Alloc;

[Collection("Z1 serial")]
public class ValueLeaseTests
{
    [Fact]
    public void ValueAndKey_AreKeptSideBySide_WithExplicitPresence()
    {
        var lease = ValueLease.Rent();
        Assert.False(lease.HasValue);
        Assert.Equal(-1, lease.KeyLength);

        "hello"u8.CopyTo(lease.ReserveValue(5));
        "/the/key"u8.CopyTo(lease.ReserveKey(8));

        Assert.True(lease.HasValue);
        Assert.Equal("hello", Encoding.UTF8.GetString(lease.Value));
        Assert.Equal("hello", Encoding.UTF8.GetString(lease.ValueMemory.Span));
        Assert.Equal("/the/key", Encoding.UTF8.GetString(lease.KeyUtf8));
        Assert.Equal("/the/key", lease.KeyString());
        Assert.Same(lease.KeyString(), lease.KeyString()); // decoded once

        lease.TryRelease(lease.Generation);
    }

    [Fact]
    public void AnEmptyValue_IsAValue_ButAnAbsentOneIsNot()
    {
        var lease = ValueLease.Rent();
        lease.ReserveValue(0);

        Assert.True(lease.HasValue);
        Assert.True(lease.Value.IsEmpty);
        Assert.Equal(Array.Empty<byte>(), lease.CopyValueToArray());

        lease.DropValue();
        Assert.False(lease.HasValue);
        Assert.Null(lease.CopyValueToArray());
        lease.TryRelease(lease.Generation);
    }

    [Fact]
    public void GrowingTheBuffer_KeepsTheBytesAlreadyWritten()
    {
        var lease = ValueLease.Rent();
        var value = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
        value.CopyTo(lease.ReserveValue(100));

        var key = new string('k', 200_000); // far beyond whatever the first buffer was
        Encoding.UTF8.GetBytes(key).CopyTo(lease.ReserveKey(key.Length));

        Assert.Equal(value, lease.Value.ToArray());
        Assert.Equal(key, lease.KeyString());
        lease.TryRelease(lease.Generation);
    }

    [Fact]
    public void ASecondRelease_WithTheOldGeneration_IsIgnored()
    {
        var lease = ValueLease.Rent();
        int generation = lease.Generation;
        "abc"u8.CopyTo(lease.ReserveValue(3));

        Assert.True(lease.TryRelease(generation));
        Assert.False(lease.TryRelease(generation)); // a copy of the result disposing again: ignored
        Assert.NotEqual(generation, lease.Generation);

        // another user rents the very same lease (the pool is process-wide and this class runs alone): the stale release must not touch it
        var other = ValueLease.Rent();
        Assert.Same(lease, other);
        "xyz"u8.CopyTo(other.ReserveValue(3));
        Assert.False(lease.TryRelease(generation));
        Assert.Equal("xyz", Encoding.UTF8.GetString(other.Value));
        other.TryRelease(other.Generation);
    }

    [Fact]
    public void ARentedLease_StartsClean()
    {
        var lease = ValueLease.Rent();
        "value"u8.CopyTo(lease.ReserveValue(5));
        "key"u8.CopyTo(lease.ReserveKey(3));
        lease.VersionId = 5;
        lease.ClientIdentity = "me";
        lease.SecondaryIndexKey = "idx";
        lease.HasSessionId = true;
        lease.TryRelease(lease.Generation);

        var again = ValueLease.Rent();
        Assert.False(again.HasValue);
        Assert.Equal(-1, again.KeyLength);
        Assert.Equal(0, again.VersionId);
        Assert.Null(again.ClientIdentity);
        Assert.Null(again.SecondaryIndexKey);
        Assert.False(again.HasSessionId);
        again.TryRelease(again.Generation);
    }

    [Fact]
    public void ManyLeasesAtOnce_EachKeepItsOwnBytes()
    {
        var leases = new ValueLease[2000];
        for (int i = 0; i < leases.Length; i++)
        {
            leases[i] = ValueLease.Rent();
            Encoding.UTF8.GetBytes("value-" + i).CopyTo(leases[i].ReserveValue(("value-" + i).Length));
        }
        for (int i = 0; i < leases.Length; i++) Assert.Equal("value-" + i, Encoding.UTF8.GetString(leases[i].Value));
        foreach (var lease in leases) lease.TryRelease(lease.Generation);
    }

    [Fact]
    public void TheUtf8StringCache_ReturnsTheSameInstance_ForRepeatedShortAsciiOnly()
    {
        var a = Utf8StringCache.Get("identity-1"u8);
        var b = Utf8StringCache.Get("identity-1"u8);
        Assert.Same(a, b);
        Assert.Equal("identity-1", a);

        var unicode1 = Utf8StringCache.Get(Encoding.UTF8.GetBytes("naïve"));
        var unicode2 = Utf8StringCache.Get(Encoding.UTF8.GetBytes("naïve"));
        Assert.Equal("naïve", unicode1);
        Assert.Equal(unicode1, unicode2);

        var longText = new string('x', 500);
        Assert.Equal(longText, Utf8StringCache.Get(Encoding.UTF8.GetBytes(longText)));
        Assert.Equal("", Utf8StringCache.Get(default));

        // colliding / different strings never get each other's value
        for (int i = 0; i < 1000; i++)
        {
            string s = "client-" + i;
            Assert.Equal(s, Utf8StringCache.Get(Encoding.UTF8.GetBytes(s)));
        }
    }
}
