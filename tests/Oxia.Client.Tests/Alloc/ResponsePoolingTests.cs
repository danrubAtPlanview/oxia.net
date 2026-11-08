// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- the response recycling protocol is an addition of wave 2 / Z1; Go lets the GC have every response)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins the ownership protocol of the response messages (docs/performance-allocation.md):
//                * the gRPC deserializers RENT the top-level responses (WriteResponse, ReadResponse, RangeScanResponse, ListResponse,
//                  NotificationBatch) from a pool; the empty responses (KeepAlive, CloseSession) are one shared instance;
//                * Release() is idempotent and a harmless no-op on a message made with `new` (what the fakes of the other tests hand out);
//                * a released message is clean when rented again, and its children are recycled (zero-allocation re-parse);
//                * Debug builds detect a use after the release;
//                * ReadResponse.Absorb moves the gets of a pooled chunk and keeps a plain chunk intact;
//                * the repeated client identity string of a version is decoded once;
//                * concurrent rent / parse / release never mixes up the contents of two responses.
//   Divergences: n/a (new tests).  The pools are process-wide: these tests run alone ("Z1 serial").
// =====================================================================================================================
using System.Buffers;
using Grpc.Core;
using Oxia.Common.Proto;
using Oxia.Client.Tests.Proto;
using Oxia.Common.Proto.Codec;
using Xunit;
using Status = Oxia.Common.Proto.Status;

namespace Oxia.Client.Tests.Alloc;

[CollectionDefinition("Z1 serial", DisableParallelization = true)]
public sealed class Z1SerialCollection;

[Collection("Z1 serial")]
public class ResponsePoolingTests
{
    private static byte[] WriteResponseBytes(int puts, int deletes = 0, string? identity = null)
    {
        var response = new WriteResponse();
        for (int i = 0; i < puts; i++)
        {
            response.Puts.Add(new PutResponse
            {
                Status = Status.Ok,
                Version = new ProtoVersion { VersionId = 100 + i, ModificationsCount = i, CreatedTimestamp = 5, ModifiedTimestamp = 6, ClientIdentity = identity, SessionId = identity is null ? null : 9 },
            });
        }
        for (int i = 0; i < deletes; i++) response.Deletes.Add(new DeleteResponse { Status = Status.KeyNotFound });
        return MessageCodec.ToArray<WriteResponseCodec, WriteResponse>(response);
    }

    private static T Deserialize<T>(Marshaller<T> marshaller, byte[] bytes) =>
        marshaller.ContextualDeserializer(new FakeDeserializationContext(new ReadOnlySequence<byte>(bytes)));

    [Fact]
    public void TheMarshallers_RentTheirTopLevelResponses_AndShareTheEmptyOnes()
    {
        var write = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(2));
        var read = Deserialize(OxiaClientMethods.ReadResponseMarshaller, MessageCodec.ToArray<ReadResponseCodec, ReadResponse>(new ReadResponse()));
        var scan = Deserialize(OxiaClientMethods.RangeScanResponseMarshaller, MessageCodec.ToArray<RangeScanResponseCodec, RangeScanResponse>(new RangeScanResponse()));
        var list = Deserialize(OxiaClientMethods.ListResponseMarshaller, MessageCodec.ToArray<ListResponseCodec, ListResponse>(new ListResponse()));
        var notifications = Deserialize(OxiaClientMethods.NotificationBatchMarshaller, MessageCodec.ToArray<NotificationBatchCodec, NotificationBatch>(new NotificationBatch()));

        Assert.Equal(PoolStates.Rented, write.PoolState);
        Assert.Equal(PoolStates.Rented, read.PoolState);
        Assert.Equal(PoolStates.Rented, scan.PoolState);
        Assert.Equal(PoolStates.Rented, list.PoolState);
        Assert.Equal(PoolStates.Rented, notifications.PoolState);

        var a = Deserialize(OxiaClientMethods.KeepAliveResponseMarshaller, []);
        var b = Deserialize(OxiaClientMethods.KeepAliveResponseMarshaller, []);
        Assert.Same(a, b);
        Assert.Same(Deserialize(OxiaClientMethods.CloseSessionResponseMarshaller, []), Deserialize(OxiaClientMethods.CloseSessionResponseMarshaller, []));
    }

    [Fact]
    public void ReleaseIsIdempotent_AndAMessageMadeWithNew_IsLeftAlone()
    {
        var plain = new WriteResponse();
        plain.Puts.Add(new PutResponse { Status = Status.Ok });
        plain.Release();
        plain.Release();
        Assert.Single(plain.Puts); // not recycled: a fake that keeps its response can still read it
        Assert.Equal(PoolStates.Plain, plain.PoolState);

        var rented = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(3));
        Assert.Equal(3, rented.Puts.Count);
        rented.Release();
        rented.Release(); // a second Release changes nothing
        Assert.Equal(PoolStates.Pooled, rented.PoolState);
    }

    [Fact]
    public void ARentedMessage_IsCleanAndItsChildrenAreRecycled()
    {
        var first = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(3, deletes: 2));
        var children = first.Puts.ToArray();
        var version = children[0].Version;
        Assert.Equal(100, version!.VersionId);
        first.Release();

        var second = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(2));
        Assert.Same(first, second); // the pool is process-wide, this test class runs alone
        Assert.Equal(2, second.Puts.Count);
        Assert.Empty(second.Deletes); // nothing of the previous contents is left
        // the children (and their versions) were recycled into the new parse
        Assert.Contains(second.Puts[0], children);
        Assert.Contains(second.Puts[1], children);
        Assert.Equal(100, second.Puts[0].Version!.VersionId);
        Assert.Equal(101, second.Puts[1].Version!.VersionId);
        second.Release();
    }

    [Fact]
    public void TheListsOfAReleasedMessage_CannotBeUsedInDebugBuilds()
    {
#if !DEBUG
        Assert.Skip("use-after-release detection is compiled into Debug builds only");
#endif
        var write = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(1));
        var read = Deserialize(OxiaClientMethods.ReadResponseMarshaller, MessageCodec.ToArray<ReadResponseCodec, ReadResponse>(new ReadResponse()));
        write.Release();
        read.Release();

        Assert.Throws<ObjectDisposedException>(() => write.Puts);
        Assert.Throws<ObjectDisposedException>(() => write.Deletes);
        Assert.Throws<ObjectDisposedException>(() => read.Gets);

        // rented again, it is usable again
        var again = WriteResponse.Rent();
        Assert.Empty(again.Puts);
        again.Release();
    }

    [Fact]
    public void Absorb_MovesTheGetsOfAPooledChunk_AndKeepsAPlainChunkIntact()
    {
        var chunk1 = ReadResponse.Rent();
        chunk1.Gets.Add(new GetResponse { Status = Status.Ok });
        var chunk2 = ReadResponse.Rent();
        chunk2.Gets.Add(new GetResponse { Status = Status.KeyNotFound });
        chunk2.Gets.Add(new GetResponse { Status = Status.Ok });
        var moved = chunk2.Gets.ToArray();

        chunk1.Absorb(chunk2);

        Assert.Equal(3, chunk1.Gets.Count);
        Assert.Same(moved[0], chunk1.Gets[1]);
        Assert.Same(moved[1], chunk1.Gets[2]);
        Assert.Equal(PoolStates.Pooled, chunk2.PoolState); // the empty shell went back to the pool (its children did NOT: they live on in chunk1)

        // the same with a plain chunk (what the fakes of the tests hand out): left as it is
        var plain = new ReadResponse();
        plain.Gets.Add(new GetResponse { Status = Status.Ok });
        chunk1.Absorb(plain);
        Assert.Equal(4, chunk1.Gets.Count);
        Assert.Single(plain.Gets);

        chunk1.Release();
    }

    [Fact]
    public void ARepeatedClientIdentity_IsDecodedOnce()
    {
        var a = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(2, identity: "client-a"));
        var b = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(1, identity: "client-a"));
        var other = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(1, identity: "client-b"));

        string identity = a.Puts[0].Version!.ClientIdentity!;
        Assert.Equal("client-a", identity);
        Assert.Same(identity, a.Puts[1].Version!.ClientIdentity);
        Assert.Same(identity, b.Puts[0].Version!.ClientIdentity);
        Assert.Equal("client-b", other.Puts[0].Version!.ClientIdentity);
    }

    [Fact]
    public void ANonAsciiClientIdentity_IsDecodedCorrectly()
    {
        var response = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(1, identity: "cliënt-日本-\U0001F600"));
        Assert.Equal("cliënt-日本-\U0001F600", response.Puts[0].Version!.ClientIdentity);
    }

    [Fact]
    public void AGetResponseParsedForThePooledApi_HoldsItsValueAndKeyInALease()
    {
        var source = new ReadResponse();
        source.Gets.Add(new GetResponse { Status = Status.Ok, Value = [1, 2, 3], Key = "the-key", Version = new ProtoVersion { VersionId = 5 } });
        source.Gets.Add(new GetResponse { Status = Status.KeyNotFound });
        source.Gets.Add(new GetResponse { Status = Status.Ok, Value = [] });
        byte[] bytes = MessageCodec.ToArray<ReadResponseCodec, ReadResponse>(source);

        var classic = Deserialize(OxiaClientMethods.ReadResponseMarshaller, bytes);
        Assert.Equal([1, 2, 3], classic.Gets[0].Value);
        Assert.Null(classic.Gets[0].Lease);

        var pooled = Deserialize(OxiaClientMethods.ReadResponsePooledMarshaller, bytes);
        var g0 = pooled.Gets[0];
        Assert.Null(g0.Value);
        Assert.Null(g0.Key);
        Assert.NotNull(g0.Lease);
        Assert.Equal([1, 2, 3], g0.Lease!.Value.ToArray());
        Assert.Equal("the-key", g0.ResolvedKey);
        Assert.Equal(3, g0.ValueLength);
        Assert.Null(pooled.Gets[1].Lease); // not found: nothing leased
        Assert.True(pooled.Gets[2].Lease!.HasValue); // an empty value is a value
        Assert.Equal(0, pooled.Gets[2].ValueLength);

        // a classic consumer of a pooled response gets an exact-size array and the lease is given back
        byte[]? taken = g0.TakeValueArray();
        Assert.Equal([1, 2, 3], taken);
        Assert.Null(g0.Lease);
        Assert.Equal("the-key", g0.Key);

        pooled.Release();
    }

    [Fact]
    public void GetResponseClone_StaysValidAfterTheParentIsReleased()
    {
        var source = new ReadResponse();
        source.Gets.Add(new GetResponse { Status = Status.Ok, Value = [9, 8], Key = "k", SecondaryIndexKey = "idx", Version = new ProtoVersion { VersionId = 3, ClientIdentity = "me", SessionId = 4 } });
        var read = Deserialize(OxiaClientMethods.ReadResponsePooledMarshaller, MessageCodec.ToArray<ReadResponseCodec, ReadResponse>(source));

        var clone = read.Gets[0].Clone();
        read.Release(); // recycles the child and returns its lease

        Assert.Equal(Status.Ok, clone.Status);
        Assert.Equal([9, 8], clone.Value);
        Assert.Equal("k", clone.Key);
        Assert.Equal("idx", clone.SecondaryIndexKey);
        Assert.Equal(3, clone.Version!.VersionId);
        Assert.Equal("me", clone.Version.ClientIdentity);
        Assert.Equal(4, clone.Version.SessionId);
    }

    [Fact]
    public async Task ConcurrentRentParseRelease_NeverMixesUpTheContentsOfTwoResponses()
    {
        // 8 threads, each with its own sizes / version ids: a message released while another thread still reads it would show up as a wrong id
        var failures = new List<string>();
        var workers = Enumerable.Range(0, 8).Select(w => Task.Run(() =>
        {
            byte[] bytes = WriteResponseBytes(5 + w, deletes: w);
            for (int i = 0; i < 3000; i++)
            {
                var response = Deserialize(OxiaClientMethods.WriteResponseMarshaller, bytes);
                if (response.Puts.Count != 5 + w || response.Deletes.Count != w)
                {
                    lock (failures) failures.Add($"worker {w}: {response.Puts.Count} puts, {response.Deletes.Count} deletes");
                    return;
                }
                for (int p = 0; p < response.Puts.Count; p++)
                {
                    if (response.Puts[p].Version!.VersionId != 100 + p)
                    {
                        lock (failures) failures.Add($"worker {w}: put {p} has version {response.Puts[p].Version!.VersionId}");
                        return;
                    }
                }
                response.Release();
            }
        })).ToArray();

        await Task.WhenAll(workers);
        Assert.Empty(failures);
    }

    [Fact]
    public void AHugeResponse_IsTrimmedWhenItGoesBackToThePool()
    {
        // 10000 puts: more spare children than the pool keeps; releasing and re-renting must still work and stay correct
        var big = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(10_000));
        Assert.Equal(10_000, big.Puts.Count);
        big.Release();

        var again = Deserialize(OxiaClientMethods.WriteResponseMarshaller, WriteResponseBytes(10_000));
        Assert.Equal(10_000, again.Puts.Count);
        Assert.Equal(100 + 9_999, again.Puts[9_999].Version!.VersionId);
        again.Release();
    }
}
