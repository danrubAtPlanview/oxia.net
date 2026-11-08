// =====================================================================================================================
// PORT NOTE (review R4 reproduction tests)
//   Upstream : common/proto/client_vtproto.pb.go (MarshalToSizedBufferVT writes backwards into a slice of exactly SizeVT() bytes and
//              therefore cannot produce a frame whose length prefix disagrees with its body)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The C# writers go forward and trust the CachedSize memo of Size().  RawWriter-based "block" bodies verify the number of
//              bytes they wrote against the memo (ThrowStale), the chunked bodies used above ProtoWriter.BlockLimit (16 KiB) do not:
//              a message modified between Size() and WriteTo() then yields a frame whose nested length prefix disagrees with the
//              bytes that follow (a corrupt frame for the server) instead of an exception.
// =====================================================================================================================
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R4_StaleSizeTests
{
    [Fact]
    public void SmallMessage_ModifiedBetweenSizeAndWrite_Throws_Control()
    {
        var put = new PutRequest { Key = "short", Value = new byte[10] };
        int size = PutRequestCodec.Size(put);
        put.Key = "a-key-that-is-much-longer-than-the-size-memoised-above";
        var buffer = new byte[size + 1000];
        Assert.ThrowsAny<Exception>(() =>
        {
            var w = new ProtoWriter(buffer);
            PutRequestCodec.WriteTo(ref w, put);
        });
    }

    [Fact]
    public void LargeMessage_ModifiedBetweenSizeAndWrite_Throws()
    {
        // above ProtoWriter.BlockLimit the field-by-field "chunked" writer is used
        var put = new PutRequest { Key = new string('a', 20_000), Value = new byte[10] };
        int size = PutRequestCodec.Size(put);
        Assert.True(size > ProtoWriter.BlockLimit);
        put.Key = new string('a', 30_000);   // modified after Size(): the memoised size is stale

        var buffer = new byte[size + 40_000];
        long written = -1;
        var thrown = Record.Exception(() =>
        {
            var w = new ProtoWriter(buffer);
            PutRequestCodec.WriteTo(ref w, put);
            written = w.BytesWritten;
        });

        Assert.True(thrown is not null,
            $"a stale memo must be detected like on the small path; instead {written} bytes were written for a message that was sized at {size}");
    }

    [Fact]
    public void LargeNestedMessage_ModifiedBetweenSizeAndWrite_Throws()
    {
        // a PutRequest above BlockLimit inside a WriteRequest: WriteNested announces the memoised length, WriteChunked writes whatever the fields are now
        var request = new WriteRequest { Shard = 1 };
        var put = new PutRequest { Key = new string('a', 20_000), Value = new byte[10] };
        request.Puts.Add(put);
        int size = WriteRequestCodec.Size(request);
        put.Key = new string('a', 30_000);

        var buffer = new byte[size + 40_000];
        long written = -1;
        var thrown = Record.Exception(() =>
        {
            var w = new ProtoWriter(buffer);
            WriteRequestCodec.WriteTo(ref w, request);
            written = w.BytesWritten;
        });

        Assert.True(thrown is not null,
            $"a stale memo must be detected; instead {written} bytes were written for a message that was sized at {size}");
    }
}
