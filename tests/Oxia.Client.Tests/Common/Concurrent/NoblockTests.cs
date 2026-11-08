// =====================================================================================================================
// PORT NOTE
//   Upstream : common/channel/noblock_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0.
//   Divergences: `make(chan int, 1)` -> a bounded Channel<int> of capacity 1; `<-ch` -> ChannelReader.TryRead.
//   Skipped upstream files of common/channel (not used by the client): override_channel.go / _test.go, read.go / _test.go.
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Common.Concurrent;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class NoblockTests
{
    [Fact]
    public void TestPushNoBlock()
    {
        var ch = Channel.CreateBounded<int>(1);

        Assert.True(ch.Writer.PushNoBlock(1));
        Assert.False(ch.Writer.PushNoBlock(2));
        Assert.True(ch.Reader.TryRead(out var received));
        Assert.Equal(1, received);
    }

    [Fact]
    public void PushNoBlock_UnboundedChannelAlwaysAccepts_CompletedChannelRefuses()
    {
        var unbounded = Channel.CreateUnbounded<int>();
        for (var i = 0; i < 1000; i++) Assert.True(unbounded.Writer.PushNoBlock(i));

        var bounded = Channel.CreateBounded<int>(1);
        bounded.Writer.Complete();
        Assert.False(bounded.Writer.PushNoBlock(1));
    }

    [Fact]
    public void PushNoBlock_RoomIsFreedByReading()
    {
        var ch = Channel.CreateBounded<int>(2);
        Assert.True(ch.Writer.PushNoBlock(1));
        Assert.True(ch.Writer.PushNoBlock(2));
        Assert.False(ch.Writer.PushNoBlock(3));
        Assert.True(ch.Reader.TryRead(out _));
        Assert.True(ch.Writer.PushNoBlock(3));
    }
}
