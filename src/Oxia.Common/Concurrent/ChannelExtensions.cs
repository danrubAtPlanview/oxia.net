// =====================================================================================================================
// PORT NOTE
//   Upstream : common/channel/noblock.go   (PushNoBlock)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : `select { case ch <- t: return true; default: return false }` -- push to a channel only if there is room.
//   Divergences: Go channel -> System.Threading.Channels.ChannelWriter<T>.  TryWrite already is the non-blocking push: it
//              returns false on a full bounded channel (and on a completed one, where Go would panic).
//              Other files of Go's common/channel (override_channel.go, read.go) are not used by the client.
// =====================================================================================================================
using System.Threading.Channels;

namespace Oxia.Common.Concurrent;

public static class ChannelExtensions
{
    /// <summary>Go: channel.PushNoBlock(ch, t) -- true if the item was written, false if the channel is full.</summary>
    public static bool PushNoBlock<T>(this ChannelWriter<T> writer, T item) => writer.TryWrite(item);
}
