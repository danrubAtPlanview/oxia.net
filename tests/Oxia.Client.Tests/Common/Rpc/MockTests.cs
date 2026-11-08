// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/mock_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 11, ported: 11, skipped: 0.
//   Divergences (see Mock.cs): `assert.ErrorIs(err, context.Canceled)` -> OperationCanceledException; `assert.ErrorIs(err, io.EOF)` ->
//              RecvAsync returns null; `assert.Panics` -> NotImplementedException; `make(chan T)` (unbuffered) -> bounded capacity 1;
//              `cap(ch)` -> the Capacity constants of the mock types; `assert.Same` -> Assert.Same.
// =====================================================================================================================
using System.Threading.Channels;
using Grpc.Core;
using G = Io.Oxia.Proto.V1;
using R = Replication;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class MockTests
{
    [Fact]
    public async Task TestMockServerReplicateStreamSendAfterCancel()
    {
        var stream = new MockServerReplicateStream();
        // With the responses buffer full, only the cancellation can unblock Send
        for (var i = 0; i < MockServerReplicateStream.Capacity; i++)
        {
            await stream.SendAsync(new R.Ack());
        }

        stream.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.SendAsync(new R.Ack()));
    }

    [Fact]
    public async Task TestMockRpcClientCloseCancelsStreams()
    {
        var client = new MockRpcClient();
        var stream = client.GetReplicateStream(CancellationToken.None, "", "", 0, 0);

        client.Close();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.RecvAsync());
    }

    [Fact]
    public async Task TestMockReplicateStreamSendAfterCloseSend()
    {
        var stream = new MockReplicateStream(CancellationToken.None, Channel.CreateBounded<R.Append>(1), Channel.CreateBounded<R.Ack>(1));
        stream.CloseSend();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.SendAsync(new R.Append()));
    }

    [Fact]
    public async Task TestMockReplicateStreamRecvClosedChannel()
    {
        var recv = Channel.CreateBounded<R.Ack>(1);
        recv.Writer.Complete();
        var stream = new MockReplicateStream(CancellationToken.None, Channel.CreateBounded<R.Append>(1), recv);

        var ack = await stream.RecvAsync();
        Assert.Null(ack); // io.EOF
    }

    [Fact]
    public void TestMockReplicateStreamCloseSendWithoutContext()
    {
        var stream = new MockReplicateStream();
        stream.CloseSend(); // assert.NoError
    }

    [Fact]
    public async Task TestMockShardAssignmentControllerStreamSendAndClose()
    {
        var stream = new MockShardAssignmentControllerStream();
        var response = new R.CoordinationShardAssignmentsResponse();

        await stream.SendAndCloseAsync(response);
        Assert.Same(response, await stream.GetResponseAsync());
    }

    [Fact]
    public async Task TestMockServerSendSnapshotStreamSendAndClose()
    {
        var stream = new MockServerSendSnapshotStream();
        var response = new R.SnapshotResponse { AckOffset = 5 };

        await stream.SendAndCloseAsync(response);
        Assert.Same(response, await stream.GetResponseAsync());
    }

    [Fact]
    public async Task TestMockSendSnapshotClientStreamSendAfterCloseSend()
    {
        var stream = new MockSendSnapshotClientStream(CancellationToken.None);
        // With the requests buffer full, only the cancellation can unblock Send
        for (var i = 0; i < MockSendSnapshotClientStream.RequestsCapacity; i++)
        {
            await stream.SendAsync(new R.SnapshotChunk());
        }

        stream.CloseSend();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.SendAsync(new R.SnapshotChunk()));
    }

    [Fact]
    public void TestMockBaseUnimplementedMethodsPanic()
    {
        var baseStream = new MockBase();
        Assert.Throws<NotImplementedException>(() => baseStream.SendHeader(new Metadata()));
        Assert.Throws<NotImplementedException>(() => baseStream.RecvMsg(null));
        Assert.Throws<NotImplementedException>(() => baseStream.SendMsg(null));
        Assert.Throws<NotImplementedException>(() => baseStream.SetTrailer(new Metadata()));
        Assert.Throws<NotImplementedException>(() => baseStream.Header());
        Assert.Throws<NotImplementedException>(() => baseStream.Trailer());
    }

    [Fact]
    public void TestMockBaseSetHeader()
    {
        var baseStream = new MockBase();
        var md = new Metadata { { "key", "value" } };

        baseStream.SetHeader(md);
        Assert.Same(md, baseStream.Md);
    }

    [Fact]
    public void TestMockBaseContextDefaultsToBackground()
    {
        Assert.Equal(CancellationToken.None, new MockBase().Context);
    }

    // ----- extra tests (not in Go) --------------------------------------------------------------------------------------
    [Fact]
    public async Task TheMockServerStreamRoundTripsRequestsAndResponses()
    {
        var stream = new MockServerReplicateStream();
        var append = new R.Append { Term = 3 };
        await stream.AddRequestAsync(append);
        Assert.Same(append, await stream.RecvAsync());

        var ack = new R.Ack { Offset = 9 };
        await stream.SendAsync(ack);
        Assert.Same(ack, await stream.GetResponseAsync());
    }

    [Fact]
    public async Task TheClientDoublesShareTheirChannels()
    {
        var client = new MockRpcClient();
        var stream = client.GetReplicateStream(CancellationToken.None, "ns", "node", 1, 0);
        var append = new R.Append { Term = 1 };

        await stream.SendAsync(append);
        Assert.Same(append, await client.AppendReqs.Reader.ReadAsync());

        var ack = new R.Ack { Offset = 4 };
        await client.AckResps.Writer.WriteAsync(ack);
        Assert.Same(ack, await stream.RecvAsync());

        var truncate = new R.TruncateRequest { Term = 2 };
        var pending = client.TruncateAsync("node", truncate).AsTask();
        Assert.Same(truncate, await client.TruncateReqs.Reader.ReadAsync());
        var response = new R.TruncateResponse { HeadEntryId = new R.EntryId { Offset = 7 } };
        await client.TruncateRespsChannel.Writer.WriteAsync(new TruncateResps { Response = response });
        Assert.Same(response, await pending);

        var failing = client.TruncateAsync("node", truncate).AsTask();
        await client.TruncateRespsChannel.Writer.WriteAsync(new TruncateResps { Error = new InvalidOperationException("failed") });
        await Assert.ThrowsAsync<InvalidOperationException>(() => failing);
    }

    [Fact]
    public async Task TheStreamContextFollowsTheParent()
    {
        using var parent = new CancellationTokenSource();
        var stream = new MockReplicateStream(parent.Token, Channel.CreateBounded<R.Append>(1), Channel.CreateBounded<R.Ack>(1));
        Assert.False(stream.Context.IsCancellationRequested);

        parent.Cancel();
        Assert.True(stream.Context.IsCancellationRequested);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.RecvAsync());
    }

    [Fact]
    public async Task CloseAndRecvCancelsClosesRequestsAndReturnsTheResponse()
    {
        var stream = new MockSendSnapshotClientStream(CancellationToken.None);
        var response = new R.SnapshotResponse { AckOffset = 1 };
        await stream.Response.Writer.WriteAsync(response);

        Assert.Same(response, await stream.CloseAndRecvAsync());
        Assert.True(stream.Context.IsCancellationRequested);
        Assert.False(stream.Requests.Writer.TryWrite(new R.SnapshotChunk())); // closed
    }

    [Fact]
    public async Task TheClientSideShardAssignmentStreamQueuesResponses()
    {
        var stream = new MockShardAssignmentClientStream();
        var assignments = new G.ShardAssignments();
        await stream.SendAsync(assignments);
        Assert.Same(assignments, await stream.GetResponseAsync());

        stream.Cancel();
        Assert.True(stream.Context.IsCancellationRequested);
    }

    [Fact]
    public async Task TheControllerAndServerSnapshotStreamsQueueRequests()
    {
        var controller = new MockShardAssignmentControllerStream();
        var assignments = new G.ShardAssignments();
        await controller.AddRequestAsync(assignments);
        Assert.Same(assignments, await controller.RecvAsync());

        var snapshot = new MockServerSendSnapshotStream();
        var chunk = new R.SnapshotChunk { Name = "c" };
        await snapshot.AddChunkAsync(chunk);
        Assert.Same(chunk, await snapshot.RecvAsync());
    }
}
