// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sequence_updates.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The subscription to the updates of a sequential key: it streams the highest sequence key of the partition's
//              shard to the caller, re-subscribing with an exponential backoff (1s initial interval) whenever the stream
//              fails, until either the caller's token is done or the client is closed, which closes the channel.
//              A server rejects a subscription (eg. the node is not the leader) as soon as it receives it, while an accepted
//              subscription gets no response until a sequence key exists: a stream that stayed open for longer than
//              AcceptedAfter (1s) was accepted, and only then does its failure reset the backoff.  The first response is
//              empty when no sequence key exists yet: it is not an update.
//   Divergences:
//      * ctx + context.AfterFunc(clientCtx, cancel) -> one linked CancellationTokenSource over both tokens.
//      * `chan string` (unbuffered) -> bounded Channel<string> of capacity 1 (DESIGN mapping rule); sending honours the token.
//      * getSequenceUpdates() returns an error in Go; here a failed attempt THROWS (the retry loop's contract), and io.EOF
//        is OxiaErrors.EndOfStream.  The fields are internal and settable because the Go tests build the struct by hand.
// =====================================================================================================================
using System.Diagnostics;
using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;

namespace Oxia.Client;

internal sealed class SequenceUpdates
{
    /// <summary>sequenceUpdatesAcceptedAfter.</summary>
    internal static readonly TimeSpan AcceptedAfter = TimeSpan.FromSeconds(1);

    internal string PrefixKey = "";
    internal string PartitionKey = "";
    internal Channel<string> Ch = null!;
    internal IShardManager ShardManager = null!;
    internal IRpcProvider RpcProvider = null!;

    internal CancellationToken Ctx;
    internal IBackOff Backoff = null!;
    internal ILogger Log = null!;

    internal SequenceUpdates() { }

    /// <summary>
    /// Go: newSequenceUpdates.  The subscription ends when either <paramref name="ctx"/> is done or the client is closed
    /// (<paramref name="clientCtx"/>); the returned reader completes then.
    /// </summary>
    public static ChannelReader<string> Create(CancellationToken ctx, CancellationToken clientCtx, string prefixKey,
        string partitionKey, IRpcProvider rpc, IShardManager sm)
    {
        // The subscription ends when either the caller's context is done or the client is closed
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx, clientCtx);
        var token = cts.Token;

        var su = new SequenceUpdates
        {
            PrefixKey = prefixKey,
            PartitionKey = partitionKey,
            Ch = Channel.CreateBounded<string>(new BoundedChannelOptions(1)
            {
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false,
            }),
            ShardManager = sm,
            RpcProvider = rpc,
            Ctx = token,
            Backoff = BackOffFactory.NewBackOffWithInitialInterval(token, TimeSpan.FromSeconds(1)),
            Log = OxiaLog.Create("oxia-get-sequence-updates"),
        };

        _ = Task.Run(async () =>
        {
            try
            {
                await su.GetSequenceUpdatesWithRetriesAsync().ConfigureAwait(false);
            }
            finally
            {
                cts.Dispose();   // Go: defer stop()
            }
        });

        return su.Ch.Reader;
    }

    internal async ValueTask GetSequenceUpdatesWithRetriesAsync()
    {
        try
        {
            await Retry.RetryNotifyAsync(GetSequenceUpdatesAsync, Backoff, OnRetry, Ctx).ConfigureAwait(false);
        }
        catch
        {
            // The retries ended (Go: `_ = backoff.RetryNotify(...)`)
        }

        // Signal that the background go-routine is now done
        Ch.Writer.TryComplete();
    }

    private void OnRetry(Exception err, TimeSpan duration)
    {
        // The stream fails with Canceled when the client connection is closing, eg. while the client itself is being closed
        if (!OxiaErrors.IsCanceled(err) && OxiaErrors.GrpcCode(err) != StatusCode.Cancelled)
        {
            Log.LogError(err, "Error while getting sequence updates prefix-key={PrefixKey} partition-key={PartitionKey} retry-after={RetryAfter}",
                PrefixKey, PartitionKey, duration);
        }
    }

    /// <summary>One subscription attempt (Go: getSequenceUpdates).  Ends by throwing the error that ends the stream.</summary>
    internal async ValueTask GetSequenceUpdatesAsync()
    {
        long shard = ShardManager.Get(PartitionKey);
        string leader = ShardManager.Leader(shard);

        IServerStreamingClient<GetSequenceUpdatesResponse> updates;
        try
        {
            updates = await RpcProvider.GetSequenceUpdates(Ctx, leader,
                new GetSequenceUpdatesRequest { Shard = shard, Key = PrefixKey }).ConfigureAwait(false);
        }
        catch when (Ctx.IsCancellationRequested)
        {
            throw new OperationCanceledException(Ctx);
        }

        long openedAt = Stopwatch.GetTimestamp();
        while (true)
        {
            GetSequenceUpdatesResponse? res;
            try
            {
                res = await updates.RecvAsync(Ctx).ConfigureAwait(false);
            }
            catch
            {
                // The stream gets created even if the server rejects the subscription.  Reset the backoff only when an
                // accepted one fails (eg. on a leader change), so a persistent rejection keeps escalating the retry delay.
                ResetIfAccepted(openedAt);
                throw;
            }

            if (res is null)
            {
                // io.EOF
                ResetIfAccepted(openedAt);
                throw OxiaErrors.ToThrowable(OxiaErrors.EndOfStream);
            }

            if (res.HighestSequenceKey.Length == 0)
            {
                // Ignore first response if there are no sequences for the key
                continue;
            }

            // select { case su.ch <- key: case <-su.ctx.Done(): return su.ctx.Err() }
            await Ch.Writer.WriteAsync(res.HighestSequenceKey, Ctx).ConfigureAwait(false);
        }
    }

    private void ResetIfAccepted(long openedAt)
    {
        if (Stopwatch.GetElapsedTime(openedAt) >= AcceptedAfter) Backoff.Reset();
    }
}
