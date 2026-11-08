// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/shard_manager_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 11, ported: 11, skipped: 0   (subtests: ReceiveFailsOnInvalidAssignments 2, FailsOnPermanentErrors 3,
//             RetriesPermanentErrorsAfterStart 3 -> [Theory] rows; KeySorting's loop over the 3 key sortings -> [Theory] rows)
//   Intent   : overlap detection; successors recording and the ShardsReplaced listener (called under the writer lock, before
//              the new map is visible); the Changed() broadcast; Get without a shard for the key; the leader of an unknown
//              shard; invalid assignments; Close while waiting to retry; key sorting; permanent errors only fail the creation;
//              the timeout of the initial wait stops the receive loop.
//   Adaptations:
//      * Go builds `&shardManagerImpl{...}` literals; the tests use the internal constructor (no receive loop started) and
//        call the internal Update / ReceiveAsync / ReceiveWithRecoveryAsync.
//      * `assert.False(t, s.TryRLock())` inside the listener -> `Assert.True(s.IsUpdateLockHeld)` (Monitor.IsEntered; a Monitor
//        is reentrant so a TryEnter would always succeed).
//      * `assert.PanicsWithValue(t, "shard not found", ...)` -> InvalidOperationException("shard not found").
//      * The Go `warningsHandler` (slog) is CapturingLogger injected into the shard manager.
//      * context.DeadlineExceeded -> OxiaErrors.IsDeadlineExceeded (TimeoutException).
//      * Verified against fakes (needs component B: WaitGroup / Retry / BackOff).
// =====================================================================================================================
using Xunit;
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Time;

namespace Oxia.Client.Tests.Internal;

public class ShardManagerTests
{
    private static HashRange HashRange(uint min, uint max) => ShardConvert.MakeHashRange(min, max);

    private static Shard ShardOf(long id, HashRange hashRange, string leader = "") => new(id, leader, hashRange);

    /// <summary>A shard manager that is not started, like the `&amp;shardManagerImpl{...}` literals of the Go tests.</summary>
    private static ShardManager NewManager(IShardStrategy? strategy = null, IRpcProvider? rpc = null, string serviceAddress = "",
        string ns = "default", ShardsReplacedListener? listener = null, Microsoft.Extensions.Logging.ILogger? logger = null) =>
        new(strategy ?? ShardStrategy.New(), rpc, serviceAddress, ns, TimeSpan.FromSeconds(10), listener, logger);

    private static async Task WithinAsync(Task task, TimeSpan timeout, string failure)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout, TestContext.Current.CancellationToken));
        Assert.True(ReferenceEquals(completed, task), failure);
        await task;
    }

    [Fact]
    public void TestOverlap()
    {
        (HashRange A, HashRange B, bool IsOverlap)[] items =
        [
            (HashRange(1, 2), HashRange(3, 6), false),
            (HashRange(1, 4), HashRange(3, 6), true),
            (HashRange(4, 5), HashRange(3, 6), true),
            (HashRange(5, 8), HashRange(3, 6), true),
            (HashRange(7, 8), HashRange(3, 6), false),
        ];
        foreach (var item in items)
        {
            Assert.Equal(ShardManager.Overlap(item.A, item.B), item.IsOverlap);
        }
    }

    /// <summary>Go: numericKeyStrategy -- hashes a key, a number, to itself.</summary>
    private sealed class NumericKeyStrategy : IShardStrategy
    {
        public Func<Shard, bool> Get(string key)
        {
            uint code = uint.Parse(key, System.Globalization.CultureInfo.InvariantCulture);
            return shard => shard.HashRange.MinInclusive <= code && code <= shard.HashRange.MaxInclusive;
        }
    }

    private static void AssertElementsMatch(long[] expected, long[]? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Order().ToArray(), actual.Order().ToArray());
    }

    [Fact]
    public void TestShardManagerSuccessors()
    {
        var replaced = new List<IReadOnlyDictionary<long, long[]>>();
        ShardManager? s = null;
        s = NewManager(new NumericKeyStrategy(), listener: r =>
        {
            // The new shard map is not visible yet
            Assert.True(s!.IsUpdateLockHeld);
            replaced.Add(r);
        });
        s.Update([
            ShardOf(0, HashRange(0, 99)),
            ShardOf(1, HashRange(100, 199)),
        ]);
        Assert.Null(s.GetSuccessors(0));
        Assert.Empty(replaced);

        // Shard 0 is split into shards 2 and 3
        s.Update([
            ShardOf(1, HashRange(100, 199)),
            ShardOf(2, HashRange(0, 49)),
            ShardOf(3, HashRange(50, 99)),
        ]);
        AssertElementsMatch([2, 3], s.GetSuccessors(0));
        Assert.Null(s.GetSuccessors(1));
        AssertSuccessor(s, 0, "10", 2);
        AssertSuccessor(s, 0, "60", 3);
        Assert.Single(replaced);
        AssertElementsMatch([2, 3], replaced[0][0]);

        // Shard 3 is split too, before the calls pending on shard 0 are rerouted:
        // they still go to shard 3 first, which reroutes them in turn, so they
        // keep their order
        s.Update([
            ShardOf(1, HashRange(100, 199)),
            ShardOf(2, HashRange(0, 49)),
            ShardOf(4, HashRange(50, 74)),
            ShardOf(5, HashRange(75, 99)),
        ]);
        AssertElementsMatch([2, 3], s.GetSuccessors(0));
        AssertElementsMatch([4, 5], s.GetSuccessors(3));
        AssertSuccessor(s, 0, "60", 3);
        AssertSuccessor(s, 3, "60", 4);
        AssertSuccessor(s, 3, "80", 5);
        Assert.False(s.GetSuccessor(1, "150", out _));
        Assert.Equal(2, replaced.Count);
        AssertElementsMatch([4, 5], replaced[1][3]);
    }

    private static void AssertSuccessor(ShardManager s, long shardId, string key, long expected)
    {
        bool ok = s.GetSuccessor(shardId, key, out long successor);
        Assert.True(ok);
        Assert.Equal(expected, successor);
    }

    [Fact]
    public void TestShardManagerChanged()
    {
        var s = NewManager();
        var changed = s.Changed();
        Assert.False(changed.IsCompleted, "the shard map has not changed");

        s.Update([ShardOf(0, HashRange(0, 99))]);
        Assert.True(changed.IsCompleted, "the shard map has changed");
        Assert.NotSame(changed, s.Changed());
    }

    [Fact]
    public void TestShardManagerGetPanicsWithoutShardForKey()
    {
        var s = NewManager(new NumericKeyStrategy());
        s.Update([ShardOf(0, HashRange(0, 99))]);
        Assert.Equal(0, s.Get("10"));
        var panic = Assert.Throws<InvalidOperationException>(() => s.Get("150"));
        Assert.Equal("shard not found", panic.Message);
    }

    // The requests for a shard missing from the shard map go to the service address.
    [Fact]
    public void TestShardManagerLeaderOfUnknownShard()
    {
        var s = NewManager(serviceAddress: "service:6648");
        s.Update([ShardOf(0, default, "leader:6648")]);
        Assert.Equal("leader:6648", s.Leader(0));
        Assert.Equal("service:6648", s.Leader(1));
    }

    [Theory]
    [InlineData("other-namespace")]
    [InlineData("unsupported-boundaries")]
    public async Task TestShardManagerReceiveFailsOnInvalidAssignments(string name)
    {
        var (assignments, ns, err) = name switch
        {
            "other-namespace" => (new NamespaceShardsAssignment(), "other", "namespace not found in shards assignments"),
            _ => (UnsupportedBoundaries(), "default", "shard 5 has an unsupported hash range boundary"),
        };

        var shardAssignments = new ShardAssignments();
        shardAssignments.Namespaces[ns] = assignments;
        var s = NewManager(rpc: new SingleAssignmentsRpcProvider(assignments: shardAssignments), ns: "default");
        try
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => s.ReceiveAsync(new ZeroBackOff()));
            Assert.Contains(err, ex.Message);
            Assert.Empty(s.GetAll());
        }
        finally
        {
            s.Dispose();
        }

        static NamespaceShardsAssignment UnsupportedBoundaries()
        {
            var a = new NamespaceShardsAssignment();
            a.Assignments.Add(new ShardAssignment { Shard = 5 });
            return a;
        }
    }

    // Closing the shard manager interrupts the wait before it retries to receive the assignments. It must stop without
    // reporting a failure: once assignments were received, reporting it blocks forever.
    [Fact]
    public async Task TestShardManagerCloseWhileWaitingToRetry()
    {
        var logs = new CapturingLogger();
        var s = NewManager(rpc: new FailingAssignmentsRpcProvider(
                new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "connection reset")), assignments: 1),
            logger: logs);
        var stopped = s.ReceiveWithRecoveryAsync();

        await WithinAsync(logs.Warned, TimeSpan.FromSeconds(10), "the failed stream was not retried");

        s.Dispose();
        await WithinAsync(stopped, TimeSpan.FromSeconds(10), "the shard manager kept receiving the assignments after it was closed");
    }

    // The shard manager reports the key sorting of the namespace that the server sends with the assignments, or none when an
    // older server does not send it.
    [Theory]
    [InlineData(KeySorting.Unknown)]
    [InlineData(KeySorting.Natural)]
    [InlineData(KeySorting.Hierarchical)]
    public async Task TestShardManagerKeySorting(KeySorting keySorting)
    {
        var provider = new SingleAssignmentsRpcProvider(keySorting);
        var sm = await ShardManager.CreateAsync(ShardStrategy.New(), provider, "localhost:6648", "default", TimeSpan.FromSeconds(10), null);
        Assert.Equal(keySorting, sm.KeySorting);
        sm.Dispose();
    }

    private static Exception PermanentError(string name) => name switch
    {
        "unauthenticated" => new RpcException(new Grpc.Core.Status(StatusCode.Unauthenticated, "token expired")),
        "permission-denied" => new RpcException(new Grpc.Core.Status(StatusCode.PermissionDenied, "oxia: unexpected authority")),
        _ => OxiaErrors.NamespaceNotFound,
    };

    // A shard manager fails to start right away on an error that a retry can't fix, and reports that error rather than a timeout.
    [Theory]
    [InlineData("unauthenticated")]
    [InlineData("permission-denied")]
    [InlineData("namespace-not-found")]
    public async Task TestNewShardManagerFailsOnPermanentErrors(string name)
    {
        var permanentErr = PermanentError(name);
        var provider = new FailingAssignmentsRpcProvider(permanentErr);
        var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ShardManager.CreateAsync(ShardStrategy.New(), provider, "localhost:6648", "default", TimeSpan.FromSeconds(10), null));
        Assert.True(OxiaErrors.Is(err, permanentErr));
        Assert.False(OxiaErrors.IsDeadlineExceeded(err));
    }

    // A shard manager that does not receive the initial assignments in time fails to start, and must stop requesting them.
    [Fact]
    public async Task TestNewShardManagerStopsReceivingOnTimeout()
    {
        var provider = new PendingAssignmentsRpcProvider();
        var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ShardManager.CreateAsync(ShardStrategy.New(), provider, "localhost:6648", "default", TimeSpan.FromMilliseconds(100), null));
        Assert.True(OxiaErrors.IsDeadlineExceeded(err)); // context.DeadlineExceeded

        await WithinAsync(provider.Canceled, TimeSpan.FromSeconds(10),
            "the shard manager kept requesting the assignments after it failed to start");
    }

    // The errors that fail the creation of the shard manager are retried once it has received the initial assignments.
    [Theory]
    [InlineData("unauthenticated")]
    [InlineData("permission-denied")]
    [InlineData("namespace-not-found")]
    public async Task TestShardManagerRetriesPermanentErrorsAfterStart(string name)
    {
        var err = PermanentError(name);
        var logs = new CapturingLogger();
        // The initial assignments and an update, then the error
        var s = NewManager(rpc: new FailingAssignmentsRpcProvider(err, assignments: 2), logger: logs);
        var stopped = s.ReceiveWithRecoveryAsync();

        await WithinAsync(logs.Warned, TimeSpan.FromSeconds(10), "the error was not retried");

        s.Dispose();
        await WithinAsync(stopped, TimeSpan.FromSeconds(10), "the shard manager kept receiving the assignments after it was closed");
    }
}
