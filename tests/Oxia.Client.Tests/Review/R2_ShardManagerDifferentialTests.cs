// =====================================================================================================================
// R2 review (fidelity) -- differential test of ShardManager against a literal port of Go's shard_manager.go (update,
// recordSuccessors, overlap, Get = linear predicate scan, GetSuccessor, GetSuccessors, Exists, Leader, Changed).
//
// Random shard layouts (uniform, clustered so that the bucket table of the snapshot degenerates, with gaps), followed by random
// sequences of server updates: splits, merges, arbitrary overlapping new shards, same-id leader changes, same-id range changes
// (which make the ranges overlap without any deletion, like Go), updates carrying the complete map in random order, and updates
// with several shards.  After every update the C# manager must agree with the reference on:
//   * Get(code) for every boundary (min-1, min, max, max+1), the extremes 0 and uint.MaxValue and random codes -- including
//     "no shard" (Go: panic("shard not found"));
//   * GetAll / Exists / Leader;
//   * the replaced map handed to the listener, GetSuccessors and GetSuccessor of every shard ever removed;
//   * Changed(): the task of the previous map is completed, the new one is not.
// Go iterates a map in random order, so where ranges overlap the reference accepts ANY of the matching shards.
// =====================================================================================================================
using System.Globalization;
using Oxia.Client.Internal;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R2_ShardManagerDifferentialTests
{
    /// <summary>A literal port of the Go data structure and algorithm.</summary>
    private sealed class GoModel
    {
        public readonly Dictionary<long, Shard> Shards = new();
        public readonly Dictionary<long, List<Shard>> Successors = new();

        /// <summary>Go: the argument of onShardsReplaced (null: not invoked).</summary>
        public Dictionary<long, List<long>>? Replaced;

        public void Update(IReadOnlyList<Shard> updates)
        {
            var removed = new List<Shard>();
            foreach (var update in updates)
            {
                if (!Shards.ContainsKey(update.Id))
                {
                    foreach (var (shardId, existing) in Shards.ToArray())
                    {
                        if (Overlap(update.HashRange, existing.HashRange))
                        {
                            Shards.Remove(shardId);
                            removed.Add(existing);
                        }
                    }
                }

                Shards[update.Id] = update;
            }

            Replaced = null;
            if (removed.Count > 0)
            {
                Replaced = new Dictionary<long, List<long>>();
                foreach (var removedShard in removed)
                {
                    var successors = new List<Shard>();
                    var ids = new List<long>();
                    foreach (var shard in Shards.Values)
                    {
                        if (Overlap(shard.HashRange, removedShard.HashRange))
                        {
                            successors.Add(shard);
                            ids.Add(shard.Id);
                        }
                    }

                    Successors[removedShard.Id] = successors;
                    Replaced[removedShard.Id] = ids;
                }
            }
        }

        public static bool Overlap(HashRange a, HashRange b) => a.MinInclusive <= b.MaxInclusive && a.MaxInclusive >= b.MinInclusive;

        /// <summary>Go: the shards the predicate of Get(key) accepts.</summary>
        public List<long> Matches(uint code) =>
            Shards.Values.Where(s => s.HashRange.MinInclusive <= code && code <= s.HashRange.MaxInclusive).Select(s => s.Id).ToList();

        public List<long> SuccessorMatches(long removedId, uint code) =>
            Successors.TryGetValue(removedId, out var list)
                ? list.Where(s => s.HashRange.MinInclusive <= code && code <= s.HashRange.MaxInclusive).Select(s => s.Id).ToList()
                : [];
    }

    private static string KeyOf(uint code) => code.ToString(CultureInfo.InvariantCulture);

    private static ShardManager NewManager(Action<IReadOnlyDictionary<long, long[]>> onReplaced) =>
        new(new ShardStrategy(static key => uint.Parse(key, CultureInfo.InvariantCulture)), null, "service:6648", "default",
            TimeSpan.FromSeconds(10), replaced => onReplaced(replaced));

    // ----- random layouts -------------------------------------------------------------------------------------------

    private static List<uint> Cuts(Random rng, int count, bool clustered)
    {
        var set = new SortedSet<uint> { 0 };
        uint windowStart = (uint)rng.NextInt64(0, uint.MaxValue - 1_000_000L);
        while (set.Count < count)
        {
            uint cut = clustered && rng.Next(10) != 0
                ? windowStart + (uint)rng.Next(0, 1_000_000)
                : (uint)rng.NextInt64(0, uint.MaxValue + 1L);
            set.Add(cut);
        }

        return [.. set];
    }

    /// <summary>Partitions [lo, hi] into at most <paramref name="parts"/> contiguous ranges.</summary>
    private static List<HashRange> Partition(Random rng, uint lo, uint hi, int parts, bool clustered)
    {
        var cuts = new SortedSet<uint> { lo };
        int guard = 0;
        while (cuts.Count < parts && guard++ < parts * 8 && hi > lo)
        {
            cuts.Add(clustered && rng.Next(10) != 0
                ? (uint)Math.Min((ulong)hi, (ulong)lo + (ulong)rng.Next(0, 1_000_000))
                : (uint)rng.NextInt64(lo, (long)hi + 1));
        }

        var list = cuts.ToList();
        var ranges = new List<HashRange>();
        for (int i = 0; i < list.Count; i++)
        {
            uint min = list[i];
            uint max = i + 1 < list.Count ? list[i + 1] - 1 : hi;
            ranges.Add(new HashRange(min, max));
        }

        return ranges;
    }

    private sealed class Run
    {
        public readonly Random Rng;
        public readonly GoModel Model = new();
        public readonly ShardManager Sut;
        public readonly List<Dictionary<long, long[]>> ListenerCalls = [];
        public readonly HashSet<long> EverRemoved = [];
        public long NextId;
        public int Leaders;
        public string Seed;

        public Run(int seed)
        {
            Seed = $"seed {seed}";
            Rng = new Random(seed);
            NextId = Rng.Next(0, 3) == 0 ? -1000 : Rng.Next(0, 100_000);
            Sut = NewManager(replaced => ListenerCalls.Add(replaced.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray())));
        }

        public long FreshId() => NextId += Rng.Next(1, 40);
        public string FreshLeader() => $"leader-{Leaders++}";
    }

    private static void ApplyAndCompare(Run run, List<Shard> updates, string what)
    {
        var changedBefore = run.Sut.Changed();
        Assert.False(changedBefore.IsCompleted);
        int callsBefore = run.ListenerCalls.Count;

        run.Model.Update(updates);
        run.Sut.Update(updates);

        string ctx = $"{run.Seed}, step '{what}', {run.Model.Shards.Count} shards";

        // Changed(): replaced by every update, the previous one completed
        Assert.True(changedBefore.IsCompleted, $"{ctx}: the previous Changed task is not completed");
        var changedNow = run.Sut.Changed();
        Assert.NotSame(changedBefore, changedNow);
        Assert.False(changedNow.IsCompleted, ctx);

        // listener
        if (run.Model.Replaced is null)
        {
            Assert.True(run.ListenerCalls.Count == callsBefore, $"{ctx}: the listener must not be invoked without removals");
        }
        else
        {
            Assert.True(run.ListenerCalls.Count == callsBefore + 1, $"{ctx}: the listener must be invoked exactly once");
            var actual = run.ListenerCalls[^1];
            Assert.Equal(run.Model.Replaced.Keys.Order(), actual.Keys.Order());
            foreach (var (id, ids) in run.Model.Replaced)
            {
                Assert.True(ids.Order().SequenceEqual(actual[id].Order()), $"{ctx}: replaced[{id}] differs");
                run.EverRemoved.Add(id);
            }
        }

        // membership
        Assert.Equal(run.Model.Shards.Keys.Order(), run.Sut.GetAll().Order());
        foreach (var (id, shard) in run.Model.Shards)
        {
            Assert.True(run.Sut.Exists(id), ctx);
            Assert.Equal(shard.Leader, run.Sut.Leader(id));
        }

        foreach (long removed in run.EverRemoved)
        {
            if (!run.Model.Shards.ContainsKey(removed)) Assert.False(run.Sut.Exists(removed), $"{ctx}: removed shard {removed} still exists");
            Assert.Equal("service:6648", run.Sut.Leader(long.MinValue + 7)); // unknown shard
        }

        // Get at the interesting codes
        var codes = new HashSet<uint> { 0, 1, uint.MaxValue, uint.MaxValue - 1, 0x7FFFFFFF, 0x80000000 };
        foreach (var s in run.Model.Shards.Values)
        {
            var (min, max) = (s.HashRange.MinInclusive, s.HashRange.MaxInclusive);
            codes.Add(min);
            codes.Add(max);
            if (min > 0) codes.Add(min - 1);
            if (max < uint.MaxValue) codes.Add(max + 1);
            if (min <= max) codes.Add(min + (max - min) / 2);
        }

        for (int i = 0; i < 200; i++) codes.Add((uint)run.Rng.NextInt64(0, uint.MaxValue + 1L));
        // codes that fall inside the tiny clustered windows
        foreach (var s in run.Model.Shards.Values.Take(5)) codes.Add(s.HashRange.MinInclusive + (uint)run.Rng.Next(0, 4));

        foreach (uint code in codes)
        {
            var expected = run.Model.Matches(code);
            if (expected.Count == 0)
            {
                var ex = Assert.Throws<InvalidOperationException>(() => run.Sut.Get(KeyOf(code)));
                Assert.Equal("shard not found", ex.Message);
            }
            else
            {
                long actual = run.Sut.Get(KeyOf(code));
                Assert.True(expected.Contains(actual), $"{ctx}: Get({code}) = {actual}, Go accepts {string.Join(",", expected)}");
            }
        }

        // successors of everything that was ever removed
        foreach (long removed in run.EverRemoved)
        {
            var model = run.Model.Successors[removed];
            var actual = run.Sut.GetSuccessors(removed);
            Assert.NotNull(actual);
            Assert.True(model.Select(s => s.Id).Order().SequenceEqual(actual.Order()), $"{ctx}: GetSuccessors({removed}) differs");

            foreach (uint code in codes.Take(40))
            {
                var expected = run.Model.SuccessorMatches(removed, code);
                bool ok = run.Sut.GetSuccessor(removed, KeyOf(code), out long actualId);
                Assert.Equal(expected.Count > 0, ok);
                if (ok) Assert.True(expected.Contains(actualId), $"{ctx}: GetSuccessor({removed}, {code})");
            }
        }

        Assert.Null(run.Sut.GetSuccessors(long.MaxValue - 3)); // never removed
    }

    // ----- scenarios ------------------------------------------------------------------------------------------------

    private static List<Shard> InitialLayout(Run run, int count, bool clustered, bool withGaps)
    {
        var ranges = new List<HashRange>();
        if (clustered)
        {
            var cuts = Cuts(run.Rng, count, clustered: true);
            for (int i = 0; i < cuts.Count; i++)
                ranges.Add(new HashRange(cuts[i], i + 1 < cuts.Count ? cuts[i + 1] - 1 : uint.MaxValue));
        }
        else
        {
            ranges = Partition(run.Rng, 0, uint.MaxValue, count, clustered: false);
        }

        var shards = new List<Shard>();
        foreach (var range in ranges)
        {
            if (withGaps && run.Rng.Next(6) == 0) continue;
            shards.Add(new Shard(run.FreshId(), run.FreshLeader(), range));
        }

        if (shards.Count == 0) shards.Add(new Shard(run.FreshId(), run.FreshLeader(), new HashRange(0, uint.MaxValue)));
        // the server does not promise any order
        for (int i = shards.Count - 1; i > 0; i--)
        {
            int j = run.Rng.Next(i + 1);
            (shards[i], shards[j]) = (shards[j], shards[i]);
        }

        return shards;
    }

    private static void RandomStep(Run run)
    {
        var current = run.Model.Shards.Values.ToList();
        int kind = run.Rng.Next(10);
        if (current.Count == 0) kind = 99;

        switch (kind)
        {
            case 0:
            case 1:
            case 2: // split one shard in 2..4, the update carries only the children
            {
                var victim = current[run.Rng.Next(current.Count)];
                if (victim.HashRange.MinInclusive >= victim.HashRange.MaxInclusive) goto case 5;
                var parts = Partition(run.Rng, victim.HashRange.MinInclusive, victim.HashRange.MaxInclusive, run.Rng.Next(2, 5), false);
                ApplyAndCompare(run, parts.Select(r => new Shard(run.FreshId(), run.FreshLeader(), r)).ToList(), "split (children only)");
                break;
            }

            case 3: // split, the update carries the complete map in random order (what the server does)
            {
                var victim = current[run.Rng.Next(current.Count)];
                var updates = current.Where(s => s.Id != victim.Id).ToList();
                if (victim.HashRange.MinInclusive < victim.HashRange.MaxInclusive)
                {
                    foreach (var r in Partition(run.Rng, victim.HashRange.MinInclusive, victim.HashRange.MaxInclusive, run.Rng.Next(2, 4), false))
                        updates.Add(new Shard(run.FreshId(), run.FreshLeader(), r));
                }
                else
                {
                    updates.Add(victim);
                }

                Shuffle(run.Rng, updates);
                ApplyAndCompare(run, updates, "split (complete map)");
                break;
            }

            case 4: // merge two or three neighbours into a new shard
            {
                var sorted = current.OrderBy(s => s.HashRange.MinInclusive).ToList();
                int at = run.Rng.Next(sorted.Count);
                int take = Math.Min(sorted.Count - at, run.Rng.Next(2, 4));
                var group = sorted.GetRange(at, take);
                var range = new HashRange(group.Min(s => s.HashRange.MinInclusive), group.Max(s => s.HashRange.MaxInclusive));
                ApplyAndCompare(run, [new Shard(run.FreshId(), run.FreshLeader(), range)], "merge");
                break;
            }

            case 5: // a new id on an arbitrary range: deletes everything it overlaps
            {
                uint a = (uint)run.Rng.NextInt64(0, uint.MaxValue + 1L);
                uint b = (uint)run.Rng.NextInt64(0, uint.MaxValue + 1L);
                if (run.Rng.Next(3) == 0) b = a + (uint)Math.Min((ulong)(uint.MaxValue - a), (ulong)run.Rng.Next(0, 5000));
                var range = new HashRange(Math.Min(a, b), Math.Max(a, b));
                ApplyAndCompare(run, [new Shard(run.FreshId(), run.FreshLeader(), range)], "new overlapping shard");
                break;
            }

            case 6: // leader change: same id, same range
            {
                var s = current[run.Rng.Next(current.Count)];
                ApplyAndCompare(run, [s with { Leader = run.FreshLeader() }], "leader change");
                break;
            }

            case 7: // same id, other range: Go replaces it without looking at the neighbours (ranges may overlap now)
            {
                var s = current[run.Rng.Next(current.Count)];
                uint min = s.HashRange.MinInclusive;
                uint max = s.HashRange.MaxInclusive;
                if (run.Rng.Next(2) == 0) min = (uint)Math.Max(0L, (long)min - run.Rng.Next(0, 50_000));
                else max = (uint)Math.Min((long)uint.MaxValue, (long)max + run.Rng.Next(0, 50_000));
                ApplyAndCompare(run, [s with { HashRange = new HashRange(min, max) }], "same id, other range");
                break;
            }

            case 8: // several shards in one update, overlapping each other (later ones delete the earlier ones)
            {
                var list = new List<Shard>();
                for (int i = 0; i < run.Rng.Next(2, 6); i++)
                {
                    uint a = (uint)run.Rng.NextInt64(0, uint.MaxValue + 1L);
                    list.Add(new Shard(run.FreshId(), run.FreshLeader(),
                        new HashRange(a, a + (uint)Math.Min((ulong)(uint.MaxValue - a), (ulong)run.Rng.Next(0, 200_000_000)))));
                }

                ApplyAndCompare(run, list, "several overlapping shards in one update");
                break;
            }

            default: // re-send the complete current map (nothing changes)
            {
                Shuffle(run.Rng, current);
                ApplyAndCompare(run, current.Count == 0 ? [new Shard(run.FreshId(), run.FreshLeader(), new HashRange(0, uint.MaxValue))] : current, "resend");
                break;
            }
        }
    }

    private static void Shuffle<T>(Random rng, List<T> list)
    {
        for (int i = list.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }

    [Theory]
    [InlineData(1, false, false)]
    [InlineData(4, false, false)]
    [InlineData(5, false, false)] // first size that builds the bucket table
    [InlineData(16, false, false)]
    [InlineData(64, false, true)]
    [InlineData(300, true, false)] // clustered: many shards per bucket -> the bucket walk gives up and binary searches
    [InlineData(3000, true, true)]
    [InlineData(5000, false, false)] // more shards than buckets (the table is capped at 4096 entries)
    public void GetAndSuccessorsAgreeWithGoOnRandomLayoutsAndUpdateSequences(int shardCount, bool clustered, bool withGaps)
    {
        int seeds = shardCount >= 3000 ? 2 : 40;
        int steps = shardCount >= 3000 ? 5 : 25;
        for (int seed = 0; seed < seeds; seed++)
        {
            var run = new Run(seed * 7919 + shardCount);
            ApplyAndCompare(run, InitialLayout(run, shardCount, clustered, withGaps), "initial");
            for (int step = 0; step < steps; step++) RandomStep(run);
        }
    }

    // Boundary exhaustiveness on a small map: every code around every boundary, against the reference.
    [Fact]
    public void GetAtEveryBoundaryOfLayoutsWithSharedBucketsAndUint32Extremes()
    {
        foreach (int n in new[] { 5, 6, 9, 17, 33 })
        {
            var run = new Run(1000 + n);
            // shards of width 1 around the extremes plus a few wide ones
            var shards = new List<Shard>();
            shards.Add(new Shard(run.FreshId(), "a", new HashRange(0, 0)));
            shards.Add(new Shard(run.FreshId(), "a", new HashRange(1, 1)));
            uint cursor = 2;
            for (int i = 0; i < n; i++)
            {
                uint width = (uint)run.Rng.Next(1, 700_000_000 / n);
                shards.Add(new Shard(run.FreshId(), "a", new HashRange(cursor, cursor + width - 1)));
                cursor += width;
            }

            shards.Add(new Shard(run.FreshId(), "a", new HashRange(uint.MaxValue - 1, uint.MaxValue - 1)));
            shards.Add(new Shard(run.FreshId(), "a", new HashRange(uint.MaxValue, uint.MaxValue)));
            ApplyAndCompare(run, shards, "extremes");
        }
    }
}
