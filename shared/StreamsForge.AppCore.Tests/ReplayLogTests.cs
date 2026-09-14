using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>Plan 026 wave 1 — the pure replay log behind every producer's attach gate.</summary>
public sealed class ReplayLogTests
{
    private static Dictionary<string, object?> Row(long i) => new() { ["id"] = i, ["_ts"] = 1_000 + i };

    private static ReplayLog<Dictionary<string, object?>> NewLog(int capacity = ReplayLog<Dictionary<string, object?>>.DefaultCapacity, long retainMs = 0, Func<long>? clock = null) =>
        new(r => (long)r["_ts"]!, r => new Dictionary<string, object?>(r), capacity, retainMs, clock);

    [Fact]
    public void Seq_is_monotonic_from_1_and_snapshot_from_seq_has_exact_boundaries()
    {
        var log = NewLog();
        for (var i = 1; i <= 500; i++) Assert.Equal(i, log.Append(Row(i)));

        var s = log.Snapshot(new ReplayFrom { Seq = 401 });
        Assert.Equal(100, s.Entries.Count);
        Assert.Equal(401, s.Entries[0].Seq);
        Assert.Equal(500, s.Entries[^1].Seq);
        Assert.Equal(1, s.FirstSeq);
        Assert.Equal(500, s.LastSeq);
        Assert.Equal(500, s.TotalSeen);
        Assert.False(s.Truncated);
        Assert.Empty(log.Snapshot(new ReplayFrom { Seq = 501 }).Entries);
    }

    [Fact]
    public void Snapshot_from_timestamp_filters_on_event_time_and_seq_wins_when_both_set()
    {
        var log = NewLog();
        for (var i = 1; i <= 500; i++) log.Append(Row(i));

        var byTs = log.Snapshot(new ReplayFrom { TimestampMs = 1_300 });
        Assert.Equal(201, byTs.Entries.Count);
        Assert.All(byTs.Entries, e => Assert.True(e.TimestampMs >= 1_300));

        var both = log.Snapshot(new ReplayFrom { Seq = 499, TimestampMs = 1_000 });
        Assert.Equal(2, both.Entries.Count);
    }

    [Fact]
    public void Count_eviction_keeps_the_newest_and_reports_truncation_honestly()
    {
        var log = NewLog(capacity: 10_000);
        for (var i = 1; i <= 10_005; i++) log.Append(Row(i));

        Assert.Equal(10_000, log.Count);
        Assert.Equal(6, log.FirstSeq);
        Assert.Equal(10_005, log.LastSeq);
        Assert.Equal(10_005, log.TotalSeen);

        Assert.True(log.Snapshot().Truncated);
        Assert.True(log.Snapshot(new ReplayFrom { Seq = 3 }).Truncated);
        Assert.False(log.Snapshot(new ReplayFrom { Seq = 6 }).Truncated);
        Assert.False(log.Snapshot(new ReplayFrom { Seq = 9_000 }).Truncated);
        Assert.True(log.Snapshot(new ReplayFrom { TimestampMs = 1_000 }).Truncated);
        Assert.False(log.Snapshot(new ReplayFrom { TimestampMs = 1_007 }).Truncated);
    }

    [Fact]
    public void Age_eviction_uses_the_wall_clock_not_event_time()
    {
        long now = 100_000;
        var log = NewLog(retainMs: 1_000, clock: () => now);
        log.Append(new Dictionary<string, object?> { ["id"] = 1, ["_ts"] = 1L }); // ancient event time
        now += 500;
        log.Append(new Dictionary<string, object?> { ["id"] = 2, ["_ts"] = 2L });
        Assert.Equal(2, log.Count); // event time did not evict anything

        now += 600; // first entry is now 1_100 ms old
        var s = log.Snapshot();
        Assert.Single(s.Entries);
        Assert.Equal(2, s.Entries[0].Seq);
        Assert.Equal(2, s.FirstSeq);
        Assert.True(s.Truncated);
    }

    [Fact]
    public void Empty_log_reports_first_seq_as_next_position()
    {
        var log = NewLog();
        Assert.Equal(1, log.FirstSeq);
        Assert.Equal(0, log.LastSeq);
        var s = log.Snapshot(new ReplayFrom { Seq = 1 });
        Assert.Empty(s.Entries);
        Assert.False(s.Truncated);
    }

    [Fact]
    public void Snapshot_entries_are_copies_on_both_sides()
    {
        var log = NewLog();
        var row = Row(1);
        log.Append(row);
        row["id"] = 99L; // producer mutates after append

        var a = log.Snapshot();
        a.Entries[0].Item["id"] = 42L; // consumer mutates its copy
        var b = log.Snapshot();
        Assert.Equal(1L, b.Entries[0].Item["id"]);
    }
}
