using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;
using StreamsForge.Engine;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>Plan 026 wave 3 D6 — <see cref="ReplayLog{T}"/> over the disk tier: positions survive the
/// activation, a snapshot spans segments + ring with no gap at the boundary, and retention is honest.</summary>
public sealed class ReplayLogPersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(
        Path.GetTempPath(), "sf-replay-log-" + Guid.NewGuid().ToString("n"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // a leftover temp directory is not a test failure
        }
    }

    private static Dictionary<string, object?> Row(long i) => new() { ["id"] = i, ["_ts"] = 1_000 + i };

    private ReplayLog<Dictionary<string, object?>> Attached(
        int capacity = ReplayLog<Dictionary<string, object?>>.DefaultCapacity,
        long segmentBytes = 8L * 1024 * 1024,
        long maxBytes = 0)
    {
        var log = new ReplayLog<Dictionary<string, object?>>(
            r => (long)r["_ts"]!, r => new Dictionary<string, object?>(r), capacity);
        log.AttachPersistence(
            new ReplaySegmentLog(_dir, new ReplayOptions { SegmentBytes = segmentBytes, MaxBytesPerStream = maxBytes }),
            ReplayPayloadJson.Serialize,
            ReplayPayloadJson.Deserialize<Dictionary<string, object?>>);
        return log;
    }

    [Fact]
    public async Task A_fresh_log_over_an_existing_directory_resumes_the_positions_and_replays_them()
    {
        var previous = Attached();
        for (var i = 1; i <= 500; i++) previous.Append(Row(i));
        await previous.FlushAsync();

        var log = Attached();
        Assert.Equal(500, log.TotalSeen);
        Assert.Equal(500, log.LastSeq);
        Assert.Equal(1, log.FirstSeq);
        Assert.True(log.Persisted);

        var all = log.Snapshot(new ReplayFrom { Seq = 1 });
        Assert.Equal(500, all.Entries.Count);
        Assert.Equal(1, all.Entries[0].Seq);
        Assert.Equal(1L, Assert.IsType<long>(all.Entries[0].Item["id"]));
        Assert.Equal(1_001L, Assert.IsType<long>(all.Entries[0].Item["_ts"]));
        Assert.Equal(1_001, all.Entries[0].TimestampMs);
        Assert.Equal(500, all.Entries[^1].Seq);
        Assert.False(all.Truncated);

        Assert.Equal(100, log.Snapshot(new ReplayFrom { Seq = 401 }).Entries.Count);

        Assert.Equal(501, log.Append(Row(501)));
        await log.FlushAsync();
        var after = Attached();
        Assert.Equal(501, after.LastSeq);
        Assert.Equal(501, after.Snapshot(new ReplayFrom { Seq = 1 }).Entries.Count);
    }

    [Fact]
    public async Task A_snapshot_spanning_the_disk_and_the_ring_has_no_gap_or_duplicate()
    {
        var log = Attached(capacity: 100);
        for (var i = 1; i <= 1_000; i++) log.Append(Row(i));
        await log.FlushAsync();

        Assert.Equal(100, log.Count);
        var s = log.Snapshot(new ReplayFrom { Seq = 1 });
        Assert.Equal(1_000, s.Entries.Count);
        Assert.False(s.Truncated);
        for (var i = 0; i < s.Entries.Count; i++)
        {
            Assert.Equal(i + 1, s.Entries[i].Seq);
            Assert.Equal((long)(i + 1), s.Entries[i].Item["id"]);
        }
    }

    [Fact]
    public async Task Byte_retention_makes_a_snapshot_honestly_truncated_from_FirstSeq()
    {
        var log = Attached(capacity: 10, segmentBytes: 2_000, maxBytes: 6_000);
        for (var i = 1; i <= 3_000; i++)
        {
            log.Append(Row(i));
            if (i % 10 == 0) await log.FlushAsync();
        }

        await log.FlushAsync();

        Assert.True(log.FirstSeq > 1, "byte retention should have dropped the oldest segment(s)");
        Assert.True(log.RetainedBytes > 0);
        Assert.True(log.SegmentCount > 0);

        var s = log.Snapshot(new ReplayFrom { Seq = 1 });
        Assert.True(s.Truncated);
        Assert.Equal(log.FirstSeq, s.Entries[0].Seq);
        Assert.Equal(3_000, s.Entries[^1].Seq);
        Assert.Equal(3_000, s.TotalSeen);
    }

    [Fact]
    public void Payload_json_round_trips_row_primitives_to_the_same_clr_types()
    {
        var row = new Dictionary<string, object?>
        {
            ["i"] = 42L,
            ["d"] = 1.5d,
            ["s"] = "hello",
            ["b"] = true,
            ["n"] = null,
            ["_ts"] = 1_700_000_000_000L,
        };

        var back = ReplayPayloadJson.Deserialize<Dictionary<string, object?>>(ReplayPayloadJson.Serialize(row));
        Assert.Equal(42L, Assert.IsType<long>(back["i"]));
        Assert.Equal(1.5d, Assert.IsType<double>(back["d"]));
        Assert.Equal("hello", Assert.IsType<string>(back["s"]));
        Assert.True(Assert.IsType<bool>(back["b"]));
        Assert.Null(back["n"]);
        Assert.Equal(1_700_000_000_000L, Assert.IsType<long>(back["_ts"]));

        var record = ReplayPayloadJson.Deserialize<EventRecord>(
            ReplayPayloadJson.Serialize(new EventRecord(row) { ["_source"] = "trades" }));
        Assert.Equal(1_700_000_000_000L, record.Timestamp);
        Assert.Equal("trades", record.Source);
        Assert.Equal(1.5d, Assert.IsType<double>(record["d"]));

        var results = ReplayPayloadJson.Deserialize<List<ResultEnvelope>>(
            ReplayPayloadJson.Serialize(new List<ResultEnvelope> { new() { PipelineId = "p", Seq = 7, Row = row } }));
        Assert.Equal(42L, Assert.IsType<long>(results[0].Row["i"]));

        var deltas = ReplayPayloadJson.Deserialize<List<TableDeltaDto>>(
            ReplayPayloadJson.Serialize(new List<TableDeltaDto> { new() { Row = row, Weight = -1 } }));
        Assert.Equal(-1, deltas[0].Weight);
        Assert.Equal(1.5d, Assert.IsType<double>(deltas[0].Row["d"]));
    }
}
