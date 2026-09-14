using System.Text;
using StreamsForge.AppCore.Streaming;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>Plan 026 wave 3 D6 — the NDJSON segment tier: rotation, recovery, a torn tail, and the two
/// retention rules. Every test owns a temp directory and an injectable clock; nothing sleeps.</summary>
public sealed class ReplaySegmentLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(
        Path.GetTempPath(), "sf-replay-seg-" + Guid.NewGuid().ToString("n"));

    private long _now = 1_700_000_000_000;

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

    private ReplaySegmentLog Open(long segmentBytes = 8L * 1024 * 1024, long retainMs = 0, long maxBytes = 0) =>
        new(_dir, new ReplayOptions { SegmentBytes = segmentBytes, RetainMs = retainMs, MaxBytesPerStream = maxBytes }, () => _now);

    private static string Payload(long i, int pad = 0) =>
        $"{{\"i\":{i},\"pad\":\"{new string('x', pad)}\"}}";

    private static long DirBytes(string dir) =>
        Directory.GetFiles(dir, "*.ndjson").Sum(f => new FileInfo(f).Length);

    [Fact]
    public async Task Rotates_and_reads_from_a_position_across_segments()
    {
        var log = Open();
        for (var i = 1; i <= 25_000; i++)
        {
            log.Append(i, 5_000 + i, Payload(i, 360));
            if (i % 1_000 == 0) await log.FlushAsync();
        }

        await log.FlushAsync();

        Assert.Equal(25_000, log.LastSeq);
        Assert.Equal(1, log.FirstSeq);
        Assert.True(log.SegmentCount >= 2, $"expected >= 2 segments, got {log.SegmentCount}");

        var read = log.ReadFrom(12_345).ToList();
        Assert.Equal(12_656, read.Count);
        Assert.Equal(12_345, read[0].Seq);
        Assert.Equal(25_000, read[^1].Seq);
        for (var i = 0; i < read.Count; i++)
        {
            Assert.Equal(12_345 + i, read[i].Seq);
            Assert.Equal(5_000 + read[i].Seq, read[i].TimestampMs);
        }
    }

    [Fact]
    public async Task Reopening_recovers_the_positions_and_continues_appending()
    {
        var first = Open(segmentBytes: 4_000);
        for (var i = 1; i <= 300; i++)
        {
            first.Append(i, 7_000 + i, Payload(i, 40));
            if (i % 20 == 0) await first.FlushAsync();
        }

        await first.FlushAsync();
        var segments = first.SegmentCount;
        Assert.True(segments >= 2, $"expected the fixture to rotate, got {segments} segment(s)");
        var bytes = first.RetainedBytes;
        first.Dispose();

        var reopened = Open(segmentBytes: 4_000);
        Assert.Equal(300, reopened.LastSeq);
        Assert.Equal(1, reopened.FirstSeq);
        Assert.Equal(segments, reopened.SegmentCount);
        Assert.Equal(bytes, reopened.RetainedBytes);

        reopened.Append(reopened.LastSeq + 1, 7_301, Payload(301, 40));
        await reopened.FlushAsync();
        Assert.Equal(301, reopened.LastSeq);
        Assert.Equal(301, reopened.ReadFrom(1).Count());
        Assert.Equal(301, reopened.ReadFrom(301).Single().Seq);
    }

    [Fact]
    public async Task A_torn_last_line_is_truncated_away_and_the_next_append_is_readable()
    {
        var log = Open();
        for (var i = 1; i <= 50; i++) log.Append(i, 9_000 + i, Payload(i));
        await log.FlushAsync();
        log.Dispose();

        var segment = Directory.GetFiles(_dir, "*.ndjson").Single();
        await File.AppendAllTextAsync(segment, new string('!', 20), Encoding.UTF8); // crash mid-write: no newline

        var reopened = Open();
        Assert.Equal(50, reopened.LastSeq);
        Assert.Equal(50, reopened.ReadFrom(1).Count());

        reopened.Append(51, 9_051, Payload(51));
        await reopened.FlushAsync();

        var lines = await File.ReadAllLinesAsync(segment);
        Assert.Equal(51, lines.Length);
        Assert.DoesNotContain(lines, l => l.Contains('!'));
        Assert.Equal(51, reopened.ReadFrom(1).Count());
        Assert.Equal(51, reopened.ReadFrom(51).Single().Seq);
    }

    [Fact]
    public async Task Age_retention_drops_whole_segments_and_never_the_newest()
    {
        // segmentBytes tiny => one segment per flush.
        var log = Open(segmentBytes: 1, retainMs: 60_000);
        for (var batch = 0; batch < 4; batch++)
        {
            for (var i = 1; i <= 10; i++) log.Append(batch * 10 + i, _now, Payload(batch * 10 + i));
            await log.FlushAsync();
            _now += 5_000;
        }

        Assert.Equal(4, log.SegmentCount);
        Assert.Equal(1, log.FirstSeq);

        // Jump far enough that every segment but the current one is older than RetainMs.
        _now += 100_000;
        await log.FlushAsync();

        Assert.Equal(1, log.SegmentCount);
        Assert.Equal(31, log.FirstSeq);
        Assert.Equal(40, log.LastSeq);
        Assert.Equal(10, log.ReadFrom(1).Count()); // only what is still retained
        Assert.Single(Directory.GetFiles(_dir, "*.ndjson"));
    }

    [Fact]
    public async Task Byte_retention_keeps_the_newest_segments()
    {
        var log = Open(segmentBytes: 2_000, maxBytes: 8_000);
        for (var i = 1; i <= 2_000; i++)
        {
            log.Append(i, 1_000 + i, Payload(i, 60));
            if (i % 10 == 0) await log.FlushAsync();
        }

        await log.FlushAsync();

        Assert.True(log.RetainedBytes <= 8_000 + 2_000, $"retained {log.RetainedBytes}");
        Assert.Equal(DirBytes(_dir), log.RetainedBytes);
        Assert.True(log.SegmentCount >= 2, $"expected several newest segments kept, got {log.SegmentCount}");
        Assert.True(log.FirstSeq > 1, "byte retention should have dropped the oldest segment(s)");
        Assert.Equal(2_000, log.LastSeq);

        var read = log.ReadFrom(1).ToList();
        Assert.Equal(log.FirstSeq, read[0].Seq);
        Assert.Equal(2_000, read[^1].Seq);
    }

    [Fact]
    [Trait("Category", "Slow")]
    public async Task One_million_entries_stay_inside_the_byte_bound()
    {
        const long segmentBytes = 4L * 1024 * 1024;
        const long maxBytes = 64L * 1024 * 1024;
        var log = Open(segmentBytes: segmentBytes, maxBytes: maxBytes);

        for (var i = 1; i <= 1_000_000; i++)
        {
            log.Append(i, 1_000 + i, Payload(i));
            if (i % 10_000 == 0)
            {
                await log.FlushAsync();
                Assert.True(log.RetainedBytes <= maxBytes + segmentBytes,
                    $"disk bound broken at seq {i}: {log.RetainedBytes} bytes in {log.SegmentCount} segments");
            }
        }

        await log.FlushAsync();
        Assert.True(DirBytes(_dir) <= maxBytes + segmentBytes);
        Assert.True(log.SegmentCount <= (maxBytes / segmentBytes) + 2, $"segments {log.SegmentCount}");
        Assert.Equal(1_000_000, log.LastSeq);
        Assert.True(log.FirstSeq > 1);
    }
}
