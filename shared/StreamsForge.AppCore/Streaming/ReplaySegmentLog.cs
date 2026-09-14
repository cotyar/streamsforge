namespace StreamsForge.AppCore.Streaming;

/// <summary>Plan 026 D6 — host-level defaults for the disk tier (<c>Replay:*</c> in configuration); a
/// definition's own <c>replay.retainMs</c>/<c>replay.maxBytes</c> override per entity when non-zero.</summary>
public sealed class ReplayOptions
{
    /// <summary>Root directory of every stream's segments; the host defaults it to <c>DataDir/replay</c>.</summary>
    public string? Directory { get; set; }

    /// <summary>Segment rotation size. Default 8 MB.</summary>
    public long SegmentBytes { get; set; } = 8L * 1024 * 1024;

    /// <summary>Age retention for persisted entries; 0 = none (bytes only).</summary>
    public long RetainMs { get; set; }

    /// <summary>Byte retention per stream; whole segments are deleted oldest first past it. Default 256 MB.</summary>
    public long MaxBytesPerStream { get; set; } = 256L * 1024 * 1024;
}

/// <summary>One persisted entry: the producer position, the item's event time, the wall clock when it
/// was appended (age retention keys on THIS, never on event time — see <see cref="ReplayLog{T}"/>), and
/// the item as JSON.</summary>
public readonly record struct ReplaySegmentEntry(long Seq, long TimestampMs, long AppendedAtMs, string PayloadJson);

/// <summary>Plan 026 D6 — the disk tier under <see cref="ReplayLog{T}"/>: append-only NDJSON segments
/// (<c>NNNNNN.ndjson</c>, one <see cref="ReplaySegmentEntry"/> per line as
/// <c>{"seq":…,"ts":…,"at":…,"payload":…}</c>) in one directory per stream, rotated at
/// <see cref="ReplayOptions.SegmentBytes"/>, retained by age and by bytes (WHOLE segments only, oldest
/// first — a segment is dropped when its newest entry is older than <c>RetainMs</c>, or while the
/// directory exceeds <c>MaxBytes</c>). Pure: no grains, no DI, one owner (the gate's grain) at a time.
///
/// <para>Durability contract, deliberately Batched-shaped: <see cref="Append"/> buffers, <see cref="FlushAsync"/>
/// writes (the owning grain calls it on its own flush cadence and on stop/deactivate); a crash loses
/// what was not yet flushed and NOTHING else. Opening a directory recovers <see cref="LastSeq"/> from the
/// last COMPLETE line of the newest segment; a torn last line (a crash mid-write) is ignored on read and
/// truncated away before the next append, never an error.</para>
///
/// <para>Pinned by the orchestrator for plan 026 wave 3; agent A implements the bodies and owns
/// <c>ReplaySegmentLogTests</c>.</para></summary>
public sealed class ReplaySegmentLog : IDisposable
{
    private readonly string _directory;
    private readonly ReplayOptions _options;
    private readonly Func<long> _clock;

    /// <param name="directory">This stream's own directory (created if missing).</param>
    /// <param name="options">Effective limits for THIS stream (the caller has already applied the
    /// definition's overrides on top of the host defaults).</param>
    /// <param name="clock">Epoch ms, injectable for tests.</param>
    public ReplaySegmentLog(string directory, ReplayOptions options, Func<long>? clock = null)
    {
        _directory = directory;
        _options = options;
        _clock = clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        throw new NotImplementedException("plan 026 wave 3 agent A");
    }

    public string Directory => _directory;

    /// <summary>Oldest retained position, or 0 when nothing is retained.</summary>
    public long FirstSeq => throw new NotImplementedException("plan 026 wave 3 agent A");

    /// <summary>Newest position ever written (recovered on open; advanced by <see cref="Append"/> even before
    /// the flush), or 0 when the directory is empty.</summary>
    public long LastSeq => throw new NotImplementedException("plan 026 wave 3 agent A");

    /// <summary>Bytes on disk across retained segments (the buffered, unflushed tail excluded).</summary>
    public long RetainedBytes => throw new NotImplementedException("plan 026 wave 3 agent A");

    public int SegmentCount => throw new NotImplementedException("plan 026 wave 3 agent A");

    /// <summary>Buffers one entry. <paramref name="seq"/> must be <c>LastSeq + 1</c>.</summary>
    public void Append(long seq, long timestampMs, string payloadJson) =>
        throw new NotImplementedException("plan 026 wave 3 agent A");

    /// <summary>Writes the buffered tail, rotates when the current segment passed <c>SegmentBytes</c>, then
    /// applies age and byte retention. Single-flight: a call while one is in progress returns the same task.</summary>
    public Task FlushAsync() => throw new NotImplementedException("plan 026 wave 3 agent A");

    /// <summary>Every retained entry with <c>Seq &gt;= fromSeq</c>, oldest first, flushed AND buffered.</summary>
    public IEnumerable<ReplaySegmentEntry> ReadFrom(long fromSeq) =>
        throw new NotImplementedException("plan 026 wave 3 agent A");

    public void Dispose()
    {
    }
}

/// <summary>Plan 026 D6 — how a stream item becomes the <c>payload</c> of a segment line and back.
/// Rows are <c>Dictionary&lt;string, object?&gt;</c> holding only primitives (long/double/string/bool/null)
/// — System.Text.Json reads those back as <c>JsonElement</c>, so <see cref="Deserialize{T}"/> MUST normalize
/// every value to the primitive the Engine expects (reuse the normalizer the connector formats already
/// have under <c>Connectors/Mapping</c> if it fits). Pinned by the orchestrator; agent A implements.</summary>
public static class ReplayPayloadJson
{
    public static string Serialize<T>(T item) => throw new NotImplementedException("plan 026 wave 3 agent A");

    public static T Deserialize<T>(string json) => throw new NotImplementedException("plan 026 wave 3 agent A");
}
