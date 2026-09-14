using System.Text;
using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Json;

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
    /// <summary>The per-segment index built at open and maintained on rotate, so a read from a position can
    /// skip whole segments without opening them.</summary>
    private sealed class Segment
    {
        public required string Path { get; init; }
        public required int Index { get; init; }
        public long FirstSeq;
        public long LastSeq;
        public long NewestAtMs;
        public long Length;
    }

    private readonly string _directory;
    private readonly ReplayOptions _options;
    private readonly Func<long> _clock;
    private readonly List<Segment> _segments = [];
    private List<ReplaySegmentEntry> _buffer = [];
    private long _lastSeq;
    private bool _rotatePending;
    private Task? _flush;

    /// <param name="directory">This stream's own directory (created if missing).</param>
    /// <param name="options">Effective limits for THIS stream (the caller has already applied the
    /// definition's overrides on top of the host defaults).</param>
    /// <param name="clock">Epoch ms, injectable for tests.</param>
    public ReplaySegmentLog(string directory, ReplayOptions options, Func<long>? clock = null)
    {
        _directory = directory;
        _options = options;
        _clock = clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        System.IO.Directory.CreateDirectory(_directory);

        var files = System.IO.Directory.GetFiles(_directory, "*.ndjson")
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        for (var i = 0; i < files.Count; i++)
        {
            var path = files[i];
            if (i == files.Count - 1)
            {
                NormalizeTail(path);
            }

            if (new FileInfo(path).Length == 0)
            {
                File.Delete(path);
                continue;
            }

            var first = Decode(FirstLine(path));
            var last = Decode(LastLine(path));
            if (first is null || last is null)
            {
                File.Delete(path);
                continue;
            }

            _segments.Add(new Segment
            {
                Path = path,
                Index = int.TryParse(Path.GetFileNameWithoutExtension(path), out var n) ? n : i,
                FirstSeq = first.Value.Seq,
                LastSeq = last.Value.Seq,
                NewestAtMs = last.Value.AppendedAtMs,
                Length = new FileInfo(path).Length,
            });
        }

        _lastSeq = _segments.Count == 0 ? 0 : _segments[^1].LastSeq;
        _rotatePending = _segments.Count > 0 && _segments[^1].Length >= _options.SegmentBytes;
    }

    public string Directory => _directory;

    /// <summary>Oldest retained position, or 0 when nothing is retained.</summary>
    public long FirstSeq => _segments.Count == 0 ? 0 : _segments[0].FirstSeq;

    /// <summary>Newest position ever written (recovered on open; advanced by <see cref="Append"/> even before
    /// the flush), or 0 when the directory is empty.</summary>
    public long LastSeq => _lastSeq;

    /// <summary>Bytes on disk across retained segments (the buffered, unflushed tail excluded).</summary>
    public long RetainedBytes => _segments.Sum(s => s.Length);

    public int SegmentCount => _segments.Count;

    /// <summary>Buffers one entry. <paramref name="seq"/> must be <c>LastSeq + 1</c>.</summary>
    public void Append(long seq, long timestampMs, string payloadJson)
    {
        if (seq != _lastSeq + 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(seq), seq, $"replay positions are contiguous — expected {_lastSeq + 1}");
        }

        _buffer.Add(new ReplaySegmentEntry(seq, timestampMs, _clock(), payloadJson));
        _lastSeq = seq;
    }

    /// <summary>Writes the buffered tail, rotates when the current segment passed <c>SegmentBytes</c>, then
    /// applies age and byte retention. Single-flight: a call while one is in progress returns the same task.</summary>
    public Task FlushAsync() => _flush is { IsCompleted: false } ? _flush : _flush = FlushCoreAsync();

    private async Task FlushCoreAsync()
    {
        // The swap happens BEFORE the first await, so an Append racing the write lands in the next flush's
        // buffer rather than in a file this one already sized.
        var pending = _buffer;
        _buffer = [];

        if (pending.Count > 0)
        {
            var sb = new StringBuilder();
            foreach (var e in pending)
            {
                sb.Append("{\"seq\":").Append(e.Seq)
                  .Append(",\"ts\":").Append(e.TimestampMs)
                  .Append(",\"at\":").Append(e.AppendedAtMs)
                  .Append(",\"payload\":").Append(e.PayloadJson)
                  .Append("}\n");
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());

            // Rotation is lazy — a new segment is only created when there is something to put in it, so
            // SegmentCount always equals the number of files on disk (and survives a reopen).
            if (_segments.Count == 0 || _rotatePending)
            {
                var index = _segments.Count == 0 ? 0 : _segments[^1].Index + 1;
                _segments.Add(new Segment { Path = Path.Combine(_directory, $"{index:D6}.ndjson"), Index = index });
                _rotatePending = false;
            }

            var seg = _segments[^1];
            await using (var fs = new FileStream(seg.Path, FileMode.Append, FileAccess.Write))
            {
                await fs.WriteAsync(bytes);
            }

            if (seg.FirstSeq == 0)
            {
                seg.FirstSeq = pending[0].Seq;
            }

            seg.LastSeq = pending[^1].Seq;
            seg.NewestAtMs = pending[^1].AppendedAtMs;
            seg.Length += bytes.Length;
            _rotatePending = seg.Length >= _options.SegmentBytes;
        }

        ApplyRetention();
    }

    /// <summary>Every retained entry with <c>Seq &gt;= fromSeq</c>, oldest first, flushed AND buffered.</summary>
    public IEnumerable<ReplaySegmentEntry> ReadFrom(long fromSeq)
    {
        // Both tiers are snapshotted now: the caller may enumerate lazily across an Append or a Flush.
        var paths = _segments.Where(s => s.LastSeq >= fromSeq).Select(s => s.Path).ToArray();
        var buffered = _buffer.Where(e => e.Seq >= fromSeq).ToArray();
        return Iterate();

        IEnumerable<ReplaySegmentEntry> Iterate()
        {
            foreach (var path in paths)
            {
                foreach (var line in File.ReadLines(path))
                {
                    if (Decode(line) is { } e && e.Seq >= fromSeq)
                    {
                        yield return e;
                    }
                }
            }

            foreach (var e in buffered)
            {
                yield return e;
            }
        }
    }

    public void Dispose()
    {
    }

    private void ApplyRetention()
    {
        var cutoff = _clock() - _options.RetainMs;
        // Never the current (newest) segment: retention drops WHOLE closed segments only.
        while (_segments.Count > 1)
        {
            var aged = _options.RetainMs > 0 && _segments[0].NewestAtMs < cutoff;
            var overflowing = _options.MaxBytesPerStream > 0 && RetainedBytes > _options.MaxBytesPerStream;
            if (!aged && !overflowing)
            {
                break;
            }

            File.Delete(_segments[0].Path);
            _segments.RemoveAt(0);
        }
    }

    private static ReplaySegmentEntry? Decode(string? line)
    {
        if (string.IsNullOrWhiteSpace(line))
        {
            return null;
        }

        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            return new ReplaySegmentEntry(
                root.GetProperty("seq").GetInt64(),
                root.GetProperty("ts").GetInt64(),
                root.GetProperty("at").GetInt64(),
                root.GetProperty("payload").GetRawText());
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
        {
            return null; // a torn or unparsable line is ignored on read, never an error
        }
    }

    /// <summary>Drops a torn tail from the newest segment — an unterminated last line (a crash mid-write),
    /// then any trailing line that does not parse — so the next append starts on a clean boundary.</summary>
    private static void NormalizeTail(string path)
    {
        var bytes = File.ReadAllBytes(path);
        var end = bytes.Length;
        while (end > 0)
        {
            if (bytes[end - 1] != (byte)'\n')
            {
                end = LastNewlineBefore(bytes, end) + 1;
                continue;
            }

            var start = LastNewlineBefore(bytes, end - 1) + 1;
            if (Decode(Encoding.UTF8.GetString(bytes, start, end - 1 - start)) is not null)
            {
                break;
            }

            end = start;
        }

        if (end != bytes.Length)
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
            fs.SetLength(end);
        }
    }

    private static int LastNewlineBefore(byte[] bytes, int exclusiveEnd)
    {
        for (var i = exclusiveEnd - 1; i >= 0; i--)
        {
            if (bytes[i] == (byte)'\n')
            {
                return i;
            }
        }

        return -1;
    }

    private static string? FirstLine(string path)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        return reader.ReadLine();
    }

    /// <summary>The last complete line, read from the tail rather than by scanning the whole segment.</summary>
    private static string? LastLine(string path)
    {
        using var fs = File.OpenRead(path);
        var chunk = 4096L;
        while (true)
        {
            var start = Math.Max(0, fs.Length - chunk);
            fs.Seek(start, SeekOrigin.Begin);
            var buf = new byte[fs.Length - start];
            fs.ReadExactly(buf);

            var end = buf.Length;
            while (end > 0 && (buf[end - 1] == (byte)'\n' || buf[end - 1] == (byte)'\r'))
            {
                end--;
            }

            var i = LastNewlineBefore(buf, end);
            if (i >= 0 || start == 0)
            {
                return Encoding.UTF8.GetString(buf, i + 1, end - i - 1);
            }

            chunk *= 4;
        }
    }
}

/// <summary>Plan 026 D6 — how a stream item becomes the <c>payload</c> of a segment line and back.
/// Rows are <c>Dictionary&lt;string, object?&gt;</c> holding only primitives (long/double/string/bool/null)
/// — System.Text.Json reads those back as <c>JsonElement</c>, so <see cref="Deserialize{T}"/> MUST normalize
/// every value to the primitive the Engine expects (reuse the normalizer the connector formats already
/// have under <c>Connectors/Mapping</c> if it fits). Pinned by the orchestrator; agent A implements.
///
/// <para>One lossy edge, inherent to JSON and not worth a type tag: a <c>double</c> whose value is integral
/// (<c>2.0</c>) is written as <c>2</c> and read back as <c>long</c>. Fractional doubles round-trip.</para></summary>
public static class ReplayPayloadJson
{
    private static readonly JsonSerializerOptions Options = new() { Converters = { new IntegralDoubleConverter() } };

    /// <summary>JSON has one number type and System.Text.Json writes the double <c>2.0</c> as <c>2</c>, which
    /// <see cref="JsonValueNormalizer"/> reads back as a long — a pipeline column would change CLR type across
    /// a restart. Writing integral doubles with an explicit <c>.0</c> keeps the reader's long/double split
    /// exactly what the producer had; nothing else about the number changes.</summary>
    private sealed class IntegralDoubleConverter : System.Text.Json.Serialization.JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetDouble();

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            if (double.IsFinite(value) && value == Math.Floor(value) && Math.Abs(value) < 1e15)
            {
                writer.WriteRawValue(value.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                writer.WriteNumberValue(value);
            }
        }
    }

    public static string Serialize<T>(T item) => JsonSerializer.Serialize(item, Options);

    public static T Deserialize<T>(string json)
    {
        var value = JsonSerializer.Deserialize<T>(json, Options)!;
        switch (value)
        {
            // EventRecord IS a Dictionary<string, object?>, so it lands in this case too.
            case Dictionary<string, object?> row:
                JsonValueNormalizer.NormalizeInPlace(row);
                break;
            case IEnumerable<ResultEnvelope> results:
                foreach (var r in results)
                {
                    JsonValueNormalizer.NormalizeInPlace(r.Row);
                }

                break;
            case IEnumerable<TableDeltaDto> deltas:
                foreach (var d in deltas)
                {
                    JsonValueNormalizer.NormalizeInPlace(d.Row);
                }

                break;
        }

        return value;
    }
}
