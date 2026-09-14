using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Streaming;

/// <summary>One retained entry: the producer's position, the item's own event time, and the item.</summary>
public sealed record ReplayEntry<T>(long Seq, long TimestampMs, T Item);

/// <summary>What a producer hands a consumer that asked to start from <see cref="ReplayFrom"/>.
/// <see cref="Entries"/> are oldest first, already COPIED (see <see cref="ReplayLog{T}"/>'s copy rule).
/// <see cref="FirstSeq"/>/<see cref="LastSeq"/> describe what the log RETAINS right now (not the
/// selection); <see cref="TotalSeen"/> is everything ever appended, so <c>TotalSeen - Entries.Count</c>
/// is never silently zero. <see cref="Truncated"/> is true when entries the request WOULD have matched
/// were already evicted — the honest "you are getting the last N of M".</summary>
public sealed record ReplaySnapshot<T>(
    List<ReplayEntry<T>> Entries, long FirstSeq, long LastSeq, long TotalSeen, bool Truncated);

/// <summary>Plan 026 D1 — a producer-owned, sequence-numbered replay log: the generalization of plan
/// 023's <c>SourceReplayBuffer</c> (a ring of rows with a count) to "a ring of positioned entries you
/// can ask for from a position or a time". Pure and framework-free; owned by ONE turn-based driver (a
/// grain/actor activation), the same not-thread-safe rule <c>DedupTracker</c>/<c>FileLedger</c> rely on.
///
/// <para>Eviction is by count (<see cref="Capacity"/>, oldest first) and, when <see cref="RetainMs"/> is
/// positive, by AGE — measured on the wall clock at append time, NOT on the entry's event time, because
/// an ingest source can push rows whose <c>_ts</c> is days old and those must not be evicted on arrival.
/// Event time is what <see cref="ReplayFrom.TimestampMs"/> filters on. The two clocks are different on
/// purpose.</para>
///
/// <para>Seq starts at 1 per log instance (i.e. per activation) — plan 026 wave 3 makes it durable.
/// <c>// ponytail: in-memory ring, count + age; wave 3 adds the persisted segment tier behind the same
/// Snapshot(from) contract.</c></para></summary>
public sealed class ReplayLog<T>
{
    public const int DefaultCapacity = 10_000;

    private readonly Func<T, long> _timestampOf;
    private readonly Func<T, T> _copy;
    private readonly Func<long> _clock;
    private readonly Queue<(ReplayEntry<T> Entry, long AppendedAtMs)> _ring = new();

    public int Capacity { get; }

    /// <summary>Age retention in ms; 0 = count only.</summary>
    public long RetainMs { get; }

    /// <summary>Everything ever appended; equals the last assigned seq. Never decreases.</summary>
    public long TotalSeen { get; private set; }

    public long LastSeq => TotalSeen;

    /// <summary>Oldest retained seq, or <c>LastSeq + 1</c> when nothing is retained (so the empty
    /// range reads as "next position", never as 0).</summary>
    public long FirstSeq => _ring.Count == 0 ? TotalSeen + 1 : _ring.Peek().Entry.Seq;

    public int Count => _ring.Count;

    /// <param name="timestampOf">The item's event time (a row's <c>_ts</c>).</param>
    /// <param name="copy">Defensive copy applied on append AND on snapshot, so neither the producer's
    /// later mutation nor a consumer's can reach the retained entry or another consumer.</param>
    /// <param name="clock">Wall clock in epoch ms, for age retention; injectable for tests.</param>
    public ReplayLog(Func<T, long> timestampOf, Func<T, T> copy, int capacity = DefaultCapacity, long retainMs = 0, Func<long>? clock = null)
    {
        _timestampOf = timestampOf;
        _copy = copy;
        _clock = clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        Capacity = capacity;
        RetainMs = retainMs;
    }

    /// <summary>Appends and returns the assigned position.</summary>
    public long Append(T item)
    {
        var seq = ++TotalSeen;
        var now = _clock();
        _ring.Enqueue((new ReplayEntry<T>(seq, _timestampOf(item), _copy(item)), now));
        while (_ring.Count > Capacity)
        {
            _ring.Dequeue();
        }
        EvictAged(now);
        return seq;
    }

    /// <summary>Entries matching <paramref name="from"/>, oldest first. Seq wins over timestamp when
    /// both are set; a null/empty request returns everything retained.</summary>
    public ReplaySnapshot<T> Snapshot(ReplayFrom? from = null)
    {
        EvictAged(_clock());
        var evicted = TotalSeen - _ring.Count;
        var entries = new List<ReplayEntry<T>>();
        bool truncated;

        if (from?.Seq is { } seq)
        {
            foreach (var (e, _) in _ring)
            {
                if (e.Seq >= seq) entries.Add(e with { Item = _copy(e.Item) });
            }
            truncated = evicted > 0 && seq < FirstSeq;
        }
        else if (from?.TimestampMs is { } ts)
        {
            long oldestRetainedTs = long.MaxValue;
            foreach (var (e, _) in _ring)
            {
                oldestRetainedTs = Math.Min(oldestRetainedTs, e.TimestampMs);
                if (e.TimestampMs >= ts) entries.Add(e with { Item = _copy(e.Item) });
            }
            // Evicted entries' timestamps are gone; if the request reaches at or below the oldest
            // retained event time, an evicted entry could have matched — say so rather than guess.
            truncated = evicted > 0 && (_ring.Count == 0 || ts <= oldestRetainedTs);
        }
        else
        {
            foreach (var (e, _) in _ring)
            {
                entries.Add(e with { Item = _copy(e.Item) });
            }
            truncated = evicted > 0;
        }

        return new ReplaySnapshot<T>(entries, FirstSeq, LastSeq, TotalSeen, truncated);
    }

    private void EvictAged(long nowMs)
    {
        if (RetainMs <= 0) return;
        var cutoff = nowMs - RetainMs;
        while (_ring.Count > 0 && _ring.Peek().AppendedAtMs < cutoff)
        {
            _ring.Dequeue();
        }
    }
}
