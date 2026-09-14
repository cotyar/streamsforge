using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;
using StreamsForge.Engine;

namespace StreamsForge.Host.Streaming;

/// <summary>Plan 026 wave 1 — the late-consumer attach gate plus the replay log, lifted VERBATIM out of
/// plan 023's <c>ConnectorGrain</c> (its <c>_replay</c>/<c>_attachHolds</c>/<c>_pending</c>/safety-timer
/// fields and the five methods around them) so the generator and ingest drivers own the identical
/// mechanism instead of a copy each. The protocol and every reason it is correct are on
/// <see cref="IConnectorGrain.BeginAttachAsync()"/>; nothing here changes it. The ONE addition is
/// <see cref="ReplayFrom"/>: the snapshot can start at a position or an event time instead of "everything
/// retained".
///
/// <para>Owned by exactly one grain activation and called only from its turns — a grain turn is the unit
/// of atomicity, so no lock is needed and none would help (plan 023's own words).</para>
///
/// <para>The safety timer is the grain's (a <c>RegisterGrainTimer</c> handed in as a delegate) because
/// the release has to run ON the grain's turn — it publishes.</para></summary>
internal sealed class SourceReplayGate(Func<IAsyncStream<EventRecord>> stream, Func<Func<Task>, IDisposable> armSafetyTimer)
{
    /// <summary>Force-release for a consumer that took a hold and never came back (it crashed, its silo
    /// went away, its StartAsync threw between the two calls). Without it one dead attacher would gate the
    /// source's publishing for the life of the activation.</summary>
    public static readonly TimeSpan SafetyRelease = TimeSpan.FromSeconds(10);

    private readonly ReplayLog<Dictionary<string, object?>> _log = new(
        r => r.TryGetValue(EventRecord.TimestampField, out var v) && v is long l ? l : 0L,
        r => new Dictionary<string, object?>(r));

    private int _holds;
    private readonly List<EventRecord> _pending = [];
    private IDisposable? _safetyTimer;

    public int Holds => _holds;
    public long LastSeq => _log.LastSeq;

    /// <summary>THE single door every row leaves the owning grain through. While a hold is outstanding the
    /// row is deferred rather than published; otherwise it goes to the stream and is then remembered for
    /// whoever attaches next. Appended AFTER the publish deliberately: a row that failed to publish is not
    /// a row a late consumer should be told it missed — the caller's own error path owns that failure.</summary>
    public async Task PublishAsync(EventRecord evt)
    {
        if (_holds > 0)
        {
            _pending.Add(evt);
            return;
        }

        await stream().OnNextAsync(evt);
        _log.Append(evt);
    }

    /// <summary>Synchronous by construction (no await before the hold is taken and the snapshot read) — a
    /// grain turn is indivisible, so no cycle or subscriber callback can slip a publish between the two.</summary>
    public SourceReplaySnapshot Begin(ReplayFrom? from)
    {
        _holds++;

        // One shared safety timer, re-armed on every Begin: the deadline that matters is "10s since the
        // most recent attach started", so several overlapping consumers each get their own full window and
        // a single abandoned hold still cannot outlive it.
        _safetyTimer?.Dispose();
        _safetyTimer = armSafetyTimer(ForceReleaseAsync);

        var s = _log.Snapshot(from is { IsEmpty: false } ? from : null);
        return new SourceReplaySnapshot
        {
            Rows = s.Entries.Select(e => e.Item).ToList(),
            Positions = s.Entries.Select(e => e.Seq).ToList(),
            TotalSeen = s.TotalSeen,
            FirstSeq = s.FirstSeq,
            LastSeq = s.LastSeq,
            Truncated = s.Truncated,
        };
    }

    public async Task EndAsync()
    {
        if (_holds > 0)
        {
            _holds--;
        }

        if (_holds == 0)
        {
            _safetyTimer?.Dispose();
            _safetyTimer = null;
            await FlushPendingAsync();
        }
    }

    /// <summary>Releases EVERY hold, not one: the only situations this runs in are "somebody is not coming
    /// back" (the safety timer) and the owner's own Start/Stop, where rows already produced are rows
    /// already owed and must not be disowned by the generation bump that follows.</summary>
    public async Task ForceReleaseAsync()
    {
        _holds = 0;
        _safetyTimer?.Dispose();
        _safetyTimer = null;
        await FlushPendingAsync();
    }

    /// <summary>Publishes everything deferred while the gate was closed, oldest first, through the stream
    /// directly rather than back through <see cref="PublishAsync"/> — a hold taken WHILE this flush is
    /// awaiting must not re-queue rows that are already on their way out and re-order them behind newer
    /// ones. A throw here abandons the rest of the batch; the deferral list is cleared up front so a
    /// failed flush cannot be replayed twice by a later one.</summary>
    private async Task FlushPendingAsync()
    {
        if (_pending.Count == 0)
        {
            return;
        }

        var pending = _pending.ToList();
        _pending.Clear();

        var s = stream();
        foreach (var evt in pending)
        {
            await s.OnNextAsync(evt);
            _log.Append(evt);
        }
    }
}
