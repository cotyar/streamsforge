using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.Engine;

namespace StreamsForge.Host.Streaming;

/// <summary>Plan 026 wave 1 — the late-consumer attach gate plus the replay log for a SOURCE driver
/// (<c>EventRecord</c> payloads), lifted VERBATIM out of plan 023's <c>ConnectorGrain</c> so the generator
/// and ingest drivers own the identical mechanism. Since wave 2 it is a thin <c>EventRecord</c>-shaped
/// face over <see cref="ReplayGate{T}"/> (the pipeline and table grains use that directly); the only
/// thing this adds is the <see cref="SourceReplaySnapshot"/> conversion — rows as plain dictionaries with a
/// parallel <c>Positions</c> list, the shape <see cref="IReplayableSourceGrain"/> promised in wave 1.
/// The protocol and every reason it is correct are on <see cref="IConnectorGrain.BeginAttachAsync()"/>.</summary>
internal sealed class SourceReplayGate
{
    public static readonly TimeSpan SafetyRelease = ReplayGate<EventRecord>.SafetyRelease;

    private readonly ReplayGate<EventRecord> _gate;

    public SourceReplayGate(Func<IAsyncStream<EventRecord>> stream, Func<Func<Task>, IDisposable> armSafetyTimer)
    {
        _gate = new ReplayGate<EventRecord>(stream, armSafetyTimer, e => e.Timestamp, e => new EventRecord(e));
    }

    public int Holds => _gate.Holds;
    public long LastSeq => _gate.LastSeq;

    public Task PublishAsync(EventRecord evt) => _gate.PublishAsync(evt);

    public SourceReplaySnapshot Begin(ReplayFrom? from)
    {
        var s = _gate.Begin(from);
        return new SourceReplaySnapshot
        {
            Rows = s.Items.Select(e => new Dictionary<string, object?>(e)).ToList(),
            Positions = s.Positions,
            TotalSeen = s.TotalSeen,
            FirstSeq = s.FirstSeq,
            LastSeq = s.LastSeq,
            Truncated = s.Truncated,
        };
    }

    public Task EndAsync() => _gate.EndAsync();

    public Task ForceReleaseAsync() => _gate.ForceReleaseAsync();
}
