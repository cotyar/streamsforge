using Orleans;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Engine;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Grains;

/// <summary>Plan 026 D3 — the turn-based owner of one ingest source's publishing. Ingest rows used to
/// go straight from <c>OrleansIngressFacade.DrainAsync</c>'s pump onto the stream (a facade, no grain),
/// which left nowhere for the late-consumer attach gate and the replay log to live; this grain is that
/// home, one activation per (environment-qualified) ingest source name. Stateless by design — the
/// buffer, overflow policy and <c>IngestStatus.DownstreamDropped</c> accounting all stay in
/// <c>SourceIngressBuffer</c>/<c>OrleansIngressFacade</c>, untouched; this grain's only job is to stamp
/// a drained batch into <see cref="EventRecord"/>s and feed them through <see cref="SourceReplayGate"/>,
/// one row at a time, in order — the exact mechanism <see cref="Grains.ConnectorGrain"/> and
/// <see cref="Grains.GeneratorGrain"/> already use.</summary>
public sealed class IngestSourceGrain : Grain, IIngestSourceGrain
{
    private SourceReplayGate? _gate;
    private SourceReplayGate Gate => _gate ??= new SourceReplayGate(
        SourceStream,
        release => this.RegisterGrainTimer(release, SourceReplayGate.SafetyRelease, Timeout.InfiniteTimeSpan));

    private IAsyncStream<EventRecord> SourceStream() =>
        this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString()));

    public Task<SourceReplaySnapshot> BeginAttachAsync(ReplayFrom? from) => Task.FromResult(Gate.Begin(from));

    public Task EndAttachAsync() => Gate.EndAsync();

    public async Task PublishAsync(List<Dictionary<string, object?>> rows)
    {
        foreach (var record in IngressEnvelopeBuilder.ToEventRecords(rows))
        {
            await Gate.PublishAsync(record);
        }
    }
}
