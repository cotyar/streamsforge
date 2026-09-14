using Orleans;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Engine;
using StreamsForge.Host.Facades;
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
    private SourceReplayGate Gate => _gate ??= CreateGate();
    private ReplayConfig? _replay;
    private IGrainTimer? _replayFlushTimer;

    /// <summary>Plan 026 D6 — this grain is the one producer with no definition of its own (see the class
    /// doc: it is deliberately stateless), so the <c>replay</c> group is read ONCE on activation from the
    /// catalog. Toggling <c>replay.persist</c> on an ingest source therefore takes effect at the next
    /// activation rather than immediately — the source's own upsert does not restart this grain — and the
    /// positions then continue from the disk's <c>LastSeq</c> (or restart at 1 when the tier is off).</summary>
    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var (env, name) = EnvKeys.Split(this.GetPrimaryKeyString());
        try
        {
            _replay = (await GrainFactory.RegistryFor(env).GetSourceAsync(name))?.Replay;
        }
        catch
        {
            // A catalog that cannot be read is not a reason to refuse ingest; the in-memory ring still works.
        }
        if (_replay is { Persist: true })
        {
            _ = Gate; // attach the disk tier BEFORE the first PublishAsync
        }
        await base.OnActivateAsync(cancellationToken);
    }

    private SourceReplayGate CreateGate()
    {
        var gate = new SourceReplayGate(
            SourceStream,
            release => this.RegisterGrainTimer(release, SourceReplayGate.SafetyRelease, Timeout.InfiniteTimeSpan));
        var disk = ReplayPersistence.Open(ServiceProvider, _replay, StreamConstants.SourcesNamespace, this.GetPrimaryKeyString());
        if (disk is not null)
        {
            gate.EnablePersistence(disk, ReplayPersistence.SerializeRow, ReplayPersistence.DeserializeRow);
            _replayFlushTimer = this.RegisterGrainTimer(ReplayFlushAsync, ReplayPersistence.FlushPeriod, ReplayPersistence.FlushPeriod);
        }
        return gate;
    }

    private Task ReplayFlushAsync() => _gate?.FlushAsync() ?? Task.CompletedTask;

    /// <summary>Plan 026 D6 — the last write-behind flush; a crash loses only what this would have written.</summary>
    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        try { await ReplayFlushAsync(); } catch { /* best-effort */ }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    private IAsyncStream<EventRecord> SourceStream() =>
        this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString()));

    public Task<SourceReplaySnapshot> BeginAttachAsync(ReplayFrom? from) => Task.FromResult(Gate.Begin(from));

    public Task EndAttachAsync() => Gate.EndAsync();

    public Task<ReplayStatus> GetReplayStatusAsync() => Task.FromResult(Gate.Status);

    public async Task PublishAsync(List<Dictionary<string, object?>> rows)
    {
        foreach (var record in IngressEnvelopeBuilder.ToEventRecords(rows))
        {
            await Gate.PublishAsync(record);
        }
    }
}
