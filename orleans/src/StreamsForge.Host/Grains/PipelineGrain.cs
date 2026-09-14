using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;
using StreamsForge.Host.Facades;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Grains;

/// <summary>Key = pipeline id. One activation per running pipeline. Subscribes to its SQL's source
/// streams, feeds events through a <see cref="PipelineExecutor"/>, and publishes emitted rows +
/// periodic metrics back onto Orleans streams for <see cref="Services.StreamBridgeService"/> to relay.</summary>
public sealed class PipelineGrain(ILogger<PipelineGrain> logger) : Grain, IPipelineGrain
{
    private const int RecentResultsCapacity = 100;
    private const int MetricsEveryNTicks = 4; // 4 * 500ms ≈ 2s

    private PipelineDefinition? _def;
    private PipelineStatus _status = PipelineStatus.Stopped;
    private PipelineExecutor? _executor;
    private IGrainTimer? _timer;
    private readonly List<StreamSubscriptionHandle<EventRecord>> _subscriptions = [];
    private readonly List<ResultEnvelope> _recentResults = [];

    private long _seq;
    private int _tickCount;
    private long _totalEventsIn;
    private long _totalRowsOut;
    private long _lastEventTsMs;
    private long _eventsInAtLastMetricsTick;
    private long _rowsOutAtLastMetricsTick;
    private DateTimeOffset _lastMetricsTickAt;
    private double _lastEventsInPerSec;
    private double _lastRowsOutPerSec;

    /// <summary>Plan 026 wave 2: every result batch leaves through this gate, so a gRPC/SignalR subscriber
    /// (or a table with <c>replayFrom</c> naming this pipeline) can attach from a batch position. A batch's
    /// event time is its first envelope's <c>TimestampMs</c>.</summary>
    private ReplayGate<List<ResultEnvelope>>? _gate;
    private ReplayGate<List<ResultEnvelope>> Gate => _gate ??= new ReplayGate<List<ResultEnvelope>>(
        OutputStream,
        release => this.RegisterGrainTimer(release, ReplayGate<List<ResultEnvelope>>.SafetyRelease, Timeout.InfiniteTimeSpan),
        batch => batch.Count > 0 ? batch[0].TimestampMs : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        batch => batch.Select(e => new ResultEnvelope { PipelineId = e.PipelineId, Seq = e.Seq, TimestampMs = e.TimestampMs, Row = new Dictionary<string, object?>(e.Row) }).ToList());

    // Plan 021 D6 — self-publish onto THIS pipeline's own output stream: this.GetPrimaryKeyString() is
    // already the D3-qualified key (IPipelineGrain is qualified uniformly like every other name/id-keyed
    // grain kind), so it is correct here without re-deriving anything from _def.
    private IAsyncStream<List<ResultEnvelope>> OutputStream() =>
        this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, this.GetPrimaryKeyString()));

    public Task<StreamReplaySnapshot<List<ResultEnvelope>>> BeginAttachAsync(ReplayFrom? from) => Task.FromResult(Gate.Begin(from));

    public Task EndAttachAsync() => Gate.EndAsync();

    public async Task StartAsync(PipelineDefinition def)
    {
        await StopAsync();

        _def = def;

        // Plan 021 D5 — a grain acting on an entity it was just handed reads that entity's OWN Environment
        // field rather than any ambient: def.Environment was stamped by the registry that created it and
        // never edited afterwards, so it is the durable answer to "which catalog does this pipeline belong
        // to" even though IPipelineGrain itself stays keyed by GUID (D3's one exception), not by environment.
        var registry = GrainFactory.RegistryFor(def.Environment);
        var sources = await registry.GetSourcesAsync();
        var schemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.Compile(def.Sql, schemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            throw new InvalidOperationException(message);
        }

        _executor = compileResult.Plan.CreateExecutor();
        _status = PipelineStatus.Running;

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        foreach (var sourceName in compileResult.SourceNames.Distinct())
        {
            await AttachToSourceAsync(streamProvider, def, sourceName, sources);
        }

        var now = DateTimeOffset.UtcNow;
        _lastMetricsTickAt = now;
        _timer = this.RegisterGrainTimer(OnTimerTickAsync, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500));

        // Keep this activation alive for as long as the pipeline is running — a grain with no
        // pending calls would otherwise be collected by idle-activation GC despite the live
        // stream subscriptions and timer.
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task StopAsync()
    {
        // Batches already produced are batches already owed — same rule as ConnectorGrain.StopAsync.
        await Gate.ForceReleaseAsync();

        _status = PipelineStatus.Stopped;

        _timer?.Dispose();
        _timer = null;

        foreach (var handle in _subscriptions)
        {
            try
            {
                await handle.UnsubscribeAsync();
            }
            catch
            {
                // best-effort; the subscription's silo-side state is torn down regardless
            }
        }
        _subscriptions.Clear();

        _executor = null;

        // Cancel the earlier keep-alive; TimeSpan.Zero restores normal idle-activation GC.
        this.DelayDeactivation(TimeSpan.Zero);
    }

    /// <summary>Subscribe-then-attach against a connector-kind source — the pipeline's copy of
    /// <c>TableGrain.AttachToStreamInputAsync</c>, which carries the full rationale. Short version: memory
    /// streams have no replay, so a pipeline written after its source was already enabled and polling used
    /// to see nothing of what that source had already emitted. <c>BeginAttachAsync</c> holds the source's
    /// publishing and hands back its recent rows; those are fed through the SAME
    /// <see cref="OnSourceEventAsync"/> handler live traffic uses (so windows/joins are built from them
    /// rather than bypassed) after the subscription exists; the hold is released in a <c>finally</c>, at
    /// which point anything the source produced meanwhile is delivered — to this subscription too. Nothing
    /// is replayed and delivered twice; nothing falls in the gap. Only
    /// <see cref="SourceKindDispatch.ActorKind.Connector"/> sources have that driver — generators, ingest
    /// sources and CRDT documents are subscribed to exactly as before.</summary>
    private async Task AttachToSourceAsync(
        IStreamProvider streamProvider, PipelineDefinition def, string sourceName, IEnumerable<SourceDefinition> sources)
    {
        var qualified = EnvKeys.Qualify(def.Environment, sourceName);

        var sourceDef = sources.FirstOrDefault(s => s.Name == sourceName);
        IConnectorGrain? connector = null;
        SourceReplaySnapshot? snapshot = null;
        if (sourceDef is not null && SourceKindDispatch.Classify(sourceDef.Kind) == SourceKindDispatch.ActorKind.Connector)
        {
            connector = GrainFactory.GetGrain<IConnectorGrain>(qualified);
            try
            {
                snapshot = await connector.BeginAttachAsync();
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Pipeline '{Pipeline}': could not attach to source '{Source}' for replay — subscribing without it.", def.Name, sourceName);
                connector = null;
                snapshot = null;
            }
        }

        try
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, qualified));
            var handle = await stream.SubscribeAsync((evt, _) => OnSourceEventAsync(sourceName, evt));
            _subscriptions.Add(handle);

            if (snapshot is not null && snapshot.Rows.Count > 0)
            {
                if (snapshot.TotalSeen > snapshot.Rows.Count)
                {
                    logger.LogWarning(
                        "Pipeline '{Pipeline}': late attach to source '{Source}' replayed {Replayed} of {TotalSeen} row(s); " +
                        "earlier rows are not recoverable (the source's replay ring holds the most recent {Capacity}).",
                        def.Name, sourceName, snapshot.Rows.Count, snapshot.TotalSeen, SourceReplayBuffer.Capacity);
                }

                foreach (var row in snapshot.Rows)
                {
                    await OnSourceEventAsync(sourceName, new EventRecord(row));
                }
            }
        }
        finally
        {
            if (connector is not null)
            {
                try { await connector.EndAttachAsync(); }
                catch (Exception ex) { logger.LogDebug(ex, "Pipeline '{Pipeline}': releasing the attach hold on source '{Source}' failed; the source's own safety timer covers it.", def.Name, sourceName); }
            }
        }
    }

    public Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit)
    {
        var take = Math.Max(0, Math.Min(limit, _recentResults.Count));
        var start = _recentResults.Count - take;
        return Task.FromResult(_recentResults.GetRange(start, take));
    }

    public Task<PipelineMetrics> GetMetricsAsync() => Task.FromResult(new PipelineMetrics
    {
        PipelineId = _def?.Id ?? this.GetPrimaryKeyString(),
        Status = _status,
        EventsInPerSec = _lastEventsInPerSec,
        RowsOutPerSec = _lastRowsOutPerSec,
        TotalEventsIn = _totalEventsIn,
        TotalRowsOut = _totalRowsOut,
        WindowsClosed = 0,
        LastEventTsMs = _lastEventTsMs,
        LateEvents = _executor?.LateEvents ?? 0,
    });

    private async Task OnSourceEventAsync(string sourceName, EventRecord evt)
    {
        if (_executor is null)
        {
            return;
        }

        _totalEventsIn++;
        _lastEventTsMs = evt.Timestamp;

        var rows = _executor.OnEvent(sourceName, evt);
        if (rows.Count > 0)
        {
            await PublishRowsAsync(rows);
        }
    }

    private async Task OnTimerTickAsync()
    {
        if (_executor is null)
        {
            return;
        }

        var rows = _executor.AdvanceWatermark(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        if (rows.Count > 0)
        {
            await PublishRowsAsync(rows);
        }

        _tickCount++;
        if (_tickCount % MetricsEveryNTicks == 0)
        {
            await PublishMetricsAsync();
        }
    }

    private async Task PublishRowsAsync(IReadOnlyList<EventRecord> rows)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = new List<ResultEnvelope>(rows.Count);

        foreach (var row in rows)
        {
            _seq++;
            _totalRowsOut++;

            var envelope = new ResultEnvelope
            {
                PipelineId = _def!.Id,
                Seq = _seq,
                TimestampMs = row.Timestamp != 0 ? row.Timestamp : nowMs,
                Row = new Dictionary<string, object?>(row),
            };

            batch.Add(envelope);
            _recentResults.Add(envelope);
        }

        if (_recentResults.Count > RecentResultsCapacity)
        {
            _recentResults.RemoveRange(0, _recentResults.Count - RecentResultsCapacity);
        }

        await Gate.PublishAsync(batch);
    }

    private async Task PublishMetricsAsync()
    {
        var now = DateTimeOffset.UtcNow;
        var elapsedSec = Math.Max(0.001, (now - _lastMetricsTickAt).TotalSeconds);

        _lastEventsInPerSec = (_totalEventsIn - _eventsInAtLastMetricsTick) / elapsedSec;
        _lastRowsOutPerSec = (_totalRowsOut - _rowsOutAtLastMetricsTick) / elapsedSec;

        _eventsInAtLastMetricsTick = _totalEventsIn;
        _rowsOutAtLastMetricsTick = _totalRowsOut;
        _lastMetricsTickAt = now;

        var metrics = await GetMetricsAsync();

        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<PipelineMetrics>(StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.MetricsKey));
        await stream.OnNextAsync(metrics);
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
