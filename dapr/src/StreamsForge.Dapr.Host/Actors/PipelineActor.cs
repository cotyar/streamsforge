using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Json;
using StreamsForge.Dapr.Host.Streaming;
using StreamsForge.Engine;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: Dapr counterpart of Orleans' <c>PipelineGrain</c>
/// (orleans/src/StreamsForge.Host/Grains/PipelineGrain.cs) — one actor per running pipeline (actor type
/// "PipelineActor", key = the pipeline's <see cref="PipelineDefinition.Id"/>), compiling the pipeline's
/// streaming SQL via the shared <see cref="StreamsForge.Engine"/> and feeding it batches of source events
/// routed in by <see cref="Streaming.PipelineEventRouter"/>, publishing emitted rows + periodic metrics
/// back onto Dapr pub/sub (<c>sf-pipeline-out</c> / <c>sf-metrics</c>) for <see cref="Streaming.DaprStreamBridge"/>
/// to relay onto SignalR — same fields/shapes/cadence as <c>PipelineGrain</c>, translated from Orleans
/// streams to the fixed-topic transport (decision D-D). Read every method next to its Orleans equivalent;
/// deviations are called out explicitly.
///
/// <para><b>Acyclic by construction — see <see cref="IPipelineActor"/>'s class doc.</b> Everything this
/// actor needs (definition, full source catalog, per-batch events) arrives as a method parameter; it
/// never resolves <see cref="ICatalogFacade"/> or any other actor proxy.</para>
///
/// <para><b>State: the definition, source list, and running flag ARE persisted</b> — same rationale as
/// <see cref="GeneratorActor"/>'s own doc comment: Dapr actor timers do NOT survive deactivation/
/// reactivation, so <see cref="OnActivateAsync"/> recompiles from persisted state and immediately re-arms
/// the watermark timer if the last known state was Running, instead of waiting for
/// <see cref="Services.PipelineSupervisorService"/>'s next sweep. Recompiling always yields a BRAND NEW
/// <see cref="PipelineExecutor"/> (fresh window/join buffers) — exactly like a fresh
/// <c>compileResult.Plan.CreateExecutor()</c> call on the Orleans side inside <c>PipelineGrain.StartAsync</c>
/// — so a reactivation loses in-flight (not yet emitted) windowing state, same as Orleans losing it on a
/// silo restart. What is NOT reset on a mere reactivation-with-still-Running-state: nothing, because
/// reactivation always starts from fresh (zeroed) instance fields anyway — this mirrors
/// <c>PipelineGrain.StartAsync</c> itself never resetting its own counters/results ring (only a brand new
/// grain/actor activation does, since those are just C# instance fields).</para>
///
/// <para><b>Read surface (results/metrics):</b> <see cref="GetRecentResultsAsync"/> returns a bounded
/// in-memory ring (capacity 100, same as <c>PipelineGrain</c>) — never persisted, exactly like Orleans
/// (a read cache, not the ledger of truth; losing it on reactivation is an accepted consequence of Dapr
/// actors idle-deactivating, same tradeoff <see cref="GeneratorActor"/> already accepts for its own
/// transient fields).</para>
///
/// <para><b>Plan 025 (PARITY.md D6 "late-consumer replay") — this actor now registers its OWN router
/// subscription and attaches to its connector-kind sources, inside its own turn.</b> Before this plan
/// <c>Lifecycle.DaprLifecycleOrchestrator.StartPipelineAsync</c> registered
/// <see cref="Streaming.PipelineEventRouter"/> AFTER <see cref="StartAsync"/> returned, and a pipeline
/// written after its source had already polled simply started empty — the Dapr half of the gap
/// <c>PipelineGrain.AttachToSourceAsync</c> closes on Orleans. Registration moves inside for the same
/// reason it moved inside <see cref="TableActor"/> for PARITY.md D2, and
/// <see cref="RegisterRouterAndAttachToSourcesAsync"/>'s own doc comment carries the ordering argument.
/// The orchestrator's later <c>Register</c> call is left in place: it is idempotent (it replaces this
/// pipeline's subscription set with the identical set) and harmless.</para>
/// </summary>
public sealed class PipelineActor(
    ActorHost host, DaprClient daprClient, PipelineEventRouter pipelineRouter, IConfiguration configuration,
    ILogger<PipelineActor> logger)
    : Actor(host), IPipelineActor
{
    /// <summary>Configuration key for <see cref="TransportLagAllowanceMs"/>.</summary>
    public const string TransportLagAllowanceKey = "Pipelines:TransportLagAllowanceMs";

    /// <summary>Default for <see cref="TransportLagAllowanceMs"/>: 4 s on top of the Engine's own 1 s.</summary>
    public const int DefaultTransportLagAllowanceMs = 4000;

    /// <summary>Plan 025 — how far behind wall-clock the watermark tick may run on THIS flavor.
    ///
    /// <para>The Engine discards an event whose <c>_ts</c> is already behind the watermark, and
    /// <see cref="OnTimerTickAsync"/> advances that watermark to <c>now − 1000 ms</c> every 500 ms —
    /// identical to <c>PipelineGrain</c>. On Orleans an event reaches the grain a few ms after its
    /// <c>_ts</c> was stamped, so 1 s of allowed lateness is generous. On Dapr the same row crosses
    /// Redis pub/sub, the sidecar's HTTP delivery and one more sidecar hop for the actor invocation,
    /// and under CPU pressure that path takes more than a second: measured live during plan 025
    /// (whole-solution builds running alongside) the seeded generator pipelines were discarding 15–25 %
    /// of their input as late, and a 50–100-row file batch arrived whole seconds after its stamp and was
    /// dropped entire — <c>totalEventsIn 100, totalRowsOut 0</c> — with nothing saying so until
    /// <c>PipelineMetrics.LateEvents</c> existed. The cause is transport latency, not the Engine, so the
    /// fix is a transport-shaped allowance: the tick advances the watermark to <c>now − allowance</c>
    /// (the Engine then subtracts its own 1000 ms), i.e. a window closes <c>allowance</c> later than it
    /// would on Orleans and events up to <c>allowance + 1 s</c> old are still admitted. Event-time
    /// advancement (a newer event's own <c>_ts − 1 s</c>) is untouched, so out-of-order batches keep
    /// Orleans' semantics. Configurable because the right value is a property of the deployment's
    /// sidecar latency; 0 restores the Orleans-identical behaviour.</para></summary>
    private readonly long _transportLagAllowanceMs =
        Math.Max(0, configuration.GetValue(TransportLagAllowanceKey, DefaultTransportLagAllowanceMs));

    private const string StateName = "pipeline";
    private const string TimerName = "pipeline-watermark";
    private const int RecentResultsCapacity = 100;
    private const int MetricsEveryNTicks = 4; // 4 * 500ms ≈ 2s — same cadence as PipelineGrain.

    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(500);

    private PipelineDefinition? _def;
    private List<SourceDefinition> _sources = [];
    private bool _running;
    private bool _timerArmed;
    private PipelineExecutor? _executor;
    private List<string> _sourceNames = [];
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

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<PipelineActorState>(StateName);
        if (existing.HasValue)
        {
            _def = existing.Value.Def;
            _sources = existing.Value.Sources;
            _running = existing.Value.Running;
        }

        // Self-heal: a fresh activation never has a timer registered yet (Dapr timers don't survive
        // deactivation) — if the last StartAsync we persisted left this pipeline Running, recompile and
        // re-arm immediately instead of waiting for PipelineSupervisorService's next sweep.
        if (_running && _def is not null)
        {
            var (executor, sourceNames, error) = PipelineCompilation.TryCompile(_def, _sources);
            if (executor is not null)
            {
                _executor = executor;
                _sourceNames = sourceNames;
                _lastMetricsTickAt = DateTimeOffset.UtcNow;

                // Plan 025: the self-heal path needs the attach just as much as StartAsync does — more so,
                // in fact. A reactivation recompiles a BRAND NEW executor (see this class's own doc comment)
                // and nothing else in this process registers the router for it until
                // Services.PipelineSupervisorService's next ~15s sweep, so without this a self-healed
                // pipeline is both unrouted and empty for up to a sweep period. Mirrors what TableActor's
                // OnActivateAsync branch does for PARITY.md D2.
                await RegisterRouterAndAttachToSourcesAsync();

                await ArmTimerAsync();
            }
            else
            {
                logger.LogWarning(
                    "PipelineActor[{Id}]: self-heal compile failed on reactivation — leaving stopped: {Error}",
                    _def.Id, error);
                _running = false;
            }
        }
    }

    public async Task<ActorResult<List<string>>> StartAsync(PipelineStartRequest request)
    {
        await DisarmTimerIfArmedAsync();

        // Plan 026 D5: replayFrom is applied by the Orleans grains' attach gates at start; this flavor has
        // no position-addressed gate yet (dapr/PARITY.md D11) — same rule as TableActor's shardBy/replayFrom
        // refusals: the definition stores fine here and never runs here with it set.
        var (executor, sourceNames, error) = request.Def.ReplayFrom.Count > 0
            ? (null, [], $"replayFrom must be empty on the Dapr flavor (set for {string.Join(", ", request.Def.ReplayFrom.Keys)}) — replay from a position is Orleans-only until dapr/PARITY.md D11 is closed. The definition is stored as-is so it can be promoted back to an Orleans instance without loss, but it cannot run here.")
            : PipelineCompilation.TryCompile(request.Def, request.Sources);
        if (executor is null)
        {
            _def = request.Def;
            _sources = request.Sources;
            _executor = null;
            _sourceNames = [];
            _running = false;
            await SaveAsync();
            return ActorResult<List<string>>.Failure(error!);
        }

        _def = request.Def;
        _sources = request.Sources;
        _executor = executor;
        _sourceNames = sourceNames;
        _running = true;
        await SaveAsync();

        _lastMetricsTickAt = DateTimeOffset.UtcNow;

        // Plan 025: after _running/_executor are set (ProcessEventsAsync below is a no-op otherwise) and
        // before the watermark timer is armed — a replayed row must reach the executor before the first
        // AdvanceWatermark closes a window over rows it has not seen yet.
        await RegisterRouterAndAttachToSourcesAsync();

        await ArmTimerAsync();

        return ActorResult<List<string>>.Success(sourceNames);
    }

    /// <summary>
    /// Plan 025 (PARITY.md D6 "late-consumer replay"), consumer half — the Dapr counterpart of
    /// <c>PipelineGrain.AttachToSourceAsync</c>. Registers this pipeline with
    /// <see cref="Streaming.PipelineEventRouter"/> and back-fills, exactly once, whatever its
    /// connector-kind sources already published before it existed.
    ///
    /// <para><b>The order below is BeginAttach-first, then register, then feed — NOT register-first.</b>
    /// <see cref="TableActor.RegisterRouterAndAttachToTableInputsAsync"/> registers before it reads any
    /// upstream snapshot and is still correct, because a table delta carries an <c>Epoch</c> and
    /// <see cref="TableAttachPolicy.FilterAdmissible"/> discards anything the snapshot already contained.
    /// A source event carries no such marker. So the exclusion has to come from the SOURCE side instead:
    /// <see cref="IConnectorActor.BeginAttachAsync"/> stops that source publishing before the router can
    /// route anything here, the snapshot is read under that hold, and
    /// <see cref="IConnectorActor.EndAttachAsync"/> releases it — everything produced meanwhile is flushed
    /// then, and reaches this pipeline through the router, once. Nothing falls in the gap and nothing is
    /// delivered twice. Holding EVERY connector source before registering (rather than looping
    /// Begin→register→feed→End per source) is what keeps that true when a pipeline joins two of them.</para>
    ///
    /// <para><b>Why registering mid-turn is safe:</b> Dapr actors process at most one invocation at a time
    /// per actor id (dapr/ARCHITECTURE.md's reentrancy decision — the analogue of Orleans grain
    /// non-reentrancy). This method runs INSIDE the caller's own <see cref="StartAsync"/>/
    /// <see cref="OnActivateAsync"/> turn on THIS actor id, so anything
    /// <see cref="Streaming.PipelineEventRouter.OnSourceEventsAsync"/> routes here from the moment
    /// registration lands is a NEW invocation, which Dapr QUEUES behind this still-in-flight turn rather
    /// than dropping it (the router did not know to route here before) or interleaving with it. The
    /// snapshot rows are therefore guaranteed to reach <see cref="_executor"/> ahead of any live row that
    /// raced them — the ordering the replay depends on. Same argument
    /// <see cref="ITableActor.StartAsync"/>'s doc comment makes for tables.</para>
    ///
    /// <para><b>Outbound proxy calls from inside <see cref="OnActivateAsync"/>.</b> Same shape as
    /// <see cref="TableActor"/>'s D2 attach, and without that one's cycle hazard: the graph here is
    /// pipeline → source, and no source ever calls a pipeline actor, so this can never form the
    /// A.activate → B.attach → B.activate → A.attach chain dapr/ARCHITECTURE.md documents for
    /// table-over-table.</para>
    ///
    /// <para>Generator, Ingest and Crdt kinds are skipped — only
    /// <see cref="SourceKindDispatch.ActorKind.Connector"/> sources have the driver that owns a replay
    /// ring. A <see cref="IConnectorActor.BeginAttachAsync"/> that fails (a source never started, an actor
    /// error) is logged at Debug and the pipeline registers and runs without the replay, exactly as it did
    /// before this plan.</para>
    /// </summary>
    private async Task RegisterRouterAndAttachToSourcesAsync()
    {
        var environment = _def!.Environment;

        // Phase 1: take every hold BEFORE the router can route anything here. Held in a local list so the
        // finally below releases exactly the ones actually taken, in the presence of a partial failure.
        var held = new List<(string Name, string Qualified, IConnectorActor Actor, SourceAttachSnapshot Snapshot)>();
        try
        {
            foreach (var sourceName in _sourceNames.Distinct())
            {
                var sourceDef = _sources.FirstOrDefault(s => s.Name == sourceName);
                if (sourceDef is null || SourceKindDispatch.Classify(sourceDef.Kind) != SourceKindDispatch.ActorKind.Connector)
                {
                    continue;
                }

                // Plan 021 D6: _sourceNames are BARE (compiled against this pipeline's own environment's
                // catalog); connector actors are keyed by the QUALIFIED name — the exact key
                // DaprLifecycleOrchestrator.ConnectorActorProxy uses.
                var qualified = EnvKeys.Qualify(environment, sourceName);
                var connector = ActorProxy.Create<IConnectorActor>(new ActorId(qualified), nameof(ConnectorActor), ActorProxyDefaults.Options);
                try
                {
                    held.Add((sourceName, qualified, connector, await connector.BeginAttachAsync()));
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex,
                        "PipelineActor[{Id}]: could not attach to source '{Source}' for replay — subscribing without it.",
                        _def.Id, sourceName);
                }
            }

            // Phase 2: routable from here on. Idempotent (Register replaces this pipeline's subscription
            // set), so a supervisor sweep's repair (BootResume.EnsurePipelineRunningAsync) is a no-op.
            pipelineRouter.Register(_def.Id, _sourceNames.Select(s => EnvKeys.Qualify(environment, s)).ToList());

            // Phase 3: feed the snapshots through the SAME handler live traffic uses, so windows/joins are
            // built up from them rather than bypassed — and so the JsonElement re-normalization
            // ProcessEventsAsync does (these rows crossed the actor wire too) happens exactly once, there.
            foreach (var (name, qualified, _, snapshot) in held)
            {
                if (snapshot.Rows.Count == 0)
                {
                    continue;
                }

                if (snapshot.TotalSeen > snapshot.Rows.Count)
                {
                    logger.LogWarning(
                        "Pipeline '{Pipeline}': late attach to source '{Source}' replayed {Replayed} of {TotalSeen} row(s); " +
                        "earlier rows are not recoverable (the source's replay ring holds the most recent {Capacity}).",
                        _def.Name, name, snapshot.Rows.Count, snapshot.TotalSeen, SourceReplayBuffer.Capacity);
                }

                await ProcessEventsAsync(new SourceEventsEnvelope
                {
                    // The QUALIFIED name, because ProcessEventsAsync strips the environment off it exactly
                    // like it does for a live envelope — feeding the bare name would strip nothing and, in
                    // a non-default environment, hand the Engine a relation name it never compiled.
                    Source = qualified,
                    Events = snapshot.Rows.Select(r => new Dictionary<string, object?>(r)).ToList(),
                });
            }
        }
        finally
        {
            foreach (var (name, _, connector, _) in held)
            {
                try
                {
                    await connector.EndAttachAsync();
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex,
                        "PipelineActor[{Id}]: releasing the attach hold on source '{Source}' failed; the source's own safety timer covers it.",
                        _def.Id, name);
                }
            }
        }
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();
        _executor = null;
        _sourceNames = [];
        _running = false;
        await SaveAsync();
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_running);

    public Task<List<string>> GetSourceNamesAsync() => Task.FromResult(_running ? _sourceNames.ToList() : []);

    public async Task ProcessEventsAsync(SourceEventsEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        foreach (var raw in envelope.Events)
        {
            // See IPipelineActor.ProcessEventsAsync's doc comment: this envelope crosses the Dapr
            // actor-invocation wire, which re-boxes every Dictionary<string, object?> value as a
            // JsonElement regardless of whether it was already normalized once at the sf-sources
            // pub/sub ingress (StreamingRuntimeSetup.NormalizeSourceEvents). Re-normalize before the
            // Engine ever sees it.
            JsonValueNormalizer.NormalizeInPlace(raw);

            var evt = new EventRecord(raw);
            _totalEventsIn++;
            _lastEventTsMs = evt.Timestamp;

            // Plan 021 D6: envelope.Source is qualified for routing (see PipelineEventRouter/
            // GeneratorActor/ConnectorActor); this pipeline's own compiled SourceNames are bare, local to
            // its own environment's catalog — strip before the Engine sees the name (mirrors TableActor's
            // identical ProcessSourceEventsAsync treatment).
            var rows = _executor.OnEvent(EnvKeys.Split(envelope.Source).Key, evt);
            if (rows.Count > 0)
            {
                await PublishRowsAsync(rows);
            }
        }
    }

    public Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit) =>
        Task.FromResult(PipelineResultRing.Take(_recentResults, limit));

    public Task<PipelineMetrics> GetMetricsAsync() => Task.FromResult(new PipelineMetrics
    {
        PipelineId = _def?.Id ?? Id.ToString(),
        Status = _running ? PipelineStatus.Running : PipelineStatus.Stopped,
        EventsInPerSec = _lastEventsInPerSec,
        RowsOutPerSec = _lastRowsOutPerSec,
        TotalEventsIn = _totalEventsIn,
        TotalRowsOut = _totalRowsOut,
        WindowsClosed = 0,
        LastEventTsMs = _lastEventTsMs,
        LateEvents = _executor?.LateEvents ?? 0,
    });

    private async Task OnTimerTickAsync()
    {
        if (_executor is null)
        {
            return;
        }

        // See _transportLagAllowanceMs: wall-clock minus the transport allowance, never bare wall-clock.
        var rows = _executor.AdvanceWatermark(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - _transportLagAllowanceMs);
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
            PipelineResultRing.Append(_recentResults, envelope, RecentResultsCapacity);
        }

        try
        {
            await daprClient.PublishEventAsync(
                StreamingRuntimeSetup.PubsubName,
                StreamingRuntimeSetup.PipelineOutTopic,
                new PipelineResultsEnvelope { PipelineId = _def!.Id, Results = batch });
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not tear down the timer or lose the in-memory
            // results/counters bookkeeping above — mirrors GeneratorActor.TickAsync's own try/catch
            // rationale (drop this publish, the next tick/event tries again).
            logger.LogWarning(ex, "PipelineActor[{Id}]: failed to publish {Count} result row(s).", _def?.Id, batch.Count);
        }
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

        try
        {
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.MetricsTopic, metrics);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "PipelineActor[{Id}]: failed to publish metrics.", _def?.Id);
        }
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, new PipelineActorState
    {
        Def = _def,
        Sources = _sources,
        Running = _running,
    });

    private async Task ArmTimerAsync()
    {
        await RegisterTimerAsync(TimerName, nameof(OnTimerTickAsync), null, TickPeriod, TickPeriod);
        _timerArmed = true;
    }

    private async Task DisarmTimerIfArmedAsync()
    {
        if (!_timerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(TimerName);
        _timerArmed = false;
    }
}

/// <summary>
/// Pure bounded-ring append/read logic for <see cref="PipelineActor"/>'s in-memory recent-results cache,
/// extracted for the same reason as <see cref="PipelineCompilation"/>/<see cref="GeneratorBatching"/> —
/// unit-testable without any actor/timer/Dapr-sidecar machinery. Mirrors <c>PipelineGrain</c>'s own
/// inline capacity-trim/range-read logic exactly (capacity 100, oldest-evicted-first, "last N" read).
/// </summary>
public static class PipelineResultRing
{
    /// <summary>Appends <paramref name="item"/>, then trims the ring's oldest entries down to
    /// <paramref name="capacity"/> if it now exceeds it.</summary>
    public static void Append(List<ResultEnvelope> ring, ResultEnvelope item, int capacity)
    {
        ring.Add(item);
        if (ring.Count > capacity)
        {
            ring.RemoveRange(0, ring.Count - capacity);
        }
    }

    /// <summary>The last (up to) <paramref name="limit"/> entries, oldest-to-newest — a non-positive or
    /// larger-than-available <paramref name="limit"/> is clamped, never throws.</summary>
    public static List<ResultEnvelope> Take(List<ResultEnvelope> ring, int limit)
    {
        var take = Math.Max(0, Math.Min(limit, ring.Count));
        var start = ring.Count - take;
        return ring.GetRange(start, take);
    }
}

/// <summary>Persisted shape of a PipelineActor's state — see that class's doc comment for why the
/// definition, source list, and running flag are persisted at all (self-healing across deactivation/
/// reactivation). Plain get/set properties, same style as <see cref="Actors.GeneratorActorState"/>/
/// <see cref="Catalog.CatalogState"/>, for a clean System.Text.Json round trip through Dapr's actor state
/// store.</summary>
public sealed class PipelineActorState
{
    public PipelineDefinition? Def { get; set; }

    public List<SourceDefinition> Sources { get; set; } = [];

    public bool Running { get; set; }
}

/// <summary>
/// Pure SQL-compile-to-executor logic, extracted from <see cref="PipelineActor"/> specifically so it can
/// be unit tested without any actor/timer/Dapr-sidecar machinery (mirrors
/// <see cref="GeneratorBatching"/>'s own extraction rationale) — see
/// dapr/tests/StreamsForge.Dapr.Tests/PipelineCompilationTests.cs. Builds the same schema dictionary +
/// <see cref="SqlCompiler.Compile"/> call <c>PipelineGrain.StartAsync</c> makes.
/// </summary>
public static class PipelineCompilation
{
    public static (PipelineExecutor? Executor, List<string> SourceNames, string? Error) TryCompile(
        PipelineDefinition def, IReadOnlyList<SourceDefinition> sources)
    {
        var schemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.Compile(def.Sql, schemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            return (null, [], message);
        }

        return (compileResult.Plan.CreateExecutor(), compileResult.SourceNames.Distinct().ToList(), null);
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
