using Orleans;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;
using StreamsForge.Engine.Dataflow;
using StreamsForge.Host.Facades;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Grains;

/// <summary>
/// Plan 003 M2: key = "{tableName}:{inputName}". One activation per (table, real external input) — a
/// stream source name or an upstream table name the table's SQL reads from directly (compileResult.
/// StreamInputs / TableInputs — the exact same input set a Parallelism==1 TableGrain subscribes to).
/// Subscribes to that input's EXISTING stream identity (StreamConstants.SourcesNamespace or
/// TableDeltaNamespace) — unchanged from today, so the source/upstream-table side of the system doesn't
/// know or care whether a subscriber is a plain TableGrain or this partitioned path.
///
/// EPOCHING: buffers admitted (weight-normalized) deltas locally and flushes on a 250ms timer tick OR
/// once 1000 events have buffered, whichever first (plan 003's stated epoch-advance rule) — each flush is
/// one epoch, stamped from a local monotonically-increasing counter.
///
/// ROUTING (this is the "ingest per input" stage's actual home — see StreamsForge.Engine.Dataflow.
/// TableDataflowPlan's class doc: an Ingest-kind engine stage always has partition count 1 and exists
/// mainly as a graph-shape concept; this grain performs its real job directly rather than adding an extra
/// grain hop): for each edge TableDataflowPlan.EdgesForExternalInput(inputName) returns, either (a) the
/// edge already targets a Join/SemiAnti stage directly with Broadcast mode (a scalar-subquery/semi-anti
/// join's residual inputs) — route on that edge as-is; or (b) the edge targets an Ingest-kind stage
/// (Mode always Local) — the REAL routing decision is that Ingest stage's own one outbound edge
/// (TableDataflowPlan.OutEdgeOf), always HashPartition — route on THAT edge instead. Every downstream
/// partition gets a call each flush, even an empty one, so a quiet ingest doesn't stall a downstream
/// FrontierTracker (batched per (edge, epoch) per the plan's protocol, not per-row).
/// </summary>
public sealed class TableIngestGrain : Grain, ITableIngestGrain
{
    private string _tableName = "";
    private string _inputName = "";
    private bool _isTableInput;
    private TableDataflowPlan? _dataflow;
    private List<TableEdgeDescriptor> _externalEdges = [];

    private PipelineStatus _status = PipelineStatus.Stopped;
    private StreamSubscriptionHandle<EventRecord>? _streamSub;
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _tableSub;
    /// <summary>Table-over-pipeline: the P&gt;=2 twin of <c>TableGrain._pipelineSubs</c>. One more handle
    /// field rather than a reuse of <see cref="_streamSub"/>, because the payload type differs.</summary>
    private StreamSubscriptionHandle<List<ResultEnvelope>>? _pipelineSub;
    private IGrainTimer? _flushTimer;

    private readonly List<TableDelta> _pending = [];
    private long _epochCounter;

    private const int FlushEventThreshold = 1000;
    // Epoch flush cadence. 250ms is the throughput-friendly default (amortizes cross-partition
    // frontier coordination); it is also the second-largest contributor to end-to-end tableDelta
    // latency after the memory-stream pull period — Tables:FlushMs tunes it (see Program.cs's
    // Streams:PullPeriodMs comment and comparison.html's latency root-cause note).
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(
        int.TryParse(Environment.GetEnvironmentVariable("TABLES__FLUSHMS"), out var ms) && ms > 0 ? ms : 250);

    public async Task StartAsync(TableDefinition def, string inputName)
    {
        await StopAsync();

        // Plan 021 D3 — _tableName is the QUALIFIED table name (used only to compose this table's OWN
        // sibling ITableStageGrain keys below — never crosses into the Engine); _inputName stays BARE
        // (compileResult.TableInputs/StreamInputs are bare — see class doc — and it is compared against
        // the compiled TableDataflowPlan's own bare edge/input names, e.g. `edge.ExternalInputNames.
        // IndexOf(_inputName)` in FlushAsync, an Engine-boundary comparison that a qualified string would
        // simply never match).
        _tableName = EnvKeys.Qualify(def.Environment, def.Name);
        _inputName = inputName;
        var (compile, dataflow) = await TableDataflowFactory.BuildAsync(GrainFactory, def);
        _dataflow = dataflow;
        _isTableInput = compile.TableInputs.Contains(inputName);
        // Plan 003 M3: an "In" edge whose Ingest-kind target stage's OWN outbound edge (the real routing
        // decision — see this grain's class doc) was marked arrangeable is served by a shared
        // ArrangementGrain instead (see TableGrain.StartCoordinatorAsync's arrangement-attach loop) — exclude
        // it here so this ingest grain doesn't ALSO route to it (that would double-deliver, and the target
        // TableStageGrain's upstream set no longer even reserves fromPartition==0 for this edge — see
        // TableStageGrain.StartAsync's producerCount fix). An input with EVERY edge arrangeable ends up with
        // an empty _externalEdges list; TableGrain doesn't even start this grain for such an input (see its
        // _deployedInputs filter) — the check below is defense-in-depth.
        _externalEdges = dataflow.EdgesForExternalInput(inputName)
            .Where(e => e.Mode == TableEdgeMode.Broadcast || dataflow.OutEdgeOf(e.ToStageId).ArrangeKeyFields is null)
            .ToList();

        // The input's own stream is qualified with THIS table's environment (an input can only ever be
        // another entity in the same catalog) — env-qualified for the subscription, distinct from the
        // bare _inputName kept above for Engine-facing comparisons.
        var qualifiedInputName = EnvKeys.Qualify(def.Environment, inputName);
        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        if (_isTableInput)
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, qualifiedInputName));
            _tableSub = await stream.SubscribeAsync((batch, _) => OnTableDeltaBatchAsync(batch));
            _status = PipelineStatus.Running;
        }
        else
        {
            // Table-over-pipeline: a stream input whose name belongs to a PIPELINE reads a different
            // namespace with a different payload, keyed by the pipeline's ID — split off before the source
            // path, the same way TableGrain.StartClassicAsync splits it. Plain subscribe unless plan 026
            // D5's replayFrom names it, in which case the pipeline's own result-batch gate replays it —
            // TableGrain.SubscribeToPipelineInputAsync is the Parallelism == 1 twin of this.
            var pipeline = await PipelineInputs.FindAsync(GrainFactory, def.Environment, inputName);
            if (pipeline is not null)
            {
                await AttachToPipelineInputAsync(streamProvider, def, inputName, pipeline);
            }
            else
            {
                // Subscribe-then-attach against a connector-kind source — the coordinator-mode (Parallelism >= 2)
                // copy of TableGrain.AttachToStreamInputAsync, which carries the full rationale. The one
                // difference that matters here: _status is set to Running BEFORE the replay is fed, because
                // OnStreamEventAsync no-ops while the grain is Stopped and the replayed rows would otherwise be
                // dropped on the floor — the very loss this exists to close.
                await AttachToStreamSourceAsync(streamProvider, def, inputName, qualifiedInputName);
            }
        }

        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, FlushInterval, FlushInterval);
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    /// <summary>See the call site's comment and <c>TableGrain.AttachToStreamInputAsync</c>'s doc for the
    /// protocol and why it is exactly-once. Sets <see cref="_status"/> to Running itself (the caller no
    /// longer does, on either branch) so the replayed rows reach <see cref="OnStreamEventAsync"/>'s
    /// buffering rather than its Stopped guard; a flush of that buffer can only happen on the timer the
    /// caller arms afterwards or at the 1000-event threshold, both of which are safe here.</summary>
    private async Task AttachToStreamSourceAsync(
        IStreamProvider streamProvider, TableDefinition def, string inputName, string qualifiedInputName)
    {
        IReplayableSourceGrain? connector = null;
        SourceReplaySnapshot? snapshot = null;
        // Plan 026 D5 — same rule as the Parallelism == 1 twin: a NAMED input replays from its position
        // through the kind-dispatched driver (generator and ingest included); an unnamed one keeps plan
        // 023's connector-only default attach verbatim.
        var from = ReplayInputs.For(def.ReplayFrom, inputName);

        // GetSourceAsync is on RegistryGrain's [MayInterleave] allowlist (verified), so calling it from here
        // is safe even when the registry is itself awaiting the TableGrain.StartAsync that led to this call.
        try
        {
            var sourceDef = await GrainFactory.RegistryFor(def.Environment).GetSourceAsync(inputName);
            if (sourceDef is not null && from is not null)
            {
                connector = ReplayInputs.DriverFor(GrainFactory, sourceDef.Kind, qualifiedInputName);
                snapshot = connector is null ? null : await connector.BeginAttachAsync(from);
            }
            else if (sourceDef is not null && SourceKindDispatch.Classify(sourceDef.Kind) == SourceKindDispatch.ActorKind.Connector)
            {
                var driver = GrainFactory.GetGrain<IConnectorGrain>(qualifiedInputName);
                connector = driver;
                snapshot = await driver.BeginAttachAsync();
            }
        }
        catch
        {
            // Best-effort, exactly like the other two consumers: losing the backfill is bad, refusing to
            // start the table is worse.
            connector = null;
            snapshot = null;
        }

        try
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, qualifiedInputName));
            _streamSub = await stream.SubscribeAsync((evt, _) => OnStreamEventAsync(evt));
            _status = PipelineStatus.Running;

            if (snapshot is not null)
            {
                foreach (var row in snapshot.Rows)
                {
                    await OnStreamEventAsync(new EventRecord(row));
                }
            }
        }
        finally
        {
            if (connector is not null)
            {
                try { await connector.EndAttachAsync(); } catch { /* the source's own safety timer covers it */ }
            }
        }
    }

    /// <summary>Plan 026 D5 — the Parallelism &gt;= 2 twin of <c>TableGrain.SubscribeToPipelineInputAsync</c>:
    /// plain subscribe unless <see cref="TableDefinition.ReplayFrom"/> names this pipeline, in which case
    /// Begin(from) → subscribe → feed the retained batches through the same
    /// <see cref="OnPipelineBatchAsync"/> handler → End in a <c>finally</c>. <see cref="_status"/> goes
    /// Running before the replay is fed, for the reason the source twin documents: the handler no-ops while
    /// Stopped and the replayed rows would be dropped on the floor.</summary>
    private async Task AttachToPipelineInputAsync(
        IStreamProvider streamProvider, TableDefinition def, string inputName, PipelineDefinition pipeline)
    {
        var key = PipelineInputs.OutputStreamKey(def.Environment, pipeline);
        var from = ReplayInputs.For(def.ReplayFrom, inputName);

        IPipelineGrain? driver = null;
        StreamReplaySnapshot<List<ResultEnvelope>>? snapshot = null;
        if (from is not null)
        {
            try
            {
                driver = GrainFactory.GetGrain<IPipelineGrain>(key);
                snapshot = await driver.BeginAttachAsync(from);
            }
            catch
            {
                // Best-effort, exactly like the source path.
                driver = null;
                snapshot = null;
            }
        }

        try
        {
            var stream = streamProvider.GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, key));
            _pipelineSub = await stream.SubscribeAsync((batch, _) => OnPipelineBatchAsync(batch));
            _status = PipelineStatus.Running;

            if (snapshot is not null)
            {
                foreach (var batch in snapshot.Items)
                {
                    await OnPipelineBatchAsync(batch);
                }
            }
        }
        finally
        {
            if (driver is not null)
            {
                try { await driver.EndAttachAsync(); } catch { /* the pipeline's own safety timer covers it */ }
            }
        }
    }

    public async Task StopAsync()
    {
        _status = PipelineStatus.Stopped;
        _flushTimer?.Dispose();
        _flushTimer = null;

        if (_streamSub is not null) { try { await _streamSub.UnsubscribeAsync(); } catch { /* best-effort */ } _streamSub = null; }
        if (_tableSub is not null) { try { await _tableSub.UnsubscribeAsync(); } catch { /* best-effort */ } _tableSub = null; }
        // Table-over-pipeline — symmetric with the two above.
        if (_pipelineSub is not null) { try { await _pipelineSub.UnsubscribeAsync(); } catch { /* best-effort */ } _pipelineSub = null; }

        _pending.Clear();
        _dataflow = null;
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _status = PipelineStatus.Stopped;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    /// <summary>Table-over-pipeline: one published batch from a pipeline input, fed row by row through the
    /// SAME <see cref="OnStreamEventAsync"/> buffering the source path uses — the deltas this grain
    /// routes downstream are then indistinguishable from a source's, which is the point. Only the
    /// subscription differs, never the ingest.</summary>
    private async Task OnPipelineBatchAsync(List<ResultEnvelope> batch)
    {
        foreach (var envelope in batch)
        {
            await OnStreamEventAsync(PipelineInputs.ToEventRecord(envelope, _inputName));
        }
    }

    private Task OnStreamEventAsync(EventRecord evt)
    {
        if (_status != PipelineStatus.Running) return Task.CompletedTask;
        _pending.Add(new TableDelta(evt, 1)); // a stream event always asserts — mirrors TableGrain.OnStreamEventAsync's weight=1 convention
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnTableDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        if (_status != PipelineStatus.Running) return Task.CompletedTask;
        foreach (var d in batch) _pending.Add(new TableDelta(new EventRecord(d.Row), d.Weight));
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnFlushTickAsync() => FlushAsync();

    private async Task FlushAsync()
    {
        if (_status != PipelineStatus.Running || _dataflow is null) return;

        var epoch = _epochCounter++;
        var batch = _pending.Count == 0 ? [] : new List<TableDelta>(_pending);
        _pending.Clear();

        foreach (var edge in _externalEdges)
        {
            if (edge.Mode == TableEdgeMode.Broadcast)
            {
                int fromPartition = Math.Max(0, edge.ExternalInputNames.ToList().IndexOf(_inputName));
                await RouteAsync(edge, fromPartition, epoch, batch);
            }
            else
            {
                var realEdge = _dataflow.OutEdgeOf(edge.ToStageId); // Ingest-kind stage's own (always HashPartition) forward edge
                await RouteAsync(realEdge, 0, epoch, batch);
            }
        }
    }

    private async Task RouteAsync(TableEdgeDescriptor edge, int fromPartition, long epoch, List<TableDelta> batch)
    {
        int pcount = _dataflow!.PartitionCountOf(edge.ToStageId);
        var tasks = new List<Task>(pcount);

        if (edge.Mode == TableEdgeMode.Broadcast)
        {
            for (int p = 0; p < pcount; p++) tasks.Add(SendAsync(edge, fromPartition, p, epoch, batch));
        }
        else
        {
            var byPartition = batch.Count == 0
                ? new Dictionary<int, List<TableDelta>>()
                : batch.GroupBy(d => _dataflow.PartitionOf(edge.EdgeId, d.Row)).ToDictionary(g => g.Key, g => g.ToList());
            for (int p = 0; p < pcount; p++)
                tasks.Add(SendAsync(edge, fromPartition, p, epoch, byPartition.TryGetValue(p, out var l) ? l : []));
        }

        await Task.WhenAll(tasks);
    }

    private Task SendAsync(TableEdgeDescriptor edge, int fromPartition, int targetPartition, long epoch, List<TableDelta> deltas)
    {
        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
        var target = GrainFactory.GetGrain<ITableStageGrain>($"{_tableName}:{edge.ToStageId}:{targetPartition}");
        return target.PushBatchAsync(edge.EdgeId.Value, fromPartition, epoch, _inputName, dtos);
    }
}
