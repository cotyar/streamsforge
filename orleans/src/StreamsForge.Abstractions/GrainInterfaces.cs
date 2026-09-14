namespace StreamsForge.Abstractions;

/// <summary>Singleton (key = StreamConstants.RegistryKey). Catalog of sources + pipelines; orchestrates
/// start/stop. Plan 005 (Dapr sibling runtime) W1: inherits <see cref="ICatalogFacade"/> — every member
/// except <see cref="EnsureInitializedAsync"/> (Orleans-boot-only) now lives there, runtime-neutral, so
/// the REST/gRPC layer can depend on the facade type instead of this grain interface directly. See
/// Facades.cs's class doc for the full seam rationale.</summary>
public interface IRegistryGrain : ICatalogFacade, IGrainWithStringKey
{
    /// <summary>Seeds defaults on first run, re-activates generators, resumes Running pipelines.</summary>
    Task EnsureInitializedAsync();
}

/// <summary>Plan 026 wave 2 — the batch-typed twin of <see cref="SourceReplaySnapshot"/>: what a
/// pipeline (<c>T = List&lt;ResultEnvelope&gt;</c>) or a table (<c>T = List&lt;TableDeltaDto&gt;</c>) hands a
/// consumer that asked to attach from <see cref="ReplayFrom"/>. <see cref="Items"/> are the retained
/// stream items (one item = one published batch, the unit the stream carries), oldest first, with
/// <see cref="Positions"/> parallel to them; the rest reads as on <see cref="SourceReplaySnapshot"/>.</summary>
[GenerateSerializer]
public sealed class StreamReplaySnapshot<T>
{
    [Id(0)] public List<T> Items { get; set; } = [];
    [Id(1)] public List<long> Positions { get; set; } = [];
    [Id(2)] public long TotalSeen { get; set; }
    [Id(3)] public long FirstSeq { get; set; }
    [Id(4)] public long LastSeq { get; set; }
    [Id(5)] public bool Truncated { get; set; }
}

/// <summary>Plan 026 wave 2 — the attach gate as the replay API for a BATCH-publishing grain (a pipeline's
/// result batches, a table's delta batches), the exact protocol of <see cref="IReplayableSourceGrain"/>
/// with a batch-typed snapshot: Begin(from) → subscribe → feed the items through the same handler → End in
/// a <c>finally</c>. A position is per published BATCH (the stream item), not per row.</summary>
public interface IReplayableBatchGrain<T> : IGrainWithStringKey
{
    Task<StreamReplaySnapshot<T>> BeginAttachAsync(ReplayFrom? from);

    /// <summary>Drops one hold; at zero, everything deferred while held is published. Always in a
    /// <c>finally</c>.</summary>
    Task EndAttachAsync();
}

/// <summary>Key = pipeline id. One activation per running pipeline. Plan 026 wave 2: also a
/// <see cref="IReplayableBatchGrain{T}"/> over its result batches (position = batch; a batch's event time
/// is its first envelope's <c>TimestampMs</c>).</summary>
public interface IPipelineGrain : IReplayableBatchGrain<List<ResultEnvelope>>
{
    Task StartAsync(PipelineDefinition def);
    Task StopAsync();
    Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit);
    Task<PipelineMetrics> GetMetricsAsync();
}

/// <summary>Key = source name. Publishes synthetic events on a grain timer.</summary>
public interface IGeneratorGrain : IReplayableSourceGrain
{
    Task StartAsync(SourceDefinition def);
    Task StopAsync();
    /// <summary>Keep-alive; timers alone don't extend activation lifetime.</summary>
    Task PingAsync();

    /// <summary>Wishlist #8: run-on-demand for a <see cref="GeneratorProfiles.Scenario"/>-profile source
    /// — generates the whole deterministic batch (via <c>StreamsForge.Host.Generators.ScenarioGenerator</c>,
    /// shared/StreamsForge.AppCore/Generators/ScenarioGenerator.cs) and publishes each row onto this
    /// source's existing stream (the SAME (StreamConstants.SourcesNamespace, source name) stream
    /// StartAsync's timer publishes onto — a table/pipeline reading this source doesn't know or care
    /// whether a row arrived from a tick or from RunAsync), then returns the result. Uses whatever
    /// SourceDefinition this activation was last StartAsync'd with — StartAsync unconditionally caches Def
    /// regardless of EventsPerSecond (see GeneratorGrain.StartAsync), so a scenario source with
    /// EventsPerSecond == 0 (the wishlist's "ignored/0 for this kind") still has one on file the moment
    /// the registry creates/enables it, before any run is ever requested.</summary>
    Task<ScenarioRunResult> RunAsync(ScenarioRunRequest request);
}

/// <summary>Wishlist #14 option (a) — the atomic (rows, epoch) pair <see cref="ITableGrain.AttachSnapshotAsync"/>
/// returns. Deliberately defined here (Abstractions, Orleans-only) rather than in shared Contracts: the
/// per-file ownership for this change scopes Contracts to ONLY the additive <c>TableDeltaDto.Epoch</c> field
/// (a wire field every consumer of that DTO — including Dapr and any future polyglot subscriber — must be
/// able to read), whereas this combined-snapshot response is purely an Orleans grain-call return shape, used
/// by exactly one caller (another TableGrain, mid-StartClassicAsync) and never serialized onto any shared
/// pub/sub stream. See <c>StreamsForge.Engine.TableExecutor.LastEpoch</c>'s own doc comment (PublicApi.cs)
/// for the full backfill-on-attach protocol this exists to serve, and TableGrain.AttachSnapshotAsync's own
/// doc comment for exactly how the two fields here are captured atomically.</summary>
[GenerateSerializer]
public sealed class TableAttachSnapshot
{
    [Id(0)] public List<TableRowDto> Rows { get; set; } = [];

    /// <summary>-1 means "this table has never admitted anything yet" (equivalent to
    /// <c>TableExecutor.LastEpoch</c>'s own default) — a caller applies EVERY subsequently-received delta
    /// unconditionally in that case, since nothing is yet reflected in <see cref="Rows"/> to double-count
    /// against.</summary>
    [Id(1)] public long Epoch { get; set; } = -1;

    /// <summary>Plan 026 D7 (additive): the position of the last delta batch this table had PUBLISHED when
    /// the snapshot was taken — the same number a concurrent replaying subscriber sees on that batch — so a
    /// consumer can take (rows, epoch, position) atomically and ask the delta log for everything after it.
    /// 0 = nothing published yet this activation. Coordinator-mode tables (Parallelism ≥ 2) report 0: their
    /// deltas are published by <c>TableOutputGrain</c>, not through this grain's gate (wave 3 owes that).</summary>
    [Id(2)] public long LastSeq { get; set; }
}

/// <summary>Key = table name. One activation per running table. Materializes a Z-set (DBSP-style)
/// incremental view: subscribes to its SQL's stream and table inputs, feeds deltas through a
/// StreamsForge.Engine TableExecutor, and publishes emitted deltas + persists a consolidated snapshot for
/// rehydration-free reads.</summary>
public interface ITableGrain : IReplayableBatchGrain<List<TableDeltaDto>>
{
    Task StartAsync(TableDefinition def);
    Task StopAsync();
    Task<List<TableRowDto>> GetRowsAsync(int limit, int offset);
    Task<int> GetRowCountAsync();
    Task<TableMetrics> GetMetricsAsync();
    Task<long> GetSeqAsync();
    /// <summary>Reverse-index lookup over this table's current rows (see StreamsForge.Host.Search.TableSearchIndex).
    /// Empty query or a table with SearchEnabled=false both yield an empty list — callers that need to tell
    /// those apart go through the /api/tables/{id}/search endpoint instead, which checks SearchEnabled first.</summary>
    Task<List<TableRowDto>> SearchAsync(string query, int limit);

    /// <summary>Plan 003 M4: the epoch a coordinator-mode (Parallelism &gt;= 2) table's read-side snapshot
    /// currently reflects — null for a Parallelism==1 table (see TableMetrics.SnapshotFrontierEpoch, which
    /// carries the identical value; this method exists so the /rows endpoint can read it in O(1) without
    /// paying for GetMetricsAsync's full per-partition fan-out on every poll).</summary>
    Task<long?> GetSnapshotFrontierEpochAsync();

    /// <summary>Plan 003 M4: called by ITableOutputGrain.PublishAsync (one call per terminal-stage
    /// partition's own frontier advance) — the dedicated, epoch-aware ingestion path for a coordinator-mode
    /// table's read-side snapshot, replacing the pre-M4 design of self-subscribing to the table's own output
    /// delta stream (see TableGrain's class doc for why that design couldn't give an honest frontier: batches
    /// from different terminal partitions arrived and were applied independently, so a read between two such
    /// arrivals could observe a partially-applied epoch). This method instead buffers per (partition, epoch)
    /// with the same FrontierTracker+EpochBuffer primitives every other dataflow hop uses, and only
    /// consolidates a batch into the read-side snapshot once EVERY terminal partition has reported reaching
    /// that epoch — making "the snapshot reflects all deltas &lt;= frontierEpoch and none beyond" true by
    /// construction, not by convention. fromPartition/epoch are the terminal stage's own UpstreamId
    /// components (plain ints/longs, matching ITableStageGrain.PushBatchAsync's no-Dataflow-dependency
    /// rule). Not part of the table's public read surface — internal to the M2 grain topology, called only
    /// by this table's own ITableOutputGrain.
    ///
    /// WISHLIST #15/#14, PART 2 — coordinator mode's own "one epoch, one consolidated wire publish": the
    /// RETURN VALUE (additive relative to the pre-existing <c>Task</c> shape — this method has exactly one
    /// caller, <c>TableOutputGrain.PublishAsync</c>, owned by the same change) is this epoch's consolidated,
    /// ready-to-publish delta batch for THIS table's own (StreamConstants.TableDeltaNamespace, tableName)
    /// stream — empty when this call didn't advance the frontier (another terminal partition still holds it
    /// back) or when the epoch's net effect is empty. <see cref="TableGrain"/>'s implementation stays
    /// entirely synchronous (no `await`) to preserve the existing MayInterleave safety argument (see
    /// <c>TableGrain</c>'s own class doc: "safe to interleave because OnOutputBatchAsync's body has no
    /// await") — the caller, which already awaits freely, performs the actual
    /// <c>stream.OnNextAsync(...)</c> publish with what this method returns.</summary>
    Task<List<TableDeltaDto>> OnOutputBatchAsync(int fromPartition, long epoch, List<TableDeltaDto> deltas);

    /// <summary>Wishlist #14 option (a) — REAL backfill on attach, replacing the option (b) warning. Called
    /// by a NEW downstream table's own StartClassicAsync, for each of its declared table inputs, AFTER that
    /// input's delta-stream subscription is already active (see the caller's own doc comment for why that
    /// order matters and why it's sufficient — Orleans queues any stream delivery to a non-reentrant grain
    /// still mid-StartAsync rather than dropping it, so nothing published between "subscribed" and "this
    /// call returns" is lost, only deferred). Atomically (no `await` before both fields are read — see
    /// <see cref="TableAttachSnapshot"/>'s own doc comment) returns this table's CURRENT consolidated
    /// snapshot together with the exact epoch it reflects: <c>TableExecutor.LastEpoch</c> for a classic
    /// (Parallelism==1) table, <c>SnapshotFrontierEpoch</c> for a coordinator-mode one (both are the same
    /// "epoch this table's own read-side state is fully caught up to and no further" concept — see each
    /// field's own doc comment). Idempotent and safely re-callable — reads only, no side effect on this
    /// table's own state or subscriptions.</summary>
    Task<TableAttachSnapshot> AttachSnapshotAsync();
}

/// <summary>Key = table name. One activation per table with row history ever configured. Subscribes to
/// that table's delta stream (StreamConstants.TableDeltaNamespace, table name) and maintains per-row-
/// identity version history per the table's configured retention mode. See
/// StreamsForge.Host.Grains.TableHistoryGrain's class comment for the full design: identity-key derivation,
/// retention semantics, and why this is a plain state grain fed by the delta stream rather than a
/// JournaledGrain.</summary>
public interface ITableHistoryGrain : IGrainWithStringKey
{
    /// <summary>(Re)configures history collection from the table's current definition — applies
    /// HistoryEnabled/HistoryMode/HistoryLimit/HistoryByField/HistoryWindowMs, re-derives the row-identity
    /// column mapping from the table's SQL, and subscribes to (or, if HistoryEnabled is now false,
    /// unsubscribes from) the table's delta stream. Always clears previously accumulated history. Call on
    /// table create and on any history-config or SQL change (mirrors TableGrain's own SQL/search-config
    /// restart semantics).</summary>
    Task ResetAsync(TableDefinition def);

    /// <summary>Disables history collection, unsubscribes, and clears all state — call on table delete.</summary>
    Task DisableAsync();

    /// <summary>Re-subscribes to the delta stream (and keeps the grain alive) WITHOUT clearing previously
    /// accumulated history — unlike ResetAsync. Call on silo/table resume (RegistryGrain.
    /// EnsureInitializedAsync) so history survives a restart the same way the persisted Entries dictionary
    /// already does; a no-op when def.HistoryEnabled is false.</summary>
    Task ResumeAsync(TableDefinition def);

    /// <summary>Version history for one row identity key (as produced by TableGroupKeyExtractor +
    /// RowKeyCodec — see the /api/tables/{id}/rows response's historyKeys). limit &lt;= 0 means "all
    /// retained versions". KeyFound is false when the key has never been observed.</summary>
    Task<TableHistoryQueryResult> GetHistoryAsync(string key, int limit);

    Task<TableHistoryStats> GetStatsAsync();
}

// ============================================================================
// Plan 003 M2: partitioned table execution grain topology. Additive — ITableGrain is unchanged; these
// three grain kinds only exist for a table whose Parallelism &gt;= 2 (see TableGrain's coordinator-mode
// doc comment). Every method receives the table's TableDefinition (already Orleans-serializable) rather
// than a serialized StreamsForge.Engine.Dataflow.TableDataflowPlan — each grain independently recompiles
// the same SQL (via IRegistryGrain, exactly like TableGrain.StartAsync already does) and calls
// TablePlan.CreateDataflow(def.Parallelism) itself; since compilation is a pure function of
// (Sql, streamSchemas, tableSchemas), every grain deterministically arrives at the identical stage/edge
// graph (same stage ids, same EdgeId values) without transmitting Expr-bearing internals over the wire.
// ============================================================================

/// <summary>Key = "{tableName}:{inputName}". One activation per (table, real external input) — a stream
/// source name or an upstream table name the table's SQL reads from directly. Subscribes to that input's
/// existing stream (StreamConstants.SourcesNamespace or TableDeltaNamespace — same identity a Parallelism==1
/// TableGrain would use), stamps epochs (advance every 250ms tick OR 1000 buffered events, whichever
/// first), and batches routed deltas into the dataflow graph's stage-0 partitions per
/// TableDataflowPlan.EdgesForExternalInput's routing (hash/broadcast/local — see TableStageGrain's
/// PushBatchAsync).</summary>
public interface ITableIngestGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def, string inputName);
    Task StopAsync();
}

/// <summary>Key = "{tableName}:{stageId}:{partition}". One activation per (table, dataflow stage,
/// partition) — see StreamsForge.Engine.Dataflow.TableDataflowPlan.Stages. Holds one
/// StreamsForge.Engine.Dataflow.ITableStageExecutor plus a FrontierTracker + EpochBuffer per the M0
/// primitives; PushBatchAsync buffers an inbound (edge, epoch) batch, and on frontier advance processes
/// all newly-ready batches deterministically (EpochBuffer.OnFrontier's ordering), routes emitted deltas to
/// downstream stage partitions (or to TableOutputGrain when this stage's outbound edge is terminal), and
/// propagates its own advanced frontier downstream (as an empty-delta DeltaBatch marker on every
/// downstream partition, so a quiet stage doesn't stall a downstream FrontierTracker forever).</summary>
public interface ITableStageGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def, int stageId, int partition);
    Task StopAsync();

    /// <summary>edgeId/fromPartition/epoch are the raw StreamsForge.Engine.Dataflow EdgeId.Value/UpstreamId
    /// components — passed as plain ints/longs (not the Dataflow types themselves) so this interface has
    /// no Orleans-serialization dependency on StreamsForge.Engine.Dataflow. originName is the real external
    /// input name this batch ultimately traces back to (only meaningful on a Broadcast edge feeding a
    /// scalar-subquery/semi-anti join's nested residual execution — see TableDataflowPlan's class doc);
    /// pass "" when not applicable. deltas.Count == 0 is a valid, expected frontier-marker call.</summary>
    Task PushBatchAsync(int edgeId, int fromPartition, long epoch, string originName, List<TableDeltaDto> deltas);

    Task<TablePartitionMetrics> GetMetricsAsync();
}

/// <summary>Key = table name. One activation per Parallelism &gt;= 2 table. The single terminal publisher
/// (plan 003 M2's terminal-publisher choice — see TableGrain's coordinator-mode doc comment for why a
/// dedicated grain was chosen over "gather to partition 0"): every terminal-stage TableStageGrain (there
/// may be up to Parallelism of them, one per partition of the plan's terminal stage) calls PublishAsync as
/// its own epochs advance; this grain does no buffering/reordering of its own (Z-set consolidation is
/// commutative — see TableDataflowPlan's class doc) and simply republishes each incoming batch onto the
/// SAME (StreamConstants.TableDeltaNamespace, tableName) stream a Parallelism==1 TableGrain would have
/// published to directly — so SignalR (StreamBridgeService), TableHistoryGrain, and any downstream
/// table-over-table subscriber keep working unchanged regardless of which mode produced the table.
///
/// PLAN 003 M4: <paramref name="fromPartition"/>/<paramref name="epoch"/> (a caller's own EdgeId.Value/
/// UpstreamId components, plain ints/longs per the same no-Dataflow-dependency rule ITableStageGrain.
/// PushBatchAsync already follows) additively carry the terminal-stage partition's own frontier at the
/// moment it routed this batch out. PublishAsync forwards them, ALONGSIDE the unchanged stream republish,
/// to the owning ITableGrain's OnOutputBatchAsync — see that method's doc comment for why frontier
/// tracking rides a second, dedicated channel instead of being folded into the shared delta stream's
/// payload (answer: because that stream's item type is a public contract several OTHER unrelated
/// consumers depend on unchanged).</summary>
public interface ITableOutputGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def);
    Task StopAsync();
    Task PublishAsync(int fromPartition, long epoch, List<TableDeltaDto> deltas);
}

// ============================================================================
// Plan 003 M3: shared arrangements. Key = "{inputName}:{keySpecHash}:{partition}" — keySpecHash is
// StreamsForge.Engine.Dataflow.ArrangementKeySpec.HashOf(canonicalKeySpec); two tables that each join the
// SAME raw input on the SAME raw field(s), at the SAME consuming partition count, compute the IDENTICAL
// keySpecHash and therefore land on the SAME ArrangementGrain activations — that's the whole sharing
// mechanism, no separate directory/registry needed (mirrors the M2 "recompile-per-grain" philosophy: any
// caller that recompiles the same table SQL deterministically arrives at the same key). One activation per
// (input, keySpec, partition) — maintains that partition's consolidated Z-set index of the raw input,
// subscribed to the input's stream directly (replacing what a private TableIngestGrain would do for the
// join edges it covers — see TableDataflowBuilder's arrangeability rule for exactly which edges those are).
//
// LIFECYCLE is refcount-driven, not Start/Stop: AttachAsync/DetachAsync increment/decrement a consumer
// count; 0->1 lazily activates (subscribes, and if a persisted checkpoint exists, seeds the index from it
// and marks Rebuilding until live traffic confirms catch-up); ->0 clears all in-memory state AND the
// persisted checkpoint (a stopped/deleted table's arrangement should rebuild fresh on next attach, not serve
// another table's stale leftover data — "rebuild lazily on first attach" per the M3 plan).
//
// RACE-FREE SEED HANDOFF: AttachAsync itself (not a separate call) pushes the current consolidated snapshot
// directly to the new consumer's ITableStageGrain (via TargetGrainKey/TargetEdgeId in the request) BEFORE
// returning and BEFORE the consumer is added to the live-push list used by subsequent flushes — since this
// all happens synchronously within one grain turn (Orleans serializes calls to one activation; nothing else
// can interleave inside AttachAsync's own execution), the target's FrontierTracker is guaranteed to observe
// the snapshot epoch strictly before any live-delta epoch for this arrangement partition's upstream identity
// — no coordinator-mediated relay, no snapshot/live race. SnapshotAsync (separate, side-effect-free) exists
// for read-only inspection (metrics, tests) without touching the live-push set.
// ============================================================================

/// <summary>See this section's class doc above. Every method receives plain/serializable info rather than
/// StreamsForge.Engine.Dataflow types (same InternalsVisibleTo-avoidance rule as ITableStageGrain).</summary>
public interface IArrangementGrain : IGrainWithStringKey
{
    /// <summary>Refcount++ (lazily activates — subscribes to the input's stream, seeds from a persisted
    /// checkpoint if present — on 0-&gt;1); atomically pushes the current consolidated snapshot to
    /// <see cref="ArrangementAttachRequest.TargetGrainKey"/> and starts including this consumer in future
    /// live-delta pushes. Idempotent-ish: re-attaching an already-attached consumerId just re-seeds it (safe,
    /// if occasionally redundant, on a caller retry).</summary>
    Task AttachAsync(ArrangementAttachRequest request);

    /// <summary>Refcount-- ; removes consumerId from the live-push list. At refcount 0: unsubscribes, clears
    /// the in-memory index AND the persisted checkpoint, and allows the grain to deactivate.</summary>
    Task DetachAsync(string consumerId);

    /// <summary>Read-only: the current consolidated index as a batch of assert-weight deltas. No side
    /// effects on the live-push set (use AttachAsync for the race-free seed-then-subscribe handshake).</summary>
    Task<List<TableDeltaDto>> SnapshotAsync();

    /// <summary>Point-in-time metrics — backs TableGrain's Rebuilding fold-in and GET /api/meta/arrangements.</summary>
    Task<ArrangementInfo> GetInfoAsync();
}

/// <summary>Singleton (key = StreamConstants.UsersKey). Plan 005 (Dapr sibling runtime) W1: inherits
/// <see cref="IUserStoreFacade"/> — every member except <see cref="EnsureInitializedAsync"/>
/// (Orleans-boot-only) now lives there, runtime-neutral. See Facades.cs's class doc.</summary>
public interface IUserStoreGrain : IUserStoreFacade, IGrainWithStringKey
{
    /// <summary>Seeds admin/editor/viewer on first run.</summary>
    Task EnsureInitializedAsync();
}

// ============================================================================
// Plan 006 (ingestion connectors), W3A: the connector driver. New interface (frozen-fake constraint,
// D-C) rather than an extension of IGeneratorGrain/ICatalogFacade — FakeRegistryGrain (test fixture)
// implements IRegistryGrain : ICatalogFacade, so any change to those would force a test-file edit.
// See ConnectorGrain (orleans/src/StreamsForge.Host/Grains/ConnectorGrain.cs) for the implementation
// and RegistryGrain's Kind dispatch (UpsertSourceAsync/DeleteSourceAsync/EnsureInitializedAsync).
// ============================================================================

/// <summary>The (rows, totalSeen) pair <see cref="IConnectorGrain.BeginAttachAsync"/> returns — the
/// source-side twin of <see cref="TableAttachSnapshot"/>, and defined here for the same reason that one is
/// (an Orleans grain-call return shape, never serialized onto any shared pub/sub stream).
/// <see cref="Rows"/> are the recent rows this source has already published, oldest first, in the exact
/// shape they went onto the stream (already `_ts`/`_source`-stamped — no `_seq` or other attach-only
/// column is added). <see cref="TotalSeen"/> is everything the source published since its activation came
/// up, including rows the ring has already evicted: <c>TotalSeen &gt; Rows.Count</c> is the honest
/// statement "you are getting the last N of M and the rest are not recoverable", which the caller logs
/// rather than swallowing.</summary>
[GenerateSerializer]
public sealed record SourceReplaySnapshot
{
    [Id(0)] public List<Dictionary<string, object?>> Rows { get; set; } = [];
    [Id(1)] public long TotalSeen { get; set; }

    // Plan 026 wave 1 — additive. Positions[i] is Rows[i]'s producer-assigned position; FirstSeq/LastSeq
    // describe what the producer RETAINS (not this selection); Truncated is "an entry the request would
    // have matched was already evicted". A pre-026 caller reading only Rows/TotalSeen is unaffected.
    [Id(2)] public List<long> Positions { get; set; } = [];
    [Id(3)] public long FirstSeq { get; set; }
    [Id(4)] public long LastSeq { get; set; }
    [Id(5)] public bool Truncated { get; set; }
}

/// <summary>Plan 026 D2 — the attach gate IS the replay API. Every source-kind driver that publishes
/// onto (<c>StreamConstants.SourcesNamespace</c>, source name) implements this: connector
/// (<see cref="IConnectorGrain"/>), generator (<see cref="IGeneratorGrain"/>) and ingest
/// (<see cref="IIngestSourceGrain"/>). The protocol is plan 023's, verbatim (see the long doc on
/// <see cref="IConnectorGrain"/>): Begin → subscribe → feed the snapshot through the same handler → End
/// in a <c>finally</c>. <paramref name="from"/> selects where the snapshot starts (null = everything
/// retained, exactly what the parameterless plan 023 call meant). Resolve the concrete interface by
/// <c>SourceKindDispatch.Classify(def.Kind)</c> — three grain classes implement this, so
/// <c>GetGrain&lt;IReplayableSourceGrain&gt;</c> alone is ambiguous.</summary>
public interface IReplayableSourceGrain : IGrainWithStringKey
{
    Task<SourceReplaySnapshot> BeginAttachAsync(ReplayFrom? from);

    /// <summary>Drops one hold taken by <c>BeginAttachAsync</c>; at zero, everything the source
    /// produced while held is published. Always call it in a <c>finally</c>.</summary>
    Task EndAttachAsync();
}

/// <summary>Plan 026 D3 — the turn-based owner of an INGEST source's publishing. Ingest rows used to go
/// from <c>OrleansIngressFacade</c>'s drain pump straight onto the stream (a facade, no grain), which
/// left nowhere for the attach gate and the replay log to live. One grain per (qualified) ingest source
/// name, one call per drained batch; <c>SourceIngressBuffer</c>, overflow policy and the
/// <c>DownstreamDropped</c> accounting stay in the facade, untouched.</summary>
public interface IIngestSourceGrain : IReplayableSourceGrain
{
    /// <summary>Stamps and publishes one drained batch through the gate, in order.</summary>
    Task PublishAsync(List<Dictionary<string, object?>> rows);
}

/// <summary>Key = source name. Drives one connector-kind source ("url" | "file" | "folder" | "grpc" —
/// see SourceKinds; "generator"-kind sources use IGeneratorGrain instead, never this interface).
/// url/file/folder poll on a schedule (Cronos cron or fixed interval, D-E backoff on failure); grpc is
/// a persistent subscription to a remote StreamsForge instance (D-G federation) fed by a background
/// task whose callbacks marshal back through <see cref="EmitRowsAsync"/> (grain-safe re-entry — the
/// callbacks run off this grain's turn and must not touch grain state directly).</summary>
public interface IConnectorGrain : IReplayableSourceGrain
{
    Task StartAsync(SourceDefinition def);
    Task StopAsync();
    /// <summary>Keep-alive; timers alone don't extend activation lifetime (mirrors IGeneratorGrain).</summary>
    Task PingAsync();
    Task<ConnectorRuntimeStatus> GetStatusAsync();
    /// <summary>gRPC-subscriber callback entry: rows decoded off this grain's turn are handed back here
    /// via a captured self-reference so publishing/counter updates happen inside a normal grain call.</summary>
    Task EmitRowsAsync(List<Dictionary<string, object?>> rows, long remoteSeq);

    /// <summary>Subscribe-then-attach for STREAM inputs — the source-side twin of
    /// <see cref="ITableGrain.AttachSnapshotAsync"/>, and the fix for the creation-time replay window (a
    /// table/pipeline written after its source was already enabled and polling used to get nothing, because
    /// memory streams have no replay and the source had no memory of what it emitted).
    ///
    /// <para>Protocol, from the CONSUMER's side, in this order: (1) <c>BeginAttachAsync</c> — takes a hold
    /// on this source's publishing and returns its recent rows; (2) subscribe to the source's stream;
    /// (3) feed the returned rows through the SAME handler the subscription uses; (4)
    /// <see cref="EndAttachAsync"/>, in a <c>finally</c>. While a hold is outstanding the source publishes
    /// nothing — new rows queue inside the driver and are released (to every subscriber, including the new
    /// one) when the last hold is dropped. That is what makes the replay exactly-once: nothing can be
    /// published into the gap between the snapshot being taken and the subscription existing, so no row is
    /// both replayed and delivered live, and none is missed.</para>
    ///
    /// <para>THE ONE GAP, MEASURED: the hold stops the source PUBLISHING, and has no reach into the stream
    /// provider's own pipeline. A row already handed to <c>OnNextAsync</c> may still be in the memory
    /// stream's queue, not yet pulled into the cache the pulling agent serves new subscribers from (default
    /// pull period 100 ms) — a consumer subscribing inside that window receives it live AND replays it from
    /// the ring. Subscribing the instant a source's counter reached 500 rows produced 501 deliveries idle
    /// and 554 under load; waiting ~2 s for the stream to quiesce produced exactly 500. So: exactly-once
    /// outside a window roughly one pull period wide, at-least-once inside it. That window is not the case
    /// this exists for (a table written minutes after its source started), and closing it would mean the
    /// gate reaching into the stream provider's delivery pipeline — which is not a seam Orleans offers. A
    /// consumer that cannot tolerate a duplicate at all should key its admission (a table's
    /// <c>LATEST BY</c> already does, which is why every table test here is exact regardless).</para>
    ///
    /// <para>Reentrant-safe and cheap; holds are counted, so several consumers may attach at once. A
    /// consumer that dies between the two calls does NOT gate the source forever — the driver arms its own
    /// short safety release. Only sources classified <c>SourceKindDispatch.ActorKind.Connector</c> have
    /// this driver at all; generators (continuous), ingest sources and CRDT documents are attached to
    /// without it. (Plan 026: generators and ingest sources now carry the same gate through
    /// <see cref="IReplayableSourceGrain"/>, but tables/pipelines still attach to them without it until
    /// a definition opts in via <c>replayFrom</c> — wave 2. Only gRPC/SignalR subscribers use theirs.)</para></summary>
    Task<SourceReplaySnapshot> BeginAttachAsync();
}
