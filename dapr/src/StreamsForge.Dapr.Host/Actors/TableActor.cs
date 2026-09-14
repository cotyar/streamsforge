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
using StreamsForge.Host.Search;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: Dapr counterpart of Orleans' <c>TableGrain</c>
/// (orleans/src/StreamsForge.Host/Grains/TableGrain.cs) — one actor per running table (actor type
/// "TableActor", key = the table's <see cref="TableDefinition.Name"/>), CLASSIC (Parallelism==1) PATH
/// ONLY (see <see cref="ITableActor"/>'s class doc for the D-F descope). Compiles the table's SQL via the
/// shared <see cref="StreamsForge.Engine"/> Z-set (DBSP-style) <see cref="TableExecutor"/>, feeds it batches
/// of source events / upstream-table deltas routed in by <see cref="Streaming.TableEventRouter"/>,
/// publishes emitted delta batches to Dapr pub/sub (<c>sf-table-delta</c>) for <see cref="Streaming.
/// DaprStreamBridge"/>/W7-B's <c>TableHistoryActor</c> to relay/consume, and persists a consolidated
/// snapshot with write-behind (dirty flag, flushed every 2s or on stop/deactivate — mirrors
/// <c>TableGrain</c>'s identical cadence rationale: one JSON write per delta would thrash). Read every
/// method next to its Orleans equivalent; deviations are called out explicitly.
///
/// <para><b>Acyclic by construction — see <see cref="ITableActor"/>'s class doc.</b> Everything this actor
/// needs (definition, full source/table catalog, per-batch events/deltas) arrives as a method parameter; it
/// never resolves <see cref="ICatalogFacade"/> or any other actor.</para>
///
/// <para><b>State: the definition, source/table lists, running flag, AND the write-behind snapshot/seq ARE
/// persisted</b> — a superset of <see cref="PipelineActor"/>'s own persisted state (which has no
/// snapshot-equivalent to persist: a pipeline's "output" is a transient results ring, not a durable Z-set).
/// Same self-heal rationale as <see cref="PipelineActor"/>: Dapr actor timers do NOT survive deactivation/
/// reactivation, so <see cref="OnActivateAsync"/> recompiles from persisted state and immediately re-arms
/// the flush timer if the last known state was Running, instead of waiting for
/// <see cref="Services.TableSupervisorService"/>'s next sweep.</para>
///
/// <para><b>Plan 008 — <see cref="TableDefinition.Persistence"/> governs how the periodic flush WRITE
/// reaches the Redis-backed actor state store; it never changes how/when the in-memory read cache
/// (<see cref="_flushed"/>) is refreshed</b> — see <see cref="TablePersistencePolicy"/> for the pure
/// per-tick decision, made once per <see cref="OnFlushTickAsync"/> in <see cref="DecidePersistAction"/>.
/// <see cref="TablePersistenceMode.Batched"/> (default) is byte-for-byte the pre-008 behavior above: the
/// write is awaited inside the same actor turn as the tick. <see cref="TablePersistenceMode.FireAndForget"/>
/// captures the DTO synchronously (so a later mutation of <see cref="_flushed"/> can never race the
/// in-flight write) then hands it to a background <see cref="Task.Run(Func{Task})"/> that explicitly calls
/// <c>StateManager.SaveStateAsync()</c> itself (mirroring <see cref="TableHistoryActor.OnDeactivateAsync"/>'s
/// own note: outside a normal actor turn, nothing auto-saves for you) — single-flight via
/// <see cref="_inFlightPersist"/>, so an overlapping tick is skipped rather than started, and a failure is
/// always logged (see <see cref="StartBackgroundPersist"/>), never silently dropped.
/// <see cref="TablePersistenceMode.MemoryOnly"/> never calls <c>StateManager</c> on ANY path — flush tick,
/// <see cref="StartAsync"/>, <see cref="StopAsync"/>, or <see cref="OnDeactivateAsync"/> (see
/// <see cref="SaveControlStateAsync"/>'s guard) — so a MemoryOnly table's actor state row in Redis is
/// whatever it was before the table was switched to MemoryOnly (nothing, for a table that has always been
/// MemoryOnly). <b>Deliberate divergence from Batched/FireAndForget's self-heal:</b> because nothing is
/// ever written, <see cref="OnActivateAsync"/> has no persisted <c>Running=true</c> to find after a host
/// restart, so a MemoryOnly table does NOT self-heal immediately on reactivation the way the other two
/// modes do — it comes back via <see cref="Services.TableSupervisorService"/>'s next ~15s sweep instead
/// (which restarts it from the catalog's own independently-persisted Running status). This is the honest
/// consequence of "never touches storage on any path", not an oversight — reads stay live either way
/// (<see cref="_flushed"/> is still refreshed every tick purely in memory) once that sweep has run.
/// <see cref="TableDefinition.FlushMs"/> (0 → 2000 ms, see <see cref="TablePersistencePolicy.
/// ResolveFlushIntervalMs"/>) is honored by all three modes for the in-memory read-cache cadence; per its
/// own doc comment it is otherwise irrelevant to MemoryOnly (nothing to durably flush), so
/// <see cref="ResolveFlushPeriod"/> pins MemoryOnly's timer to the plain default instead of reading a knob
/// that would misleadingly suggest a durability tradeoff MemoryOnly doesn't have.</para>
///
/// <para><b>RESTART-RESUME LIMITATION — identical to <c>TableGrain</c>'s, not a new one:</b> the persisted
/// snapshot only ever captures this table's OUTPUT rows, not its operators' internal state (join indexes,
/// GROUP BY multisets/accumulators) — that cannot be reconstructed from the output alone. Exactly like
/// <c>TableGrain.StartClassicAsync</c>, <see cref="ActivateExecutor"/> (shared by both <see cref="StartAsync"/>
/// and <see cref="OnActivateAsync"/>'s self-heal branch) detects a non-empty persisted snapshot as "this is
/// a resume, not a first start" and wipes it (marking <see cref="_rebuilding"/>) rather than serving stale
/// rows behind a freshly-empty executor — the table rebuilds purely from live traffic going forward.
/// <b>One honest deviation from Orleans' incidental behavior</b> (see <see cref="OnActivateAsync"/>'s own
/// doc comment): because Dapr actors activate on-demand (any RPC — including a read call — triggers
/// <see cref="OnActivateAsync"/> first), there is no window where a stale pre-restart snapshot is briefly
/// served before the resume-reset runs, the way there incidentally is on Orleans (a REST read arriving
/// between silo boot and <c>RegistryGrain</c>'s resume loop reaching this specific grain). The very first
/// read after a Dapr restart already reflects <c>Rebuilding=true</c> with empty rows — earlier/more honest
/// disclosure, not later.</para>
///
/// <para><b>Two distinct sequence counters — do not conflate them:</b> <see cref="GetSeqAsync"/> exposes a
/// FLUSH-GENERATION counter (incremented once per <see cref="FlushAsync"/> call, mirroring
/// <c>TableGrain.state.State.Seq</c> exactly — a REST read-cursor concept), while <c>TableDeltaEnvelope.Seq</c>
/// (published on <c>sf-table-delta</c>, see <see cref="ApplyAndPublishAsync"/>) is a SEPARATE, transient,
/// per-published-BATCH counter this actor owns — see <c>Streaming.DaprStreamBridge.OnTableDeltaAsync</c>'s
/// own doc comment: "unlike the Orleans side (which assigns its own monotonic <c>_tableSeq</c> counter
/// locally per subscription), the Dapr envelope already carries the table's own <c>TableDeltaEnvelope.Seq</c>
/// — this bridge only relays it, it never invents one; <see cref="TableActor"/> is the single source of
/// truth for sequence numbers on this flavor." Not persisted (resets to 0 across a reactivation, exactly
/// like <see cref="PipelineActor"/>'s own unpersisted <c>_seq</c> for <c>ResultEnvelope.Seq</c>) — it is a
/// live-ordering aid for SignalR subscribers, not a durability guarantee.</para>
///
/// <para><b>Reads: flushed snapshot vs. live executor — same split as <c>TableGrain</c>'s classic path.</b>
/// <see cref="GetRowsAsync"/>/<see cref="GetRowCountAsync"/>/<see cref="GetSeqAsync"/> serve the
/// write-behind-flushed copy (up to ~2s stale — <c>TableGrain</c>'s classic path has this exact same
/// staleness; only its Parallelism&gt;=2 coordinator mode reads live). <see cref="SearchAsync"/> instead
/// reads the LIVE <see cref="TableExecutor.Snapshot"/> for weight lookup (the search INDEX itself is kept
/// live too, updated incrementally on every delta in <see cref="ReflectDeltasInSearchIndex"/>) — mirroring
/// <c>TableGrain.SearchAsync</c>'s identical live-vs-flushed split precisely.</para>
///
/// <para><b>Plan 009 A2 — JOURNALED MODE</b>, mirroring <c>TableGrain</c>'s identical Plan 009 A2 paragraph
/// (read that one first — the design rationale is stated there once and applies verbatim to both flavors;
/// see especially "WHY A SECOND STATE, NOT AN APPEND-ONLY STORAGE PROVIDER"). <see cref="TablePersistenceMode.
/// Journaled"/> adds a SECOND actor state key, <see cref="JournalStateName"/> ("table-journal"), holding
/// <see cref="TableJournalState.Entries"/> — canonical row keys changed since the last compaction, coalesced
/// by key. A dirty flush tick under Journaled dispatches to <see cref="JournalFlushAsync"/> instead of
/// <see cref="SaveAsync"/>: it merges <see cref="_pendingJournalEntries"/> (populated by
/// <see cref="RecordJournalEntries"/>, called from <see cref="ApplyAndPublishAsync"/> exactly where
/// <see cref="ReflectDeltasInSearchIndex"/> is) into <see cref="_journalEntries"/> and writes ONLY that small
/// state — <see cref="StateName"/> ("table", the Def/Sources/Tables/Running/Snapshot/Seq bundle) is left
/// UNTOUCHED on an ordinary Journaled tick, which is the entire point (O(changed), not O(|table|)). Once
/// <see cref="_journalEntries"/> reaches <see cref="TableJournalPolicy.ResolveJournalMaxEntries"/>, it
/// compacts (<see cref="CompactJournalAsync"/>): writes the full "table" state once (the same
/// <see cref="SaveAsync"/> a Batched tick uses) and truncates the journal.
///
/// <b>Lifecycle saves (Start/Stop/Deactivate) are the ONE deliberate exception</b> — unlike <c>TableGrain</c>
/// (which has nowhere else to persist "Running", so its own Stop/Deactivate final flush stays a small
/// journal-only write, same as any other tick), this actor's "table" state key is ALSO the self-heal record
/// (<see cref="OnActivateAsync"/> reads <see cref="TableActorState.Running"/>/<c>Def</c> from it). Every
/// lifecycle save therefore stays a FULL <see cref="SaveAsync"/> regardless of mode (already true pre-009 —
/// see <see cref="SaveControlStateAsync"/>'s own doc comment: "rare, config-change-triggered calls, not the
/// hot per-tick path"), and — Journaled-specific — additionally clears the journal right after, since that
/// full write just made "table" authoritative again and any leftover journal entries would otherwise be
/// replayed A SECOND TIME on top of it at the next activation.
///
/// <b>Resume:</b> because every lifecycle save above already compacts (folds the journal into "table" and
/// clears it), an ordinary Stop/restart never leaves a non-empty journal behind to replay. The one path that
/// DOESN'T go through <see cref="SaveControlStateAsync"/> first is a hard process crash — no clean
/// Stop/Deactivate at all — which can leave a real, unread journal sitting next to a stale "table" snapshot.
/// <see cref="ActivateExecutor"/> handles exactly that: it calls <see cref="TableJournalPolicy.ReplayOntoSnapshot"/>
/// UNCONDITIONALLY (not gated on the mode being activated INTO, defense-in-depth for a mode switch too) to
/// fold <see cref="_journalEntries"/> onto <see cref="_flushed"/> BEFORE the existing "non-empty flushed
/// snapshot means resume" check just below — so that check, and the reset it performs, sees the TRUE
/// last-known row set. The journal is then unconditionally cleared (in memory immediately; on disk via the
/// caller's own follow-up save) whenever it held anything, exactly like <c>TableGrain</c>'s
/// <c>ClearStaleJournalAsync</c>.</para>
///
/// <para><b>Plan 011 wave C — the read-cache refresh is O(changed), in every mode.</b> The per-tick
/// <see cref="CaptureSnapshot"/> no longer rebuilds <see cref="_flushed"/> from the whole executor snapshot;
/// it applies only the row keys whose entry changed since the previous capture (<see cref="_touchedRowKeys"/>).
/// This mirrors <c>TableGrain.CaptureSnapshotIntoState</c> — same fix, same invariant, same reason: the old
/// shape allocated one fresh row dictionary PER ROW every FlushMs forever, i.e. allocation proportional to
/// the table at a rate set by the clock. No persistence mode's contract moves; the one flavor-specific
/// consequence (FireAndForget's background write can no longer alias _flushed) is handled in
/// <see cref="StartBackgroundPersist"/>. What this does NOT fix, stated plainly: the executor's ledger,
/// _flushed, and the search index are all still O(distinct keys) and nothing evicts — a table over an
/// unbounded key space still grows without bound. See orleans/DESIGN.md's "Known ceilings", which covers
/// both flavors.</para>
/// </summary>
public sealed class TableActor(ActorHost host, DaprClient daprClient, TableEventRouter tableRouter, ILogger<TableActor> logger)
    : Actor(host), ITableActor
{
    private const string StateName = "table";
    private const string JournalStateName = "table-journal";
    private const string FlushTimerName = "table-flush";

    private TableDefinition? _def;
    private List<SourceDefinition> _sources = [];
    private List<TableDefinition> _tables = [];

    /// <summary>Table-over-pipeline (plan 025) — every pipeline the catalog knew about at the last
    /// StartAsync/self-heal, the same "full list, not just this table's own dependencies" shape as
    /// <see cref="_sources"/>/<see cref="_tables"/> (see <see cref="TableStartRequest.Pipelines"/>'s own
    /// doc comment). Persisted (<see cref="TableActorState.Pipelines"/>) for the identical self-heal
    /// reason <see cref="_sources"/>/<see cref="_tables"/> are.</summary>
    private List<PipelineDefinition> _pipelines = [];

    private bool _running;
    private bool _timerArmed;

    private TableExecutor? _executor;
    private TableSearchIndex? _searchIndex;
    private List<string> _streamInputs = [];
    private List<string> _tableInputs = [];

    /// <summary>Table-over-pipeline (plan 025) — the pipeline relation NAMES this compile resolved (a
    /// subset of <c>TableCompileResult.StreamInputs</c>, split off by pipeline-name membership exactly
    /// like <c>Catalog.CatalogStore.ApplyCompileResult</c> does for the persisted
    /// <see cref="TableDefinition.PipelineInputs"/> field). Used to build <see cref="_pipelineNameById"/>
    /// in <see cref="RegisterRouterAndAttachToTableInputsAsync"/>.</summary>
    private List<string> _pipelineInputs = [];

    /// <summary>Table-over-pipeline (plan 025) — pipeline id (bare GUID, what a live
    /// <c>PipelineResultsEnvelope.PipelineId</c> actually carries — see <see cref="TableInputNames.
    /// PipelineInputs"/>'s own doc comment) → the LOCAL relation NAME this table's SQL used for it, so
    /// <see cref="ProcessPipelineResultsAsync"/> knows which compiled relation an incoming envelope
    /// belongs to. Rebuilt from scratch by <see cref="RegisterRouterAndAttachToTableInputsAsync"/> on
    /// every StartAsync/self-heal, exactly like <see cref="_tableInputCutoffEpoch"/>.</summary>
    private readonly Dictionary<string, string> _pipelineNameById = new(StringComparer.Ordinal);

    private string? _lastCompileError;

    /// <summary>PARITY.md debt item D2 — Dapr counterpart of <c>TableGrain</c>'s identical field: BARE
    /// upstream table name -&gt; the epoch (<see cref="TableAttachSnapshot.Epoch"/>) this table's backfill
    /// for that input was taken at, for <see cref="ProcessTableDeltasAsync"/>'s own epoch filter — see
    /// <see cref="RegisterRouterAndAttachToTableInputsAsync"/>'s doc comment for the full protocol.
    /// Populated once per table input during that method, before it returns; cleared on <see cref="StopAsync"/>
    /// and re-populated from scratch by every subsequent <see cref="StartAsync"/>/self-heal reactivation.</summary>
    private readonly Dictionary<string, long> _tableInputCutoffEpoch = new(StringComparer.Ordinal);

    /// <summary>Write-behind-flushed consolidated snapshot (canonical row key -&gt; row/weight) —
    /// <see cref="GetRowsAsync"/>/<see cref="GetRowCountAsync"/> read THIS, not the live executor (see
    /// class doc's "flushed vs. live" split). Persisted verbatim as <see cref="TableActorState.Snapshot"/>.</summary>
    private Dictionary<string, TableRowDto> _flushed = [];

    /// <summary>Flush-generation counter — see class doc's "two distinct sequence counters" note.</summary>
    private long _seq;

    /// <summary>Per-published-delta-batch counter riding on <c>TableDeltaEnvelope.Seq</c> — see class doc's
    /// "two distinct sequence counters" note. Deliberately NOT persisted (transient live-ordering aid).</summary>
    private long _deltaSeq;

    private bool _dirty;
    private bool _rebuilding;
    private long _deltasIn;
    private long _deltasOut;
    private long _lastUpdateMs;

    /// <summary>Plan 008 single-flight guard for <see cref="TablePersistenceMode.FireAndForget"/> — true
    /// while a background <see cref="StateManager"/> write started by <see cref="StartBackgroundPersist"/>
    /// has not yet completed. Checked by <see cref="DecidePersistAction"/> so an overlapping flush tick
    /// skips starting a second write instead of racing the first one on the same "table" state key.</summary>
    private volatile bool _persistInProgress;

    /// <summary>The currently in-flight (or most recently completed) background write, if any — joined by
    /// <see cref="SaveControlStateAsync"/> before a lifecycle save (Start/Stop/Deactivate) issues its own
    /// write, so the two never race the same state key. The background task catches and logs its own
    /// exceptions (see <see cref="StartBackgroundPersist"/>), so awaiting it here never throws.</summary>
    private Task? _inFlightPersist;

    /// <summary>Plan 009 A2 — <see cref="TablePersistenceMode.Journaled"/>'s persisted (in-memory mirror of
    /// the <see cref="JournalStateName"/> actor state key) journal: canonical row key -&gt; latest
    /// (row, weight) changed since the last compaction, coalesced by key. See class doc's Plan 009 A2
    /// paragraph.</summary>
    private Dictionary<string, TableJournalEntry> _journalEntries = [];

    /// <summary>Plan 009 A2 — canonical row keys touched since the LAST flush tick (not since the last
    /// compaction — that's <see cref="_journalEntries"/>), populated by <see cref="RecordJournalEntries"/>
    /// from the same call site that feeds <see cref="ReflectDeltasInSearchIndex"/>. Only ever non-empty
    /// transiently, between a delta batch landing and the next <see cref="JournalFlushAsync"/> merging it in.</summary>
    private readonly Dictionary<string, TableJournalEntry> _pendingJournalEntries = [];

    /// <summary>Plan 011 wave C — canonical row keys touched since the last <see cref="CaptureSnapshot"/>,
    /// i.e. exactly the entries of <see cref="_flushed"/> that are currently out of date with respect to the
    /// live executor. This is what makes the per-tick read-cache refresh O(changed) instead of O(|table|).
    /// Unlike the Orleans mirror's equivalent set, this one is maintained for EVERY persistence mode
    /// including <see cref="TablePersistenceMode.MemoryOnly"/> — on this flavor _flushed is the READ source,
    /// not just a durability mirror, so it is refreshed on every dirty tick regardless of mode (see
    /// <see cref="CaptureSnapshot"/>'s own doc comment) and the set is therefore always drained.</summary>
    private readonly HashSet<string> _touchedRowKeys = new(StringComparer.Ordinal);

    /// <summary>Plan 011 wave C — forces the next <see cref="CaptureSnapshot"/> to rebuild
    /// <see cref="_flushed"/> wholesale instead of applying <see cref="_touchedRowKeys"/>. Set by
    /// <see cref="ActivateExecutor"/>, where a brand-new executor and a reset _flushed are both empty, so
    /// the incremental path never has to trust state carried across an activation or a restart-resume.</summary>
    private bool _fullCaptureNeeded = true;

    /// <summary>Plan 009 A2 — set by <see cref="ActivateExecutor"/> when it found (and in-memory-cleared) a
    /// non-empty <see cref="_journalEntries"/> during resume; <see cref="StartAsync"/>/<see cref="OnActivateAsync"/>'s
    /// self-heal branch persist that clear via <see cref="SaveJournalAsync"/> right after, so a stale journal
    /// never survives past the very next activation regardless of which mode it's activating into — see
    /// class doc's "Resume" paragraph.</summary>
    private bool _journalNeedsPersistedClear;

    /// <summary>Self-heal on (re)activation — same rationale as <see cref="PipelineActor.OnActivateAsync"/>:
    /// Dapr actor timers do not survive deactivation, so a fresh activation whose persisted state says
    /// "Running" recompiles and re-arms the flush timer immediately rather than waiting for
    /// <see cref="Services.TableSupervisorService"/>'s next ~15s sweep.
    ///
    /// <para>See class doc's "RESTART-RESUME LIMITATION" paragraph for why this collapses the
    /// stale-snapshot-serving window Orleans incidentally has: <see cref="ActivateExecutor"/> runs
    /// synchronously here, before ANY method on this actor (including a read call that itself triggered
    /// this very activation) can execute — so the resume-reset always happens before the first
    /// post-restart read is observable, not after.</para></summary>
    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<TableActorState>(StateName);
        if (existing.HasValue)
        {
            _def = existing.Value.Def;
            _sources = existing.Value.Sources;
            _tables = existing.Value.Tables;
            _pipelines = existing.Value.Pipelines;
            _running = existing.Value.Running;
            _flushed = existing.Value.Snapshot;
            _seq = existing.Value.Seq;
        }

        // Plan 009 A2: load the journal too (whether or not this activation ends up Journaled — see
        // ActivateExecutor's unconditional replay for why it must be read regardless of the CURRENT mode).
        var existingJournal = await StateManager.TryGetStateAsync<TableJournalState>(JournalStateName);
        if (existingJournal.HasValue)
        {
            _journalEntries = existingJournal.Value.Entries;
        }

        if (_running && _def is not null)
        {
            ActivateExecutor();
            if (_executor is not null)
            {
                // PARITY.md D2: a self-healed reactivation compiles a BRAND-NEW executor exactly like a
                // fresh StartAsync does (see class doc's "RESTART-RESUME LIMITATION" — ActivateExecutor
                // resets _flushed/marks _rebuilding whenever it finds a non-empty persisted snapshot), so
                // it needs the identical router-registration-then-attach treatment StartAsync gets, not
                // just the timer re-arm — otherwise a table that self-heals (rather than going through an
                // explicit start) over an already-warm upstream would silently skip both the backfill AND
                // the warm-upstream diagnostic. See RegisterRouterAndAttachToTableInputsAsync's own doc
                // comment for why running it here, before this OnActivateAsync call returns, is exactly as
                // race-free as running it inside StartAsync — this whole method is itself part of the one
                // actor turn that gates the very first post-activation invocation.
                await RegisterRouterAndAttachToTableInputsAsync();

                // Plan 009 A2: persist the (possibly just-cleared) journal BEFORE arming the timer — see
                // class doc's "Resume" paragraph and _journalNeedsPersistedClear's own doc comment for why
                // this can't just wait for the next periodic tick (a mode switch away from Journaled would
                // never take the JournalFlushAsync branch again, leaving a stale journal on disk forever).
                if (_journalNeedsPersistedClear)
                {
                    await SaveJournalAsync();
                    _journalNeedsPersistedClear = false;
                }

                await ArmTimerAsync(ResolveFlushPeriod());
            }
            else
            {
                logger.LogWarning(
                    "TableActor[{Name}]: self-heal compile failed on reactivation — leaving stopped: {Error}",
                    _def.Name, _lastCompileError);
                _running = false;
            }
        }
    }

    public async Task<ActorResult<TableInputNames>> StartAsync(TableStartRequest request)
    {
        await DisarmTimerIfArmedAsync();

        _def = request.Def;
        _sources = request.Sources;
        _tables = request.Tables;
        _pipelines = request.Pipelines ?? [];

        // Defensive-only: Catalog.CatalogStore.ValidateParallelism already rejects Parallelism != 1 at
        // CRUD time (decision D-F) — partitioned execution never legitimately reaches this actor. Assert
        // anyway per the plan brief, so a TableActor can never silently run a partitioned definition if
        // some future caller ever skips that validation.
        if (_def.Parallelism > 1)
        {
            _executor = null;
            _running = false;
            await SaveControlStateAsync();
            return ActorResult<TableInputNames>.Failure(
                $"Parallelism must be 1 on the Dapr flavor (got {_def.Parallelism}) — partitioned execution is Orleans-only.");
        }

        // Plan 011 D1: key sharding is Orleans-only (see Catalog.CatalogStore's ValidateParallelism doc
        // for why this refusal lives here and not at upsert). Unlike the Parallelism check above this one
        // is NOT merely defensive — it is the only place the flavor says no, so the message has to be the
        // one a user reads. A sharded table stores fine here and never runs here.
        if (_def.ShardBy.Count > 0)
        {
            _executor = null;
            _running = false;
            await SaveControlStateAsync();
            return ActorResult<TableInputNames>.Failure(
                $"shardBy must be empty on the Dapr flavor (got {string.Join(", ", _def.ShardBy)}) — key sharding is Orleans-only. The definition is stored as-is so it can be promoted back to an Orleans instance without loss, but it cannot run here.");
        }

        // Plan 026 D5: replayFrom is applied by the Orleans grains' attach gates at start; this flavor has
        // no position-addressed gate yet (dapr/PARITY.md D11), so — same rule as shardBy above — the
        // definition stores fine here and never runs here with it set.
        if (_def.ReplayFrom.Count > 0)
        {
            _executor = null;
            _running = false;
            await SaveControlStateAsync();
            return ActorResult<TableInputNames>.Failure(
                $"replayFrom must be empty on the Dapr flavor (set for {string.Join(", ", _def.ReplayFrom.Keys)}) — replay from a position is Orleans-only until dapr/PARITY.md D11 is closed. The definition is stored as-is so it can be promoted back to an Orleans instance without loss, but it cannot run here.");
        }

        ActivateExecutor();
        if (_executor is null)
        {
            _running = false;
            await SaveControlStateAsync();
            return ActorResult<TableInputNames>.Failure(_lastCompileError!);
        }

        await RegisterRouterAndAttachToTableInputsAsync();

        // Plan 009 A2: see OnActivateAsync's self-heal branch for why this has to happen here, immediately,
        // rather than waiting for the next periodic tick.
        if (_journalNeedsPersistedClear)
        {
            await SaveJournalAsync();
            _journalNeedsPersistedClear = false;
        }

        _running = true;
        await SaveControlStateAsync();
        await ArmTimerAsync(ResolveFlushPeriod());

        return ActorResult<TableInputNames>.Success(new TableInputNames(_streamInputs.ToList(), _tableInputs.ToList(), _pipelineNameById.Keys.ToList()));
    }

    /// <summary>
    /// PARITY.md debt item D2 — REAL backfill on attach, Dapr counterpart of <c>TableGrain.
    /// AttachToTableInputAsync</c> (see its own doc comment for the full protocol this ports), closing the
    /// gap this actor's former <c>WarnIfTableInputsAlreadyHoldRowsAsync</c> left open. That method's own
    /// doc comment (see git history) named two blockers: no atomic (snapshot, epoch) read reachable on the
    /// upstream actor, and no subscribe-before-attach ordering point to hook. Both are closed as of this
    /// change: <see cref="ITableActor.AttachSnapshotAsync"/> is the new interface method (added because
    /// this change now owns <c>ITableActor.cs</c>) providing the first; THIS method registering
    /// <paramref name="tableRouter"/> before reading any upstream snapshot, called from inside
    /// <see cref="StartAsync"/>/<see cref="OnActivateAsync"/>'s self-heal branch rather than from
    /// <c>Lifecycle.DaprLifecycleOrchestrator</c> after the fact, provides the second.
    ///
    /// <para><b>WHY REGISTERING FIRST IS SAFE, NOT JUST CONVENTIONAL — Dapr shape of the Orleans argument:</b>
    /// Dapr actors process at most one invocation at a time per actor id (dapr/ARCHITECTURE.md's reentrancy
    /// decision) — the direct analogue of Orleans grain non-reentrancy. This method runs INSIDE the caller's
    /// own <see cref="StartAsync"/>/<see cref="OnActivateAsync"/> turn on THIS actor id. Registering
    /// <paramref name="tableRouter"/> here — BEFORE any `await` that reads an upstream snapshot — makes this
    /// actor id routable while that turn is still executing; the router's fan-out (<see cref="Streaming.
    /// TableEventRouter.OnSourceEventsAsync"/>/<see cref="Streaming.TableEventRouter.OnTableDeltaAsync"/>)
    /// then issues an ordinary <c>ActorProxy</c> call against THIS SAME actor id for anything published from
    /// here on — a NEW invocation, which Dapr queues behind the still-in-flight
    /// StartAsync/OnActivateAsync turn rather than dropping (the router simply did not know to route to
    /// this actor at all before registration) or interleaving with it. Nothing published between
    /// "registered" and "this method returns" is ever lost — at worst it is deferred until after this whole
    /// method (and its caller's turn) returns, at which point <see cref="_tableInputCutoffEpoch"/> is
    /// already populated for every input, so <see cref="ProcessTableDeltasAsync"/>'s own epoch filter makes
    /// replaying it either a correct application (Epoch &gt; cutoff) or a correct no-op (Epoch &lt;= cutoff
    /// — already reflected in the snapshot just admitted). Registering ALL of this table's inputs (both
    /// stream and table) in the ONE <see cref="Streaming.TableEventRouter.Register"/> call below, before ANY
    /// upstream attach read, extends the identical guarantee to plain stream-fed tables too — a source event
    /// published between compile and registration used to have the same theoretical loss window this item's
    /// PARITY.md entry calls out for table-over-table chaining specifically.</para>
    ///
    /// <para>Rows are admitted through <see cref="TableExecutor.OnTableDeltaBatch"/> — the SAME entry point
    /// a live batch from <paramref name="upstreamName"/> (informal — see the loop below) uses — so GROUP
    /// BY/JOIN/LATEST BY state is correctly built up from them rather than bypassed, and the result is
    /// republished via <see cref="ApplyAndPublishAsync"/> exactly like live traffic, so a table chained off
    /// THIS one sees this table's backfilled rows too when it, in turn, attaches to this one.</para>
    ///
    /// <para><b>STILL WARNS</b> — same shape as before (table name, upstream name, row count,
    /// docs/otc-demo-wishlist.md #14 reference), re-worded now that the rows it reports ARE being backfilled
    /// rather than silently dropped, and driven by the SAME atomic <see cref="ITableActor.AttachSnapshotAsync"/>
    /// read the backfill itself uses (the old best-effort <see cref="ITableActor.GetRowCountAsync"/> probe,
    /// which could itself race the routing it described, is gone).</para>
    ///
    /// <para><b>Plan 025 extends this same argument to STREAM inputs (B3):</b> BEFORE the
    /// <see cref="Streaming.TableEventRouter.Register"/> call below, this method now also opens an attach
    /// hold (<see cref="IConnectorActor.BeginAttachAsync"/>) on every stream input whose source classifies
    /// as <see cref="SourceKindDispatch.ActorKind.Connector"/> — Dapr's counterpart of <c>TableGrain.
    /// AttachToStreamInputAsync</c>. Because registration happens inside THIS SAME actor turn, a live batch
    /// the router routes here while the hold is open queues behind this invocation exactly like a
    /// table-input delta does (see the paragraph above) — but a connector-kind source publishes NOTHING
    /// while its own hold is open (<see cref="IConnectorActor.BeginAttachAsync"/>'s own doc comment: "while
    /// at least one attach hold is open this actor publishes NOTHING"), so the only thing that CAN arrive
    /// queued here is a batch from BEFORE the hold that was already in flight on the topic when this call
    /// started — and that batch MAY duplicate rows the snapshot itself already replayed, because a source's
    /// replay ring carries no per-input epoch the way <see cref="_tableInputCutoffEpoch"/> fences a table
    /// input. Stated honestly rather than hidden: this is the SAME measured gap Orleans documents on
    /// <c>IConnectorGrain.BeginAttachAsync</c> — an ordinary table over an ordinary stream source can, in
    /// the narrow window between this call's registration and its own return, double-count a handful of
    /// rows that were already in flight when the attach began. <c>LATEST BY</c> tables are unaffected
    /// (re-admitting the same key/value under that semantics is idempotent). A hold that fails to open
    /// (<see cref="IConnectorActor.BeginAttachAsync"/> itself threw, or the source isn't a Connector kind
    /// at all) degrades to plain registration with no replay — losing the backfill is bad, refusing to
    /// start is worse, the identical rule <c>TableGrain.AttachToStreamInputAsync</c>'s own doc comment
    /// states.</para>
    ///
    /// <para><b>Plan 025 also registers this table's PIPELINE inputs</b> in this same call
    /// (<see cref="Streaming.TableEventRouter.RegisterPipelineInputs"/>, by resolved pipeline ID) —
    /// there is nothing to attach to for a pipeline input (see <see cref="ProcessPipelineResultsAsync"/>'s
    /// own doc comment: a pipeline holds no replay ring, so this is registration only).</para>
    /// </summary>
    private async Task RegisterRouterAndAttachToTableInputsAsync()
    {
        // PARITY.md D2: cleared and fully repopulated on every call rather than merely overwritten per key
        // — see this field's own doc comment. A table whose SQL dropped a former table input must not keep
        // serving that input's stale cutoff (harmless today since the router no longer routes that input's
        // deltas here either, but this keeps the invariant "this dictionary's keys are exactly this table's
        // CURRENT table inputs" true by construction rather than by coincidence).
        _tableInputCutoffEpoch.Clear();
        _pipelineNameById.Clear();

        // Plan 025 (B3): open an attach hold on every STREAM input that is a Connector-kind source, BEFORE
        // this table registers with the router below — see this method's own doc comment for the full
        // race-free argument and the honest residual it states. Best-effort per source: a Begin failure (or
        // a non-Connector-kind source, which has no BeginAttachAsync to call at all) just means this input
        // gets a plain registration with no replay, exactly like TableGrain.AttachToStreamInputAsync's own
        // one-directional best-effort gate.
        var connectorHolds = new List<(string SourceName, IConnectorActor Proxy, SourceAttachSnapshot Snapshot)>();
        foreach (var sourceName in _streamInputs.Distinct())
        {
            var sourceDef = _sources.FirstOrDefault(s => s.Name == sourceName);
            if (sourceDef is null || SourceKindDispatch.Classify(sourceDef.Kind) != SourceKindDispatch.ActorKind.Connector)
            {
                continue;
            }

            var proxy = ActorProxy.Create<IConnectorActor>(new ActorId(EnvKeys.Qualify(_def!.Environment, sourceName)), nameof(ConnectorActor), ActorProxyDefaults.Options);
            try
            {
                var snapshot = await proxy.BeginAttachAsync();
                connectorHolds.Add((sourceName, proxy, snapshot));
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "TableActor[{Name}]: could not attach to stream input '{Source}' for replay — registering without it.", _def!.Name, sourceName);
            }
        }

        try
        {
            // Register EVERY input (stream AND table) this compile resolved BEFORE reading any upstream
            // snapshot below — see this method's own doc comment for why that ordering is what makes the whole
            // handshake race-free. Idempotent (Streaming.TableEventRouter.Register replaces this table's prior
            // subscription set), so a restart-after-edit registers the new set cleanly.
            //
            // Plan 021 D6: _streamInputs/_tableInputs are BARE (compiled against this table's own environment's
            // catalog); the router is shared process-wide, so both the router's own key (this table's qualified
            // name) and every input it fans in from must be qualified with THIS table's own environment — same
            // reasoning DaprLifecycleOrchestrator.StartTableAsync used to apply after the fact.
            tableRouter.Register(
                EnvKeys.Qualify(_def!.Environment, _def.Name),
                _streamInputs.Select(s => EnvKeys.Qualify(_def!.Environment, s)).ToList(),
                _tableInputs.Select(t => EnvKeys.Qualify(_def!.Environment, t)).ToList());

            // Plan 025 (table-over-pipeline): register this table's pipeline inputs too, keyed by resolved
            // pipeline ID — see TableEventRouter.RegisterPipelineInputs's own doc comment for why this is a
            // SEPARATE call rather than a wider Register parameter list, and TableInputNames.PipelineInputs's
            // doc comment for why a pipeline id needs no EnvKeys.Qualify the way a source/table NAME does.
            var pipelineIds = new List<string>();
            foreach (var pipelineName in _pipelineInputs)
            {
                var pipeline = _pipelines.FirstOrDefault(p => p.Name == pipelineName);
                if (pipeline is null)
                {
                    // Should not happen (BuildTableStreamSchemas only ever offers a pipeline relation when
                    // the pipeline is present with a non-empty OutputFields — Catalog.CatalogStore's
                    // BuildTableStreamSchemas), but a missing entry here must not throw: starting empty for
                    // this input is the same "losing the extra is bad, refusing to start is worse" rule
                    // every other lookup on this path follows.
                    logger.LogDebug(
                        "TableActor[{Name}]: pipeline input '{Pipeline}' was not found in the pipelines list passed to StartAsync — this input will receive nothing.",
                        _def!.Name, pipelineName);
                    continue;
                }

                pipelineIds.Add(pipeline.Id);
                _pipelineNameById[pipeline.Id] = pipelineName;

                if (pipeline.Status != PipelineStatus.Running)
                {
                    // Not a refusal — mirrors TableGrain.SubscribeToPipelineInputAsync's identical log: a
                    // table over a stopped pipeline is legal and simply receives nothing until that
                    // pipeline runs (see ProcessPipelineResultsAsync's own doc comment: pipelines have no
                    // replay).
                    logger.LogInformation(
                        "TableActor[{Name}]: pipeline input '{Pipeline}' is {Status} — this table will receive rows only once that pipeline runs (pipelines have no replay).",
                        _def!.Name, pipelineName, pipeline.Status);
                }
            }
            tableRouter.RegisterPipelineInputs(EnvKeys.Qualify(_def!.Environment, _def.Name), pipelineIds);

            // Plan 025 (B3): feed each held connector's replay snapshot through the SAME source-event path
            // live traffic uses (ProcessSourceEventsAsync's own JsonValueNormalizer.NormalizeInPlace +
            // TableExecutor.OnStreamEvent), now that the registration above makes this actor id routable —
            // see this method's own doc comment for the race-free argument this extends to stream inputs.
            foreach (var (sourceName, _, snapshot) in connectorHolds)
            {
                if (snapshot.TotalSeen > snapshot.Rows.Count)
                {
                    // Same wording as Orleans' TableGrain.AttachToStreamInputAsync — each placeholder name
                    // appears exactly once (see the table-input warning below for why that matters).
                    logger.LogWarning(
                        "Table '{Table}': late attach to source '{Source}' replayed {Replayed} of {TotalSeen} row(s); " +
                        "earlier rows are not recoverable (the source's replay ring holds the most recent {Capacity}).",
                        _def!.Name, sourceName, snapshot.Rows.Count, snapshot.TotalSeen, SourceReplayBuffer.Capacity);
                }

                foreach (var row in snapshot.Rows)
                {
                    JsonValueNormalizer.NormalizeInPlace(row);
                    _deltasIn++;
                    _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    _rebuilding = false;

                    var deltas = _executor!.OnStreamEvent(sourceName, row);
                    if (deltas.Count > 0)
                    {
                        await ApplyAndPublishAsync(deltas);
                    }
                }
            }

            foreach (var upstreamName in _tableInputs.Distinct())
            {
            // A table cannot legitimately depend on itself (the SQL compiler has no recursive-table feature
            // to produce one) — skip defensively rather than ever calling back into this same actor id's
            // not-yet-finished StartAsync/OnActivateAsync turn, which would deadlock.
            if (upstreamName == _def!.Name) continue;

            TableAttachSnapshot snapshot;
            try
            {
                // Plan 021: upstreamName is BARE (TableInputs, compiled against this table's own
                // environment's catalog — see CatalogStore.BuildTableSchemas). The plan cuts cross-
                // environment table dependencies entirely (see plans/021-environment-isolation.md's "Cut"
                // list), so the upstream lives in THIS table's own environment — qualify with it.
                var upstream = ActorProxy.Create<ITableActor>(new ActorId(EnvKeys.Qualify(_def!.Environment, upstreamName)), nameof(TableActor), ActorProxyDefaults.Options);
                snapshot = await upstream.AttachSnapshotAsync();
            }
            catch (Exception ex)
            {
                // Best-effort: an upstream table that hasn't been created/started yet (or errors for its
                // own reasons) has no snapshot to backfill from — this table starts empty for that input and
                // relies on live traffic from here, exactly like every input already does. Never let this
                // block the table from starting. Dapr virtual actors auto-activate on first call, so a table
                // input that has never run answers a fresh AttachSnapshotAsync (empty rows, epoch -1)
                // harmlessly rather than throwing; this catch is for the genuinely-unreachable/erroring case.
                logger.LogDebug(ex, "TableActor[{Name}]: could not attach to table input '{Upstream}' for backfill — starting empty for this input.", _def!.Name, upstreamName);
                snapshot = new TableAttachSnapshot([], -1);
            }

            // Recorded BEFORE this method returns — see this method's own doc comment for why that
            // ordering, relative to StartAsync's/OnActivateAsync's turn ending, is what makes
            // ProcessTableDeltasAsync's own filter correct for anything the router queued while this call
            // was in flight.
            _tableInputCutoffEpoch[upstreamName] = snapshot.Epoch;

            if (snapshot.Rows.Count > 0)
            {
                // NOTE: each placeholder name appears exactly once — Microsoft.Extensions.Logging's
                // structured-logging formatter binds placeholders to args POSITIONALLY, so repeating a name
                // (e.g. two "{Table}" occurrences) silently desyncs every placeholder after the first repeat
                // from the argument list actually supplied, rather than substituting the same value twice.
                logger.LogWarning(
                    "TableActor[{Name}] is starting with table input '{Upstream}' ({RowCount} row(s) already present) " +
                    "— replaying them now as this table's initial state (wishlist #14 option (a); see " +
                    "docs/otc-demo-wishlist.md #14).",
                    _def!.Name, upstreamName, snapshot.Rows.Count);

                var seedDeltas = snapshot.Rows.Select(r => new TableDelta(new EventRecord(r.Row), r.Weight)).ToList();
                _deltasIn += seedDeltas.Count;
                var outAll = _executor!.OnTableDeltaBatch(upstreamName, seedDeltas);
                _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (outAll.Count > 0)
                {
                    await ApplyAndPublishAsync(outAll);
                }
            }
            }
        }
        finally
        {
            // Plan 025 (B3): release every attach hold this call opened, regardless of how the try block
            // above exited — mirrors TableGrain.AttachToStreamInputAsync's identical guarded finally. A
            // release failure is logged and swallowed: the source's own 10s safety timer force-releases a
            // hold nobody ends, so a stray failure here cannot gate that source forever.
            foreach (var (sourceName, proxy, _) in connectorHolds)
            {
                try
                {
                    await proxy.EndAttachAsync();
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "TableActor[{Name}]: releasing the attach hold on source '{Source}' failed; the source's own safety timer covers it.", _def!.Name, sourceName);
                }
            }
        }
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();

        if (_dirty)
        {
            CaptureSnapshot();
        }

        _executor = null;
        _searchIndex = null;
        _running = false;
        _tableInputCutoffEpoch.Clear(); // PARITY.md D2 — a restart re-attaches and re-populates this fresh.
        _pipelineNameById.Clear(); // plan 025 — same reasoning, re-populated fresh by the next start.
        await SaveControlStateAsync();
    }

    /// <summary>Best-effort final flush before this activation is evicted — mirrors
    /// <c>TableGrain.OnDeactivateAsync</c> exactly (same "don't lose the last &lt;2s of deltas just because
    /// the flush timer hadn't ticked yet" rationale). Plan 008: the in-memory capture always happens (cheap,
    /// no I/O); the actual persist is <see cref="SaveControlStateAsync"/>'s own mode-aware, best-effort
    /// call — a no-op for <see cref="TablePersistenceMode.MemoryOnly"/>, same as every other path.</summary>
    protected override async Task OnDeactivateAsync()
    {
        if (!_dirty)
        {
            return;
        }

        CaptureSnapshot();
        try { await SaveControlStateAsync(); } catch { /* best-effort */ }
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_running);

    public Task<TableInputNames> GetInputNamesAsync() => Task.FromResult(
        _running
            ? new TableInputNames(_streamInputs.ToList(), _tableInputs.ToList(), _pipelineNameById.Keys.ToList())
            : new TableInputNames([], [], []));

    public async Task ProcessSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        foreach (var raw in envelope.Events)
        {
            // See ITableActor.ProcessSourceEventsAsync's doc comment: this envelope crosses the Dapr
            // actor-invocation wire, which re-boxes every Dictionary<string, object?> value as a
            // JsonElement regardless of whether it was already normalized once at the sf-sources pub/sub
            // ingress. Re-normalize before the Engine ever sees it.
            JsonValueNormalizer.NormalizeInPlace(raw);

            var evt = new EventRecord(raw);
            _deltasIn++;
            _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _rebuilding = false; // live traffic observed since resume (or this is a first-ever start — already false)

            // Plan 021 D6: envelope.Source is the QUALIFIED name (every publisher stamps its own actor id
            // — see GeneratorActor/ConnectorActor) so the router (Streaming/TableEventRouter.cs) can
            // dispatch cross-environment-safely, but this table's own compiled StreamInputs — and every
            // key TableExecutor was built against — are BARE, local to THIS table's own environment's
            // catalog. Strip the qualification back off before the Engine ever sees the name.
            var deltas = _executor.OnStreamEvent(EnvKeys.Split(envelope.Source).Key, evt);
            if (deltas.Count > 0)
            {
                await ApplyAndPublishAsync(deltas);
            }
        }
    }

    /// <summary>
    /// Wishlist #15 — Dapr-flavor mirror of <c>TableGrain.OnTableDeltaBatchAsync</c>'s identical fix (see
    /// its doc comment for the full mechanism). <paramref name="envelope"/> carries everything the upstream
    /// table's own ApplyAndPublishAsync published for ONE of ITS epochs; admitting it through
    /// <see cref="TableExecutor.OnTableDeltaBatch"/> in ONE call — instead of the old per-element
    /// <see cref="TableExecutor.OnTableDelta"/> loop — keeps it one epoch here too, so an upstream
    /// retract(-1)+assert(+1) pair (a changed GROUP BY row, a changed LATEST BY key) is admitted,
    /// consolidated (see Engine-side ConsolidateEpochOutput) and republished atomically instead of being
    /// split across as many downstream epochs as the envelope had elements, with a wrong intermediate state
    /// observable in between.
    ///
    /// <para><b>PARITY.md D2 — epoch cutoff filter:</b> <see cref="TableDeltaDto.Epoch"/> on each element of
    /// <paramref name="envelope"/> is the upstream's own <c>TableExecutor.LastEpoch</c> at the moment IT
    /// admitted that delta (stamped by ITS <see cref="ApplyAndPublishAsync"/>). Anything at or below the
    /// cutoff <see cref="RegisterRouterAndAttachToTableInputsAsync"/> recorded for this specific upstream in
    /// <see cref="_tableInputCutoffEpoch"/> is already reflected in the snapshot this table backfilled from
    /// — applying it again would double its Z-set weight — so it is filtered out before admission. See
    /// <c>TableGrain.OnTableDeltaBatchAsync</c>'s own doc comment for why this filter, not delivery timing,
    /// is what actually makes the register-then-attach handshake race-free.</para>
    /// </summary>
    public async Task ProcessTableDeltasAsync(TableDeltaEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        foreach (var d in envelope.Deltas)
        {
            // Actor-wire re-normalization: this envelope crosses the Dapr actor-invocation wire, which
            // re-boxes every Dictionary<string, object?> value as a JsonElement regardless of whether it
            // was already normalized once at the sf-table-delta pub/sub ingress. Re-normalize before the
            // Engine ever sees it — same requirement as ProcessSourceEventsAsync's identical call. Still
            // done per-element (unrelated to epoch atomicity below): each row's own dictionary needs its
            // own pass.
            JsonValueNormalizer.NormalizeInPlace(d.Row);
        }

        // Plan 021 D6: same strip as ProcessSourceEventsAsync above — envelope.Table is qualified for
        // routing, TableExecutor's own upstream-table bookkeeping (and _tableInputCutoffEpoch's own keys)
        // are bare.
        var upstream = EnvKeys.Split(envelope.Table).Key;

        // PARITY.md D2: a cutoff we never recorded for this upstream (upstream had no snapshot to attach
        // to, or this table doesn't actually declare it as a table input) means -1 — admit unconditionally,
        // same as TableGrain.OnTableDeltaBatchAsync's identical default. See TableAttachPolicy's own doc
        // comment for why this decision is a separate, unit-testable static method rather than inlined here.
        var cutoff = _tableInputCutoffEpoch.GetValueOrDefault(upstream, -1);
        var admissible = TableAttachPolicy.FilterAdmissible(envelope.Deltas, cutoff);
        if (admissible.Count == 0)
        {
            return;
        }

        var deltas = admissible.Select(d => new TableDelta(new EventRecord(d.Row), d.Weight)).ToList();
        _deltasIn += deltas.Count;
        var outAll = _executor.OnTableDeltaBatch(upstream, deltas);
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false;

        if (outAll.Count > 0)
        {
            await ApplyAndPublishAsync(outAll);
        }
    }

    /// <summary>Table-over-pipeline (plan 025) — see <see cref="ITableActor.ProcessPipelineResultsAsync"/>'s
    /// doc comment for the full contract this implements (routed here by <see cref="Streaming.
    /// TableEventRouter.OnPipelineResultsAsync"/>, no backfill, per-row publish). A no-op if the table isn't
    /// currently running, or if <paramref name="envelope"/>'s <c>PipelineId</c> is not (or is no longer, a
    /// stale delivery racing an unregister/re-register on a compile change) one of this table's current
    /// pipeline inputs.</summary>
    public async Task ProcessPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        if (!_pipelineNameById.TryGetValue(envelope.PipelineId, out var relationName))
        {
            return;
        }

        foreach (var result in envelope.Results)
        {
            // Same actor-wire re-normalization requirement as ProcessSourceEventsAsync/
            // ProcessTableDeltasAsync — this envelope crosses the Dapr actor-invocation wire, which
            // re-boxes every Dictionary<string, object?> value as a JsonElement regardless of whether the
            // sf-pipeline-out endpoint already normalized this exact envelope once.
            JsonValueNormalizer.NormalizeInPlace(result.Row);

            var evt = PipelineResultMapping.ToEventRecord(result, relationName);
            _deltasIn++;
            _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _rebuilding = false; // live traffic observed since resume (or this is a first-ever start — already false)

            // ONE ROW AT A TIME — mirrors TableGrain.OnPipelineBatchAsync exactly: a pipeline emits
            // appended result rows, never retractions, so (unlike ProcessTableDeltasAsync's upstream-TABLE
            // batch) there is no epoch/atomicity a per-row publish could break.
            var deltas = _executor.OnStreamEvent(relationName, evt);
            if (deltas.Count > 0)
            {
                await ApplyAndPublishAsync(deltas);
            }
        }
    }

    public Task<List<TableRowDto>> GetRowsAsync(int limit, int offset)
    {
        var rows = _flushed.Values
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(0, limit))
            .ToList();
        return Task.FromResult(rows);
    }

    public Task<int> GetRowCountAsync() => Task.FromResult(_flushed.Count);

    /// <summary>PARITY.md debt item D2 — see <see cref="ITableActor.AttachSnapshotAsync"/>'s own doc
    /// comment for the contract and <see cref="RegisterRouterAndAttachToTableInputsAsync"/> for the caller
    /// side. Purely synchronous (no `await`) so the (rows, epoch) pair is atomic by construction: Dapr
    /// actors process one invocation at a time per actor id, so nothing can advance <see cref="_executor"/>
    /// between the two reads below within this one call.
    ///
    /// Reads the LIVE <see cref="_executor"/> snapshot — NOT <see cref="_flushed"/> (which
    /// <see cref="GetRowsAsync"/>/<see cref="GetRowCountAsync"/> read, up to one flush interval stale and
    /// carrying no epoch at all — see class doc's "flushed vs. live" split and <see cref="ITableActor.
    /// AttachSnapshotAsync"/>'s own doc comment for why using either as a stand-in here would be WRONG, not
    /// just stale).</summary>
    public Task<TableAttachSnapshot> AttachSnapshotAsync()
    {
        if (_executor is null)
        {
            return Task.FromResult(new TableAttachSnapshot([], -1));
        }

        var rows = _executor.Snapshot().Values
            .Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            .ToList();
        return Task.FromResult(new TableAttachSnapshot(rows, _executor.LastEpoch));
    }

    public Task<long> GetSeqAsync() => Task.FromResult(_seq);

    public Task<TableMetrics> GetMetricsAsync() => Task.FromResult(new TableMetrics
    {
        TableId = _def?.Id ?? Id.ToString(),
        Status = _running ? PipelineStatus.Running : PipelineStatus.Stopped,
        RowCount = _flushed.Count,
        DeltasIn = _deltasIn,
        DeltasOut = _deltasOut,
        LastUpdateMs = _lastUpdateMs,
        Rebuilding = _rebuilding,
        // Partitioned execution (and therefore per-partition detail / shared arrangements / frontier
        // epoch) is Orleans-only — decision D-F. Always null/absent on this flavor, independent of
        // Parallelism (which is always 1 here by construction).
        Partitions = null,
        ArrangedInputs = null,
        SnapshotFrontierEpoch = null,
    });

    public Task<List<TableRowDto>> SearchAsync(string query, int limit)
    {
        if (_searchIndex is null || string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(new List<TableRowDto>());
        }

        // Live executor snapshot for weight lookup — NOT the flushed copy GetRowsAsync reads (see class
        // doc's "flushed vs. live" split; mirrors TableGrain.SearchAsync exactly).
        var snapshot = _executor?.Snapshot();
        var hits = _searchIndex.Search(query, limit);
        var rows = hits.Select(h =>
        {
            long weight = snapshot is not null && snapshot.TryGetValue(h.RowKey, out var current) ? current.Weight : 1;
            return new TableRowDto { Row = new Dictionary<string, object?>(h.Row), Weight = weight };
        }).ToList();
        return Task.FromResult(rows);
    }

    /// <summary>Shared by <see cref="StartAsync"/> and <see cref="OnActivateAsync"/>'s self-heal branch —
    /// compiles <see cref="_def"/>'s SQL (via <see cref="TableCompilation.TryCompile"/>) and, on success,
    /// applies the SAME "non-empty persisted snapshot means resume, not first start" reset
    /// <c>TableGrain.StartClassicAsync</c> applies (see class doc's restart-resume paragraph), then builds
    /// the search index FROM the (now-empty-either-way) snapshot. Sets <see cref="_executor"/> to null and
    /// <see cref="_lastCompileError"/> on failure; callers check <see cref="_executor"/>.</summary>
    private void ActivateExecutor()
    {
        var (executor, streamInputs, tableInputs, pipelineInputs, error) = TableCompilation.TryCompile(_def!, _sources, _tables, _pipelines);
        if (executor is null)
        {
            _executor = null;
            _lastCompileError = error;
            return;
        }

        _executor = executor;
        _streamInputs = streamInputs;
        _tableInputs = tableInputs;
        _pipelineInputs = pipelineInputs;
        _lastCompileError = null;

        ApplyRetentionPolicy();

        // Plan 009 A2: fold whatever the persisted journal holds onto _flushed BEFORE the resume-detection
        // check below — UNCONDITIONALLY, not gated on _def.Persistence == Journaled. See class doc's
        // "Resume" paragraph: a non-empty journal can only exist here because a PRIOR activation ran
        // Journaled, and the mode may have since changed; TableJournalPolicy.ReplayOntoSnapshot is a no-op
        // when _journalEntries is empty, so this costs nothing for a table that was never Journaled.
        TableJournalPolicy.ReplayOntoSnapshot(_flushed, _journalEntries);

        // See TableGrain.StartClassicAsync's identical comment: a non-empty persisted snapshot means this
        // is a resume (not a first start) — operator internal state (join indexes/GROUP BY multisets)
        // can't be reconstructed from the output alone, so mark rebuilding and reset to empty; it rebuilds
        // purely from live traffic going forward.
        if (_flushed.Count > 0)
        {
            _rebuilding = true;
            _flushed = [];
            _seq = 0;
            _dirty = true;
        }

        // Plan 009 A2: whatever the journal held is now either folded into the reset-to-empty _flushed above
        // (this activation IS Journaled) or simply STALE (a different/former mode) — clear it in memory
        // unconditionally; the caller (StartAsync/OnActivateAsync's self-heal branch) persists that clear
        // via SaveJournalAsync when _journalNeedsPersistedClear is set, immediately, not deferred to the
        // next tick — see class doc's "Resume" paragraph for why deferring would leave a stale journal
        // behind forever for a table that switches away from Journaled without a clean Stop first.
        _journalNeedsPersistedClear = _journalEntries.Count > 0;
        if (_journalNeedsPersistedClear)
        {
            _journalEntries = [];
        }

        // Plan 011 wave C: re-establish the incremental capture's invariant from scratch — a brand-new
        // (empty) executor above and an empty _flushed either way, so the one forced full capture this
        // schedules is O(0). See CaptureSnapshot's doc comment.
        _touchedRowKeys.Clear();
        _fullCaptureNeeded = true;

        // Either branch above leaves the row set empty (fresh start, or reset-for-rebuild) — see
        // TableGrain's identical comment — so rebuilding the index from _flushed here is accurate (empty
        // in, empty out); it fills back in incrementally as Process*Async observes deltas going forward.
        _searchIndex = _def!.SearchEnabled ? BuildSearchIndex(_def.SearchMode) : null;
    }

    /// <summary>Plan 011 C2 — installs this table's row retention policy on the freshly compiled executor,
    /// the Dapr mirror of <c>TableGrain.ApplyRetentionPolicy</c>. The eviction retractions it produces come
    /// back through the executor's own OnStreamEvent/OnTableDelta return values, so
    /// <see cref="ApplyAndPublishAsync"/> publishes, indexes and journals them with no retention-specific
    /// branch on the hot path.
    ///
    /// ONE DELIBERATE DIFFERENCE FROM ORLEANS, stated rather than hidden: TableGrain checks
    /// <c>TablePlan.SupportsRetention</c> before configuring, because it has the compiled plan in hand.
    /// Here the plan is not available — <see cref="TableCompilation.TryCompile"/> returns the executor and
    /// nothing else, and widening that tuple would mean editing a pre-existing test file. So this relies on
    /// <c>TableExecutor.ConfigureRetention</c>'s own refusal instead, which enforces exactly the same rule
    /// (and is a no-op, never a throw, for the default off policy). Same outcome, same log line;
    /// Catalog.CatalogStore.ValidateRetention rejects the combination at create/update either way, so this
    /// branch only fires for a definition that arrived some other way.</summary>
    private void ApplyRetentionPolicy()
    {
        var policy = new TableRetentionPolicy(_def!.RetentionMaxRows, _def.RetentionTtlMs);
        try
        {
            _executor!.ConfigureRetention(policy);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex,
                "TableActor[{Name}]: row retention (maxRows={MaxRows}, ttlMs={TtlMs}) is not supported for this table's plan shape — starting WITHOUT the bound.",
                _def.Name, _def.RetentionMaxRows, _def.RetentionTtlMs);
        }
    }

    private TableSearchIndex BuildSearchIndex(TableSearchMode mode)
    {
        var index = new TableSearchIndex(mode);
        index.Rebuild(_flushed.Select(kv =>
            new KeyValuePair<string, IReadOnlyDictionary<string, object?>>(kv.Key, kv.Value.Row)));
        return index;
    }

    private async Task ApplyAndPublishAsync(IReadOnlyList<TableDelta> deltas)
    {
        _dirty = true;
        _deltasOut += deltas.Count;

        // Plan 011 wave C: one key-derivation pass per batch, shared by the search index, the journal and
        // the incremental read-cache refresh — see TouchedKeys' own doc comment (verbatim port of
        // TableGrain's).
        var touched = TouchedKeys(deltas);
        foreach (var key in touched) _touchedRowKeys.Add(key);

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(touched);
        }

        if (_def?.Persistence == TablePersistenceMode.Journaled)
        {
            RecordJournalEntries(touched);
        }

        _deltaSeq++;
        // Plan 011 C2: Evicted rides along so TableHistoryActor can tell a retention eviction apart from an
        // ordinary upstream retraction — see TableDeltaDto.Evicted. Every other subscriber ignores it.
        //
        // Wishlist #14 option (a) / PARITY.md D2 — this Epoch is both the wire-contract stamp AND, as of
        // closing D2, the value RegisterRouterAndAttachToTableInputsAsync's epoch-cutoff filter on a
        // DOWNSTREAM table relies on (see ProcessTableDeltasAsync's own doc comment for the consumer side).
        // Epoch is TableExecutor.LastEpoch read HERE, synchronously — no `await` since the
        // _executor.OnStreamEvent/OnTableDelta/OnTableDeltaBatch call that produced `deltas` returned — so
        // it means exactly the same thing here as on the Orleans side (see that property's own doc comment
        // in PublicApi.cs): NOT _deltaSeq (a per-publish-call counter local to this actor, unrelated to the
        // Engine's own admission epoch) and NOT GetSeqAsync's flush-generation counter (ticks on a timer,
        // not once per admission) — either of those would be a DIFFERENT number from what an
        // epoch-cutoff-based consumer needs, which is exactly the "wrong answer by a different route" this
        // field exists to avoid producing.
        var epoch = _executor!.LastEpoch;
        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight, Evicted = d.Retention, Epoch = epoch }).ToList();

        try
        {
            // Plan 021 D6: Id.GetId() is this actor's own qualified name (EnvKeys.Qualify(def.Environment,
            // def.Name) — see DaprLifecycleOrchestrator.TableActorProxy) — the envelope's entity key
            // downstream routers/sinks (TableEventRouter, TableHistoryDeltaSink, NatsSinkPublisherService)
            // dispatch on. Byte-identical to the pre-021 `_def.Name` for the default environment (D2).
            await daprClient.PublishEventAsync(
                StreamingRuntimeSetup.PubsubName,
                StreamingRuntimeSetup.TableDeltaTopic,
                new TableDeltaEnvelope { Table = Id.GetId(), Seq = _deltaSeq, Deltas = dtos });
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not tear down the timer or lose the in-memory
            // counters/search-index updates above — mirrors PipelineActor.PublishRowsAsync's own
            // try/catch rationale (drop this publish, the next delta/flush tick tries again).
            logger.LogWarning(ex, "TableActor[{Name}]: failed to publish {Count} delta(s).", _def?.Name, dtos.Count);
        }
    }

    /// <summary>Keeps the search index in sync with the consolidated Z-set as deltas land — verbatim port
    /// of <c>TableGrain.ReflectDeltasInSearchIndex</c>: for each row touched by this batch, look its
    /// canonical key up in the already-updated, live <see cref="TableExecutor.Snapshot"/> — present with
    /// weight &gt; 0 means Add/update, absent means the row's weight returned to 0 (Remove).</summary>
    private void ReflectDeltasInSearchIndex(List<string> touched)
    {
        var snapshot = _executor!.Snapshot();
        foreach (var key in touched)
        {
            if (snapshot.TryGetValue(key, out var current))
            {
                _searchIndex!.Add(key, current.Row);
            }
            else
            {
                _searchIndex!.Remove(key);
            }
        }
    }

    /// <summary>Plan 011 wave C — the distinct canonical row keys one delta batch touched, derived ONCE and
    /// then shared by every per-batch consumer (search index, journal, incremental capture). Verbatim port
    /// of <c>TableGrain.TouchedKeys</c>; see its doc comment. Order is batch order, first occurrence wins —
    /// the same order the per-consumer loops produced before.</summary>
    private List<string> TouchedKeys(IReadOnlyList<TableDelta> deltas)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var keys = new List<string>(deltas.Count);
        foreach (var delta in deltas)
        {
            var key = _executor!.CanonicalRowKey(delta.Row);
            if (seen.Add(key)) keys.Add(key); // a batch can touch the same row's key more than once
        }
        return keys;
    }

    /// <summary>Plan 009 A2 — populates <see cref="_pendingJournalEntries"/>; callers only invoke this when
    /// <c>_def.Persistence == Journaled</c> (mirrors <see cref="ReflectDeltasInSearchIndex"/>'s guard shape —
    /// same per-batch, dedup-by-key pattern, verbatim port of <c>TableGrain.RecordJournalEntries</c>). Looks
    /// each touched key up in the ALREADY-UPDATED live <see cref="TableExecutor.Snapshot"/>: present with
    /// weight &gt; 0 records a live entry; absent means the key's running weight just dropped to &lt;= 0,
    /// recorded as an explicit removal tombstone (Weight = 0) — see <see cref="TableJournalEntry"/>'s own
    /// doc comment for why skipping this instead would resurrect the row on replay.</summary>
    private void RecordJournalEntries(List<string> touched)
    {
        var snapshot = _executor!.Snapshot();
        foreach (var key in touched)
        {
            _pendingJournalEntries[key] = snapshot.TryGetValue(key, out var current)
                ? new TableJournalEntry { Row = new Dictionary<string, object?>(current.Row), Weight = current.Weight }
                : new TableJournalEntry { Row = [], Weight = 0 };
        }
    }

    /// <summary>Refreshes the in-memory read cache (<see cref="_flushed"/>/<see cref="_seq"/>) from the live
    /// executor and clears <see cref="_dirty"/> — pure in-process work, no I/O. Split out from the old
    /// "FlushAsync" specifically so this always runs (keeping <see cref="GetRowsAsync"/> et al. fresh) even
    /// for <see cref="TablePersistenceMode.MemoryOnly"/>, which skips only the durability WRITE below, never
    /// this read-cache refresh — see class doc's plan-008 paragraph.
    ///
    /// <para>PLAN 011 WAVE C — O(CHANGED), NOT O(|TABLE|), mirroring <c>TableGrain.CaptureSnapshotIntoState</c>
    /// exactly (see its doc comment for the full argument). This used to rebuild <see cref="_flushed"/>
    /// wholesale — a fresh <c>Dictionary&lt;string, object?&gt;</c> per row from the whole executor snapshot,
    /// on the actor turn, every <see cref="TableDefinition.FlushMs"/> — so allocation was proportional to the
    /// TABLE at a rate set by the CLOCK. It now applies only <see cref="_touchedRowKeys"/>. The invariant:
    /// _flushed with _touchedRowKeys applied always equals the live executor snapshot, because the only
    /// writer of that snapshot is <see cref="ApplyAndPublishAsync"/> (which records every key it touched
    /// before returning) and the only place _flushed is otherwise replaced is <see cref="ActivateExecutor"/>
    /// (which sets <see cref="_fullCaptureNeeded"/> where both sides are empty). A key in the set but absent
    /// from the snapshot means its running weight fell to &lt;= 0, i.e. removal — the same tombstone rule
    /// <see cref="RecordJournalEntries"/> encodes.</para>
    ///
    /// <para>ONE FLAVOR-SPECIFIC CONSEQUENCE, handled in <see cref="StartBackgroundPersist"/>: because
    /// _flushed is now mutated IN PLACE rather than replaced, a <see cref="TablePersistenceMode.FireAndForget"/>
    /// background write can no longer just capture a reference to it — see that method's own doc comment.</para></summary>
    private void CaptureSnapshot()
    {
        if (_executor is null)
        {
            _dirty = false;
            return;
        }

        var snapshot = _executor.Snapshot();
        if (_fullCaptureNeeded)
        {
            _flushed = snapshot.ToDictionary(
                kv => kv.Key,
                kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
            _fullCaptureNeeded = false;
        }
        else
        {
            foreach (var key in _touchedRowKeys)
            {
                if (snapshot.TryGetValue(key, out var current))
                {
                    _flushed[key] = new TableRowDto { Row = new Dictionary<string, object?>(current.Row), Weight = current.Weight };
                }
                else
                {
                    _flushed.Remove(key);
                }
            }
        }

        _touchedRowKeys.Clear();
        _seq++;
        _dirty = false;
    }

    /// <summary>Per-tick entry point: refreshes the read cache, then dispatches the durability write per
    /// <see cref="TableDefinition.Persistence"/> via the pure <see cref="TablePersistencePolicy.
    /// DecideFlushAction"/> decision (see <see cref="DecidePersistAction"/>) — see class doc's plan-008
    /// paragraph for what each outcome does.</summary>
    private async Task OnFlushTickAsync()
    {
        if (!_dirty)
        {
            return;
        }

        CaptureSnapshot();

        switch (DecidePersistAction())
        {
            case TablePersistAction.AwaitedWrite:
                await SaveAsync();
                break;

            case TablePersistAction.BackgroundWrite:
                StartBackgroundPersist();
                break;

            case TablePersistAction.JournalWrite:
                await JournalFlushAsync();
                break;

            case TablePersistAction.Skip:
            default:
                break;
        }
    }

    private TablePersistAction DecidePersistAction() =>
        TablePersistencePolicy.DecideFlushAction(_def?.Persistence ?? TablePersistenceMode.Batched, dirty: true, writeInProgress: _persistInProgress);

    /// <summary>Kicks off the <see cref="TablePersistenceMode.FireAndForget"/> background write — captures
    /// <see cref="BuildState"/> synchronously (on this actor turn, before any later mutation of
    /// <see cref="_flushed"/> can occur) so the DTO handed to the background task is a stable snapshot, then
    /// writes it on a <see cref="Task.Run(Func{Task})"/> that explicitly calls both <c>SetStateAsync</c> and
    /// <c>SaveStateAsync</c> itself — outside a normal actor turn nothing auto-saves for you (same finding
    /// <see cref="TableHistoryActor.OnDeactivateAsync"/>'s doc comment makes). Guarded single-flight by
    /// <see cref="_persistInProgress"/> (checked by <see cref="TablePersistencePolicy.DecideFlushAction"/>
    /// before this is ever called); a failure is always logged, never silently swallowed.</summary>
    private void StartBackgroundPersist()
    {
        // Plan 011 wave C: BuildState() aliases _flushed, and _flushed is now mutated IN PLACE by
        // CaptureSnapshot (it used to be replaced wholesale, which is what made a bare reference safe).
        // The single-flight guard stops a second WRITE from starting, but not the next tick's capture — so
        // hand the background task its own dictionary. This is a SHALLOW copy on purpose: the TableRowDto
        // values are never mutated in place (a changed row replaces its map entry with a new DTO), so
        // copying N references is enough for the serializer to see a stable graph, and it costs one array
        // rather than the N per-row dictionaries the old whole-snapshot rebuild allocated every tick.
        var state = BuildState();
        state.Snapshot = new Dictionary<string, TableRowDto>(_flushed);
        _persistInProgress = true;
        _inFlightPersist = Task.Run(async () =>
        {
            try
            {
                await StateManager.SetStateAsync(StateName, state);
                await StateManager.SaveStateAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "TableActor[{Name}]: FireAndForget background state write failed — this flush's snapshot may be lost.",
                    state.Def?.Name);
            }
            finally
            {
                _persistInProgress = false;
            }
        });
    }

    private TableActorState BuildState() => new()
    {
        Def = _def,
        Sources = _sources,
        Tables = _tables,
        Pipelines = _pipelines,
        Running = _running,
        Snapshot = _flushed,
        Seq = _seq,
    };

    private async Task SaveAsync()
    {
        await StateManager.SetStateAsync(StateName, BuildState());
        await StateManager.SaveStateAsync();
    }

    /// <summary>Plan 009 A2 — <see cref="TablePersistenceMode.Journaled"/>'s flush: merges
    /// <see cref="_pendingJournalEntries"/> (this tick's touched keys, already deduped by key) into
    /// <see cref="_journalEntries"/> — later values overwrite earlier ones for the same key, exactly like
    /// <see cref="_flushed"/> itself does, which is what keeps the journal's size bounded by DISTINCT keys
    /// touched since the last compaction rather than by delta volume — then writes ONLY that small journal
    /// state via <see cref="SaveJournalAsync"/>, leaving <see cref="StateName"/> ("table" — Def/Sources/
    /// Tables/Running/Snapshot/Seq) untouched. This is the O(changed) write the class doc promises. Once
    /// <see cref="_journalEntries"/> reaches <see cref="TableJournalPolicy.ResolveJournalMaxEntries"/>,
    /// immediately compacts via <see cref="CompactJournalAsync"/>.</summary>
    private async Task JournalFlushAsync()
    {
        if (_pendingJournalEntries.Count > 0)
        {
            foreach (var (key, entry) in _pendingJournalEntries)
            {
                _journalEntries[key] = entry;
            }
            _pendingJournalEntries.Clear();
        }

        await SaveJournalAsync();

        var max = TableJournalPolicy.ResolveJournalMaxEntries(_def?.JournalMaxEntries ?? 0);
        if (TableJournalPolicy.ShouldCompact(_journalEntries.Count, max))
        {
            await CompactJournalAsync();
        }
    }

    /// <summary>Plan 009 A2 — writes the full "table" state once (<see cref="_flushed"/> was already
    /// refreshed this tick by <see cref="OnFlushTickAsync"/>'s own <see cref="CaptureSnapshot"/> call — the
    /// same capture <see cref="SaveAsync"/>'s Batched callers rely on) and truncates the journal back to
    /// empty, resetting the O(changed-since-compaction) counter to zero. Only ever called from
    /// <see cref="JournalFlushAsync"/>, immediately after that method's own awaited journal write — an
    /// ordinary tick and the compaction it triggers therefore never overlap (both always awaited, on the
    /// same actor turn — Dapr actors, like Orleans grains, process one turn at a time).</summary>
    private async Task CompactJournalAsync()
    {
        await SaveAsync();
        _journalEntries.Clear();
        await SaveJournalAsync();
    }

    private async Task SaveJournalAsync()
    {
        await StateManager.SetStateAsync(JournalStateName, new TableJournalState { Entries = _journalEntries });
        await StateManager.SaveStateAsync();
    }

    /// <summary>Lifecycle (Start/Stop/Deactivate) persist — rare, config-change-triggered calls, not the hot
    /// per-tick path <see cref="TableDefinition.Persistence"/> trades off (same rationale
    /// <see cref="TableHistoryActor"/>'s class doc gives for its own Reset/Disable persisting immediately),
    /// so this always awaits/blocks for <see cref="TablePersistenceMode.Batched"/> AND
    /// <see cref="TablePersistenceMode.FireAndForget"/> alike — only <see cref="TablePersistenceMode.
    /// MemoryOnly"/> skips it, on every path, per that mode's "never touches storage" contract (see class
    /// doc's plan-008 paragraph). Joins any still-in-flight <see cref="TablePersistenceMode.FireAndForget"/>
    /// background write first (it never throws — see <see cref="_inFlightPersist"/>'s own doc comment) so
    /// this call is strictly ordered after it, rather than racing it on the same "table" state key.</summary>
    private async Task SaveControlStateAsync()
    {
        if (_def is null || _def.Persistence == TablePersistenceMode.MemoryOnly)
        {
            return;
        }

        if (_inFlightPersist is { IsCompleted: false } pending)
        {
            await pending;
        }

        await SaveAsync();
    }

    /// <summary>Resolves this activation's flush-timer cadence from <see cref="TableDefinition.FlushMs"/>
    /// (0 → 2000 ms default, see <see cref="TablePersistencePolicy.ResolveFlushIntervalMs"/>) — except for
    /// <see cref="TablePersistenceMode.MemoryOnly"/>, which per that field's own doc comment ("Ignored for
    /// MemoryOnly") always uses the plain default: there is nothing durable for the knob to tune, only the
    /// unrelated in-memory read-cache refresh cadence (see class doc's plan-008 paragraph).</summary>
    private TimeSpan ResolveFlushPeriod()
    {
        var ms = _def is { Persistence: TablePersistenceMode.MemoryOnly }
            ? TablePersistencePolicy.DefaultFlushMs
            : TablePersistencePolicy.ResolveFlushIntervalMs(_def?.FlushMs ?? 0);
        return TimeSpan.FromMilliseconds(ms);
    }

    private async Task ArmTimerAsync(TimeSpan period)
    {
        await RegisterTimerAsync(FlushTimerName, nameof(OnFlushTickAsync), null, period, period);
        _timerArmed = true;
    }

    private async Task DisarmTimerIfArmedAsync()
    {
        if (!_timerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(FlushTimerName);
        _timerArmed = false;
    }
}

/// <summary>Persisted shape of a TableActor's state — see that class's doc comment for why the definition,
/// source/table lists, running flag, AND the write-behind snapshot/seq are all persisted (self-healing
/// across deactivation/reactivation, plus read availability of the last-flushed rows). Plain get/set
/// properties, same style as <see cref="PipelineActorState"/>/<see cref="Catalog.CatalogState"/>, for a
/// clean System.Text.Json round trip through Dapr's actor state store.</summary>
public sealed class TableActorState
{
    public TableDefinition? Def { get; set; }

    public List<SourceDefinition> Sources { get; set; } = [];

    public List<TableDefinition> Tables { get; set; } = [];

    /// <summary>Table-over-pipeline (plan 025) — every pipeline the catalog knew about at the last
    /// StartAsync/self-heal, same self-heal rationale as <see cref="Sources"/>/<see cref="Tables"/>.</summary>
    public List<PipelineDefinition> Pipelines { get; set; } = [];

    public bool Running { get; set; }

    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];

    public long Seq { get; set; }
}

/// <summary>Plan 009 A2 — one journaled change since the last compaction; Dapr's counterpart of
/// <c>StreamsForge.Host.Grains.TableJournalEntry</c> (Orleans), same shape, same semantics — see that class's
/// doc comment (or <see cref="TableActor"/>'s own Plan 009 A2 class-doc paragraph) for the full rationale.
/// <see cref="Weight"/> &gt; 0 means "this canonical row key is (now) present with this row/weight"; &lt;= 0
/// is an explicit REMOVAL TOMBSTONE (Row left empty — replay only needs to know to remove the key). An
/// ABSENT key means "unchanged since the last compaction", never a removal.</summary>
public sealed class TableJournalEntry
{
    public Dictionary<string, object?> Row { get; set; } = [];
    public long Weight { get; set; }
}

/// <summary>Plan 009 A2 — <see cref="TablePersistenceMode.Journaled"/>'s second actor state key
/// (<see cref="TableActor"/>'s <c>JournalStateName</c>, "table-journal"), separate from <see cref="TableActorState"/>
/// ("table") so a Journaled flush can write just this small object instead of the whole snapshot bundle.
/// Keyed by canonical row key so repeated changes to the SAME key before the next compaction coalesce into
/// one entry.</summary>
public sealed class TableJournalState
{
    public Dictionary<string, TableJournalEntry> Entries { get; set; } = [];
}

/// <summary>What a flush tick should do about the durability WRITE — decided by
/// <see cref="TablePersistencePolicy.DecideFlushAction"/>, dispatched by <see cref="TableActor.
/// OnFlushTickAsync"/>. Never governs the in-memory read-cache refresh, which always happens on a dirty
/// tick regardless of mode (see <see cref="TableActor.CaptureSnapshot"/>).</summary>
public enum TablePersistAction
{
    /// <summary>Nothing to write this tick — <see cref="TablePersistenceMode.MemoryOnly"/> (always), or a
    /// <see cref="TablePersistenceMode.FireAndForget"/> tick that found a previous background write still
    /// in flight (single-flight: the next tick retries with fresher state instead of overlapping it).</summary>
    Skip,

    /// <summary>Write now, awaited inside the current actor turn — <see cref="TablePersistenceMode.
    /// Batched"/>'s entire contract.</summary>
    AwaitedWrite,

    /// <summary>Start a background write and return without awaiting it — <see cref="TablePersistenceMode.
    /// FireAndForget"/>'s non-blocking half of its contract (see <see cref="TableActor.
    /// StartBackgroundPersist"/> for the single-flight bookkeeping and failure logging).</summary>
    BackgroundWrite,

    /// <summary>Plan 009 A2 — write only the small journal state, awaited inside the current actor turn —
    /// <see cref="TablePersistenceMode.Journaled"/>'s entire contract (see <see cref="TableActor.
    /// JournalFlushAsync"/>). Same durability as AwaitedWrite (Batched), different write VOLUME.</summary>
    JournalWrite,
}

/// <summary>
/// Plan 008: pure per-table-durability-policy decisions, extracted from <see cref="TableActor"/> for the
/// same testability reason as <see cref="TableCompilation"/>/<see cref="TableHistoryApplication"/> — see
/// dapr/tests/StreamsForge.Dapr.Tests/TablePersistencePolicyTests.cs. No actor, timer, or Dapr-sidecar
/// machinery involved; every input/output here is a plain CLR value. See <see cref="TableActor"/>'s class
/// doc ("Plan 008" paragraph) for how each outcome is dispatched.
/// </summary>
public static class TablePersistencePolicy
{
    /// <summary>Mirrors <see cref="TableDefinition.FlushMs"/>'s own doc comment: "0 = the 2000 ms
    /// default."</summary>
    public const int DefaultFlushMs = 2000;

    /// <summary>0 (unset) resolves to <see cref="DefaultFlushMs"/>; any positive value is used verbatim.
    /// Mirrors <see cref="TableDefinition.FlushMs"/>'s doc comment exactly.</summary>
    public static int ResolveFlushIntervalMs(int flushMs) => flushMs > 0 ? flushMs : DefaultFlushMs;

    /// <summary>The pure decision for one flush tick's durability write, given the table's configured
    /// <paramref name="mode"/>, whether there is anything unflushed (<paramref name="dirty"/>), and whether
    /// a previous <see cref="TablePersistenceMode.FireAndForget"/> background write has not yet completed
    /// (<paramref name="writeInProgress"/> — always <see langword="false"/> for <see cref="TablePersistenceMode.
    /// Batched"/>/<see cref="TablePersistenceMode.MemoryOnly"/>, which never leave a write in flight across
    /// ticks).</summary>
    public static TablePersistAction DecideFlushAction(TablePersistenceMode mode, bool dirty, bool writeInProgress)
    {
        if (!dirty)
        {
            return TablePersistAction.Skip;
        }

        return mode switch
        {
            TablePersistenceMode.MemoryOnly => TablePersistAction.Skip,
            TablePersistenceMode.FireAndForget => writeInProgress ? TablePersistAction.Skip : TablePersistAction.BackgroundWrite,
            TablePersistenceMode.Journaled => TablePersistAction.JournalWrite,
            _ => TablePersistAction.AwaitedWrite, // Batched (and any future default)
        };
    }
}

/// <summary>
/// Plan 009 A2 — pure journal-compaction/replay decisions, extracted from <see cref="TableActor"/> for the
/// same testability reason as <see cref="TablePersistencePolicy"/>/<see cref="TableCompilation"/> (that
/// class needs a live <c>ActorHost</c>/<c>DaprClient</c> to construct — see
/// dapr/tests/StreamsForge.Dapr.Tests/TableDeltaSequencingTests.cs's own doc comment for why nothing in this
/// project can instantiate it directly) — see dapr/tests/StreamsForge.Dapr.Tests/TableJournalPolicyTests.cs.
/// No actor/timer/Dapr-sidecar machinery involved; every input/output here is a plain CLR value. Mirrors
/// <c>StreamsForge.Host.Grains.TableGrain</c>'s equivalent inline logic (DefaultJournalMaxEntries,
/// ReplayJournalIntoSnapshot) — see that class's Plan 009 A2 doc paragraph for the shared design rationale.
/// </summary>
public static class TableJournalPolicy
{
    /// <summary>Mirrors <c>TableGrain.DefaultJournalMaxEntries</c> (Orleans) exactly — same value, same
    /// reasoning: large enough that ordinary per-tick churn doesn't compact on nearly every flush; small
    /// enough that activation's journal replay stays cheap.</summary>
    public const int DefaultJournalMaxEntries = 200;

    /// <summary>0 (unset) resolves to <see cref="DefaultJournalMaxEntries"/>; any positive configured value
    /// is used verbatim. Mirrors <see cref="TableDefinition.JournalMaxEntries"/>'s own doc comment exactly
    /// (same shape as <see cref="TablePersistencePolicy.ResolveFlushIntervalMs"/>'s 0-means-default
    /// convention).</summary>
    public static int ResolveJournalMaxEntries(int configured) => configured > 0 ? configured : DefaultJournalMaxEntries;

    /// <summary>True once the journal has reached (or somehow passed) its configured threshold — the pure
    /// trigger <see cref="TableActor.JournalFlushAsync"/> checks after every journal write.</summary>
    public static bool ShouldCompact(int journalEntryCount, int maxEntries) => journalEntryCount >= maxEntries;

    /// <summary>Merges <paramref name="journal"/> onto <paramref name="snapshot"/> in place: a Weight &gt; 0
    /// entry inserts/overwrites that key, a Weight &lt;= 0 entry (an explicit removal tombstone — see
    /// <see cref="TableJournalEntry"/>'s own doc comment) removes it. A no-op when <paramref name="journal"/>
    /// is empty. Mirrors <c>TableGrain.ReplayJournalIntoSnapshot</c> exactly.</summary>
    public static void ReplayOntoSnapshot(Dictionary<string, TableRowDto> snapshot, IReadOnlyDictionary<string, TableJournalEntry> journal)
    {
        foreach (var (key, entry) in journal)
        {
            if (entry.Weight > 0)
            {
                snapshot[key] = new TableRowDto { Row = new Dictionary<string, object?>(entry.Row), Weight = entry.Weight };
            }
            else
            {
                snapshot.Remove(key);
            }
        }
    }
}

/// <summary>
/// Pure SQL-compile-to-executor logic, extracted from <see cref="TableActor"/> specifically so it can be
/// unit tested without any actor/timer/Dapr-sidecar machinery (mirrors <see cref="PipelineCompilation"/>'s
/// own extraction rationale) — see dapr/tests/StreamsForge.Dapr.Tests/TableCompilationTests.cs. Builds the
/// same stream/table schema dictionaries + <see cref="SqlCompiler.CompileTable"/> call
/// <c>TableGrain.StartClassicAsync</c> makes.
///
/// <para><b>Table-over-pipeline (plan 025):</b> <paramref name="pipelines"/> — additive, nullable, defaults
/// to none so every pre-025 3-argument call site (including this project's own existing tests) keeps
/// compiling and behaving unmodified — widens the stream-relation dictionary with every pipeline that has a
/// compiled output schema, mirroring <c>Catalog.CatalogStore.BuildTableStreamSchemas</c> exactly (sources
/// win a name collision, same defensive tiebreak). <see cref="TryCompile"/>'s result then splits the
/// compiled <c>StreamInputs</c> by pipeline-name membership, returning the pipeline names separately as
/// <c>PipelineInputs</c> — mirrors <c>Catalog.CatalogStore.ApplyCompileResult</c>'s identical split for the
/// persisted <see cref="TableDefinition.PipelineInputs"/> field.</para>
/// </summary>
public static class TableCompilation
{
    public static (TableExecutor? Executor, List<string> StreamInputs, List<string> TableInputs, List<string> PipelineInputs, string? Error) TryCompile(
        TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables, IReadOnlyList<PipelineDefinition>? pipelines = null)
    {
        pipelines ??= [];

        var streamSchemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);
        foreach (var p in pipelines)
        {
            if (p.OutputFields.Count == 0) continue;
            streamSchemas[p.Name] = new SourceSchema(p.Name, p.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
        }
        foreach (var s in sources)
        {
            streamSchemas[s.Name] = new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
        }

        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            return (null, [], [], [], message);
        }

        var pipelineNames = pipelines.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        var allStreamInputs = compileResult.StreamInputs.Distinct().ToList();

        return (
            compileResult.Plan.CreateExecutor(),
            allStreamInputs.Where(n => !pipelineNames.Contains(n)).ToList(),
            compileResult.TableInputs.Distinct().ToList(),
            allStreamInputs.Where(pipelineNames.Contains).ToList(),
            null);
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

/// <summary>Table-over-pipeline (plan 025) — converts one pipeline's emitted result row into the
/// <see cref="EventRecord"/> shape <see cref="TableExecutor.OnStreamEvent"/> expects, the Dapr counterpart
/// of Orleans' <c>PipelineInputs.ToEventRecord</c> (orleans/src/StreamsForge.Host/Grains/PipelineInputs.cs).
/// Back-fills <c>_ts</c>/<c>_source</c> from the envelope only when the row does not already carry them — a
/// projection that selected them explicitly has already said what they should be, and overwriting a value
/// the query asked for would be a silent rewrite of the user's own output. Pulled out to its own static
/// class, rather than inlined in <see cref="TableActor.ProcessPipelineResultsAsync"/>, for the same
/// unit-testability reason as <see cref="TableAttachPolicy"/>/<see cref="TablePersistencePolicy"/>: a
/// <see cref="TableActor"/> instance needs a live Dapr sidecar to construct at all — see dapr/tests/
/// StreamsForge.Dapr.Tests/PipelineResultMappingTests.cs.</summary>
public static class PipelineResultMapping
{
    public static EventRecord ToEventRecord(ResultEnvelope envelope, string pipelineName)
    {
        var record = new EventRecord(envelope.Row);
        if (!record.ContainsKey(EventRecord.TimestampField))
        {
            record[EventRecord.TimestampField] = envelope.TimestampMs;
        }
        if (!record.ContainsKey(EventRecord.SourceField))
        {
            record[EventRecord.SourceField] = pipelineName;
        }
        return record;
    }
}

/// <summary>PARITY.md debt item D2 — the pure epoch-cutoff decision <see cref="TableActor.
/// ProcessTableDeltasAsync"/> applies to an upstream table-delta batch, pulled out to its own static class
/// (same reason <see cref="TablePersistencePolicy"/>/<see cref="TableJournalPolicy"/> exist: a
/// <see cref="TableActor"/> instance needs a live Dapr sidecar to construct at all, so the ACTOR method
/// itself is not unit-testable, but the decision it delegates to can be — see dapr/tests/
/// StreamsForge.Dapr.Tests/TableAttachPolicyTests.cs). Verbatim port of the inline filter
/// <c>TableGrain.OnTableDeltaBatchAsync</c> (Orleans) applies: a delta whose <c>TableDeltaDto.Epoch</c> is
/// at or below <paramref name="cutoff"/> is already reflected in the snapshot this table backfilled from
/// (<see cref="TableActor.RegisterRouterAndAttachToTableInputsAsync"/>) — admitting it again would double
/// its Z-set weight — so it is dropped before admission. <paramref name="cutoff"/> &lt; 0 (this table never
/// recorded a cutoff for this upstream — no snapshot existed to backfill from, or this upstream isn't a
/// declared table input at all) admits everything unconditionally, the same "nothing to double-count
/// against" default <see cref="TableAttachSnapshot"/>'s own doc comment states.</summary>
public static class TableAttachPolicy
{
    public static List<TableDeltaDto> FilterAdmissible(IReadOnlyList<TableDeltaDto> batch, long cutoff) =>
        cutoff < 0 ? batch.ToList() : batch.Where(d => d.Epoch > cutoff).ToList();
}
