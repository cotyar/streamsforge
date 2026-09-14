using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Host.Grains;

public sealed class RegistryState
{
    public List<SourceDefinition> Sources { get; set; } = [];
    public List<PipelineDefinition> Pipelines { get; set; } = [];
    public List<TableDefinition> Tables { get; set; } = [];

    /// <summary>entityKey ("source:{name}" / "pipeline:{id}" / "table:{id}") → FieldNumberMap JSON.
    /// See IRegistryGrain.EnsureFieldNumbersAsync.</summary>
    public Dictionary<string, string> FieldNumberMaps { get; set; } = [];
}

/// <summary>Singleton grain (key = StreamConstants.RegistryKey). Catalog of sources + pipelines; orchestrates start/stop.
/// Not [Reentrant] overall (mutations must stay serialized), but the read-only Get* methods are allowed to
/// interleave: PipelineGrain.StartAsync calls back into GetSourcesAsync while it is itself being started
/// from inside RegistryGrain.EnsureInitializedAsync / SetPipelineStatusAsync — without interleaving that
/// call would deadlock waiting on this grain's own in-flight turn.</summary>
[MayInterleave(nameof(MayInterleave))]
public sealed class RegistryGrain(
    [PersistentState("catalog", StreamConstants.StorageName)] IPersistentState<RegistryState> state)
    : Grain, IRegistryGrain
{
    private static readonly HashSet<string> InterleavableMethods = new(StringComparer.Ordinal)
    {
        nameof(IRegistryGrain.GetSourcesAsync),
        nameof(IRegistryGrain.GetSourceAsync),
        nameof(IRegistryGrain.GetPipelinesAsync),
        nameof(IRegistryGrain.GetPipelineAsync),
        nameof(IRegistryGrain.GetTablesAsync),
        nameof(IRegistryGrain.GetTableAsync),
    };

    public static bool MayInterleave(IInvokable req) => InterleavableMethods.Contains(req.GetMethodName());

    /// <summary>Plan 021 D1/D5 — this grain's OWN environment, learned once from its own primary key
    /// (never re-derived per call: a grain's key never changes across one activation's lifetime). On an
    /// instance where no environment is ever created or mentioned, this activation's key is the literal
    /// string <see cref="StreamConstants.RegistryKey"/> ("catalog") — <see cref="EnvKeys.EnvOf"/> on a key
    /// with no separator returns <see cref="EnvKeys.Default"/>, so <c>_env</c> is "" exactly as it always
    /// implicitly was, and every entity this grain creates gets <c>Environment = ""</c>, which is the D2
    /// byte-identical requirement in one field.</summary>
    private string _env = EnvKeys.Default;

    /// <summary>Plan "source data-loss fixes" section E2 — the RESUME half of
    /// <see cref="EnsureInitializedAsync"/> runs exactly ONCE per activation. Boot has two independent
    /// callers of that method (<c>Program.cs</c>'s per-environment loop and
    /// <c>StreamBridgeService.DiscoverEnvironmentsAsync</c>) and, before this latch, no idempotency guard —
    /// so the pipelines/tables resume loops ran TWICE, and the second pass's
    /// <c>ITableGrain.StartAsync</c> re-ran the resume path and RESET each table to empty
    /// (<c>TableGrain.ClearResumeMarkersAndDetect</c>), wiping whatever rows the first pass had let in.
    /// Non-reentrancy serializes the two boot callers, so the second one simply returns here.
    ///
    /// <para>Set only as the LAST statement of the method, never at the top: a storage throw part-way
    /// through the resume block must leave the latch open so the OTHER boot caller still gets a full
    /// attempt. Deliberately NOT persisted — it is per-activation, and a reactivated grain must resume its
    /// grains again. The SEED/backfill section above it stays unlatched and data-driven (it fires on an
    /// empty catalog / an empty <c>SourceNames</c>), which is what keeps
    /// <c>PipelineLineageBackfillTests</c> and <c>EnvIsolationRegistryTests</c> true across repeat
    /// calls.</para></summary>
    private bool _resumed;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _env = EnvKeys.EnvOf(this.GetPrimaryKeyString());
        return Task.CompletedTask;
    }

    public async Task EnsureInitializedAsync()
    {
        var dirty = false;
        // Plan 021 — SEEDING IS DEFAULT-ENVIRONMENT-ONLY, and the rest of this method is not. Boot calls
        // this on every environment's registry so already-Running entities resume everywhere (plan 021
        // wave 1 item 3), but the three seed blocks below fire on an EMPTY catalog — so without this
        // guard, creating an empty `staging` and restarting would fill it with the demo catalog, and
        // force-deleting an environment's contents would re-seed them on the next boot. Neither is
        // something anyone asked for, and both are silent. AGENTS.md's "seeds apply only to an empty data
        // dir" stays exactly true for the default environment and becomes "never" for any other.
        var maySeed = _env == EnvKeys.Default;
        if (maySeed && state.State.Sources.Count == 0)
        {
            state.State.Sources.AddRange(SeedCatalog.Sources());
            dirty = true;
        }

        if (maySeed && state.State.Pipelines.Count == 0)
        {
            state.State.Pipelines.AddRange(SeedCatalog.Pipelines());
            dirty = true;
        }

        // Plan 011 Wave A backfill: pipelines are seeded RAW (unlike tables, below) and Create/Update are
        // the only other writers of SourceNames (ApplyPipelineCompileResult), so any pipeline that has
        // never been through either — freshly seeded above, or durably persisted from before this backfill
        // existed — still carries an empty SourceNames and draws no lineage edge. Runs for BOTH cases
        // (seed and restore) since it's driven off the data, not off whether seeding just happened. Must
        // run after Sources are loaded (above, unconditionally) so BuildStreamSchemas() has something to
        // compile against. Draft-friendly like the create/update paths: a pipeline whose SQL doesn't
        // currently compile is left with an empty SourceNames rather than throwing or blocking
        // initialization.
        //
        // TABLE-OVER-PIPELINE extends the condition to OutputFields: that field is newer than every
        // durably persisted pipeline record, so a restored catalog carries it empty and — since a
        // pipeline with no output schema contributes NO relation (PipelineInputs.BuildStreamSchemas) —
        // every table naming such a pipeline would fail to compile at boot until somebody happened to
        // edit the pipeline. Same data-driven predicate, one more disjunct.
        //
        // AND IT MOVED ABOVE THE TABLE SEED BLOCK, which is load-bearing rather than cosmetic: the seed
        // tables compile against the stream-relation dictionary, and pipelines have to have their
        // OutputFields before they can appear in it. Below the table block, a seeded table over a seeded
        // pipeline would never compile on the very boot that created both.
        foreach (var pipeline in state.State.Pipelines.Where(p => p.SourceNames.Count == 0 || p.OutputFields.Count == 0))
        {
            var compileResult = SqlCompiler.Compile(pipeline.Sql, BuildStreamSchemas());
            if (compileResult.Ok && compileResult.SourceNames.Count > 0)
            {
                ApplyPipelineCompileResult(pipeline, compileResult);
                dirty = true;
            }
        }

        if (maySeed && state.State.Tables.Count == 0)
        {
            var streamSchemas = PipelineInputs.BuildStreamSchemas(state.State.Sources, state.State.Pipelines);
            var pipelineNames = PipelineNameSet();
            var tableSchemas = new Dictionary<string, SourceSchema>();
            var seeds = SeedCatalog.Tables();
            foreach (var t in seeds)
            {
                var result = SqlCompiler.CompileTable(t.Sql, streamSchemas, tableSchemas);
                if (result.Ok && result.OutputSchema is not null)
                {
                    t.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
                    t.StreamInputs = result.StreamInputs.Where(n => !pipelineNames.Contains(n)).ToList();
                    t.PipelineInputs = result.StreamInputs.Where(pipelineNames.Contains).ToList();
                    t.TableInputs = result.TableInputs.ToList();
                    t.KeyFields = TableKeyFields.Describe(t.Sql, result.Plan);
                    tableSchemas[t.Name] = result.OutputSchema;
                }
                else
                {
                    t.Status = PipelineStatus.Stopped;
                    t.Error = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                }
            }
            state.State.Tables.AddRange(seeds);
            dirty = true;
        }

        if (dirty)
        {
            await state.WriteStateAsync();
        }

        // ------------------------------------------------------------------------------------------
        // RESUME BLOCK — once per activation (see _resumed), CONSUMERS BEFORE PRODUCERS.
        //
        // Everything above this line is seeding/backfill: data-driven, idempotent, and deliberately
        // unlatched, so a repeat call still repairs a catalog that was hand-edited (or restored) between
        // calls. Everything below starts grains, and starting them twice is not free — see _resumed.
        // ------------------------------------------------------------------------------------------
        if (_resumed)
        {
            return;
        }

        var statusChanged = false;
        foreach (var pipeline in state.State.Pipelines.Where(p => p.Status == PipelineStatus.Running))
        {
            try
            {
                await GrainFactory.GetGrain<IPipelineGrain>(EnvKeys.Qualify(_env, pipeline.Id)).StartAsync(pipeline);
            }
            catch (Exception ex)
            {
                pipeline.Status = PipelineStatus.Failed;
                pipeline.Error = ex.Message;
                statusChanged = true;
            }
        }

        // Resume Running tables in dependency order (topological — table-over-table inputs before their
        // dependents) so a chained table's own inputs are already Running when it starts.
        var runningTables = state.State.Tables.Where(t => t.Status == PipelineStatus.Running).ToList();
        foreach (var table in TopoSortByTableInputs(runningTables, state.State.Tables))
        {
            try
            {
                await GrainFactory.GetGrain<ITableGrain>(EnvKeys.Qualify(_env, table.Name)).StartAsync(table);
            }
            catch (Exception ex)
            {
                table.Status = PipelineStatus.Failed;
                table.Error = ex.Message;
                statusChanged = true;
            }
        }

        // PRODUCERS LAST — the whole point of this ordering. Memory streams have NO replay: a table or
        // pipeline receives only rows published after it subscribed. Resuming sources first (which is what
        // this method did before) opened a window in which a url/file/folder source's first poll landed
        // before any consumer had subscribed, and with a dedup key configured those rows never come back —
        // the source has already ledgered them. The two loops above subscribe by stream NAME and compile
        // against registry STATE (GetSourcesAsync, on the [MayInterleave] allowlist), so neither needs a
        // started source grain to reach Running; nothing here depends on the old order.
        //
        // Residual, and deliberately not closed by ordering alone: a connector grain activated by an
        // unrelated API call during the boot window (GET /api/sources/{name}/status, say) self-resumes an
        // overdue poll on OnActivateAsync and can still publish early. The source-side replay ring covers
        // that case; see also GeneratorSupervisorService.PingEnvironmentAsync, whose 15s sweep would
        // otherwise be exactly such an activator and now awaits this method first.
        foreach (var src in state.State.Sources.Where(s => s.Enabled))
        {
            try
            {
                // Plan 006 D-C / plan 008 W4 / plan 009 wave D / plan 020 wave B-2: four-way Kind dispatch
                // via the shared SourceKindDispatch.Classify (StreamsForge.Abstractions — both flavors use
                // it, see its own class doc). Generator (or unset — pre-006 seeds/sources) keeps the
                // pre-existing IGeneratorGrain path unchanged; Ingest starts NO grain at all (rows arrive
                // through IIngressFacade); Crdt goes to ICrdtDocGrain (D3 — never IConnectorGrain);
                // Connector (everything else) goes to IConnectorGrain.
                var kind = SourceKindDispatch.Classify(src.Kind);
                if (kind == SourceKindDispatch.ActorKind.Generator)
                {
                    await GrainFactory.GetGrain<IGeneratorGrain>(EnvKeys.Qualify(_env, src.Name)).StartAsync(src);
                }
                else if (kind == SourceKindDispatch.ActorKind.Crdt)
                {
                    await GrainFactory.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(_env, src.Name)).StartAsync(src);
                }
                else if (kind != SourceKindDispatch.ActorKind.Ingest)
                {
                    await GrainFactory.GetGrain<IConnectorGrain>(EnvKeys.Qualify(_env, src.Name)).StartAsync(src);
                }
            }
            catch
            {
                // best-effort on boot; supervisor will retry via PingAsync
            }
        }

        // Plan 020 wave C: CRDT documents re-assert their projection AFTER the pipelines/tables above are
        // Running and subscribed — never earlier. The ordering in this method is load-bearing, not
        // incidental: a document that replayed on its own activation would publish into a stream nobody
        // has subscribed to yet and the rows would be dropped. This is the recovery for TableGrain's
        // RESTART-RESUME LIMITATION, which resets a resuming table to empty and rebuilds it "purely from
        // live traffic" — a document has no further live traffic to rebuild from, because D7 makes
        // replaying its update history emit nothing. Best-effort like every other resume in this method; a
        // document that fails to replay is still a correct document, just an empty table until its next
        // edit or an explicit POST .../crdt/replay.
        //
        // ONCE PER ACTIVATION, and that is now the whole story. This comment used to justify re-running
        // the replay on EVERY call, because the method had two boot callers and no idempotency guard: the
        // tables loop ran twice, the second ITableGrain.StartAsync re-ran the resume path and RESET the
        // table to empty (TableGrain's ClearResumeMarkersAndDetect), so a replay that fired only on the
        // first pass was wiped by the second — verified live at the time, latching the replay alone put
        // the table straight back to rowCount 0 / rebuilding true. The _resumed latch above removes the
        // second pass itself, so the replay's single pass is no longer wiped by anything and the
        // observation that produced that live evidence can no longer occur. Re-asserting stays idempotent
        // for a LATEST BY consumer, so an operator-issued replay is still safe at any time.
        foreach (var src in state.State.Sources.Where(s =>
                     s.Enabled && SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Crdt))
        {
            try
            {
                await GrainFactory.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(_env, src.Name)).ReplayAsync();
            }
            catch
            {
                // best-effort on boot; an operator can re-issue the replay by hand.
            }
        }

        if (statusChanged)
        {
            await state.WriteStateAsync();
        }

        // Re-subscribe history grains for every table with HistoryEnabled — independent of the table's own
        // Running status (history just won't see new deltas until/unless the table is running). Unlike the
        // table-resume loop above, this uses ResumeAsync (not ResetAsync) so previously accumulated history
        // survives a silo restart.
        foreach (var table in state.State.Tables.Where(t => t.HistoryEnabled && t.ShardBy.Count == 0))
        {
            try
            {
                await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, table.Name)).ResumeAsync(table);
            }
            catch
            {
                // best-effort — a stale/misconfigured history grain shouldn't block boot.
            }
        }

        // Plan 011 D1: the same treatment for every SHARDED table's router — ResumeAsync (not ResetAsync)
        // so the per-key shards persisted on disk survive a silo restart exactly like the history grain's
        // entries do. Note the loop above now skips sharded tables: on a sharded table the per-key history
        // REPLACES the table-wide one, and resuming both would hold the same version trails twice, which
        // is precisely the memory this wave exists to stop holding.
        foreach (var table in state.State.Tables.Where(t => t.ShardBy.Count > 0))
        {
            try
            {
                await GrainFactory.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(_env, table.Name)).ResumeAsync(table);
            }
            catch
            {
                // best-effort — a stale/misconfigured shard router shouldn't block boot.
            }
        }

        // LAST statement, on purpose — see _resumed. Anything that throws above leaves the latch open so
        // the other boot caller still gets a full attempt at the resume block.
        _resumed = true;
    }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(state.State.Sources.ToList());

    public Task<SourceDefinition?> GetSourceAsync(string name) =>
        Task.FromResult(state.State.Sources.FirstOrDefault(s => s.Name == name));

    /// <summary>Wishlist #8's run-on-demand. The registry owns the catalog, so it is the one place that
    /// can turn a source NAME into "the generator activation for that source" — and forwarding is all it
    /// does: the batch is generated and published inside <see cref="IGeneratorGrain.RunAsync"/>, on the
    /// activation that owns that source's stream. Doing the work here instead would publish from the
    /// wrong activation and bypass the generator's own backpressure.</summary>
    public async Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request)
    {
        var src = state.State.Sources.FirstOrDefault(s => s.Name == name);
        if (src is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        return await GrainFactory.GetGrain<IGeneratorGrain>(EnvKeys.Qualify(_env, name)).RunAsync(request);
    }

    public async Task UpsertSourceAsync(SourceDefinition def)
    {
        // Plan 021 D3/write-path guard — checked on EVERY upsert (create AND update), same "no
        // rename-only special case" shape ValidateUniquePipelineName gave pipeline names in plan 016.
        ValidateQualifiableName("Source", def.Name);

        // Table-over-pipeline — the mirror of ValidateUniquePipelineName's source check. A source name is
        // a relation name too, and it must not become the second meaning of a name a pipeline already
        // owns. Checked on every upsert (create AND update) rather than only on create: a source cannot be
        // renamed, so an update can only re-assert its own name, and re-asserting a name that has since
        // collided is exactly the case worth catching.
        if (state.State.Pipelines.Any(p => string.Equals(p.Name, def.Name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{def.Name}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");
        }

        var idx = state.State.Sources.FindIndex(s => s.Name == def.Name);
        bool schemaChanged;
        if (idx >= 0)
        {
            // Plan 016 wave 2 — sources are the ONE entity with no CatalogRecordMerge overload (they are
            // upserted whole, on both flavours), so their counter carry has to happen here, at the upsert
            // site, on both flavours. Wave 0 left a note about exactly this on CatalogRecordMerge.
            // CarryAndBumpSource does both halves in one call so the two flavours cannot drift.
            var existing = state.State.Sources[idx];
            CatalogRevisions.CarryAndBumpSource(existing, def);
            // Plan 021 D5 — Environment is NEVER edited after creation, even though CarryAndBumpSource
            // (shared/, frozen) has no notion of it and an incoming payload from a client that predates
            // this plan carries "" by default: force the STORED value forward regardless of what `def`
            // said, exactly like CatalogRecordMerge.CarryServerOwnedFields does for every other
            // server-owned field on tables/pipelines.
            def.Environment = existing.Environment;
            schemaChanged = def.SchemaRevision != existing.SchemaRevision;
            state.State.Sources[idx] = def;
        }
        else
        {
            // A brand-new source is revision 1, not 0: 0 is reserved for "written before plan 016" (and,
            // on a pin, for "declared edge with no compatibility claim"), so a fresh entity that reported
            // 0 would be indistinguishable from one whose revision was never assigned.
            def.Revision = 1;
            def.SchemaRevision = 1;
            // Plan 021 D5 — a NEW entity belongs to the environment its OWN registry grain is activated
            // at, never to whatever (if anything) the caller's payload said.
            def.Environment = _env;
            state.State.Sources.Add(def);
            // A source appearing can make a table that never compiled compile — same refresh, same reason.
            schemaChanged = true;
        }

        if (schemaChanged)
        {
            RefreshTableSchemas();
        }

        RecomputeStaleReasons();
        await state.WriteStateAsync();

        // Plan 006 D-C / plan 009 wave D / plan 020 wave B-2: Kind dispatch via the shared
        // SourceKindDispatch.Classify. On an update where Kind CHANGED (e.g. generator -> url), the grain
        // for the OLD kind is still activated and would otherwise keep polling/ticking forever — so every
        // non-selected kind's StopAsync always runs (cheap/idempotent no-ops on whichever kind wasn't
        // actually running) rather than tracking the previous Kind separately just to target one Stop call.
        var generator = GrainFactory.GetGrain<IGeneratorGrain>(EnvKeys.Qualify(_env, def.Name));
        var connector = GrainFactory.GetGrain<IConnectorGrain>(EnvKeys.Qualify(_env, def.Name));
        // CRDT-as-a-plugin — VERIFIED LIVE, THE HARD WAY: GrainFactory.GetGrain<ICrdtDocGrain>(...) throws
        // System.ArgumentException("Could not find an implementation for interface ...") IMMEDIATELY, at
        // reference-construction time, not lazily on the first RPC — when no 'crdt' plugin is loaded.
        // Constructing this reference unconditionally, the way generator/connector are just above, would
        // therefore break EVERY source upsert on a plugin-absent host, not just crdt-kind ones, because
        // every OTHER branch below still calls crdt.StopAsync() for the "kind switched away from crdt"
        // cleanup. Booting a real host with `--Plugins:Path <empty dir>` and creating a plain non-crdt
        // source reproduced exactly that 500 before this guard moved up here. So: check plugin presence
        // ONCE, before constructing the reference at all — `crdt` stays null when the plugin is absent,
        // every non-crdt branch below skips its StopAsync (nothing crdt-related could be running without
        // the plugin in the first place, so there is nothing to stop), and only the Crdt case — the one
        // branch that actually needs the grain — surfaces the readable refusal.
        var crdtPluginLoaded = StreamsForge.AppCore.Plugins.StreamsForgePlugins.Loaded.Any(p => p.Plugin.Name == "crdt");
        var crdt = crdtPluginLoaded ? GrainFactory.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(_env, def.Name)) : null;
        if (def.Enabled)
        {
            switch (SourceKindDispatch.Classify(def.Kind))
            {
                case SourceKindDispatch.ActorKind.Generator:
                    await generator.StartAsync(def);
                    await connector.StopAsync();
                    if (crdt is not null)
                    {
                        await crdt.StopAsync();
                    }
                    break;
                case SourceKindDispatch.ActorKind.Ingest:
                    await generator.StopAsync();
                    await connector.StopAsync();
                    if (crdt is not null)
                    {
                        await crdt.StopAsync();
                    }
                    break;
                case SourceKindDispatch.ActorKind.Crdt:
                    // An operator hitting this from POST /api/sources needs to know WHY, not just that it
                    // 500'd — this call site, unlike the boot-resume/ping loops elsewhere in this class,
                    // has no surrounding try/catch. Checked here, once, rather than inside CrdtDocGrain
                    // itself, because a grain that failed to activate has no path back to the caller at
                    // all.
                    if (crdt is null)
                    {
                        throw new InvalidOperationException(
                            $"source '{def.Name}' is kind 'crdt', but no 'crdt' plugin is loaded on this " +
                            "host — install plugins/StreamsForge.Plugins.Crdt.dll (or check the " +
                            "Plugins:Path this instance was started with) before starting a CRDT source.");
                    }
                    await crdt.StartAsync(def);
                    await generator.StopAsync();
                    await connector.StopAsync();
                    break;
                default: // Connector
                    await connector.StartAsync(def);
                    await generator.StopAsync();
                    if (crdt is not null)
                    {
                        await crdt.StopAsync();
                    }
                    break;
            }
        }
        else
        {
            await generator.StopAsync();
            await connector.StopAsync();
            if (crdt is not null)
            {
                await crdt.StopAsync();
            }
        }

        // Sources finally get lifecycle events, like pipelines and tables already had. Before this, the
        // console's live source tape depended entirely on StreamBridgeService's 30s add-only source poll:
        // a source created in the UI produced nothing on the wire for up to 30 seconds, and a deleted one
        // kept a relay forever. PipelineId carries the source NAME — the same convention the "table-"
        // kinds use for a table's name, sources having no id at all.
        //
        // Deliberately published AFTER the start/stop dispatch above and NOT inside a try: if starting the
        // grain threw, this upsert did not take effect the way the caller asked, and announcing
        // "source-started" for it would tell the bridge to relay a stream nothing is producing on. The
        // publish itself is a plain stream OnNextAsync exactly like the table path's, so it needs no
        // InterleavableMethods entry — the bridge subscribes, it never calls back into this grain.
        await PublishLifecycleAsync(
            def.Name,
            def.Enabled ? "source-started" : "source-stopped",
            def.Enabled ? PipelineStatus.Running : PipelineStatus.Stopped);
    }

    public async Task<bool> DeleteSourceAsync(string name)
    {
        var removed = state.State.Sources.RemoveAll(s => s.Name == name) > 0;
        if (!removed)
        {
            return false;
        }

        RecomputeStaleReasons(); // a deleted source breaks every pin that named it.
        await state.WriteStateAsync();
        // Stop both kinds unconditionally — see UpsertSourceAsync's dispatch comment (cheap/idempotent).
        await GrainFactory.GetGrain<IGeneratorGrain>(EnvKeys.Qualify(_env, name)).StopAsync();
        await GrainFactory.GetGrain<IConnectorGrain>(EnvKeys.Qualify(_env, name)).StopAsync();
        // See UpsertSourceAsync's publish for the convention (PipelineId = the source NAME). This is the
        // event that lets StreamBridgeService drop the relay — its 30s source poll is add-only and never
        // unsubscribed, so before this a deleted source's subscription outlived the source itself.
        await PublishLifecycleAsync(name, "source-deleted", PipelineStatus.Stopped);
        return true;
    }

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(state.State.Pipelines.ToList());

    public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
        Task.FromResult(state.State.Pipelines.FirstOrDefault(p => p.Id == id));

    public async Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        ValidateUniquePipelineName(def.Name, excludePipelineId: null);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;
        def.Revision = 1; // see UpsertSourceAsync for why a fresh entity is 1 and not 0.
        def.Environment = _env; // plan 021 D5 — belongs to this grain's own environment, never the caller's.

        // A pipeline still reads SOURCES only — the bare BuildStreamSchemas(), never the table-side
        // dictionary that also carries pipelines. Table-over-pipeline is one-directional by construction:
        // that is the whole reason no cycle is possible (a pipeline cannot read a table either), which is
        // why nothing here needs the cycle check table-over-table has.
        var createCompile = SqlCompiler.Compile(def.Sql, BuildStreamSchemas());
        ValidateReplayFrom("pipeline", def.ReplayFrom, createCompile.Ok, createCompile.SourceNames, []);
        ApplyPipelineCompileResult(def, createCompile);

        state.State.Pipelines.Add(def);
        // Table-over-pipeline: a pipeline APPEARING can make a table that never compiled compile — the
        // same reason UpsertSourceAsync refreshes on a new source.
        RefreshTableSchemas();
        RecomputeStaleReasons();
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Id, "created", def.Status);
        return def;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var idx = state.State.Pipelines.FindIndex(p => p.Id == def.Id);
        if (idx < 0)
        {
            return null;
        }

        var existing = state.State.Pipelines[idx];

        // Plan 016 wave 1-C: checked on EVERY update, not only when the name changes — that is what
        // "existing violators keep working until someone edits one" means. A catalog that already holds
        // two pipelines called 'trades' keeps serving both; the first edit to either is refused until the
        // name is unique. Nothing scans the catalog at boot, and no migration runs.
        ValidateUniquePipelineName(def.Name, excludePipelineId: existing.Id);

        var sqlChanged = existing.Sql != def.Sql;
        // Plan 026 D5: executor-affecting exactly like the SQL — replayFrom is applied when the executor is
        // built (StartAsync) and nowhere else, so a change to it on a Running pipeline restarts it.
        var replayFromChanged = !ReplayFromEquals(existing.ReplayFrom, def.ReplayFrom);
        var wasRunning = existing.Status == PipelineStatus.Running;

        // Compile-check against the prospective SQL before anything is stored — draft-friendly like
        // tables' CompileTableSql/ApplyCompileResult pair: never blocks the update on its own, only
        // populates/clears SourceNames.
        var compileResult = SqlCompiler.Compile(def.Sql, BuildStreamSchemas());
        ValidateReplayFrom("pipeline", def.ReplayFrom, compileResult.Ok, compileResult.SourceNames, []);

        // See CarryServerOwnedFields' doc comment: the incoming definition IS the new record, with only
        // the server-owned fields carried over from the stored one — rather than a hand-written list of
        // editable fields copied onto the stored record.
        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Plan 021 D5 — CarryServerOwnedFields (shared/, frozen) has no notion of Environment; force the
        // STORED value forward regardless of what `def` said, same as UpsertSourceAsync does for sources.
        def.Environment = existing.Environment;
        // Plan 016 wave 2: the carry put the STORED counter on `def`; this moves it, and only if the
        // definition actually changed by the same canonical-JSON test ImportPlanner uses for
        // "skipped" vs "updated". Before the restart block below, so a restart's Status change (which
        // is not a definition change) cannot influence the comparison.
        CatalogRevisions.BumpPipeline(existing, def);
        state.State.Pipelines[idx] = def;

        var outputFieldsBefore = existing.OutputFields;
        ApplyPipelineCompileResult(def, compileResult);
        // Table-over-pipeline: this pipeline's output schema IS the relation any dependent table compiled
        // against, so a shape change here has exactly the consequence a source's schema change has —
        // refresh the dependent tables' persisted OutputFields/field numbers so /proto stops describing a
        // shape they no longer produce. Same rules as the source path: pure Engine work, no grain calls,
        // no cascading restarts, and a table whose SQL no longer compiles is left completely alone.
        if (SchemaCompatibility.ShapeChanged(outputFieldsBefore, def.OutputFields))
        {
            RefreshTableSchemas();
        }
        RecomputeStaleReasons();

        if ((sqlChanged || replayFromChanged) && wasRunning)
        {
            var pipelineGrain = GrainFactory.GetGrain<IPipelineGrain>(EnvKeys.Qualify(_env, def.Id));
            try
            {
                await pipelineGrain.StopAsync();
                await pipelineGrain.StartAsync(def);
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            catch (Exception ex)
            {
                def.Status = PipelineStatus.Failed;
                def.Error = ex.Message;
            }
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Id, "updated", def.Status);
        return def;
    }

    public async Task<bool> DeletePipelineAsync(string id)
    {
        var existing = state.State.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return false;
        }

        if (existing.Status == PipelineStatus.Running)
        {
            try
            {
                await GrainFactory.GetGrain<IPipelineGrain>(EnvKeys.Qualify(_env, id)).StopAsync();
            }
            catch
            {
                // best-effort
            }
        }

        state.State.Pipelines.Remove(existing);

        // Table-over-pipeline: a table reading this pipeline is NOT refused, and is NOT stopped — exactly
        // what happens today when a source a table reads is deleted (DeleteSourceAsync, above: it takes no
        // census of dependents either). The table stays Running on its already-compiled plan and simply
        // receives nothing more from this input; its persisted OutputFields are left alone, because
        // RefreshTableSchemas skips a table whose SQL no longer compiles, and stale beats absent for a
        // published /proto contract. The visible signal is StaleReason, when the table carries a pin that
        // named this pipeline. The RUNNING-dependents refusal (ThrowIfRunningDependents) is deliberately
        // NOT extended here: it exists for table-over-table, where the upstream table's shard/delta tier is
        // the dependent's durable state; a pipeline owns no such state on the dependent's behalf.
        RecomputeStaleReasons();
        await state.WriteStateAsync();
        await PublishLifecycleAsync(id, "deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.State.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return null;
        }

        var grain = GrainFactory.GetGrain<IPipelineGrain>(EnvKeys.Qualify(_env, id));
        if (status == PipelineStatus.Running)
        {
            try
            {
                await grain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await PublishLifecycleAsync(id, "started", existing.Status);
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
                await PublishLifecycleAsync(id, "failed", existing.Status);
            }
        }
        else
        {
            try
            {
                await grain.StopAsync();
            }
            catch
            {
                // best-effort
            }

            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await PublishLifecycleAsync(id, "stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await state.WriteStateAsync();
        return existing;
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(state.State.Tables.ToList());

    public Task<TableDefinition?> GetTableAsync(string id) =>
        Task.FromResult(state.State.Tables.FirstOrDefault(t => t.Id == id));

    public async Task<TableDefinition> CreateTableAsync(TableDefinition def)
    {
        ValidateUniqueTableName(def.Name, excludeTableId: null);
        ValidateQualifiableName("Table", def.Name); // plan 021 D3/write-path guard
        ValidateParallelism(def.Parallelism);
        ValidateFlushMs(def.FlushMs);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;
        def.Revision = 1;       // see UpsertSourceAsync for why a fresh entity is 1 and not 0.
        def.SchemaRevision = 1;
        def.Environment = _env; // plan 021 D5 — belongs to this grain's own environment, never the caller's.

        var compileResult = CompileTableSql(def.Sql, excludeTableId: def.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);
        ValidateShardBy(def, compileResult);
        ValidateReplayFrom("table", def.ReplayFrom, compileResult.Ok, compileResult.StreamInputs, compileResult.TableInputs);
        ApplyCompileResult(def, compileResult);

        state.State.Tables.Add(def);
        RecomputeStaleReasons();
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Name, "table-created", def.Status);

        // Configures (and, if HistoryEnabled, subscribes) the history grain up front — independent of
        // whether the table itself is Running, exactly like SearchEnabled's index is built incrementally
        // once the table starts producing deltas.
        await ResetHistoryTiersAsync(def);
        return def;
    }

    /// <summary>Plan 011 D1: (re)configures BOTH per-row-history tiers from one place, because on any one
    /// table exactly one of them may run. A sharded table's per-key shards hold the version trails, so the
    /// table-wide <see cref="ITableHistoryGrain"/> is disabled rather than left subscribed — running both
    /// over the same delta stream would hold every version twice and hold the sharded copy's worth of it
    /// resident, which is exactly the memory the shard tier exists to stop holding.</summary>
    private async Task ResetHistoryTiersAsync(TableDefinition def)
    {
        if (def.ShardBy.Count > 0)
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, def.Name)).DisableAsync();
        }
        else
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, def.Name)).ResetAsync(def);
        }
        await GrainFactory.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(_env, def.Name)).ResetAsync(def);
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var idx = state.State.Tables.FindIndex(t => t.Id == def.Id);
        if (idx < 0)
        {
            return null;
        }

        var existing = state.State.Tables[idx];
        var renamed = !string.Equals(existing.Name, def.Name, StringComparison.Ordinal);
        if (renamed)
        {
            ValidateUniqueTableName(def.Name, excludeTableId: existing.Id);
            ValidateQualifiableName("Table", def.Name); // plan 021 D3/write-path guard
            ValidateTableRenameAllowed(existing);
        }
        ValidateParallelism(def.Parallelism);
        ValidateFlushMs(def.FlushMs);

        // Compile + validate against the *prospective* SQL/history config before mutating `existing` at
        // all, so a rejected update (bad name, bad historyByField) never leaves partially-applied state
        // behind in the in-memory list entry.
        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);
        ValidateShardBy(def, compileResult);
        ValidateReplayFrom("table", def.ReplayFrom, compileResult.Ok, compileResult.StreamInputs, compileResult.TableInputs);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
        // Plan 003 M2: a Parallelism change (1->N, N->1, or N->M) changes which grain topology the table's
        // grain(s) run as (classic vs. coordinator mode, or a differently-shaped partitioned graph) — mirror
        // the search-config restart semantics below so it takes effect immediately on a Running table
        // instead of only on the next manual stop/start.
        var parallelismChanged = existing.Parallelism != def.Parallelism;
        // Plan 008 W2.5: a Persistence/FlushMs change picks a different write-behind policy for the SAME
        // running grain(s) — mirror the SQL/search-config/Parallelism restart semantics below so it takes
        // effect immediately on a Running table instead of only on the next manual stop/start (TableGrain's
        // StartAsync/StartClassicAsync/StartCoordinatorAsync only re-reads Persistence/FlushMs from the
        // TableDefinition it's (re)started with — see its own class doc's persistence-mode paragraph).
        var persistenceChanged = existing.Persistence != def.Persistence || existing.FlushMs != def.FlushMs
            || existing.JournalMaxEntries != def.JournalMaxEntries;
        // Plan 011 C2: the retention policy is installed on the executor at StartAsync and nowhere else
        // (see TableGrain.ApplyRetentionPolicy), so a change to it has the same "only picked up on
        // (re)start" property SQL/search/parallelism/persistence changes have — restart for the same
        // reason, so tightening or lifting a bound on a Running table takes effect now rather than at the
        // next manual stop/start.
        var retentionChanged = existing.RetentionMaxRows != def.RetentionMaxRows
            || existing.RetentionTtlMs != def.RetentionTtlMs;
        // Plan 026 D5: replayFrom is applied at StartAsync and nowhere else (a replay only targets a FRESH
        // executor — into a live one every replayed row is a late event), so it has exactly the
        // "only picked up on (re)start" property retention and the fields above it have — restart for the
        // same reason, so setting or clearing a position on a Running table takes effect now.
        var replayFromChanged = !ReplayFromEquals(existing.ReplayFrom, def.ReplayFrom);
        // Plan 011 D1: a ShardBy change re-keys the entire shard tier (or turns it on/off), so it resets
        // the tier below. Plan 011 D2 additionally RESTARTS the table, which D1 explicitly did not: the
        // tier is still only a delta-stream consumer and the grain topology is still unaffected, but
        // ShardBy now also decides whether TableGrain keeps a persisted snapshot mirror at all (see its
        // D2 paragraph), and TableGrain only re-reads its definition on StartAsync. Without the restart,
        // turning sharding on would leave the duplicate copy in place — the entire memory claim — until
        // somebody happened to bounce the table. Same reasoning, and the same code path, as the
        // Persistence/Parallelism changes beside it.
        var shardByChanged = !existing.ShardBy.SequenceEqual(def.ShardBy, StringComparer.Ordinal);
        var historyConfigChanged =
            existing.HistoryEnabled != def.HistoryEnabled ||
            existing.HistoryMode != def.HistoryMode ||
            existing.HistoryLimit != def.HistoryLimit ||
            existing.HistoryByField != def.HistoryByField ||
            existing.HistoryWindowMs != def.HistoryWindowMs;
        var wasRunning = existing.Status == PipelineStatus.Running;

        // Plan 016 wave 1-C: BEFORE the store, so a rename never leaves the old name's tiers addressable.
        if (renamed)
        {
            await ReleaseRenamedTiersAsync(existing.Name);
        }

        // See CarryServerOwnedFields' doc comment: the incoming definition IS the new record, with only
        // the server-owned fields carried over from the stored one.
        // Plan 016 wave 2: captured BEFORE the merge, which aliases def.OutputFields to the stored list;
        // ApplyCompileResult then hands `def` a fresh list, leaving this one holding the old shape. It is
        // the comparand SchemaRevision is computed from.
        var previousOutputFields = existing.OutputFields;

        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Plan 021 D5 — CarryServerOwnedFields (shared/, frozen) has no notion of Environment; force the
        // STORED value forward regardless of what `def` said, same as UpsertSourceAsync/UpdatePipelineAsync.
        def.Environment = existing.Environment;
        state.State.Tables[idx] = def;

        ApplyCompileResult(def, compileResult);

        // Plan 016 wave 2: after ApplyCompileResult (which is what recomputes OutputFields) and before
        // the restart block below (whose Status change is not a definition change).
        CatalogRevisions.BumpTable(existing, def, previousOutputFields);
        if (def.SchemaRevision != existing.SchemaRevision)
        {
            // This table's own /proto surface, refreshed at the write rather than at the next read.
            ComputeFieldNumbers(EntitySchemas.TableKey(def.Id), def.OutputFields);
            // …and every table downstream of it, which is the same job a source schema change does.
            RefreshTableSchemas();
        }

        RecomputeStaleReasons();

        // A running table's grain only picks up SQL/search config/parallelism/persistence changes on
        // (re)StartAsync — mirror the SQL-changed restart below for search config, parallelism, and
        // persistence too, so toggling SearchEnabled/SearchMode/Parallelism/Persistence/FlushMs on a
        // Running table takes effect immediately instead of only on the next manual stop/start.
        if ((sqlChanged || searchChanged || parallelismChanged || persistenceChanged || retentionChanged || shardByChanged || replayFromChanged) && wasRunning)
        {
            var tableGrain = GrainFactory.GetGrain<ITableGrain>(EnvKeys.Qualify(_env, def.Name));
            try
            {
                await tableGrain.StopAsync();
                await tableGrain.StartAsync(def);
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            catch (Exception ex)
            {
                def.Status = PipelineStatus.Failed;
                def.Error = ex.Message;
            }
        }

        // History config or the SQL it was derived from changed — the row-identity mapping and/or
        // retention policy is now stale, so reset (not resume) the history grain, exactly like a
        // SQL/search-config change restarts the table's own grain above.
        // `renamed` is in this list because the tiers are keyed by NAME: ReleaseRenamedTiersAsync above
        // tore down the OLD name's, so without this the renamed table would have no history tier at all
        // under its new name — history would silently stop working for it. Found by the test that asserts
        // the new name's tier is live, not by reading the code.
        if (renamed || sqlChanged || historyConfigChanged || shardByChanged)
        {
            await ResetHistoryTiersAsync(def);
        }
        else if (persistenceChanged && def.ShardBy.Count > 0)
        {
            // Plan 011 D1: same "re-read the write-behind policy without discarding what was accumulated"
            // distinction the history grain draws below — ResumeAsync re-derives the TableShardConfig
            // (which carries Persistence/FlushMs down to every shard on the next routed batch) and leaves
            // the existing shards on disk untouched.
            await GrainFactory.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(_env, def.Name)).ResumeAsync(def);
        }
        else if (persistenceChanged && def.HistoryEnabled)
        {
            // Plan 008 W2.5: a Persistence/FlushMs-only change doesn't invalidate the row-identity mapping
            // or retention policy, so use ResumeAsync (not ResetAsync) — it re-reads Persistence/FlushMs
            // from the updated definition and re-registers the flush timer with the new interval/mode, but (unlike
            // ResetAsync) deliberately leaves Entries/Seq untouched, so accumulated history survives a mode
            // tweak the same way it survives a silo restart.
            await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, def.Name)).ResumeAsync(def);
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Name, "table-updated", def.Status);
        return def;
    }

    public async Task<bool> DeleteTableAsync(string id)
    {
        var existing = state.State.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return false;
        }

        ThrowIfRunningDependents(existing.Name, "delete");

        if (existing.Status == PipelineStatus.Running)
        {
            try
            {
                await GrainFactory.GetGrain<ITableGrain>(EnvKeys.Qualify(_env, existing.Name)).StopAsync();
            }
            catch
            {
                // best-effort
            }
        }

        try
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, existing.Name)).DisableAsync();
        }
        catch
        {
            // best-effort
        }

        // Plan 011 D1: unsubscribe the router AND delete every shard's persisted state. Unlike every other
        // teardown here this one genuinely activates each shard once — clearing a grain's persisted state
        // portably means asking the grain to do it — which is the correct trade for an explicit delete and
        // exactly the wrong one for a read (see TableShardRouterGrain.PurgeShardsAsync).
        try
        {
            await GrainFactory.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(_env, existing.Name)).DisableAsync();
        }
        catch
        {
            // best-effort
        }

        state.State.Tables.Remove(existing);
        RecomputeStaleReasons(); // a deleted table breaks every pin that named it.
        await state.WriteStateAsync();
        await PublishLifecycleAsync(existing.Name, "table-deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.State.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return null;
        }

        var grain = GrainFactory.GetGrain<ITableGrain>(EnvKeys.Qualify(_env, existing.Name));
        if (status == PipelineStatus.Running)
        {
            var missing = existing.TableInputs
                .Where(name => state.State.Tables.FirstOrDefault(t => t.Name == name)?.Status != PipelineStatus.Running)
                .ToList();

            if (missing.Count > 0)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = $"table input(s) not running: {string.Join(", ", missing)}";
                existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await state.WriteStateAsync();
                await PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
                return existing;
            }

            try
            {
                await grain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await PublishLifecycleAsync(existing.Name, "table-started", existing.Status);
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
                await PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
            }
        }
        else
        {
            ThrowIfRunningDependents(existing.Name, "stop");

            try
            {
                await grain.StopAsync();
            }
            catch
            {
                // best-effort
            }

            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await PublishLifecycleAsync(existing.Name, "table-stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await state.WriteStateAsync();
        return existing;
    }

    /// <summary>Plan 021 D3/write-path guard — a name containing <see cref="EnvKeys.Separator"/> can never
    /// be safely qualified into a runtime key (<see cref="EnvKeys.IsQualifiableEntityName"/> is the
    /// predicate; see its own doc comment and <see cref="EnvKeys"/>'s class doc for why '.' was chosen and
    /// why a dotted name is already unusable in SQL for an unrelated reason). Refused going forward only —
    /// the same shape <see cref="ValidateUniquePipelineName"/> gave pipeline-name uniqueness in plan 016:
    /// nothing scans the catalog at boot, and an existing entity with a dotted name (there are none in the
    /// seeds) keeps working until the next create/rename that would introduce or keep one.</summary>
    private static void ValidateQualifiableName(string kind, string name)
    {
        if (!EnvKeys.IsQualifiableEntityName(name))
        {
            throw new InvalidOperationException(
                $"{kind} name '{name}' cannot contain '{EnvKeys.Separator}' — that character qualifies a " +
                "runtime key by environment, so a name containing it could never be safely addressed once qualified.");
        }
    }

    private void ValidateUniqueTableName(string name, string? excludeTableId)
    {
        if (state.State.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by a stream source");
        }
        // Table-over-pipeline — the third leg of the relation-name uniqueness triangle; see
        // ValidateUniquePipelineName for the argument.
        if (state.State.Pipelines.Any(p => string.Equals(p.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");
        }
        if (state.State.Tables.Any(t => t.Id != excludeTableId && string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another table");
        }
    }

    /// <summary>Plan 003 M2: 409-style guard (same pattern as ValidateUniqueTableName) — Parallelism must be
    /// in [1, 16] (see TableDefinition.Parallelism's doc comment: 1 = classic path, 2..16 deploys the
    /// partitioned dataflow graph; the upper bound documents the grain-explosion ceiling from plan
    /// 003's risk section).</summary>
    private static void ValidateParallelism(int parallelism)
    {
        if (parallelism is < 1 or > 16)
        {
            throw new InvalidOperationException($"Parallelism must be between 1 and 16 (got {parallelism}).");
        }
    }

    /// <summary>Plan 008 W2.5: 409-style guard (same pattern as ValidateParallelism) — TableDefinition.FlushMs
    /// (see its own doc comment: 0 = the 2000ms default; ignored for MemoryOnly) must be non-negative. A
    /// negative interval has no sane meaning for either Orleans' RegisterGrainTimer (which throws its own,
    /// less caller-friendly ArgumentOutOfRangeException) or the "0 = default" convention.</summary>
    private static void ValidateFlushMs(int flushMs)
    {
        if (flushMs < 0)
        {
            throw new InvalidOperationException($"FlushMs must be >= 0 (got {flushMs}).");
        }
    }

    /// <summary>Plan 011 C2: 409-style guard (same pattern as <see cref="ValidateParallelism"/>) for the
    /// opt-in row retention policy. Everything here is a refusal to accept a policy the runtime could not
    /// actually honor, because the failure mode of accepting one is the worst kind: metrics and the console
    /// would report a bounded table while the structures that hold the memory kept growing.
    ///
    ///  * Negative bounds have no meaning (0 is already "off").
    ///  * Parallelism &gt;= 2 runs the partitioned dataflow, where the rows live in the stage grains rather
    ///    than in this table's own executor — a policy installed on the coordinator's scratch executor would
    ///    evict nothing at all. Retention is Parallelism == 1 only, and says so.
    ///  * A plan shape the Engine cannot reclaim state for (joins, set operations, derived sources, GROUP
    ///    BY/aggregates) is refused with the Engine's own reasoning — see TablePlan.SupportsRetention.
    ///
    /// Draft-friendly in exactly the way <see cref="ValidateHistoryConfig"/> is: SQL that does not compile
    /// is not rejected here (the table is saved as a draft with diagnostics, as always) — the shape check
    /// simply has nothing to check yet and re-runs on the next update that does compile.</summary>
    /// <summary>Plan 026 D5: every key of <c>ReplayFrom</c> must name one of this entity's compiled STREAM
    /// or PIPELINE inputs — the set <paramref name="streamInputs"/> carries (a table's
    /// <c>TableCompileResult.StreamInputs</c> already mixes sources and pipelines; a pipeline's is its
    /// <c>SourceNames</c>). Three refusals, each with its reason:
    ///  * a TABLE input — those rows arrive through plan 023's backfill snapshot, which has no position;
    ///  * a name that is no input of this entity at all, answered WITH the valid names, because the
    ///    commonest mistake is a pipeline's ID where its NAME belongs (the dictionary is keyed by name);
    ///  * a <c>crdt</c>-kind source — a CRDT document keeps its own replay (plan 020's <c>ReplayAsync</c>)
    ///    and is not in any <c>ReplayLog</c>, so a position on it could only ever be silently ignored.
    ///
    /// Draft-friendly exactly like <see cref="ValidateRetention"/>/<see cref="ValidateShardBy"/>: SQL that
    /// does not compile has no input set to check against, so this re-runs on the next update that does.
    /// An empty dictionary — the default — costs one <c>Count</c>.</summary>
    private void ValidateReplayFrom(
        string entityKind, IReadOnlyDictionary<string, ReplayFrom> replayFrom, bool compiled,
        IReadOnlyList<string> streamInputs, IReadOnlyList<string> tableInputs)
    {
        if (replayFrom.Count == 0 || !compiled)
        {
            return;
        }

        foreach (var input in replayFrom.Keys)
        {
            if (tableInputs.Contains(input, StringComparer.Ordinal))
            {
                throw new InvalidOperationException(
                    $"replayFrom names table input '{input}': a table input replays through the backfill snapshot; a position on a table input is wave 3 work.");
            }

            if (!streamInputs.Contains(input, StringComparer.Ordinal))
            {
                var valid = streamInputs.Count == 0 ? "(none)" : string.Join(", ", streamInputs);
                throw new InvalidOperationException(
                    $"replayFrom names '{input}', which is not an input of this {entityKind}. Valid inputs: {valid}.");
            }

            var source = state.State.Sources.FirstOrDefault(s => string.Equals(s.Name, input, StringComparison.Ordinal));
            if (source is not null && SourceKindDispatch.Classify(source.Kind) == SourceKindDispatch.ActorKind.Crdt)
            {
                throw new InvalidOperationException(
                    $"replayFrom names crdt source '{input}': crdt sources replay through their own ReplayAsync.");
            }
        }
    }

    /// <summary>Value equality for a <c>ReplayFrom</c> map — key set plus each entry's Seq/TimestampMs
    /// (<see cref="ReplayFrom"/> is a record, so <c>==</c> is already by value). Used to decide whether an
    /// update changed an executor-affecting field; reference equality would call every update a change.</summary>
    private static bool ReplayFromEquals(IReadOnlyDictionary<string, ReplayFrom> a, IReadOnlyDictionary<string, ReplayFrom> b) =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var other) && kv.Value == other);

    private static void ValidateRetention(TableDefinition def, TableCompileResult compileResult)
    {
        if (def.RetentionMaxRows < 0 || def.RetentionTtlMs < 0)
        {
            throw new InvalidOperationException(
                $"Retention bounds must be >= 0 (got maxRows={def.RetentionMaxRows}, ttlMs={def.RetentionTtlMs}); 0 means unbounded.");
        }

        if (def.RetentionMaxRows == 0 && def.RetentionTtlMs == 0)
        {
            return; // retention off — the default, and nothing further to check
        }

        if (def.Parallelism > 1)
        {
            throw new InvalidOperationException(
                $"Row retention requires Parallelism = 1 (got {def.Parallelism}) — a partitioned table's rows live in its stage grains, so the policy could not reclaim them.");
        }

        if (!compileResult.Ok || compileResult.Plan is null)
        {
            return; // draft: nothing to validate the shape against yet
        }

        if (!compileResult.Plan.SupportsRetention)
        {
            throw new InvalidOperationException(
                "Row retention is not supported for this table's SQL: joins, set operations, derived sources and GROUP BY/aggregates are excluded, because evicting an output row would leave their per-key state (join indexes, aggregate accumulators) growing — or, for aggregates, would restart the group from zero and emit a wrong value.");
        }
    }

    /// <summary>Plan 011 D1: 409-style guard (same shape as <see cref="ValidateParallelism"/>) for opt-in
    /// KEY SHARDING. Empty <c>ShardBy</c> — the default — returns immediately and nothing below applies, so
    /// an existing table is untouched by the feature's existence.
    ///
    ///  * Blank or duplicate column names are refused: a duplicate would silently widen the key encoding
    ///    for no effect, and a blank one would encode a column that cannot exist.
    ///  * <see cref="TableDefinition.SearchEnabled"/> + ShardBy is refused OUTRIGHT rather than
    ///    half-served. The reverse index is table-wide and row-keyed (see TableSearchIndex — five maps, a
    ///    4-5x multiplier on whatever the table holds); keeping it alongside a shard tier would keep every
    ///    row resident and defeat the entire point, while a per-shard index would answer a table-wide
    ///    query by waking every shard, which is worse. Refusing loudly is the honest option, matching wave
    ///    C2's refusal of retention on plan shapes it could not honor.
    ///  * Every ShardBy column must be one of the table's COMPILED output columns. This is the one place
    ///    the explicit-columns rule is enforced, and it is enforced against the compiler's own output
    ///    rather than against the SQL text: the shard key decides which grain owns a row, so a column that
    ///    silently does not exist would put every row under the same "missing value" key. Draft-friendly
    ///    in exactly the way <see cref="ValidateHistoryConfig"/> and <see cref="ValidateRetention"/> are —
    ///    SQL that does not compile is saved as a draft with diagnostics, and this check re-runs on the
    ///    next update that does compile.
    ///
    /// NOT restricted by Parallelism, deliberately, unlike retention: the shard tier consumes the table's
    /// delta stream, and TableOutputGrain republishes onto that same stream for Parallelism &gt;= 2, so a
    /// shard consumer is identical in both modes.</summary>
    private static void ValidateShardBy(TableDefinition def, TableCompileResult compileResult)
    {
        if (def.ShardBy.Count == 0)
        {
            return; // not sharded — the default, and nothing below applies
        }

        if (def.ShardBy.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException("shardBy column names must be non-empty.");
        }

        var duplicates = def.ShardBy.GroupBy(c => c, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException($"shardBy has duplicate column(s): {string.Join(", ", duplicates)}.");
        }

        if (def.SearchEnabled)
        {
            throw new InvalidOperationException(
                "searchEnabled cannot be combined with shardBy: the reverse index is table-wide and row-keyed, so it would keep every row resident and defeat the point of sharding. Turn search off, or shard off.");
        }

        // Plan 011 D2 — MemoryOnly + ShardBy is now REFUSED, where D1 honored it literally and documented
        // the consequence. The reason it had to change is that D2 is what made it dangerous. A shard's
        // final write on deactivation is not a durability nicety, it IS the swap-out; a mode that never
        // writes turns every idle deactivation into silent data loss for that key, and D1 could still say
        // "the table's own snapshot has the rows" because TableGrain kept a full persisted mirror. D2
        // deliberately stops keeping it (see TableGrain's sharded-tables paragraph), so on a sharded
        // MemoryOnly table there would be nothing behind the shards at all. Refusing beats redefining
        // MemoryOnly's contract underneath the user: the mode promises "a RESTART brings the table back
        // empty", not "an idle minute loses this key".
        if (def.Persistence == TablePersistenceMode.MemoryOnly)
        {
            throw new InvalidOperationException(
                "persistence 'MemoryOnly' cannot be combined with shardBy: a shard's write on deactivation IS how its state is swapped out, so a shard that never writes would lose an idle key's rows and history rather than saving them. Pick Batched, FireAndForget or Journaled, or shard off.");
        }

        if (!compileResult.Ok || compileResult.OutputSchema is null)
        {
            return; // draft: no compiled output columns to validate against yet
        }

        var missing = def.ShardBy.Where(c => !compileResult.OutputSchema.Fields.ContainsKey(c)).ToList();
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"shardBy column(s) {string.Join(", ", missing)} are not among this table's output fields ({string.Join(", ", compileResult.OutputSchema.Fields.Keys)}).");
        }
    }

    /// <summary>Plan 011 D2 — REFUSES TO RENAME A SHARDED TABLE, and this deserves the paragraph because
    /// the alternative fix is better and was not taken.
    ///
    /// THE HAZARD. The whole shard tier is keyed by the table's NAME: the router grain, the directory
    /// grain, and every shard grain (<c>TableShardKeys.GrainKey(tableName, shardKey)</c>). A rename
    /// therefore does not move anything — it makes the table address a fresh, empty tier while every
    /// existing shard keeps its rows and its full version trail on disk under a key nothing will ever look
    /// up again. Nothing throws, nothing logs, and the table simply appears to have lost its per-key
    /// history. A silent loss is the worst shape this could take, which is why it is refused rather than
    /// documented.
    ///
    /// THE BETTER FIX, and why it is not here. Keying the tier on <see cref="TableDefinition.Id"/> — which
    /// is immutable — costs nothing while the feature is unreleased and removes the hazard outright rather
    /// than fencing it off. It was implemented and then backed out for one concrete, non-technical reason:
    /// wave D1's own cluster tests address the tier by name in a dozen places, and this wave's brief
    /// forbids modifying a pre-existing test file. The change is mechanical (grain keys + those call
    /// sites) and is the right thing to do the moment that call is made; until then the rename is refused,
    /// which closes the hole completely, at the cost of one restriction.
    ///
    /// THE WAY AROUND IT for a user who genuinely wants to rename: clear <c>shardBy</c> first, which
    /// deletes the shards explicitly and visibly, then rename, then shard again. The tier rebuilds from
    /// live traffic exactly as it does when sharding is first switched on.
    ///
    /// NOTE the same exposure exists, pre-existing and unchanged, for <c>TableHistoryGrain</c> and for
    /// <c>TableGrain</c> itself, both also keyed by name. Neither is in this wave's scope; this guard
    /// deliberately covers only what this wave is responsible for, rather than quietly widening to a
    /// restriction nobody asked for.</summary>
    private static void ValidateShardedTableIsNotRenamed(TableDefinition existing)
    {
        if (existing.ShardBy.Count == 0)
        {
            return;
        }

        throw new InvalidOperationException(
            $"'{existing.Name}' is sharded by {string.Join(", ", existing.ShardBy)} and cannot be renamed: its shard grains are keyed by the table name, so a rename would strand every shard's rows and version trails under a key nothing looks up again. Clear shardBy first (which deletes the shards), rename, then shard again.");
    }

    /// <summary>Plan 016 wave 1-C — the whole rename policy for tables, in one place: allowed IFF the
    /// table is <b>Stopped</b>, IFF <c>ShardBy</c> is empty, and IFF no other table lists it in
    /// <c>TableInputs</c>. Everything else is refused with the reason.
    ///
    /// <para><b>Why three conditions rather than a flat prohibition.</b> A table rename is already
    /// semantically broken well beyond the grain keys the sharded guard above talks about:
    /// <c>TableInputs</c> holds NAMES, a dependent's SQL says <c>FROM oldname</c>, and nothing recompiles
    /// it — so the dependent keeps running against a name that no longer resolves and fails only at its
    /// next start. But the 95% case is fixing a typo on a table created a minute ago, which none of that
    /// applies to. The three conditions are exactly the set that separates the two: Stopped means no live
    /// grain is mid-flight under the old key, empty ShardBy means there is no name-keyed shard tier to
    /// strand (see <see cref="ValidateShardedTableIsNotRenamed"/> for that hazard in full), and no
    /// dependents means no other table's SQL or <c>TableInputs</c> is about to go stale.</para>
    ///
    /// <para><b>Dependents are checked at ANY status</b>, unlike <see cref="ThrowIfRunningDependents"/>
    /// which only guards against Running ones. A stopped dependent still holds the old name in its SQL
    /// text and in its persisted <c>TableInputs</c>; renaming out from under it would turn a table that
    /// merely is not running into one that can never start again, which is worse than the refusal.</para>
    ///
    /// <para><b>Re-keying the tier on <see cref="TableDefinition.Id"/> stays the right eventual fix</b>
    /// and is deliberately NOT done here — see <see cref="ValidateShardedTableIsNotRenamed"/>'s "THE
    /// BETTER FIX" paragraph. Its blocker is unchanged and is now written down as a condition rather than
    /// a mood: it is unblocked the moment somebody is willing to edit
    /// <c>ShardedTableClusterTests</c> and <c>ShardedTableD2ClusterTests</c>, which address the tier by
    /// name in a dozen places. Nothing else stands in the way.</para></summary>
    private void ValidateTableRenameAllowed(TableDefinition existing)
    {
        if (existing.Status != PipelineStatus.Stopped)
        {
            throw new InvalidOperationException(
                $"'{existing.Name}' is {existing.Status} and cannot be renamed: its grain, history and delta stream are all keyed by the table name, so the rename would leave the running copy addressed under the old one. Stop it first, rename, then start it again.");
        }

        ValidateShardedTableIsNotRenamed(existing);

        var dependents = state.State.Tables
            .Where(t => t.Id != existing.Id && t.TableInputs.Contains(existing.Name, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException(
                $"'{existing.Name}' cannot be renamed: table(s) {string.Join(", ", dependents)} read from it by name and nothing rewrites their SQL. Update them first, or rename this table before anything depends on it.");
        }
    }

    /// <summary>Plan 016 wave 1-C — the other half of the rename policy, and the half that makes it safe:
    /// the OLD name's tiers are torn down explicitly before the new name is persisted. Without this the
    /// rename would leave a history grain (and, for a table that was once sharded, a router) holding state
    /// under a key the catalog no longer mentions — invisible until somebody creates a NEW table with the
    /// old name and inherits its predecessor's version trails.
    ///
    /// <para>Same two calls <see cref="DeleteTableAsync"/> makes, for the same reason: as far as the
    /// old name is concerned, a rename IS a delete. Both are best-effort — a tier that fails to tear down
    /// must not roll back a rename the caller has already been told is legal.</para>
    ///
    /// <para><b>ponytail: TableGrain's OWN persisted snapshot under the old name is NOT cleared here.</b>
    /// Ceiling: after a rename, a brand-new table later created with the old name and READ BEFORE IT IS
    /// STARTED would serve the previous table's last flushed rows (starting it clears them —
    /// <c>ClearResumeMarkersAndDetect</c> resets the snapshot on every StartAsync). Reason it is not
    /// closed here: <c>ITableGrain</c> has no clear/purge method, and adding one is a change to
    /// <c>StreamsForge.Abstractions</c>, which this wave does not own. The exposure is not new — it is
    /// exactly what delete-then-recreate-with-the-same-name has always done. Upgrade path: an additive
    /// <c>ITableGrain.ClearSnapshotAsync</c> (and its Dapr <c>ITableActor</c> twin) called from here and
    /// from <see cref="DeleteTableAsync"/>, which would close both at once.</para></summary>
    private async Task ReleaseRenamedTiersAsync(string oldName)
    {
        try
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(_env, oldName)).DisableAsync();
        }
        catch
        {
            // best-effort
        }

        try
        {
            await GrainFactory.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(_env, oldName)).DisableAsync();
        }
        catch
        {
            // best-effort
        }
    }

    /// <summary>Plan 016 wave 1-C — pipeline names are unique among PIPELINES, and deliberately not
    /// against sources and tables. Pipelines are not in the SQL namespace: a pipeline named <c>trades</c>
    /// that reads <c>FROM trades</c> is legal today, is in the seed data, and stays legal. Sources and
    /// tables ARE in that namespace and keep their own cross-kind guard
    /// (<see cref="ValidateUniqueTableName"/>).
    ///
    /// <para><b>Why this is worth enforcing at all when a pipeline is addressed by id.</b> Every
    /// name-keyed read of a pipeline — config export/import, the entitlement scope
    /// (<c>PipelinesEndpoints</c> scopes on the NAME, not the id), the CLI, <c>/proto</c> — has to pick
    /// one of the duplicates, and until this guard the pick was silent. It also removes the state that
    /// made <c>ImportPlanner.Plan</c> throw, i.e. that made <c>POST /api/config/import</c> answer 500 on a
    /// catalog the platform itself had allowed.</para>
    ///
    /// <para><b>ponytail: no migration and no boot check.</b> Ceiling: a catalog written before this guard
    /// can still hold duplicates and nothing here notices until one of them is edited; they surface
    /// through <c>CatalogWarnings.Compute</c> (wave 5 hangs it off <c>GET /api/meta/instance</c>) and as a
    /// diagnostic on any import that plans against one. Refusing to boot on a catalog that was legal when
    /// it was written is the one outcome that must not happen.</para></summary>
    private void ValidateUniquePipelineName(string name, string? excludePipelineId)
    {
        if (state.State.Pipelines.Any(p => p.Id != excludePipelineId && string.Equals(p.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another pipeline");
        }

        // Table-over-pipeline: a pipeline name is now a RELATION name a table's SQL may write, so it has
        // to be unique against the other things a relation name can resolve to. Without this, `FROM foo`
        // in a table would name both a source and a pipeline, and which one it got would be decided by
        // dictionary-insertion order in PipelineInputs.BuildStreamSchemas — an executable catalog whose
        // meaning is an implementation detail. Refused going forward only, on the same terms as the
        // pipeline-vs-pipeline check above (no boot scan, no migration): a catalog that already holds
        // such a pair keeps working until one of the two is next written.
        if (state.State.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a stream source — a table's SQL resolves a relation name to exactly one entity");
        }
        if (state.State.Tables.Any(t => string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a table — a table's SQL resolves a relation name to exactly one entity");
        }
    }

    /// <summary>409-style guard: refuses to stop/delete a table that a currently-Running table depends on.</summary>
    private void ThrowIfRunningDependents(string tableName, string action)
    {
        var dependents = state.State.Tables
            .Where(t => t.Status == PipelineStatus.Running && t.TableInputs.Contains(tableName, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException($"Cannot {action} '{tableName}': running table(s) depend on it: {string.Join(", ", dependents)}");
        }
    }

    /// <summary>Compile-check for diagnostics — draft-friendly like pipelines: never blocks create/update
    /// on its own (see ValidateHistoryConfig for the one thing that DOES block: an invalid MinBy/MaxBy
    /// historyByField). Pure — does not mutate <paramref name="def"/>; pair with ApplyCompileResult.</summary>
    private TableCompileResult CompileTableSql(string sql, string? excludeTableId)
    {
        // Table-over-pipeline: a table's stream relations are sources PLUS every pipeline that has a
        // compiled output schema (a pipeline still reads sources only — CreatePipelineAsync/
        // UpdatePipelineAsync keep using the bare BuildStreamSchemas()). PipelineInputs.BuildStreamSchemas
        // is the SINGLE definition of that dictionary; TableGrain/TableIngestGrain/TableDataflowFactory
        // build theirs from the same method against GetSourcesAsync/GetPipelinesAsync, so what the grain
        // compiles at start is the plan this compiled at create.
        var streamSchemas = PipelineInputs.BuildStreamSchemas(state.State.Sources, state.State.Pipelines);
        var tableSchemas = BuildTableSchemas(excludeTableId);
        return SqlCompiler.CompileTable(sql, streamSchemas, tableSchemas);
    }

    /// <summary>Plan 008 W5: stores SourceNames from a pipeline compile result when it compiles; leaves it
    /// empty otherwise — the pipeline-side counterpart of <see cref="ApplyCompileResult"/> below (tables'
    /// StreamInputs/TableInputs). Draft-friendly like that one: called from Create/UpdatePipelineAsync,
    /// never blocks either on a compile failure, only decides what SourceNames holds afterward.
    ///
    /// <para>Table-over-pipeline also fills <see cref="PipelineDefinition.OutputFields"/> here, from the
    /// same <c>OutputSchema</c>/<c>MapFieldType</c> pair the table side has always used — a pipeline needs
    /// a published schema before a table can name it as a relation, and this is the only place a
    /// pipeline's compile result is ever stored. Cleared on a failed compile for the same reason the
    /// table side clears its own: a stale schema for SQL that no longer compiles is worse than none,
    /// because <see cref="PipelineInputs.BuildStreamSchemas"/> would keep offering it as a live
    /// relation.</para></summary>
    private static void ApplyPipelineCompileResult(PipelineDefinition def, CompileResult result)
    {
        def.SourceNames = result.Ok ? result.SourceNames.ToList() : [];
        def.OutputFields = result.Ok && result.OutputSchema is not null
            ? result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList()
            : [];
    }

    /// <summary>Every pipeline NAME in this environment's catalog — the key that splits a compiled
    /// <c>StreamInputs</c> list into source inputs and pipeline inputs. Names are unique across sources
    /// and pipelines by construction (see <see cref="ValidateUniquePipelineName"/> and
    /// <see cref="UpsertSourceAsync"/>'s guard), so membership here is decisive, not a guess.</summary>
    private HashSet<string> PipelineNameSet() =>
        state.State.Pipelines.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

    /// <summary>Stores OutputFields/StreamInputs/PipelineInputs/TableInputs from a compile result when it
    /// compiles; leaves them empty otherwise.
    ///
    /// <para>Table-over-pipeline: the engine returns ONE list of stream relations, because a relation is
    /// (name, schema) and it has no reason to care which catalog entity published the schema. Splitting
    /// that list by "is this name a pipeline?" is this method's new job, and it is why this stopped being
    /// static — the answer lives in this grain's own state. The split must be total: a name in neither
    /// set cannot occur (the compile would have rejected an unknown relation), and a name in both cannot
    /// occur (the create paths refuse it).</para></summary>
    private void ApplyCompileResult(TableDefinition def, TableCompileResult result)
    {
        if (result.Ok && result.OutputSchema is not null)
        {
            var pipelineNames = PipelineNameSet();
            def.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
            def.StreamInputs = result.StreamInputs.Where(n => !pipelineNames.Contains(n)).ToList();
            def.PipelineInputs = result.StreamInputs.Where(pipelineNames.Contains).ToList();
            def.TableInputs = result.TableInputs.ToList();
            def.KeyFields = TableKeyFields.Describe(def.Sql, result.Plan);
        }
        else
        {
            def.OutputFields = [];
            def.StreamInputs = [];
            def.PipelineInputs = [];
            def.TableInputs = [];
            def.KeyFields = null;
        }
    }

    /// <summary>The one history-config check that actually blocks create/update (409-style, like
    /// ValidateUniqueTableName): MinBy/MaxBy needs a historyByField that is one of this table's compiled
    /// output columns and numeric/timestamp-kinded — a MinBy/MaxBy history with no comparable value to
    /// rank on can never produce a sensible "extreme" version. A table with no GROUP BY identity (and thus
    /// no derivable per-row identity beyond the whole row) is explicitly NOT rejected here — see
    /// TableGroupKeyExtractor/RowKeyCodec for that documented whole-row fallback.</summary>
    private static void ValidateHistoryConfig(TableDefinition def, TableCompileResult compileResult)
    {
        if (!def.HistoryEnabled || def.HistoryMode is not (TableHistoryMode.MinBy or TableHistoryMode.MaxBy))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(def.HistoryByField))
        {
            throw new InvalidOperationException($"History mode {def.HistoryMode} requires historyByField to be set.");
        }

        if (compileResult.OutputSchema is null || !compileResult.OutputSchema.Fields.TryGetValue(def.HistoryByField, out var kind))
        {
            throw new InvalidOperationException($"historyByField '{def.HistoryByField}' is not one of this table's output fields.");
        }

        if (kind is not (FieldKind.Double or FieldKind.Long or FieldKind.Timestamp))
        {
            throw new InvalidOperationException($"historyByField '{def.HistoryByField}' must be numeric or timestamp (found {kind}).");
        }
    }

    private Dictionary<string, SourceSchema> BuildStreamSchemas() =>
        state.State.Sources.ToDictionary(s => s.Name, s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    private Dictionary<string, SourceSchema> BuildTableSchemas(string? excludeTableId) =>
        state.State.Tables
            .Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0)
            .ToDictionary(t => t.Name, t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    /// <summary>Kahn-style topological sort of `running` by TableInputs edges (inputs first) — used to
    /// resume table-over-table chains in dependency order on EnsureInitializedAsync.</summary>
    private static List<TableDefinition> TopoSortByTableInputs(List<TableDefinition> running, List<TableDefinition> all)
    {
        var byName = all.ToDictionary(t => t.Name, StringComparer.Ordinal);
        var runningSet = running.ToHashSet();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<TableDefinition>();

        void Visit(TableDefinition t, HashSet<string> stack)
        {
            if (visited.Contains(t.Name) || !stack.Add(t.Name)) return;
            foreach (var dep in t.TableInputs)
            {
                if (byName.TryGetValue(dep, out var depDef) && runningSet.Contains(depDef))
                {
                    Visit(depDef, stack);
                }
            }
            visited.Add(t.Name);
            result.Add(t);
        }

        foreach (var t in running)
        {
            Visit(t, new HashSet<string>(StringComparer.Ordinal));
        }
        return result;
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

    private static FieldType MapFieldType(FieldKind kind) => kind switch
    {
        FieldKind.String => FieldType.String,
        FieldKind.Double => FieldType.Double,
        FieldKind.Long => FieldType.Long,
        FieldKind.Bool => FieldType.Bool,
        FieldKind.Timestamp => FieldType.Timestamp,
        FieldKind.Json => FieldType.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown field kind"),
    };

    // Demo sources/pipelines/tables are seeded from shared StreamsForge.AppCore.SeedCatalog (plan 005
    // W2) — see EnsureInitializedAsync above — so both the Orleans and Dapr flavors seed the same demo
    // world from one place.

    public async Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields)
    {
        var (json, changed) = ComputeFieldNumbers(entityKey, fields);
        if (changed)
        {
            await state.WriteStateAsync();
        }

        return json;
    }

    /// <summary>Plan 016 wave 2 split the persist out of <see cref="EnsureFieldNumbersAsync"/> so the
    /// write path can refresh several entities' numbers inside one turn and pay for one
    /// <c>WriteStateAsync</c> at the end, instead of one per entity. Behaviour-identical for the public
    /// method above — it still writes iff the serialized map changed.</summary>
    private (string Json, bool Changed) ComputeFieldNumbers(string entityKey, List<FieldDef> fields)
    {
        var existingJson = state.State.FieldNumberMaps.GetValueOrDefault(entityKey);
        var existing = existingJson is null
            ? null
            : System.Text.Json.JsonSerializer.Deserialize<FieldNumberMap>(existingJson);
        var updatedJson = System.Text.Json.JsonSerializer.Serialize(FieldNumberMap.Assign(fields, existing));
        if (updatedJson == existingJson)
        {
            return (updatedJson, false);
        }

        state.State.FieldNumberMaps[entityKey] = updatedJson;
        return (updatedJson, true);
    }

    /// <summary>
    /// Plan 016 wave 2 — <b>the highest-value line in the wave</b>: when an upstream schema moves,
    /// recompile the tables that read from it and REFRESH THEIR PERSISTED <c>OutputFields</c> and field
    /// numbers, so <c>GET /api/tables/{id}/proto</c> and <c>/api/meta/grpc</c> stop handing generated
    /// clients a schema the table no longer produces. Before this, editing a source's fields left every
    /// dependent table's stored <c>OutputFields</c> frozen at whatever the last create/update compiled —
    /// the table went on producing the NEW shape at runtime while its published contract described the
    /// old one, which is the worst combination available (a typed client compiles, connects, and
    /// misreads).
    ///
    /// <para><b>Pure Engine work, no grain calls, so no reentrancy hazard.</b> <c>RegistryGrain</c> is
    /// non-reentrant with a <c>[MayInterleave]</c> allowlist and any orchestrator↔worker call cycle
    /// deadlocks; this method calls <c>SqlCompiler</c> and touches this grain's own state, and nothing
    /// else. That is a deliberate constraint on what it is allowed to do, not an accident of the current
    /// implementation.</para>
    ///
    /// <para><b>Dependents are NOT restarted, and that is the decision, not an omission.</b> The
    /// restart-on-change machinery in <c>UpdateTableAsync</c> is for SELF edits — the caller asked for
    /// this table to change and is waiting for the answer. Cascading a restart down an upstream edit
    /// would stop and start tables the caller never mentioned, on a request whose failure mode would then
    /// be "my unrelated table went Failed because somebody added a column three hops upstream". A
    /// dependent keeps running on its compiled plan (which is exactly what it does today) and, when it
    /// carries a pin that no longer holds, gains a <c>StaleReason</c> — visible instead of silent.</para>
    ///
    /// <para><b>A table whose SQL no longer compiles is left completely alone</b> — persisted fields,
    /// status and all. "Keep them running either way" means the refresh may only ever improve what is
    /// stored; wiping <c>OutputFields</c> to empty because an unrelated edit made a query invalid would
    /// take a live table's <c>/proto</c> from stale to absent.</para>
    ///
    /// <para>Topo order matters: a table's own compile reads its input tables' <c>OutputFields</c>
    /// (<see cref="BuildTableSchemas"/>), so refreshing upstream first is what lets a two-hop chain
    /// converge in one pass rather than one hop per source edit.</para>
    ///
    /// <para><b>ponytail: it sweeps every table, not the dependency closure of what changed.</b>
    /// Ceiling: O(tables) compiles per source/table schema change — irrelevant at catalog scale (tens),
    /// and it only runs when a schema ACTUALLY moved, not on every edit. Upgrade path: seed the sweep
    /// from the changed name and walk <c>StreamInputs</c>/<c>TableInputs</c> forward. Not done because
    /// the closure walk is more code than the thing it optimises and would need its own correctness
    /// argument, while the sweep's is "recompiling a table that did not change produces the same
    /// schema".</para>
    /// </summary>
    private void RefreshTableSchemas()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var table in TopoSortByTableInputs([.. state.State.Tables], state.State.Tables))
        {
            var result = CompileTableSql(table.Sql, excludeTableId: table.Id);
            if (!result.Ok || result.OutputSchema is null)
            {
                continue; // draft or broken: keep what is stored, keep it running.
            }

            var before = table.OutputFields;
            ApplyCompileResult(table, result);
            if (!SchemaCompatibility.ShapeChanged(before, table.OutputFields))
            {
                continue;
            }

            table.SchemaRevision++;
            table.UpdatedAtMs = now;
            ComputeFieldNumbers(EntitySchemas.TableKey(table.Id), table.OutputFields);
        }
    }

    /// <summary>Plan 016 wave 2 — <see cref="PipelineDefinition.StaleReason"/>/
    /// <see cref="TableDefinition.StaleReason"/> is set BY THE UPSTREAM CHANGE, at the moment the break
    /// happens, and cleared the moment the pins are satisfied again. Recomputed wholesale rather than
    /// incrementally because "which entities does this change affect" is exactly as expensive to answer
    /// as re-evaluating every pin, and there is only ever a handful of pinned entities (the loop early-
    /// outs on an empty <c>DependsOn</c>, which is the default).</summary>
    private void RecomputeStaleReasons()
    {
        foreach (var t in state.State.Tables)
        {
            t.StaleReason = CatalogRevisions.EvaluatePins(t.DependsOn, state.State.Sources, state.State.Tables);
        }

        foreach (var p in state.State.Pipelines)
        {
            p.StaleReason = CatalogRevisions.EvaluatePins(p.DependsOn, state.State.Sources, state.State.Tables);
        }
    }

    /// <summary>Plan 021 D6 — one lifecycle stream PER ENVIRONMENT: <c>EnvKeys.Qualify(_env, ...)</c> keeps
    /// this grain's own events on this grain's own environment's stream, so a <c>staging</c> deploy does
    /// not wake every <c>default</c>/<c>prod</c> subscriber. On the default environment this is
    /// <c>EnvKeys.Qualify("", "events") == "events"</c> — byte-identical to the pre-plan-021 stream id.
    /// See <c>StreamBridgeService</c> for the subscriber half; a half-changed lifecycle stream is silent,
    /// not loud, which is why both halves are called out explicitly in the plan.</summary>
    private async Task PublishLifecycleAsync(string pipelineId, string kind, PipelineStatus status)
    {
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<LifecycleEvent>(StreamId.Create(StreamConstants.LifecycleNamespace, EnvKeys.Qualify(_env, StreamConstants.LifecycleEventsKey)));
        await stream.OnNextAsync(new LifecycleEvent
        {
            PipelineId = pipelineId,
            Kind = kind,
            Status = status,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }
}
