using Orleans;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Engine;
using StreamsForge.Engine.Dataflow;
using StreamsForge.Host.Auth;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Facades;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W3: Orleans-side implementations of the runtime-neutral facade
// interfaces (StreamsForge.Abstractions/Facades.cs) that shared/StreamsForge.Api's endpoints depend on.
//
//   - ICatalogFacade / IUserStoreFacade: IRegistryGrain/IUserStoreGrain already inherit these
//     interfaces (see GrainInterfaces.cs), so a real grain reference IS-A facade with zero adapter
//     code — registered as singletons resolving the grain proxy once.
//   - IPipelineReadFacade / ITableReadFacade / ITableHistoryFacade: keyed read surfaces: the grain's
//     implicit key becomes an explicit first parameter, so these need tiny per-call adapter classes
//     that resolve IClusterClient.GetGrain<T>(key) on every call. Deliberately dumb — all
//     id/name-resolution logic (e.g. mapping a table's REST {id} to its grain-key Name) already lives
//     in the shared endpoints (TablesEndpoints/PipelinesEndpoints), which call these adapters with the
//     already-resolved key.
//   - IArrangementMetaFacade: backs GET /api/meta/arrangements. Partitioned execution (and shared
//     arrangements) is Orleans-only (decision D-F), so this is the one facade whose entire body is
//     Orleans-specific (TableDataflowFactory + IArrangementGrain) — moved here verbatim from the old
//     MetaEndpoints.cs's /arrangements handler.
// ============================================================================

public static class OrleansFacadesExtensions
{
    public static IServiceCollection AddOrleansFacades(this IServiceCollection services)
    {
        // Plan 021 D1/D4 — MUST be per-use, not a singleton bound at container-build time: a singleton
        // captured "catalog" (the default environment's key) forever, before any request's environment
        // was known. AddTransient with a factory reading EnvironmentAmbient.Current re-resolves the
        // environment on every injection, which is what a minimal-API handler parameter (resolved once
        // per request) and every other consumer of this facade needs. Verified (this wave): nothing on the
        // Orleans side registers a component that injects ICatalogFacade as a SINGLETON — the one
        // constructor-injection site outside this file, IngestGrpcService, is a gRPC service class
        // instantiated per-call by the ASP.NET Core gRPC framework, not registered via AddSingleton, so a
        // Transient dependency resolved into its constructor is still fresh per call.
        services.AddTransient<ICatalogFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().RegistryFor(EnvironmentAmbient.Current));
        // Plan 021 — the environment directory itself IS a fixed-key singleton (like Access/Approvals
        // below), unlike ICatalogFacade above: IEnvironmentRegistryGrain inherits IEnvironmentFacade, so
        // the grain reference IS-A facade with zero adapter code, and its own key
        // (StreamConstants.EnvironmentsKey) never depends on which environment a request selected — it is
        // the thing that answers "which environments exist" in the first place.
        services.AddSingleton<IEnvironmentFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey));
        services.AddSingleton<IUserStoreFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IUserStoreGrain>(StreamConstants.UsersKey));
        // Plan 015 W1: the access-policy singleton. Same zero-adapter shape as the two above —
        // IAccessPolicyGrain inherits IAccessPolicyFacade, so the grain reference IS-A facade. Needs no
        // Program.cs edit: Orleans discovers the grain CLASS by assembly scanning, and this method is
        // already called from there.
        services.AddSingleton<IAccessPolicyFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IAccessPolicyGrain>(StreamConstants.AccessKey));
        // Plan 015 W4-B: approvals, same zero-adapter shape again — IApprovalGrain inherits
        // IApprovalFacade and the store is a singleton, so the grain reference IS-A facade.
        services.AddSingleton<IApprovalFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IApprovalGrain>(StreamConstants.ApprovalsKey));
        // Audit is the one that CANNOT be a grain reference: it is day-sharded, so which grain answers
        // depends on the entry's timestamp (append) or the caller's day (query), and GetDaysAsync must
        // touch no day grain at all. OrleansAuditFacade holds that routing rule, and holds all of it.
        services.AddSingleton<IAuditFacade, OrleansAuditFacade>();
        services.AddSingleton<IPipelineReadFacade, OrleansPipelineReadFacade>();
        services.AddSingleton<ITableReadFacade, OrleansTableReadFacade>();
        services.AddSingleton<ITableHistoryFacade, OrleansTableHistoryFacade>();
        services.AddSingleton<ITableShardFacade, OrleansTableShardFacade>();
        services.AddSingleton<IArrangementMetaFacade, OrleansArrangementMetaFacade>();
        services.AddSingleton<IConnectorStatusFacade, OrleansConnectorStatusFacade>();
        // Plan 020 wave B-2 / CRDT-as-a-plugin: the real Orleans CRDT facade now lives in the
        // StreamsForge.Plugins.Crdt plugin (OrleansCrdtFacade, always Enabled — Orleans is the
        // CRDT-capable flavour, D9), not here. This registers the same DISABLED default the Dapr flavor
        // uses (shared StreamsForge.Api.Facades.DisabledCrdtFacade) so a host with the plugin absent still
        // boots and answers /api/crdt/... with 501, exactly like Dapr. When the crdt plugin loads,
        // CrdtPlugin.ConfigureServices registers OrleansCrdtFacade AFTER this call, in Program.cs's plugin
        // hook — "last registration wins" for singleton resolution, so the real facade wins whenever the
        // plugin is present.
        services.AddSingleton<ICrdtFacade, StreamsForge.Api.Facades.DisabledCrdtFacade>();
        // Plan 008 W4: client-push ingress. SourceIngressRegistry is the host-process singleton buffer
        // registry (one SourceIngressBuffer per ingest-kind source); OrleansIngressFacade is the thin
        // Orleans-side adapter IIngressFacade callers (SourcesEndpoints, IngestGrpcService) depend on.
        services.AddSingleton<SourceIngressRegistry>();
        // Plan 009 A1: idempotency cache + per-source push-key usage tracker (both host-process
        // singletons, same lifetime as SourceIngressRegistry) and the "last reported to the stats
        // grain" baseline tracker IngestDrainPumpService/OrleansIngressFacade share — see each class's
        // own doc comment.
        services.AddSingleton<IngestIdempotencyCache>();
        services.AddSingleton<IngestKeyUsageTracker>();
        services.AddSingleton<IngressStatsReportTracker>();
        services.AddSingleton<IIngressFacade, OrleansIngressFacade>();
        return services;
    }
}

// Plan 021 D3/D4 — every method below answers ONE REST request for a bare NAME/id the shared endpoints
// already resolved (TablesEndpoints/PipelinesEndpoints resolve {id-or-name} against the ambient-qualified
// ICatalogFacade and pass this facade the entity's bare Name/Id) — so it is exactly the "facade answering a
// request" case: qualify with EnvironmentAmbient.Current, never with anything read off an entity.
internal sealed class OrleansPipelineReadFacade(IClusterClient client) : IPipelineReadFacade
{
    public Task<List<ResultEnvelope>> GetRecentResultsAsync(string pipelineId, int limit) =>
        client.GetGrain<IPipelineGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, pipelineId)).GetRecentResultsAsync(limit);

    public Task<PipelineMetrics> GetMetricsAsync(string pipelineId) =>
        client.GetGrain<IPipelineGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, pipelineId)).GetMetricsAsync();
}

internal sealed class OrleansTableReadFacade(IClusterClient client) : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetRowsAsync(limit, offset);

    public Task<int> GetRowCountAsync(string tableName) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetRowCountAsync();

    public Task<long> GetSeqAsync(string tableName) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetSeqAsync();

    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetSnapshotFrontierEpochAsync();

    public Task<TableMetrics> GetMetricsAsync(string tableName) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetMetricsAsync();

    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) =>
        client.GetGrain<ITableGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).SearchAsync(query, limit);
}

internal sealed class OrleansTableHistoryFacade(IClusterClient client) : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        client.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetHistoryAsync(key, limit);

    public Task<TableHistoryStats> GetStatsAsync(string tableName) =>
        client.GetGrain<ITableHistoryGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetStatsAsync();
}

/// <summary>Plan 011 D1: the shard tier's read surface. Three of its four members deliberately never
/// touch a shard grain — <see cref="GetInfoAsync"/> and <see cref="GetKeysAsync"/> answer from the router
/// and the directory, so the console can poll them without waking a single idle key. Only
/// <see cref="GetShardAsync"/> (one named key, the point of the tier) and <see cref="ScanAsync"/> (the
/// explicit full scan, kept separate precisely so nothing reaches it by accident) activate shards.</summary>
internal sealed class OrleansTableShardFacade(IClusterClient client) : ITableShardFacade
{
    public Task<TableShardView> GetShardAsync(string tableName, string shardKey, int historyLimitPerKey) =>
        client.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName), shardKey)).GetViewAsync(historyLimitPerKey);

    public Task<TableShardingInfo> GetInfoAsync(string tableName) =>
        client.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetInfoAsync();

    public Task<List<string>> GetKeysAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableShardDirectoryGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).GetKeysAsync(limit, offset);

    public async Task<List<TableShardStats>> ScanAsync(string tableName, int limit, int offset)
    {
        var qualifiedTableName = EnvKeys.Qualify(EnvironmentAmbient.Current, tableName);
        var keys = await client.GetGrain<ITableShardDirectoryGrain>(qualifiedTableName).GetKeysAsync(limit, offset);
        var stats = new List<TableShardStats>(keys.Count);
        // Chunked rather than one WhenAll over the whole page: a scan is the one call that deliberately
        // activates shards, and activating a few hundred at once is a load spike worth not creating.
        foreach (var chunk in keys.Chunk(32))
        {
            stats.AddRange(await Task.WhenAll(chunk.Select(k =>
                client.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(qualifiedTableName, k)).GetStatsAsync())));
        }
        return stats;
    }

    /// <summary>Plan 011 D2. One grain call, deliberately: the ROUTER performs the whole scan, because the
    /// router being busy IS the fence (see TableShardRouterGrain.FencedScanAsync). Doing the fan-out here,
    /// from outside the cluster, would read shards while the router kept forwarding — which is exactly the
    /// unfenced scan above.</summary>
    public Task<TableShardScanResult> ScanFencedAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableShardRouterGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)).FencedScanAsync(limit, offset);
}

/// <summary>Plan 006, D-C: connector runtime status. Generator-kind sources (Kind unset/"generator"),
/// ingest-kind sources (plan 008 W4 — client-push, no IConnectorGrain ever backs one; use
/// IIngressFacade/GET /{name}/ingest instead), and unknown source names all return null — mirroring
/// RegistryGrain's own Kind-dispatch rule (see RegistryGrain.IsGeneratorKind/IsIngestKind) so this
/// facade never spins up a pointless IConnectorGrain activation for a source that was never a
/// connector in the first place.</summary>
internal sealed class OrleansConnectorStatusFacade(IClusterClient client) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        // Plan 021 D4 — a facade answering one request reads the ambient.
        var registry = client.RegistryFor(EnvironmentAmbient.Current);
        var def = await registry.GetSourceAsync(sourceName);
        if (def is null || string.IsNullOrEmpty(def.Kind) || def.Kind is SourceKinds.Generator or SourceKinds.Ingest)
        {
            return null;
        }
        return await client.GetGrain<IConnectorGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).GetStatusAsync();
    }
}

/// <summary>Plan 008 W4: Orleans-side <see cref="IIngressFacade"/>. A host-process singleton over
/// <see cref="SourceIngressRegistry"/> — deliberately NOT a grain (IIngressFacade's own doc comment:
/// an unbounded, unobservable grain inbox with no admission point would make the buffer's policy
/// choice decorative). The drain pump publishes through the EXACT same door
/// <c>GeneratorGrain.TickAsync</c>/<c>ConnectorGrain</c> already use — <c>client.GetStreamProvider</c>
/// instead of <c>this.GetStreamProvider</c> only because this class isn't a grain; it resolves the
/// identical provider instance from the same shared silo/web-app DI container (see
/// PushStreamHostingExtensions' doc on why that equivalence holds for this co-hosted process) — one
/// EventRecord per row, so every existing subscriber (pipelines, tables, SignalR, gRPC StreamService)
/// sees an ingest source exactly like a generator or connector one.</summary>
internal sealed class OrleansIngressFacade(
    IClusterClient client,
    SourceIngressRegistry registry,
    IngestIdempotencyCache idempotency,
    IngestKeyUsageTracker keyUsage,
    IngressStatsReportTracker statsTracker,
    IServiceProvider services) : IIngressFacade
{
    /// <summary>Plan 009 A1.1: the idempotency check runs BEFORE anything else — even source lookup —
    /// per <see cref="IngestIdempotencyCache"/>'s own doc comment: a repeat of the same key always
    /// replays the ORIGINAL outcome verbatim, never re-derives one.</summary>
    public Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial, string? idempotencyKey = null) =>
        IngestIdempotencyCache.RunAsync(idempotency, sourceName, idempotencyKey, () => PushCoreAsync(sourceName, events, partial));

    private async Task<IngestResult> PushCoreAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial)
    {
        var def = await GetSourceDefAsync(sourceName);
        if (def is null)
        {
            return new IngestResult { Outcome = IngestOutcome.NotFound, Error = $"source '{sourceName}' not found" };
        }

        if (def.Kind != SourceKinds.Ingest)
        {
            // Includes generator-kind: mixing a timer-driven rate with client pushes would make every
            // counter and the rate display unreconcilable (plan 008 W4 brief) — strictness here is
            // reversible, laxity is not.
            return new IngestResult { Outcome = IngestOutcome.WrongKind, Error = $"source '{sourceName}' is kind '{def.Kind}', not ingest" };
        }

        var config = def.Ingest ?? new IngestConfig();

        // Coercion BEFORE admission (IngestModels.cs's header): a 400 must never leave partial state.
        var arrivalMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = IngressRowAcceptance.AcceptBatch(def.Fields, sourceName, config.RejectUnknownFields, events, arrivalMs);
        if (batch.RowErrors.Count > 0 && !partial)
        {
            return new IngestResult
            {
                Outcome = IngestOutcome.Invalid,
                Invalid = batch.RowErrors.Count,
                Error = $"{batch.RowErrors.Count} row(s) failed coercion",
                RowErrors = batch.RowErrors,
            };
        }

        // Plan 021 D3/item 3 — the buffer/grain/tracker key is EnvKeys.Qualify(def.Environment, sourceName),
        // not the bare name: two environments' same-named ingest source must not share one buffer. `def`
        // is already in hand (rule B — a definition-carried environment, not the ambient re-read), and the
        // SAME qualified key is what IngestDrainPumpService.SweepAsync/ReportStatsAsync compute from the
        // identical SourceDefinition.Environment field, so push/status/drain/removal all agree.
        var qualifiedName = EnvKeys.Qualify(def.Environment, sourceName);
        var buffer = registry.GetOrCreate(qualifiedName, config, (rows, ct) => DrainAsync(qualifiedName, rows, ct));

        // Plan 009 A1.1: row-level dedup runs AFTER coercion, BEFORE admission — a duplicate never
        // consumes buffer capacity and (together with the whole-batch-Invalid return above) a 400
        // still leaves nothing behind either way.
        var dedup = buffer.FilterRowLevelDuplicates(batch.Accepted);
        var result = await buffer.PushAsync(dedup.Kept);

        if (batch.RowErrors.Count > 0)
        {
            buffer.RecordInvalid(batch.RowErrors.Count);
            result.Invalid = batch.RowErrors.Count;
            result.RowErrors = batch.RowErrors;
        }

        result.Duplicate = dedup.DuplicateCount;
        return result;
    }

    /// <summary>Plan 009 A1.2: null/blank key, unknown source, non-ingest source, and a source with no
    /// configured keys all return false (IIngressFacade.ValidateKeyAsync's own doc comment — an ingest
    /// source with zero keys is JWT-only, not open). Hash comparison is
    /// <see cref="PasswordHasher.Verify"/>'s own fixed-time compare, so this doesn't leak by timing.</summary>
    public async Task<bool> ValidateKeyAsync(string sourceName, string? presentedKey)
    {
        if (string.IsNullOrEmpty(presentedKey))
        {
            return false;
        }

        var def = await GetSourceDefAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return false;
        }

        var keys = def.Ingest?.Keys;
        if (keys is null || keys.Count == 0)
        {
            return false;
        }

        foreach (var key in keys)
        {
            if (PasswordHasher.Verify(presentedKey, key.Hash, key.Salt))
            {
                // Best-effort, in-memory, per-replica — see IngestKeyUsageTracker's own doc comment
                // for why this is never round-tripped through UpsertSourceAsync on the hot path. Qualified
                // for the same reason every other ingest key here is (see PushCoreAsync).
                keyUsage.RecordUse(EnvKeys.Qualify(def.Environment, sourceName), key.Id, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                return true;
            }
        }

        return false;
    }

    /// <summary>Plan 009 A1.3: DepthRows/Policy/CapacityRows/MaxBatchRows/LastPushMs stay THIS
    /// replica's own local view (they aren't cumulative counters, so summing them across replicas
    /// wouldn't mean anything); every TotalX/DownstreamDropped counter is answered from
    /// <see cref="IIngressStatsGrain"/> plus whatever this replica hasn't reported yet — see
    /// <see cref="IngressStatsReportTracker"/>'s own doc comment for why that combination, not the
    /// grain alone, is what keeps a push-then-immediately-GET on the same replica accurate.</summary>
    public async Task<IngestStatus?> GetStatusAsync(string sourceName)
    {
        var def = await GetSourceDefAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return null;
        }

        var qualifiedName = EnvKeys.Qualify(def.Environment, sourceName);
        var buffer = registry.TryGet(qualifiedName);
        var config = def.Ingest ?? new IngestConfig();

        // Never pushed to on THIS replica yet (SourceIngressRegistry.TryGet's own doc): report the
        // configured shape with zeroed local counters instead of creating a buffer as a side effect of
        // a GET — the aggregation below can still show other replicas' totals for this source.
        var local = buffer?.GetStatus() ?? new IngestStatus { Policy = config.Policy, CapacityRows = config.CapacityRows, MaxBatchRows = config.MaxBatchRows };

        var snapshot = await client.GetGrain<IIngressStatsGrain>(qualifiedName).GetSnapshotAsync();
        var baseline = statsTracker.GetBaseline(qualifiedName);
        var pending = IngressStatsReportTracker.ComputeDelta(baseline, local);

        local.TotalAccepted = snapshot.TotalAccepted + pending.Accepted;
        local.TotalRejected = snapshot.TotalRejected + pending.Rejected;
        local.TotalDropped = snapshot.TotalDropped + pending.Dropped;
        local.TotalInvalid = snapshot.TotalInvalid + pending.Invalid;
        local.TotalPublished = snapshot.TotalPublished + pending.Published;
        local.DownstreamDropped = snapshot.DownstreamDropped + pending.DownstreamDropped;
        local.TotalDuplicate = snapshot.TotalDuplicate + pending.Duplicate;
        local.InstanceId = IngressInstanceId.Value;
        local.Aggregated = true;
        return local;
    }

    private Task<SourceDefinition?> GetSourceDefAsync(string sourceName) =>
        // Plan 021 D4 — a facade answering one request reads the ambient.
        client.RegistryFor(EnvironmentAmbient.Current).GetSourceAsync(sourceName);

    /// <summary>The drain pump handed to every <see cref="SourceIngressBuffer"/> this facade creates:
    /// plan 026 D3 routes the whole batch through <see cref="IIngestSourceGrain.PublishAsync"/> — one
    /// grain call per drained batch, one <c>EventRecord</c> published per row inside it — instead of a
    /// direct <c>stream.OnNextAsync</c> loop, so the late-consumer attach gate and the replay log have a
    /// turn-based owner. <paramref name="qualifiedName"/> IS the grain's key, which is also this
    /// source's stream identity (<c>EnvKeys.Qualify(def.Environment, sourceName)</c>, captured by
    /// PushCoreAsync's closure) — matching TableGrain/PipelineGrain's subscription to the SAME
    /// (SourcesNamespace, qualifiedName) stream, so a row published through the grain fans out to every
    /// existing consumer unchanged.
    ///
    /// Also where the SECOND loss point (IngestModels.cs's header) is measured: under
    /// <c>Streams:Transport=push</c>, <see cref="PushStreamBus.TotalDropped"/> advances synchronously
    /// inside each <c>OnNextAsync</c> the grain performs — the co-hosted silo means this before/after
    /// read still brackets the same publish, only now across one grain hop, so the delta stays an
    /// honest — if approximate under concurrent ingest sources sharing the one process-wide counter —
    /// attribution back to THIS source's <see cref="IngestStatus.DownstreamDropped"/>.
    /// <see cref="PushStreamBus"/> isn't registered at all under the default pull (memory-streams)
    /// transport, so DownstreamDropped simply stays 0 there — pull's own loss point (pulling-agent
    /// queue overflow) isn't instrumented today.</summary>
    private async Task DrainAsync(string qualifiedName, IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
    {
        var pushBus = services.GetService<PushStreamBus>();
        var before = pushBus?.TotalDropped ?? 0;

        await client.GetGrain<IIngestSourceGrain>(qualifiedName).PublishAsync(rows.ToList());

        if (pushBus is null)
        {
            return;
        }

        var delta = pushBus.TotalDropped - before;
        if (delta > 0)
        {
            registry.TryGet(qualifiedName)?.RecordDownstreamDropped((int)delta);
        }
    }
}

internal sealed class OrleansArrangementMetaFacade(IClusterClient client) : IArrangementMetaFacade
{
    // Moved verbatim from the old StreamsForge.Host.Api.MetaEndpoints's /arrangements handler — see
    // that endpoint's original doc comment (plan 003 M3) for the full "recompile-per-grain" rationale.
    public async Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync()
    {
        // Plan 021 D4 — a facade answering one request reads the ambient.
        var registry = client.RegistryFor(EnvironmentAmbient.Current);
        var tables = await registry.GetTablesAsync();
        var running = tables.Where(t => t.Status == PipelineStatus.Running && t.Parallelism > 1).ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<ArrangementMetaInfo>();

        foreach (var def in running)
        {
            TableDataflowPlan dataflow;
            try
            {
                (_, dataflow) = await TableDataflowFactory.BuildAsync(client, def);
            }
            catch
            {
                continue; // best-effort — a table that doesn't currently compile just isn't reported
            }

            foreach (var edge in dataflow.ArrangeableExternalEdges)
            {
                var inputName = dataflow.ExternalInputNameOf(edge);
                var keySpec = dataflow.KeySpecOf(edge);
                var hash = ArrangementKeySpec.HashOf(keySpec);
                int pcount = dataflow.PartitionCountOf(edge.ToStageId);
                var setKey = $"{inputName}|{hash}|{pcount}";
                if (!seen.Add(setKey))
                {
                    continue;
                }

                var infos = new List<ArrangementInfo>(pcount);
                for (int p = 0; p < pcount; p++)
                {
                    // Plan 021 D3 — MUST match TableGrain.StartCoordinatorAsync's own arrangementKey
                    // composition (EnvKeys.Qualify(def.Environment, inputName)) exactly, or this facade
                    // would address a DIFFERENT (empty) ArrangementGrain activation than the one the table
                    // actually attached to.
                    var key = $"{EnvKeys.Qualify(def.Environment, inputName)}:{hash}:{p}";
                    infos.Add(await client.GetGrain<IArrangementGrain>(key).GetInfoAsync());
                }

                if (infos.All(i => i.ConsumerCount == 0))
                {
                    continue; // structurally arrangeable but nothing currently attached — not "live"
                }

                result.Add(new ArrangementMetaInfo
                {
                    InputName = inputName,
                    KeySpec = keySpec,
                    Partitions = pcount,
                    // Every attaching table attaches ALL P partitions (one consumer id per partition —
                    // see TableGrain.StartCoordinatorAsync's attach loop), so ConsumerCount is uniform
                    // across an arrangement set's partitions; Max is a defensive read (vs. Sum, which
                    // would misleadingly scale with P) in case of a transient in-flight attach/detach.
                    Consumers = infos.Count > 0 ? infos.Max(i => i.ConsumerCount) : 0,
                    TotalRows = infos.Sum(i => i.RowCount),
                });
            }
        }

        return result;
    }
}
