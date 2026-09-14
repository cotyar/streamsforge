using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Sql;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Api;

/// <summary>
/// Plan 015 wave 3-A — two gates on every route, the pattern <c>AccessEndpoints</c> established in wave
/// 2-C. Every <c>RequireAuthorization("Viewer"/"Editor")</c> stays exactly where it is (the compatibility
/// floor, pinned by <c>AuthorizationCoverageTests</c>), and each handler additionally asks
/// <see cref="AccessGuard"/> for its own action at its own scope, passing the table's <c>Tags</c> so
/// <c>tag:finance</c> scopes match.
///
/// <para><b>The scope is the table's NAME, not the <c>{id}</c> in the route</b> — the same decision, for
/// the same reason, as in <c>PipelinesEndpoints</c>: a table id is
/// <c>Guid.NewGuid().ToString("n")</c>, and an entitlement grammar built around <c>prod-*</c> is a
/// grammar about names. Where the table cannot be loaded the route segment is used verbatim, which only
/// ever narrows the answer.</para>
///
/// <para><b>The three sharded-table routes are <c>table.read</c>, including the fenced scan.</b> A fence
/// pauses the tier's ingest for the duration of the scan, which is a real operational cost — but it is a
/// cost of READING consistently, it changes no state, and giving it its own action would mean an
/// operator who granted <c>table.read</c> discovered a second switch they had never heard of the first
/// time somebody asked for a consistent cut. The cost is documented on the endpoint itself, where it is
/// actionable.</para>
/// </summary>
public static class TablesEndpoints
{
    public static void MapTablesEndpoints(this WebApplication app, StreamsForgeApiOptions options)
    {
        var group = app.MapGroup("/api/tables");

        // Plan 015 wave 3-A: FILTERED rather than gated at `*` — see the identical note in
        // SourcesEndpoints' list handler. Three of ten, not a 403.
        group.MapGet("/", async (ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var all = await registry.GetTablesAsync();
            var visible = new List<TableDefinition>(all.Count);
            foreach (var t in all)
            {
                if (await guard.CheckAsync(principal, Actions.TableRead, t.Name, t.Tags) is { IsAllowed: true })
                {
                    visible.Add(SecretsMasker.MaskTable(t));
                }
            }

            return Results.Ok(visible);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name (guard first — see EntityLookup's class remarks).
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            return EntityLookup.Reject(hit) ?? Results.Ok(SecretsMasker.MaskTable(hit.Value!));
        }).RequireAuthorization("Viewer");

        // Create asks at the NAME BEING CREATED with the request's own Tags — see the same note in
        // SourcesEndpoints' create handler.
        group.MapPost("/", async (CreateTableRequest req, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ILoggerFactory loggers) =>
        {
            if (await RefuseAsync(guard, principal, Actions.TableWrite, req.Name ?? "", req.Tags) is { } refusal)
            {
                return refusal;
            }

            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

            // Plan 014 K: 'INSERT INTO <sink> SELECT …' is stripped before the definition is built — the
            // stored text is the naked query and the named sink is switched on. See the identical note in
            // PipelinesEndpoints' create handler; an unknown target is a 400, never a silent no-op.
            var sinks = req.Sinks ?? [];
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "table");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: the missing ISinkTransport.Validate call site — see
            // SinkTransports.Validate's own doc for what this does and does not change. Always run on
            // create: 'sinks' is always the request's own (possibly empty) list here, never a
            // carried-over stored one.
            var sinkErrors = new List<string>();
            SinkTransports.Validate(sinks, sinkErrors);
            if (sinkErrors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
            }

            // Plan 019 D2 (wave 019-B2): the catalog-aware half — a duplex sink whose named source does
            // not exist, or is not a duplex kind, is a validation error too. Unlike a pipeline's Id, a
            // table's Name IS req.Name — known here, before CreateTableAsync — so {name} expansion is
            // always fully resolvable on this call site (contrast PipelinesEndpoints' create handler).
            await DuplexSinkCatalogValidation.ValidateAsync(sinks, req.Name, registry, sinkErrors);
            if (sinkErrors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
            }

            // Plan 016 wave 2-B: a pin's kind must be "source" or "table" — see EntityPinValidation's
            // own doc for why that is refused here rather than stored and left permanently unresolvable.
            if (EntityPinValidation.Validate(req.DependsOn) is { } pinError)
            {
                return Results.BadRequest(new ErrorResponse(pinError));
            }

            try
            {
                var def = new TableDefinition
                {
                    Name = req.Name,
                    Description = req.Description,
                    Sql = sugar.Sql,
                    CreatedBy = principal.Identity?.Name ?? "",
                    SearchEnabled = req.SearchEnabled,
                    SearchMode = req.SearchMode,
                    HistoryEnabled = req.HistoryEnabled,
                    HistoryMode = req.HistoryMode,
                    HistoryLimit = req.HistoryLimit,
                    HistoryByField = req.HistoryByField,
                    HistoryWindowMs = req.HistoryWindowMs,
                    Tags = req.Tags ?? [],
                    Metadata = req.Metadata ?? [],
                    Parallelism = req.Parallelism,
                    Persistence = req.Persistence,
                    FlushMs = req.FlushMs,
                    JournalMaxEntries = req.JournalMaxEntries,
                    RetentionMaxRows = req.RetentionMaxRows,
                    RetentionTtlMs = req.RetentionTtlMs,
                    // Plan 011 D1: null = not sharded, the default.
                    ShardBy = req.ShardBy ?? [],
                    // Plan 009 B2: see the identical note in PipelinesEndpoints' create handler — a
                    // freshly-created table has no stored secrets to merge against.
                    // Plan 014 K: 'sinks' is that same list, after the sugar enabled a member of it.
                    Sinks = sinks,
                    // Plan 016 wave 2-B: the dependsOn gap — this field never reached the registry over
                    // REST before this wave (see EntityPinValidation for the validation just above).
                    DependsOn = EntityPinValidation.Normalize(req.DependsOn),
                };
                var created = await registry.CreateTableAsync(def);
                WarnOnDegradedRowIdentity(loggers, created);

                // Plan 015 wave 5-B: inside the try, AFTER CreateTableAsync — a create that throws
                // InvalidOperationException below answers 409 and created nothing, and a row saying it
                // did would be a lie in the one log that must not contain any.
                CatalogChangeAudit.RecordTable(
                    http, principal, Actions.TableWrite, created.Name, before: null, after: created);
                return Results.Created($"/api/tables/{created.Id}", SecretsMasker.MaskTable(created));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        // The STORED table's name and tags decide, never the incoming body's: a caller who could rename
        // or re-tag their way into an entitlement would not be under one.
        group.MapPut("/{id}", async (string id, CreateTableRequest req, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ILoggerFactory loggers) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (await RefuseAsync(guard, principal, Actions.TableWrite, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (existing is null)
            {
                return Results.NotFound();
            }

            // Plan 015 wave 5-B: taken before the in-place field copies below — see the identical note
            // in PipelinesEndpoints' PUT handler. This is also the definition the size decision was made
            // for: a table's SQL body and field list are the two big fields, and recording only what
            // MOVED means an edit to HistoryLimit costs a few dozen bytes instead of two copies of both.
            var before = CatalogChangeAudit.Snapshot(existing);

            // Plan 009 B2: null Sinks = unchanged; a non-null Sinks carrying "***" is restored from the
            // stored value first (SecretsMasker.MergeSinkSecrets — see the identical, more detailed note
            // in PipelinesEndpoints' PUT handler, including the Orleans-flavor RegistryGrain gap that
            // applies here too, in TablesEndpoints' case via RegistryGrain.UpdateTableAsync).
            var sinks = req.Sinks is null ? existing.Sinks : SecretsMasker.MergeSinkSecrets(req.Sinks, existing.Sinks);

            // Plan 014 K: after the merge and before the field copies — same ordering, and the same reason
            // for it, as PipelinesEndpoints' PUT handler.
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "table");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: only when the request is actually touching Sinks (req.Sinks is
            // null means "leave unchanged" — see the comment above) so a PUT that edits some unrelated
            // field on a table whose sinks predate this validation, or were saved back before this rule
            // existed, can never be blocked by a sink spec nobody asked to change.
            if (req.Sinks is not null)
            {
                var sinkErrors = new List<string>();
                SinkTransports.Validate(sinks, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }

                // Plan 019 D2 (wave 019-B2): the catalog-aware half. req.Name is the table's new name —
                // the same value assigned to existing.Name just below — so {name} expansion uses the name
                // this table will actually have once this PUT completes, not the pre-rename one.
                await DuplexSinkCatalogValidation.ValidateAsync(sinks, req.Name, registry, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }
            }

            // Plan 016 wave 2-B: only when the request actually touches DependsOn (null means "leave
            // unchanged" — see the field's own doc on CreateTableRequest), same reasoning as the Sinks
            // gate just above — a PUT that edits something unrelated on a table whose pins predate this
            // validation can never be blocked by a pin nobody asked to change.
            if (req.DependsOn is not null && EntityPinValidation.Validate(req.DependsOn) is { } pinError)
            {
                return Results.BadRequest(new ErrorResponse(pinError));
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = sugar.Sql;
            existing.SearchEnabled = req.SearchEnabled;
            existing.SearchMode = req.SearchMode;
            existing.HistoryEnabled = req.HistoryEnabled;
            existing.HistoryMode = req.HistoryMode;
            existing.HistoryLimit = req.HistoryLimit;
            existing.HistoryByField = req.HistoryByField;
            existing.HistoryWindowMs = req.HistoryWindowMs;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            existing.Parallelism = req.Parallelism;
            existing.Persistence = req.Persistence;
            existing.FlushMs = req.FlushMs;
            existing.JournalMaxEntries = req.JournalMaxEntries;
            existing.RetentionMaxRows = req.RetentionMaxRows;
            existing.RetentionTtlMs = req.RetentionTtlMs;
            // Plan 011 D1: null means "leave as-is" (matching Tags/Metadata/Sinks' convention here), so a
            // client that predates the field cannot silently un-shard a table by omitting it.
            existing.ShardBy = req.ShardBy ?? existing.ShardBy;
            existing.Sinks = sinks;
            // Plan 016 wave 2-B: null-means-unchanged, same convention as every other optional list field
            // above — validated just above, before any field of `existing` was touched.
            existing.DependsOn = req.DependsOn is null ? existing.DependsOn : EntityPinValidation.Normalize(req.DependsOn);

            try
            {
                var updated = await registry.UpdateTableAsync(existing);
                if (updated is not null)
                {
                    WarnOnDegradedRowIdentity(loggers, updated);
                    CatalogChangeAudit.RecordTable(
                        http, principal, Actions.TableWrite, before.Name, before: before, after: updated);
                }
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        // One extra catalog read this handler did not do before: without the definition there is no name
        // and no tag list, and delete is the action an operator most wants to scope.
        group.MapDelete("/{id}", async (string id, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (await RefuseAsync(guard, principal, Actions.TableDelete, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            try
            {
                var removed = await registry.DeleteTableAsync(id);
                if (removed)
                {
                    CatalogChangeAudit.RecordTable(
                        http, principal, Actions.TableDelete, existing?.Name ?? id, before: existing, after: null);
                }

                return removed ? Results.NoContent() : Results.NotFound();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (await RefuseAsync(guard, principal, Actions.TableControl, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Running);
                // Plan 009 B2: see the identical note on PipelinesEndpoints' /start handler — found live.
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (await RefuseAsync(guard, principal, Actions.TableControl, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Stopped);
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        // `table.write` at `*`: /validate names no table, and `*` is answered only by a `*`-scoped
        // grant. Same reasoning, and the same accepted cost, as PipelinesEndpoints' /validate.
        group.MapPost("/validate", async (ValidateRequest req, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            if (await RefuseAsync(guard, principal, Actions.TableWrite, "*") is { } refusal)
            {
                return refusal;
            }

            // Plan 014 K: the sugar is stripped before compiling, and the named sink's existence is NOT
            // checked here — see the fuller note on PipelinesEndpoints' /validate for why a ValidateRequest
            // deliberately stays "SQL and nothing else".
            var sugar = SinkSugar.Desugar(req.Sql);
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.Ok(new ValidateTableResponse(
                    false,
                    [.. sugar.Diagnostics.Select(d => new SqlDiagnosticDto(d, 1, 1, nameof(DiagnosticSeverity.Error)))],
                    null,
                    [],
                    [],
                    []));
            }

            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry);
            var result = SqlCompiler.CompileTable(sugar.Sql, streamSchemas, tableSchemas);
            return Results.Ok(new ValidateTableResponse(
                result.Ok,
                result.Diagnostics.Select(d => new SqlDiagnosticDto(d.Message, d.Line, d.Column, d.Severity.ToString())).ToList(),
                result.PlanSummary,
                result.StreamInputs.ToList(),
                result.TableInputs.ToList(),
                result.OutputSchema?.Fields.Select(f => new FieldDefDto(f.Key, f.Value.ToString())).ToList() ?? []));
        }).RequireAuthorization("Editor");

        // Plan 008 W5: lineage + execution-plan view for the console's React Flow page. Physical: true
        // only for a Parallelism >= 2 table on the Orleans flavor whose plan shape supports partitioning
        // (see PlanEndpointsLogic's class doc for the full degradation matrix) — otherwise the logical
        // view (planSummary + inputs) with an explanatory UnavailableReason. Recompiled fresh every call
        // (following OrleansArrangementMetaFacade.GetArrangementsAsync's precedent); never persisted.
        group.MapGet("/{id}/plan", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry, excludeTableId: def.Id);
            return Results.Ok(PlanEndpointsLogic.BuildTablePlan(def, streamSchemas, tableSchemas, options.Flavor));
        }).RequireAuthorization("Viewer");

        // The .Produces<>() below is what puts this response's shape into the generated OpenAPI document —
        // an IResult-returning handler describes nothing on its own. That in turn is what lets the
        // per-entity document (EntityOpenApiEndpoints) replace the free-form TableRowDto.row with this
        // table's real output schema, which is most of the point of a per-entity spec. Same for /search
        // and for PipelinesEndpoints' /results.
        group.MapGet("/{id}/rows", async (string id, int? limit, int? offset, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var rows = await tables.GetRowsAsync(def.Name, limit ?? 100, offset ?? 0);
            var total = await tables.GetRowCountAsync(def.Name);
            var seq = await tables.GetSeqAsync(def.Name);
            var frontierEpoch = await tables.GetSnapshotFrontierEpochAsync(def.Name);
            // Plan 011 D1: on a sharded table, say where these rows came from — and do NOT go and get
            // them from the shards. See TableRowsShardNote's own doc for the trap this closes; the short
            // version is that this endpoint is polled every two seconds by the console, and a keyless
            // listing that fanned out across the directory would wake every shard on every poll.
            // Plan 011 D2: the wording changed with the thing it describes. A sharded table no longer KEEPS
            // a persisted consolidated snapshot (see TableGrain's "sharded tables" paragraph — the shards
            // are the durable per-key copy, and mirroring them in the table grain was a second copy of the
            // same rows). These rows are the table's own LIVE executor output, which is if anything fresher
            // than the up-to-one-flush-interval-stale mirror it replaced. Still no shard is contacted.
            var shardNote = def.ShardBy.Count == 0 ? null : new TableRowsShardNote(
                def.ShardBy,
                ShardsConsulted: false,
                "Served live from the table's own executor; no shard was activated. Use POST /shard/lookup for one key, or GET /shards/scan (optionally ?fenced=true) for an explicit full scan.");
            return Results.Ok(new TableRowsResponse(rows, total, seq, frontierEpoch, shardNote));
        }).Produces<TableRowsResponse>().RequireAuthorization("Viewer");

        // Plan 012: the same rows as a file. Deliberately its OWN route rather than a ?format=csv branch on
        // /rows: that one is .Produces<TableRowsResponse>(), and the per-entity OpenAPI document
        // (EntityOpenApiEndpoints) rewrites its row shape into this table's real output schema — a second
        // content type on the same operation would have made that document describe two things at once.
        group.MapGet("/{id}/rows.csv", async (string id, int? limit, int? offset, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            // Cap higher than /rows' 100 (this is an export, not a console poll) but still capped: an
            // unbounded default on a million-row table would render the whole thing into one string.
            var rows = await tables.GetRowsAsync(def.Name, Math.Clamp(limit ?? 10_000, 1, 100_000), offset ?? 0);
            var csv = CsvExport.Table(def, rows);
            return Results.File(Encoding.UTF8.GetBytes(csv), "text/csv; charset=utf-8", $"{def.Name}.csv");
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var metrics = await tables.GetMetricsAsync(def.Name);
            // The row-identity warning (see TableRowIdentityWarning). DERIVED from the definition, not
            // measured by the grain/actor, and stamped here for two reasons: it belongs on the object the
            // console already polls for "this table is not in the state you think it is" conditions
            // (TableMetrics.Rebuilding is the precedent), and doing it in the shared endpoint means BOTH
            // flavors report it from one implementation instead of two that can drift. Pure and cheap — a
            // textual pass over this table's own SQL — and it can never go stale relative to that SQL,
            // which a value persisted at upsert could.
            metrics.RowIdentityWarning = TableRowIdentityWarning.For(def);
            // Plan 014: the successful half of the same derivation, for the console's upsert key-column
            // prefill. Only offered when the extractor actually resolved the declared keys to output
            // columns — where it could not, the warning above is set and a suggestion would be a guess at
            // precisely the thing the operator has to get right.
            var identity = TableGroupKeyExtractor.Describe(def.Sql);
            metrics.DeclaredKeyColumns = identity.Columns is { Count: > 0 } cols ? [.. cols] : [];
            return Results.Ok(metrics);
        }).RequireAuthorization("Viewer");

        // GET /{id}/replay — plan 026 wave 3 D6: what this table's delta-batch replay log currently
        // retains (memory + disk). id-or-name via EntityLookup, same as every other GET route in this
        // file — the facade itself is keyed by table NAME (IReplayStatusFacade.GetTableAsync's own doc;
        // tables have no separate grain/actor key), which is why def.Name, not def.Id, is passed through.
        // NotSupportedException (Dapr, until PARITY D11 closes) becomes 501 rather than a bare 500.
        group.MapGet("/{id}/replay", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IReplayStatusFacade replay) =>
        {
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;
            try
            {
                var status = await replay.GetTableAsync(def.Name);
                return status is null ? Results.NotFound() : Results.Ok(status);
            }
            catch (NotSupportedException ex)
            {
                return Results.Json(new ErrorResponse(ex.Message), statusCode: StatusCodes.Status501NotImplemented);
            }
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this table: DescriptorFactory's schema (built from
        // the already-compiled TableDefinition.OutputFields, no recompilation needed) plus the
        // DynamicStreamService streaming contract. 409 if the table has never successfully compiled
        // (no OutputFields to describe), mirroring the pipeline endpoint's compile-failure behavior.
        group.MapGet("/{id}/proto", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            if (def.OutputFields.Count == 0)
            {
                return Results.Conflict(new ErrorResponse(
                    def.Error ?? "Table has no compiled output schema yet — start/re-save the table to compile its SQL."));
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.TableKey(def.Id), def.OutputFields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(def.Name, def.OutputFields, numbers);
            var protoText = ProtoFileBuilder.Build("table", def.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/search", async (string id, string? q, int? limit, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            if (!def.SearchEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Search is not enabled for this table."));
            }

            var query = q ?? "";
            List<TableRowDto> rows = string.IsNullOrWhiteSpace(query)
                ? []
                : await tables.SearchAsync(def.Name, query, limit ?? 100);
            return Results.Ok(new TableSearchResponse(rows, def.SearchMode.ToString(), def.SearchEnabled, rows.Count));
        }).Produces<TableSearchResponse>().RequireAuthorization("Viewer");

        // Row history (Feature B). POST (not GET) because the lookup key is the row's own content — an
        // arbitrary-shaped object, awkward to round-trip through a query string — not a resource
        // identifier; mirrors this API's existing "/validate" precedent for a read-only POST-with-body.
        // The server derives the row-identity key from req.Row via TableGroupKeyExtractor/RowKeyCodec, the
        // same way TableHistoryGrain derives it from live deltas, so the client never needs to know the
        // table's GROUP BY identity columns or the key encoding.
        group.MapPost("/{id}/history/lookup", async (string id, HistoryLookupRequest req, int? limit, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            // Plan 011 D1: on a SHARDED table the per-key shards hold the version trails and the
            // table-wide history grain is deliberately disabled, so this endpoint would answer 200 with a
            // convincingly empty result — the worst possible reply, because it looks like "this key has no
            // history" rather than "you are asking the wrong tier". Point the caller at the right one.
            if (def.ShardBy.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(
                    $"This table is sharded by {string.Join(", ", def.ShardBy)}; its row history lives per shard key. Use POST /api/tables/{id}/shard/lookup instead."));
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
            var key = RowKeyCodec.EncodeIdentity(req.Row, identityColumns);
            var result = await history.GetHistoryAsync(def.Name, key, limit ?? 0);
            return Results.Ok(result);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/history/stats", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            // Plan 011 D1: on a SHARDED table the per-key shards hold the version trails and the
            // table-wide history grain is deliberately disabled, so this endpoint would answer 200 with a
            // convincingly empty result — the worst possible reply, because it looks like "this key has no
            // history" rather than "you are asking the wrong tier". Point the caller at the right one.
            if (def.ShardBy.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(
                    $"This table is sharded by {string.Join(", ", def.ShardBy)}; its row history lives per shard key. Use POST /api/tables/{id}/shard/lookup instead."));
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            return Results.Ok(await history.GetStatsAsync(def.Name));
        }).RequireAuthorization("Viewer");

        // ------------------------------------------------------------------
        // Plan 011 wave D1 — SHARDED TABLES.
        //
        // Three endpoints, and the split between them IS the design. A per-key lookup activates exactly
        // one shard, which is the point. The listing endpoint activates none, which is what makes it
        // pollable. A full scan activates every shard in its page, which is why it is a separate URL that
        // nothing reaches by accident — not a query parameter on one of the other two.
        // ------------------------------------------------------------------

        // "Give me everything for this key" — the query the whole tier exists for. One grain, one ordered
        // delta stream, Orleans serializing its turns: strictly consistent by construction, no fence.
        group.MapPost("/{id}/shard/lookup", async (string id, ShardLookupRequest req, int? historyLimit, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            if (def.ShardBy.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("This table is not sharded (shardBy is empty)."));
            }

            var missing = def.ShardBy.Where(c => !req.Row.ContainsKey(c)).ToList();
            if (missing.Count > 0)
            {
                // Refused rather than defaulted to null: a missing shard column would silently address the
                // "all nulls" shard, which exists and would answer, so the caller would get a confident
                // empty result for a typo.
                return Results.BadRequest(new ErrorResponse(
                    $"row is missing shardBy column(s): {string.Join(", ", missing)}. This table shards by {string.Join(", ", def.ShardBy)}."));
            }

            var shardKey = TableShardKeys.EncodeShardKey(req.Row, def.ShardBy);
            return Results.Ok(await shards.GetShardAsync(def.Name, shardKey, historyLimit ?? 0));
        }).RequireAuthorization("Viewer");

        // Shard metrics + the live key set. Wakes NOTHING — router and directory only.
        group.MapGet("/{id}/shards", async (string id, int? limit, int? offset, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            if (def.ShardBy.Count == 0)
            {
                return Results.Ok(new TableShardsResponse(false, [], 0, 0, 0, 0, -1, 0, 0, false, []));
            }

            var info = await shards.GetInfoAsync(def.Name);
            // limit=0 means "metrics only, list no keys" — the shape a 2s console poll should use, since
            // the key list is O(distinct keys) to serialize and none of the numbers need it. (Note the
            // directory grain's own GetKeysAsync reads limit<=0 as "all", which is right for the delete
            // path and wrong here; the endpoint is where the caller-facing meaning is fixed.)
            var take = Math.Max(0, limit ?? 100);
            var keys = take == 0 ? [] : await shards.GetKeysAsync(def.Name, take, offset ?? 0);
            return Results.Ok(new TableShardsResponse(
                info.Enabled, info.ShardBy, info.ShardCount, info.ResidentShardCount,
                info.Activations, info.Deactivations, info.RouterSeq, info.RoutedBatches, info.RoutedDeltas,
                info.RouterActive, keys));
        }).RequireAuthorization("Viewer");

        // THE EXPLICIT FULL SCAN. This one does activate every shard in its page — that is the whole
        // reason it is its own endpoint rather than a flag, so that no routine poll can reach it.
        //
        // ?fenced=true (plan 011 D2) upgrades it from a set of observations to a genuine CONSISTENT CUT:
        // the router names the sequence it has forwarded up to and reads every shard without letting
        // another batch through, so no delta from at-or-before the fence is missing and none from after it
        // is present — checkably, since the shards' own deltasApplied must then sum to
        // routedDeltasAtFence. Off by default because the fence pauses the shard tier's ingest for the
        // duration of the scan; that cost is real, is paid only by the caller who asks for it, and buys no
        // extra memory anywhere (nothing versioned is retained).
        //
        // Per-key reads need none of this and get none of it: one key is one grain, already strictly
        // consistent by construction.
        group.MapGet("/{id}/shards/scan", async (string id, int? limit, int? offset, bool? fenced, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. Guard FIRST, against the raw route
            // segment when nothing resolved — an ambiguous name has no single name/tags to authorize
            // against, and answering 409 before the guard would hand an unentitled caller both ids.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            if (def.ShardBy.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("This table is not sharded (shardBy is empty)."));
            }

            var take = limit ?? 100;
            var skip = offset ?? 0;

            if (fenced == true)
            {
                var cut = await shards.ScanFencedAsync(def.Name, take, skip);
                return Results.Ok(new TableShardScanResponse(
                    cut.Shards, cut.Shards.Count, skip, take,
                    Fenced: true, FenceSeq: cut.FenceSeq, RoutedDeltasAtFence: cut.RoutedDeltasAtFence,
                    ShardCount: cut.ShardCount));
            }

            var stats = await shards.ScanAsync(def.Name, take, skip);
            return Results.Ok(new TableShardScanResponse(stats, stats.Count, skip, take));
        }).RequireAuthorization("Viewer");
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not — the same helper
    /// <c>AccessEndpoints.RefuseAsync</c> is, with the resource's tags threaded through. A
    /// <see cref="AccessDecision.RequiresApproval"/> answer is refused here too, carrying its own reason:
    /// filing the request is waves 4-5's job and that machinery does not exist yet, so refusing is the
    /// only answer that cannot be wrong in the meantime.</summary>
    private static async Task<IResult?> RefuseAsync(
        AccessGuard guard, ClaimsPrincipal principal, string action, string scope, IReadOnlyCollection<string>? tags = null)
    {
        var result = await guard.CheckAsync(principal, action, scope, tags);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }

    /// <summary>The stream-relation dictionary a TABLE compiles against — both call sites (POST /validate
    /// and GET /{id}/plan) are table compiles, never pipeline ones.
    ///
    /// <para>Table-over-pipeline: it is sources PLUS every pipeline that has a compiled
    /// <c>OutputFields</c>, the same rule the Orleans registry applies
    /// (<c>PipelineInputs.BuildStreamSchemas</c>) — otherwise the console would report "unknown relation"
    /// for SQL the server would then happily accept, which is worse than either behaviour on its own.
    /// A pipeline appears here under its NAME, so its name occupies the same namespace a source's and a
    /// table's do; the write paths enforce that.</para>
    ///
    /// <para><b>Flavor note.</b> This file is shared, and the Dapr flavor's <c>CatalogStore</c> does NOT
    /// yet compile tables against pipelines (PARITY D6), so on Dapr this endpoint is optimistic: it
    /// validates SQL that the subsequent create would refuse with an unknown-relation diagnostic. The
    /// alternative — keeping validate sources-only — makes the Orleans console reject a legal table,
    /// which is a wrong answer on the flavor where the feature exists.</para></summary>
    private static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(ICatalogFacade registry)
    {
        var schemas = new Dictionary<string, SourceSchema>();

        var pipelines = await registry.GetPipelinesAsync();
        foreach (var p in pipelines)
        {
            if (p.OutputFields.Count == 0) continue;
            schemas[p.Name] = new SourceSchema(p.Name, p.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
        }

        var sources = await registry.GetSourcesAsync();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }
        return schemas;
    }

    // excludeTableId (plan 008 W5, additive/optional — default null keeps the pre-existing /validate
    // call site unchanged): the /{id}/plan endpoint recompiles an EXISTING table's SQL, so its own
    // OutputFields shouldn't appear as a candidate FROM/JOIN target for itself — mirrors
    // RegistryGrain.CompileTableSql/BuildTableSchemas's excludeTableId parameter for updates.
    private static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(ICatalogFacade registry, string? excludeTableId = null)
    {
        var tables = await registry.GetTablesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var t in tables.Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0))
        {
            var fields = t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[t.Name] = new SourceSchema(t.Name, fields);
        }
        return schemas;
    }

    /// <summary>The upsert half of the row-identity report (the other half is GET /{id}/metrics, which
    /// serves the same sentence to the console). A create/update that leaves a table with a degraded
    /// version trail is ACCEPTED — the whole-row fallback is pre-existing behavior and real catalogs rely
    /// on it, so rejecting would break working installs — but it no longer passes in silence: the server
    /// log names the table and says what happened, at the moment the definition that caused it was
    /// written. Draft-friendly in exactly the sense ApplyCompileResult is: it reports, it never blocks.</summary>
    private static void WarnOnDegradedRowIdentity(ILoggerFactory loggers, TableDefinition def)
    {
        var warning = TableRowIdentityWarning.For(def);
        if (warning is null)
        {
            return;
        }

        loggers.CreateLogger("StreamsForge.Api.Tables")
            .LogWarning("Table '{Table}': {Warning}", def.Name, warning);
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
