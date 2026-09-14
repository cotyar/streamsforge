using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Sql;
using StreamsForge.Engine;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Api;

/// <summary>
/// Plan 015 wave 3-A — two gates on every route, the pattern <c>AccessEndpoints</c> established in wave
/// 2-C. Each <c>RequireAuthorization("Viewer"/"Editor")</c> stays exactly where it is (the compatibility
/// floor, pinned by <c>AuthorizationCoverageTests</c>), and each handler additionally asks
/// <see cref="AccessGuard"/> for its own action at its own scope, with the pipeline's <c>Tags</c> so
/// <c>tag:finance</c> scopes match.
///
/// <para><b>The scope is the pipeline's NAME, not the <c>{id}</c> in the route.</b> That is a real
/// decision and it deserves its reason: <c>RegistryGrain.CreatePipelineAsync</c> mints
/// <c>Guid.NewGuid().ToString("n")</c>, so an id is opaque. An entitlement grammar whose whole point is
/// <c>prod-*</c> versus <c>dev-trades</c> is a grammar about names — scoping on the id would leave
/// prefix scopes matching nothing at all on this entity type, i.e. the feature would be present and
/// useless. Where the pipeline cannot be loaded the route segment is used verbatim as the scope, which
/// only ever narrows the answer (the caller is about to get a 404 anyway).
/// Upgrade path if a GUID-scoped grant is ever wanted (an admin UI generating them, say): check the id
/// as a second scope and OR the two, which is strictly additive to every decision made here.</para>
///
/// <para><b>Why the guard runs before the 404.</b> Same reason as in <c>SourcesEndpoints</c>: ordering
/// it the other way lets an unentitled caller enumerate which ids exist by reading 404 against 403.</para>
/// </summary>
public static class PipelinesEndpoints
{
    public static void MapPipelinesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/pipelines");

        // Plan 015 wave 3-A: FILTERED rather than gated at `*` — a caller entitled to `pipeline.read`
        // on `prod-*` should get their three of ten, not a 403 for the whole listing. See the identical
        // note in SourcesEndpoints' list handler.
        group.MapGet("/", async (ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var all = await registry.GetPipelinesAsync();
            var visible = new List<PipelineDefinition>(all.Count);
            foreach (var p in all)
            {
                if (await guard.CheckAsync(principal, Actions.PipelineRead, p.Name, p.Tags) is { IsAllowed: true })
                {
                    visible.Add(SecretsMasker.MaskPipeline(p));
                }
            }

            return Results.Ok(visible);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name (guard first — see EntityLookup's class remarks).
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            return EntityLookup.Reject(hit) ?? Results.Ok(SecretsMasker.MaskPipeline(hit.Value!));
        }).RequireAuthorization("Viewer");

        // Create asks at the NAME BEING CREATED (the id does not exist until CreatePipelineAsync mints
        // one) with the request's own Tags — see the same note in SourcesEndpoints' create handler.
        group.MapPost("/", async (CreatePipelineRequest req, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            if (await RefuseAsync(guard, principal, Actions.PipelineWrite, req.Name ?? "", req.Tags) is { } refusal)
            {
                return refusal;
            }

            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

            // Plan 014 K: 'INSERT INTO <sink> SELECT …' is stripped here, before anything else looks at the
            // SQL — the stored text is the naked query and the named sink is switched on. Unknown target =
            // 400, because a destination that silently goes nowhere is worse than a rejected write. Note the
            // sink list is resolved against BEFORE it is handed to the definition, so the Enabled flip the
            // sugar performs is the one that gets persisted.
            var sinks = req.Sinks ?? [];
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "pipeline");
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
            // not exist, or is not a duplex kind, is a validation error too. entityName is null here
            // (pipeline Id does not exist until CreatePipelineAsync mints one below) — see
            // DuplexSinkCatalogValidation.ValidateAsync's own doc for why that means a templated
            // {name} source name is skipped rather than misreported on this specific call site.
            await DuplexSinkCatalogValidation.ValidateAsync(sinks, entityName: null, registry, sinkErrors);
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

            // Compile-check for diagnostics; draft-friendly — never blocks creation beyond the empty check above.
            var schemas = await BuildSchemasAsync(registry);
            _ = SqlCompiler.Compile(sugar.Sql, schemas);

            var def = new PipelineDefinition
            {
                Name = req.Name,
                Description = req.Description,
                Sql = sugar.Sql,
                CreatedBy = principal.Identity?.Name ?? "",
                Tags = req.Tags ?? [],
                Metadata = req.Metadata ?? [],
                // Plan 014 K: 'sinks', not 'req.Sinks' — same list, but this is the one the sugar just
                // enabled a member of.
                // Plan 009 B2: a freshly-created pipeline has no stored secrets to merge against, so a
                // "***" here (nobody would legitimately send one — the mask value only ever comes back
                // from a prior GET) would just... be a literal wrong credential. Not worth guarding —
                // mirrors sources' own CreateSourceAsync, which does the identical un-merged pass-through.
                Sinks = sinks,
                // Plan 016 wave 2-B: the dependsOn gap — this field never reached the registry over REST
                // before this wave (see EntityPinValidation for the validation just above).
                DependsOn = EntityPinValidation.Normalize(req.DependsOn),
            };
            // Plan 016 wave 1: the registry now refuses a duplicate pipeline name, and it refuses the way
            // every other catalog refusal here works — by throwing. TablesEndpoints has always caught
            // that into a 409; this handler never had to, because nothing it called could refuse. Without
            // the catch the new rule is correct and reaches the caller as a 500, which reads as "the
            // server broke" rather than "you already have one of those". No global exception handler
            // exists in this repo, so the catch has to be here.
            PipelineDefinition created;
            try
            {
                created = await registry.CreatePipelineAsync(def);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }

            // Plan 015 wave 5-B: BeforeJson null, AfterJson the whole masked document. Scoped on the
            // NAME, like the guard above and like every other 015 call site — the id was minted a line
            // ago and a prod-* scope written by an operator matches nothing at all on a GUID.
            //
            // Written only after the create returned: a refusal above created nothing, and an audit row
            // claiming otherwise would be a lie in the one log that must not contain any.
            CatalogChangeAudit.RecordPipeline(
                http, principal, Actions.PipelineWrite, created.Name, before: null, after: created);
            return Results.Created($"/api/pipelines/{created.Id}", SecretsMasker.MaskPipeline(created));
        }).RequireAuthorization("Editor");

        // The STORED pipeline's name and tags decide, never the incoming body's: a caller who could
        // rename or re-tag their way into an entitlement would not be under one.
        group.MapPut("/{id}", async (string id, CreatePipelineRequest req, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetPipelineAsync(id);
            if (await RefuseAsync(guard, principal, Actions.PipelineWrite, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (existing is null)
            {
                return Results.NotFound();
            }

            // Plan 015 wave 5-B: this handler updates the STORED object in place a few lines down, so
            // the audit's "before" has to be taken now — otherwise both sides of the diff would be the
            // same object and every update would record as having changed nothing.
            var before = CatalogChangeAudit.Snapshot(existing);

            // Plan 009 B2: null Sinks means "leave unchanged" (same convention as Tags/Metadata below);
            // a non-null Sinks that carries "***" (round-tripped from a masked GET) is restored from the
            // currently-stored value via MergeSinkSecrets — the PUT counterpart of sources'
            // SecretsMasker.MergeSecrets, so a GET -> edit-something-else -> PUT cycle never clobbers a
            // real NATS credential with the literal mask string.
            var sinks = req.Sinks is null ? existing.Sinks : SecretsMasker.MergeSinkSecrets(req.Sinks, existing.Sinks);

            // Plan 014 K: AFTER the merge (MergeSinkSecrets deep-CLONES, so a flip applied to req.Sinks
            // would land on a list that is then thrown away) and BEFORE any field of the stored entity is
            // touched, so a rejected sugar target leaves 'existing' exactly as it was found — this is the
            // one handler where the 400 can now happen after several assignments would have run.
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "pipeline");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: only when the request is actually touching Sinks (req.Sinks is
            // null means "leave unchanged" — see the comment above) so a PUT that edits some unrelated
            // field on a pipeline whose sinks predate this validation, or were saved back before this
            // rule existed, can never be blocked by a sink spec nobody asked to change.
            if (req.Sinks is not null)
            {
                var sinkErrors = new List<string>();
                SinkTransports.Validate(sinks, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }

                // Plan 019 D2 (wave 019-B2): the catalog-aware half. Unlike POST, the pipeline's id is
                // already known here (it predates this PUT) so a templated {name} source name resolves
                // fully — no skip case applies on this call site.
                await DuplexSinkCatalogValidation.ValidateAsync(sinks, existing.Id, registry, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }
            }

            // Plan 016 wave 2-B: only when the request actually touches DependsOn (null means "leave
            // unchanged") — same reasoning as the Sinks gate just above.
            if (req.DependsOn is not null && EntityPinValidation.Validate(req.DependsOn) is { } pinError)
            {
                return Results.BadRequest(new ErrorResponse(pinError));
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = sugar.Sql;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            existing.Sinks = sinks;
            // Plan 016 wave 2-B: null-means-unchanged, same convention as Tags/Metadata/Sinks above.
            existing.DependsOn = req.DependsOn is null ? existing.DependsOn : EntityPinValidation.Normalize(req.DependsOn);
            // Same reason as the create above: an update that renames a pipeline onto a taken name is
            // refused by the registry, and a refusal is a 409, not a 500.
            PipelineDefinition? updated;
            try
            {
                updated = await registry.UpdatePipelineAsync(existing);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }

            if (updated is not null)
            {
                CatalogChangeAudit.RecordPipeline(
                    http, principal, Actions.PipelineWrite, before.Name, before: before, after: updated);
            }

            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        // One extra catalog read this handler did not do before: without the definition there is no name
        // and no tag list, and delete is the action an operator most wants to scope.
        group.MapDelete("/{id}", async (string id, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetPipelineAsync(id);
            if (await RefuseAsync(guard, principal, Actions.PipelineDelete, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            var removed = await registry.DeletePipelineAsync(id);
            if (removed)
            {
                CatalogChangeAudit.RecordPipeline(
                    http, principal, Actions.PipelineDelete, existing?.Name ?? id, before: existing, after: null);
            }

            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetPipelineAsync(id);
            if (await RefuseAsync(guard, principal, Actions.PipelineControl, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            var updated = await registry.SetPipelineStatusAsync(id, PipelineStatus.Running);
            // Plan 009 B2: this handler returns the entity too — same secrets-lite masking as every
            // other read path (found live: this endpoint was leaking an unmasked Sinks credential
            // before this fix, since it returns the entity but isn't a create/update handler).
            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetPipelineAsync(id);
            if (await RefuseAsync(guard, principal, Actions.PipelineControl, existing?.Name ?? id, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            var updated = await registry.SetPipelineStatusAsync(id, PipelineStatus.Stopped);
            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        // `pipeline.write` at `*`: /validate names no pipeline (a ValidateRequest carries SQL and
        // nothing else — see the note below), and `*` is answered only by a `*`-scoped grant. A caller
        // entitled solely to `dev-*` therefore cannot use the SQL editor's validate button; widening `*`
        // to be satisfied by any scope would defeat every Deny written at `*`, which is the one
        // direction not worth going.
        group.MapPost("/validate", async (ValidateRequest req, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            if (await RefuseAsync(guard, principal, Actions.PipelineWrite, "*") is { } refusal)
            {
                return refusal;
            }

            // Plan 014 K: the editor validates the same text the save path would store, so the sugar is
            // stripped here too. What this endpoint canNOT check is whether the named sink exists — a
            // ValidateRequest carries SQL and nothing else, and inventing a sinks field on it to make the
            // check possible would put a whole entity in a request whose entire job is "does this text
            // compile". The unknown-target 400 stays where the entity is: POST/PUT.
            var sugar = SinkSugar.Desugar(req.Sql);
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.Ok(new ValidateResponse(
                    false,
                    // Line/column 1: the sugar only ever matches at the start of the statement, so that is
                    // literally where every diagnostic it produces is anchored.
                    [.. sugar.Diagnostics.Select(d => new SqlDiagnosticDto(d, 1, 1, nameof(DiagnosticSeverity.Error)))],
                    null,
                    []));
            }

            var schemas = await BuildSchemasAsync(registry);
            var result = SqlCompiler.Compile(sugar.Sql, schemas);
            return Results.Ok(new ValidateResponse(
                result.Ok,
                result.Diagnostics.Select(d => new SqlDiagnosticDto(d.Message, d.Line, d.Column, d.Severity.ToString())).ToList(),
                result.PlanSummary,
                result.SourceNames.ToList()));
        }).RequireAuthorization("Editor");

        // Downloadable, self-contained .proto for this pipeline: PipelineDefinition doesn't persist an
        // output schema (unlike TableDefinition), so its SQL is recompiled here to get one. 409 with
        // compile diagnostics if the SQL doesn't currently compile.
        group.MapGet("/{id}/proto", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver. This site used to answer 404 when two
            // pipelines shared the queried name; it now answers 409 naming both ids. Guard FIRST,
            // against the raw route segment — see EntityLookup's class remarks for why that order is
            // the security-relevant part.
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var schemas = await BuildSchemasAsync(registry);
            var result = SqlCompiler.Compile(def.Sql, schemas);
            if (!result.Ok || result.OutputSchema is null)
            {
                var message = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                return Results.Conflict(new ErrorResponse(
                    string.IsNullOrEmpty(message) ? "Pipeline SQL does not currently compile." : message));
            }

            var fields = EntitySchemas.FromOutputSchema(result.OutputSchema);
            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.PipelineKey(def.Id), fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(def.Name, fields, numbers);
            var protoText = ProtoFileBuilder.Build("pipeline", def.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        // Plan 008 W5: lineage + execution-plan view for the console's React Flow page. Always the
        // logical view (Physical: false) — pipelines have no partitioned stage/edge dataflow graph, see
        // PlanEndpointsLogic's class doc. Recompiled fresh every call; never persisted.
        group.MapGet("/{id}/plan", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            // Plan 016 wave 1: id-or-name (guard first — see EntityLookup's class remarks).
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var schemas = await BuildSchemasAsync(registry);
            return Results.Ok(PlanEndpointsLogic.BuildPipelinePlan(def, schemas));
        }).RequireAuthorization("Viewer");

        // .Produces<>() for the same reason as TablesEndpoints' /rows — see the note there.
        //
        // Plan 015 wave 3-A: this handler and /metrics below gained a catalog read they did not have.
        // It buys the name and the tags, without which a scoped entitlement could not match here at all
        // and this route would be the hole in the fence — readable by anyone the coarse Viewer policy
        // admits while /{id} next to it is scoped. It is also the read every neighbouring route in this
        // file already does, and it keeps the pre-existing behaviour on an unknown id (no 404 added).
        group.MapGet("/{id}/results", async (string id, int? limit, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IPipelineReadFacade pipelines) =>
        {
            // Plan 016 wave 1: id-or-name. This route deliberately does NOT 404 on an unknown id (see
            // the note above), so an unresolved query falls through to the facade with the raw segment,
            // exactly as before — only an AMBIGUOUS name is answered, and it gets the 409 every other
            // route gives it. Guard first, as everywhere.
            var hit = await EntityLookup.PipelineAsync(registry, id);
            var def = hit.Value;
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, def?.Name ?? id, def?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (hit.Outcome == StreamsForge.AppCore.EntityRefOutcome.Ambiguous)
            {
                return EntityLookup.Reject(hit)!;
            }

            id = def?.Id ?? id;

            return Results.Ok(await pipelines.GetRecentResultsAsync(id, limit ?? 100));
        }).Produces<List<ResultEnvelope>>().RequireAuthorization("Viewer");

        // Plan 012: the recent-results buffer as a file — the pipeline twin of /api/tables/{id}/rows.csv.
        group.MapGet("/{id}/results.csv", async (string id, int? limit, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IPipelineReadFacade pipelines) =>
        {
            // Plan 016 wave 1: id-or-name (guard first — see EntityLookup's class remarks).
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            var results = await pipelines.GetRecentResultsAsync(def.Id, Math.Clamp(limit ?? 10_000, 1, 100_000));
            var csv = CsvExport.Rows(results.Select(r => r.Row));
            return Results.File(Encoding.UTF8.GetBytes(csv), "text/csv; charset=utf-8", $"{def.Name}.csv");
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IPipelineReadFacade pipelines) =>
        {
            // Plan 016 wave 1: id-or-name, on the same terms as /results above — this route deliberately
            // does NOT 404 on an unknown id, so an unresolved query still falls through to the facade
            // with the raw segment; only an AMBIGUOUS name is answered, and only after the guard.
            var hit = await EntityLookup.PipelineAsync(registry, id);
            var def = hit.Value;
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, def?.Name ?? id, def?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (hit.Outcome == StreamsForge.AppCore.EntityRefOutcome.Ambiguous)
            {
                return EntityLookup.Reject(hit)!;
            }

            return Results.Ok(await pipelines.GetMetricsAsync(def?.Id ?? id));
        }).RequireAuthorization("Viewer");

        // GET /{id}/replay — plan 026 wave 3 D6: what this pipeline's result-batch replay log currently
        // retains (memory + disk). id-or-name via EntityLookup, same as every other GET route in this
        // file — the facade itself is keyed by pipeline id (IReplayStatusFacade.GetPipelineAsync's own
        // doc), which EntityLookup already resolved to. NotSupportedException (Dapr, until PARITY D11
        // closes) becomes 501 rather than a bare 500.
        group.MapGet("/{id}/replay", async (string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IReplayStatusFacade replay) =>
        {
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
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
                var status = await replay.GetPipelineAsync(def.Id);
                return status is null ? Results.NotFound() : Results.Ok(status);
            }
            catch (NotSupportedException ex)
            {
                return Results.Json(new ErrorResponse(ex.Message), statusCode: StatusCodes.Status501NotImplemented);
            }
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

    internal static async Task<Dictionary<string, SourceSchema>> BuildSchemasAsync(ICatalogFacade registry)
    {
        var sources = await registry.GetSourcesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }

        return schemas;
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
