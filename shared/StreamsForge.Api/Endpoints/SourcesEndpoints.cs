using System.Globalization;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Ingest;
using StreamsForge.AppCore.Json;
using StreamsForge.Host.Auth;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Api;

/// <summary>
/// Plan 015 wave 3-A — two gates on every route, the pattern <c>AccessEndpoints</c> established in wave
/// 2-C. The <c>RequireAuthorization("Viewer"/"Editor")</c> at each map site stays exactly as it was (it
/// is the compatibility floor, and <c>AuthorizationCoverageTests</c> pins it), and each handler
/// additionally asks <see cref="AccessGuard"/> for its own action AT THIS SOURCE, passing the stored
/// definition's <c>Tags</c> so <c>tag:finance</c>-scoped entitlements match.
///
/// <para><b>The scope is the source's name</b>, which is also the route segment — sources are addressed
/// by name everywhere, so unlike pipelines and tables there is no id/name choice to make here.</para>
///
/// <para><b>Why the guard runs before the 404.</b> Every handler that needs Tags looks the source up
/// first and then guards on <c>src?.Name ?? name</c>, refusing before it answers 404. Ordering it the
/// other way would let an unentitled-but-authenticated caller enumerate which source names exist by
/// reading 404 against 403. The cost is that a caller who holds the entitlement gets the same 404 they
/// got before, and a caller who does not gets 403 whether or not the name is real.</para>
/// </summary>
public static class SourcesEndpoints
{
    public static void MapSourcesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        // D-H: every read path masks secrets (URL header values; gRPC password/token) as "***".
        //
        // Plan 015 wave 3-A: FILTERED, not gated at `*`. A caller entitled to `source.read` on `prod-*`
        // asking at `*` would be refused the whole listing, which is the wrong answer to "show me what I
        // can see" — they should see their three of ten. So the coarse Viewer policy stays and each
        // entry is then dropped unless the caller has a read entitlement for it. A caller entitled to
        // nothing gets an empty list rather than a 403: that is the same statement, and it is the one a
        // console can render.
        group.MapGet("/", async (ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var all = await registry.GetSourcesAsync();
            var visible = new List<SourceDefinition>(all.Count);
            foreach (var src in all)
            {
                if (await guard.CheckAsync(principal, Actions.SourceRead, src.Name, src.Tags) is { IsAllowed: true })
                {
                    visible.Add(SecretsMasker.Mask(src));
                }
            }

            return Results.Ok(visible);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{name}", async (string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            return src is null ? Results.NotFound() : Results.Ok(SecretsMasker.Mask(src));
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this source: DescriptorFactory's schema plus the
        // DynamicStreamService streaming contract, ready for a client to compile standalone (see
        // tools/generate-client.sh). Field numbers always come from ICatalogFacade.EnsureFieldNumbersAsync
        // so they stay identical to what the dynamic gRPC reflection surface uses.
        group.MapGet("/{name}/proto", async (string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.SourceKey(src.Name), src.Fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(src.Name, src.Fields, numbers);
            var protoText = ProtoFileBuilder.Build("source", src.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        // Create asks at the NAME BEING CREATED, with the body's own Tags: on a create there is no
        // stored object to read them from, and a tag the caller invents on a source they are creating is
        // a tag they could equally have added a second later with a PUT — so honouring it costs nothing
        // that PUT does not already cost. (Contrast the transport probe, which passes no tags at all:
        // there, nothing is ever stored, so the tag would be pure self-assertion.)
        group.MapPost("/", async (SourceDefinition def, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, def.Name, def.Tags) is { } refusal)
            {
                return refusal;
            }

            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            var existing = await registry.GetSourceAsync(def.Name);
            if (existing is not null)
            {
                return Results.BadRequest(new ErrorResponse("source name already exists"));
            }

            // D-H: incoming "***" values have nothing to restore from on a create (existing is
            // null here) — MergeSecrets is still run for compositional symmetry with PUT below
            // (it's a no-op when `stored` is null).
            var effective = SecretsMasker.MergeSecrets(def, existing);
            try
            {
                await registry.UpsertSourceAsync(effective);
            }
            catch (InvalidOperationException ex)
            {
                // A registry-side refusal with an operator-readable reason (e.g. a `crdt` source on a host
                // without the crdt plugin) — surface the message, not an empty 500.
                return Results.BadRequest(new ErrorResponse(ex.Message));
            }

            // Plan 016 wave 2-B: re-read rather than return `effective` — Revision/SchemaRevision are
            // assigned INSIDE the registry (RegistryGrain.UpsertSourceAsync / CatalogStore.UpsertSourceAsync),
            // past Orleans' by-value grain-call boundary, so `effective` here is stuck at whatever the
            // caller sent (0 on a fresh source) even though the stored record is now Revision 1. Re-reading
            // is what makes the two flavours agree too — Dapr's CatalogStore is called in-process and
            // mutates `def`/`effective` in place, so it would otherwise report the right number by
            // accident while Orleans reported 0 for the identical request.
            var stored = await registry.GetSourceAsync(effective.Name) ?? effective;

            // Plan 015 wave 5-B: BeforeJson is null on a create, and the whole (masked) document is the
            // after — the STORED definition, not the pre-write input, so a masked audit row shows the
            // revision that was actually assigned. Written only here, after the store said yes — the
            // guard's "allowed" row was written before the validation above could still have answered 400.
            CatalogChangeAudit.RecordSource(
                http, principal, Actions.SourceWrite, stored.Name, before: null, after: stored);
            return Results.Created($"/api/sources/{stored.Name}", SecretsMasker.Mask(stored));
        }).RequireAuthorization("Editor");

        // The STORED definition's Tags decide, not the incoming body's: the caller is asking to change
        // an object that already exists, and letting the request's own tag list widen the entitlement
        // that authorizes the request would make every tag scope self-service. A caller who legitimately
        // holds the write may of course then re-tag it, which is the same authority they already had.
        // Plan 016 wave 2-B: `?allowBreaking` — interactive editing stays PERMISSIVE by default (a
        // breaking field change is allowed exactly as it always was), and `?allowBreaking=false` is how a
        // caller OPTS INTO the gate. Note the polarity: the query parameter is how a caller asks to be
        // protected, not how they ask to be allowed to break something. Promotion (config import) is the
        // one that defaults to gated — see ConfigImportService, a distinct code path from this one.
        group.MapPut("/{name}", async (string name, SourceDefinition def, bool? allowBreaking, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, existing?.Name ?? name, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (existing is null)
            {
                return Results.NotFound();
            }

            def.Name = name;
            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            // `allowBreaking == false` is the only value that turns the gate on — missing, true, or any
            // other value stays permissive. SchemaCompatibility.Compare is the same removal/type-change
            // walk the catalog-import gate (wave 3) will reuse, so "compatible" means one thing everywhere.
            if (allowBreaking == false)
            {
                var compat = SchemaCompatibility.Compare(existing.Fields, def.Fields);
                if (!compat.IsCompatible)
                {
                    return Results.Json(
                        new SchemaBreakingChangeResponse(
                            $"source '{name}': schema change is breaking", compat.BreakingReasons),
                        statusCode: StatusCodes.Status409Conflict);
                }
            }

            // D-H CRITICAL invariant: an incoming "***" (the SPA's GET-then-edit-then-PUT-whole-
            // object cycle round-tripping a masked value verbatim) is replaced with the STORED real
            // secret before it ever reaches UpsertSourceAsync — never persist the literal mask.
            var effective = SecretsMasker.MergeSecrets(def, existing);
            try
            {
                await registry.UpsertSourceAsync(effective);
            }
            catch (InvalidOperationException ex)
            {
                // A registry-side refusal with an operator-readable reason (e.g. a `crdt` source on a host
                // without the crdt plugin) — surface the message, not an empty 500.
                return Results.BadRequest(new ErrorResponse(ex.Message));
            }

            // Plan 016 wave 2-B: re-read rather than return `effective` — see the identical note on the
            // POST handler above for why (the counters are assigned past Orleans' by-value boundary, and
            // re-reading is also what makes Orleans and Dapr agree on what this response reports).
            var stored = await registry.GetSourceAsync(effective.Name) ?? effective;

            // Plan 015 wave 5-B: an update records only the top-level properties that MOVED, on both
            // sides — see CatalogChangeAudit for why that beats two near-identical whole documents, and
            // for why the diff is decided on the unmasked pair so a rotated credential is not reported
            // as no change. 'existing' is untouched by this handler (unlike the pipeline/table PUTs), so
            // it is still the pre-write state here. 'after' is the STORED definition, not the pre-write
            // input, so the audit row records what was actually persisted (revision included).
            CatalogChangeAudit.RecordSource(
                http, principal, Actions.SourceWrite, existing.Name, before: existing, after: stored);
            return Results.Ok(SecretsMasker.Mask(stored));
        }).RequireAuthorization("Editor");

        // One extra catalog read that this handler did not do before, and it is worth it: without the
        // definition there are no Tags, so a `tag:sandbox` entitlement to delete could never match — and
        // delete is precisely the action an operator most wants to scope. The read is the same
        // GetSourceAsync every other route in this file already does.
        group.MapDelete("/{name}", async (string name, HttpContext http, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var existing = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceDelete, existing?.Name ?? name, existing?.Tags) is { } refusal)
            {
                return refusal;
            }

            var removed = await registry.DeleteSourceAsync(name);
            if (removed)
            {
                // Plan 015 wave 5-B: AfterJson is null on a delete, and BeforeJson carries the WHOLE
                // (masked) document — after a delete this row is the only surviving copy of what was
                // there, which is exactly the case a diff cannot serve.
                CatalogChangeAudit.RecordSource(
                    http, principal, Actions.SourceDelete, existing?.Name ?? name, before: existing, after: null);
            }

            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        // GET /{name}/status — connector runtime status (D-C). null (no connector status tracked,
        // e.g. generator-kind sources) -> 204; missing source -> 404.
        group.MapGet("/{name}/status", async (string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IConnectorStatusFacade statusFacade) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            var status = src is null ? null : await statusFacade.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(status),
            };
        }).RequireAuthorization("Viewer");

        // GET /{name}/replay — plan 026 wave 3 D6: what this source's replay log currently retains
        // (memory + disk). Sources have no id, so the route segment IS the scope, same as every other
        // route in this file. NotSupportedException (Dapr, until PARITY D11 closes) becomes 501 with the
        // facade's own message rather than a bare 500 — this route is expected to be flavor-dependent.
        group.MapGet("/{name}/replay", async (string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IReplayStatusFacade replay) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            try
            {
                var status = await replay.GetSourceAsync(name);
                return status is null ? Results.NotFound() : Results.Ok(status);
            }
            catch (NotSupportedException ex)
            {
                return Results.Json(new ErrorResponse(ex.Message), statusCode: StatusCodes.Status501NotImplemented);
            }
        }).RequireAuthorization("Viewer");

        // ---- Client-push ingress (plan 008 W4). POST holds NO policy of its own — every branch below
        // is a 1:1 mapping of IngestOutcome onto an HTTP status per IngestModels.cs's own doc comments;
        // all the actual admission/coercion decisions live in IIngressFacade/AppCore.Ingest. ----

        // Plan 009 A1.2: this route deliberately does NOT `.RequireAuthorization("Editor")` — a
        // machine pushing telemetry may authenticate with a per-source push key instead of an Editor
        // JWT (AllowAnonymous below, then a manual dual check: Editor JWT OR a valid X-SF-Ingest-Key
        // for THIS source). An ingest source with zero configured keys is JWT-only, not open — see
        // IsAuthorizedToPushAsync/IIngressFacade.ValidateKeyAsync's own doc comment.
        // Plan 015 wave 3-A: the entitlement check added here sits on the JWT BRANCH ONLY (see
        // IsAuthorizedToPushAsync) — the ingest-key branch is a machine credential scoped to one source
        // by construction and has no principal to resolve entitlements for.
        group.MapPost("/{name}/events", async (
            string name, IngestEventsRequest req, IIngressFacade ingress, ICatalogFacade registry,
            IAuthorizationService authz, AccessGuard guard, HttpContext http) =>
        {
            if (!await IsAuthorizedToPushAsync(name, http, authz, guard, ingress))
            {
                return Results.Unauthorized();
            }

            // Plan 009 A1.1: body wins over the header when both are present (IngestEventsRequest's
            // own doc comment).
            var idempotencyKey = !string.IsNullOrEmpty(req.IdempotencyKey)
                ? req.IdempotencyKey
                : http.Request.Headers["Idempotency-Key"].FirstOrDefault();

            // Wishlist "explicit key retraction through ingest": a "_retract" row is only meaningful
            // to a LATEST BY consumer (TableLatestByOp is the only op that tracks "the current row for
            // a key" — see its class doc). Validated HERE, before admission, because IIngressFacade's
            // real implementations (OrleansIngressFacade/DaprIngressFacade) have no catalog access and
            // their call into IngressRowAcceptance.AcceptBatch is a frozen call site that cannot be
            // widened to take one — this REST handler is the one place upstream of admission that both
            // sees the SQL catalog and can still turn a bad request into a 400 instead of a corrupted
            // or silently-ignored table. KNOWN GAP, stated plainly: the gRPC ingest path
            // (IngestGrpcService) calls IIngressFacade.PushAsync directly and does not run this gate —
            // a retraction pushed that way is protected only by TableReduceOp's unmatched-retraction
            // handling (reports nothing, never a wrong number — see its own doc) and TableLatestByOp's
            // unknown-key no-op; safe, but silent, exactly what this gate exists to avoid on the REST
            // path the wishlist actually asked for.
            var retractRowIndexes = CollectRetractRowIndexes(req.Events);
            if (retractRowIndexes.Count > 0)
            {
                var offendingTable = RetractConsumerValidation.FindNonLatestByConsumer(
                    name, await registry.GetSourcesAsync(), await registry.GetTablesAsync());
                if (offendingTable is not null)
                {
                    var message = $"\"_retract\" is only valid when every running table reading source '{name}' directly is a LATEST BY table; '{offendingTable}' is not";
                    var retractErrors = retractRowIndexes.Select(i => $"row {i}: {message}").ToList();

                    if (!req.Partial)
                    {
                        return Results.Json(
                            new IngestErrorResponse($"{retractErrors.Count} row(s) failed retract validation", 0, retractErrors),
                            statusCode: StatusCodes.Status400BadRequest);
                    }

                    // Partial: admit everything else, fold the offending rows into Invalid/RowErrors
                    // exactly like a coercion failure would (IngestModels.cs: "Accepted + Dropped +
                    // Invalid + Duplicate accounts for every row in the request") — an admitted
                    // retraction into a wrongly-shaped table is not a lesser evil than dropping it.
                    var offendingSet = new HashSet<int>(retractRowIndexes);
                    var filteredEvents = req.Events.Where((_, i) => !offendingSet.Contains(i)).ToList();
                    var partialResult = await ingress.PushAsync(name, filteredEvents, req.Partial, idempotencyKey);
                    partialResult.Invalid += retractErrors.Count;
                    partialResult.RowErrors = [.. partialResult.RowErrors, .. retractErrors];
                    return await BuildEventsResponseAsync(name, partialResult, ingress, http);
                }
            }

            var result = await ingress.PushAsync(name, req.Events, req.Partial, idempotencyKey);
            return await BuildEventsResponseAsync(name, result, ingress, http);
        }).AllowAnonymous(); // real gate is the manual dual check above, not route-level authorization

        // GET /{name}/ingest — ingress buffer status (plan 008 W4). Deliberately NOT overloaded onto
        // /status above, which is the connector-runtime surface; reuses the same three-way
        // NotFound/NoContent/Ok decision (SourceSchemaService.DecideStatusOutcome's bool overload).
        group.MapGet("/{name}/ingest", async (string name, ICatalogFacade registry, IIngressFacade ingress) =>
        {
            var src = await registry.GetSourceAsync(name);
            var status = src is null ? null : await ingress.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status is not null) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(ToIngestStatusResponse(status!)),
            };
        }).RequireAuthorization("Viewer");

        // ---- Per-source push keys (plan 009 A1.2). All three Editor — key material is a credential-
        // management concern, the same bar as every other source-mutating endpoint in this file. ----

        // POST generates a fresh secret, stores only its salted hash (AppCore/Auth/PasswordHasher —
        // the same primitive user passwords use), and returns the plaintext secret EXACTLY ONCE. There
        // is no other way to ever read it back — GET below only ever returns identity + usage.
        group.MapPost("/{name}/ingest/keys", async (string name, CreateIngestKeyRequest req, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.Kind != SourceKinds.Ingest)
            {
                return Results.Json(new ErrorResponse($"source '{name}' is not ingest-kind"), statusCode: StatusCodes.Status409Conflict);
            }

            var secret = GenerateIngestKeySecret();
            var (hash, salt) = PasswordHasher.Hash(secret);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var key = new IngestKey
            {
                Id = Guid.NewGuid().ToString("N"),
                Hash = hash,
                Salt = salt,
                Label = req.Label ?? "",
                CreatedAtMs = nowMs,
            };

            src.Ingest ??= new IngestConfig();
            src.Ingest.Keys.Add(key);
            await registry.UpsertSourceAsync(src);

            return Results.Ok(new CreatedIngestKeyResponse(key.Id, key.Label, secret, key.CreatedAtMs));
        }).RequireAuthorization("Editor");

        // GET lists identity + best-effort usage only (IngestKeyUsageTracker.GetLastUsedMs overlays
        // this replica's own in-memory view on top of whatever was last durably stored — see that
        // class's doc comment for why LastUsedMs isn't round-tripped through UpsertSourceAsync on
        // every push).
        // source.write, not source.read — the LegacyEquivalenceMatrix row says so, and the reason is
        // that this route is Editor-gated today: it lists credential identities, which is a
        // key-management surface even though it returns no secret material.
        group.MapGet("/{name}/ingest/keys", async (string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, IngestKeyUsageTracker usage) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            var keys = (src.Ingest?.Keys ?? [])
                .Select(k => new IngestKeyResponse(k.Id, k.Label, k.CreatedAtMs, Math.Max(k.LastUsedMs, usage.GetLastUsedMs(name, k.Id))))
                .ToList();
            return Results.Ok(keys);
        }).RequireAuthorization("Editor");

        // DELETE revokes — the key immediately stops authorizing pushes to this source (ValidateKeyAsync
        // only ever checks IngestConfig.Keys as currently stored).
        group.MapDelete("/{name}/ingest/keys/{id}", async (string name, string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            var removed = (src.Ingest?.Keys.RemoveAll(k => k.Id == id) ?? 0) > 0;
            if (!removed)
            {
                return Results.NotFound();
            }

            await registry.UpsertSourceAsync(src);
            return Results.NoContent();
        }).RequireAuthorization("Editor");

        // ---- Schema helper endpoints for the UI (Editor — even mapping-validate/derive-openapi,
        // which don't themselves dial out, since they're part of the connector-authoring flow that
        // from-remote (which DOES dial out) also belongs to; keeps all three under one auth policy). ----

        // All three ask source.write at `*`, not at a source: they belong to the connector-AUTHORING
        // flow, so at the moment they run there is no source yet to name. `*` is answered only by a
        // `*`-scoped grant (PermissionEvaluator's own doc), which means a caller entitled solely to
        // `dev-*` cannot use the schema helpers — the honest reading, and the alternative (widening `*`
        // to be satisfied by any scope) would defeat every Deny written at `*`.
        group.MapPost("/schema/mapping-validate", async (MappingValidateRequest request, ClaimsPrincipal principal, AccessGuard guard) =>
        {
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, "*") is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(SourceSchemaService.ValidateMappingDocument(request));
        }).RequireAuthorization("Editor");

        group.MapPost("/schema/derive-openapi", async (SchemaDeriveRequest request, ClaimsPrincipal principal, AccessGuard guard, CancellationToken ct) =>
        {
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, "*") is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(await SourceSchemaService.DeriveOpenApiAsync(request, ct));
        }).RequireAuthorization("Editor");

        // Dials out to a remote gRPC endpoint per the request body — Editor-only (D-G).
        group.MapPost("/schema/from-remote", async (RemoteSchemaRequest request, ClaimsPrincipal principal, AccessGuard guard, CancellationToken ct) =>
        {
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, "*") is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(await SourceSchemaService.FromRemoteAsync(request, ct));
        }).RequireAuthorization("Editor");
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not — the same helper
    /// <c>AccessEndpoints.RefuseAsync</c> is, with the resource's tags threaded through.
    ///
    /// <para>A <see cref="AccessDecision.RequiresApproval"/> answer is refused here too, carrying its own
    /// reason ("grant … requires approval"). Filing the request instead is waves 4-5's job and that
    /// machinery does not exist yet; refusing is the only answer that cannot be wrong in the meantime,
    /// and it fails closed rather than reading "needs a second pair of eyes" as yes.</para></summary>
    private static async Task<IResult?> RefuseAsync(
        AccessGuard guard, ClaimsPrincipal principal, string action, string scope, IReadOnlyCollection<string>? tags = null)
    {
        var result = await guard.CheckAsync(principal, action, scope, tags);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }

    private static IngestStatusResponse ToIngestStatusResponse(IngestStatus s) => new(
        s.Policy.ToString(),
        s.CapacityRows,
        s.DepthRows,
        s.MaxBatchRows,
        s.TotalAccepted,
        s.TotalRejected,
        s.TotalDropped,
        s.TotalInvalid,
        s.TotalPublished,
        s.DownstreamDropped,
        s.LastPushMs,
        s.TotalDuplicate,
        s.InstanceId,
        s.Aggregated);

    /// <summary>Plan 009 A1.2: the dual auth check POST /{name}/events runs instead of a route-level
    /// <c>RequireAuthorization("Editor")</c> — an Editor JWT (checked via the REAL "Editor" policy
    /// through <see cref="IAuthorizationService"/>, not a hand-rolled role check, so Admin still
    /// satisfies it exactly like every other Editor-gated endpoint) always suffices; otherwise the
    /// <c>X-SF-Ingest-Key</c> header is validated against THIS source's configured keys. An ingest
    /// source with zero configured keys is JWT-only, not open — that rule lives in
    /// <see cref="IIngressFacade.ValidateKeyAsync"/>, not here, so REST and gRPC can't drift.
    ///
    /// <para>Plan 015 wave 3-A added the entitlement check, and it sits on the JWT BRANCH ONLY: a JWT
    /// holder must satisfy the Editor policy AND hold <see cref="Actions.SourceIngest"/> for this
    /// source, while a key holder is unaffected — an ingest key already names exactly one source, which
    /// is a narrower scope than any entitlement could express. A JWT that fails the entitlement falls
    /// THROUGH to the key branch rather than short-circuiting, so a caller who presents both a
    /// weakly-scoped token and a valid key still pushes.</para>
    ///
    /// <para>ponytail: the check passes NO tags, so a <c>tag:…</c>-scoped <c>source.ingest</c>
    /// entitlement will not match on this route. Ceiling stated plainly: tag-scoped ingest is the one
    /// scope form this route cannot express. The reason is that tags live on the stored
    /// <see cref="SourceDefinition"/> and reading it would add a catalog round trip to the hottest route
    /// in the platform — one per push batch, on the path whose whole design (IngressRowAcceptance,
    /// the drop-oldest buffer) exists to avoid exactly that. Upgrade path: carry the tags on whatever
    /// <see cref="IIngressFacade"/> already resolves per source, and pass them here.</para></summary>
    private static async Task<bool> IsAuthorizedToPushAsync(
        string sourceName, HttpContext http, IAuthorizationService authz, AccessGuard guard, IIngressFacade ingress)
    {
        var editorResult = await authz.AuthorizeAsync(http.User, "Editor");
        if (editorResult.Succeeded
            && (await guard.CheckAsync(http.User, Actions.SourceIngest, sourceName)).IsAllowed)
        {
            return true;
        }

        var presentedKey = http.Request.Headers["X-SF-Ingest-Key"].FirstOrDefault();
        return await ingress.ValidateKeyAsync(sourceName, presentedKey);
    }

    /// <summary>Plan 009 A1.2: cryptographically secure secret generation — 32 random bytes, hex-
    /// encoded, with a stable "sfk_" prefix so a leaked secret is recognizable as a StreamsForge ingest
    /// key at a glance (same spirit as Stripe/GitHub-style prefixed tokens).</summary>
    private static string GenerateIngestKeySecret() => "sfk_" + RandomNumberGenerator.GetHexString(64, lowercase: true);

    /// <summary>Wishlist "explicit key retraction through ingest": indexes of every row in
    /// <paramref name="events"/> that asks for a retraction, ahead of IngressRowAcceptance.Accept ever
    /// running (that happens inside <see cref="IIngressFacade.PushAsync"/>, after this validate-time
    /// gate) — so this reads the raw request body's own value for
    /// <c>IngressRowAcceptance.RetractField</c> and normalizes it itself
    /// (<see cref="JsonValueNormalizer.Normalize"/>) rather than reusing the coerced/normalized row
    /// IngressRowAcceptance would produce, which does not exist yet at this point in the request.</summary>
    private static List<int> CollectRetractRowIndexes(IReadOnlyList<Dictionary<string, object?>> events)
    {
        var indexes = new List<int>();
        for (var i = 0; i < events.Count; i++)
        {
            if (events[i].TryGetValue(IngressRowAcceptance.RetractField, out var raw)
                && JsonValueNormalizer.Normalize(raw) is true)
            {
                indexes.Add(i);
            }
        }
        return indexes;
    }

    /// <summary>The POST /{name}/events response mapping, extracted unchanged from the handler so the
    /// retract-validation branch above (which may itself call <see cref="IIngressFacade.PushAsync"/>,
    /// on the FILTERED batch, before merging its own row errors in) can reuse the exact same
    /// IngestOutcome -&gt; HTTP mapping as the plain path below it — one switch, not two copies that
    /// could drift.</summary>
    private static async Task<IResult> BuildEventsResponseAsync(string name, IngestResult result, IIngressFacade ingress, HttpContext http)
    {
        switch (result.Outcome)
        {
            case IngestOutcome.Accepted:
                // DepthRows/CapacityRows aren't on IngestResult (that contract is frozen) — a
                // second, in-memory-only GetStatusAsync call (no extra I/O; same buffer instance)
                // fills them in for the 202 body.
                var status = await ingress.GetStatusAsync(name);
                return Results.Json(
                    new IngestAcceptedResponse(
                        result.Accepted, result.Dropped, result.Invalid, status?.DepthRows ?? 0, status?.CapacityRows ?? 0,
                        result.Duplicate, result.Replayed),
                    statusCode: StatusCodes.Status202Accepted);

            case IngestOutcome.Invalid:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "one or more rows failed coercion", 0, result.RowErrors),
                    statusCode: StatusCodes.Status400BadRequest);

            case IngestOutcome.NotFound:
                return Results.NotFound();

            case IngestOutcome.WrongKind:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? $"source '{name}' is not ingest-kind", 0, result.RowErrors),
                    statusCode: StatusCodes.Status409Conflict);

            case IngestOutcome.TooLarge:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "batch exceeds the source's ingest limits", 0, result.RowErrors),
                    statusCode: StatusCodes.Status413PayloadTooLarge);

            case IngestOutcome.Overloaded:
            default:
                // Retry-After: whole seconds clamped to [1,30] — IngestResult.RetryAfterMs is the
                // unclamped estimate (IngressAdmission.Decision doc), this is where it becomes an
                // honest HTTP header.
                var retryAfterSeconds = Math.Clamp((int)Math.Ceiling(result.RetryAfterMs / 1000.0), 1, 30);
                http.Response.Headers.RetryAfter = retryAfterSeconds.ToString(CultureInfo.InvariantCulture);
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "ingress buffer has no room for this batch", retryAfterSeconds * 1000, result.RowErrors),
                    statusCode: StatusCodes.Status429TooManyRequests);
        }
    }
}
