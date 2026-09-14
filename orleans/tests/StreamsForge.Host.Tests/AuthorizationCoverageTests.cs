using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 0 spike, and the highest-leverage test in the plan: build a <see cref="WebApplication"/>
/// in-process, call <see cref="StreamsForgeApiExtensions.MapStreamsForgeApi"/>, and read the resulting
/// <see cref="EndpointDataSource"/>. It never calls <c>Run()</c>, never binds a port and never starts a
/// silo — the whole REST surface's authorization metadata becomes a table-driven assertion, which today
/// is pinned by nothing at all.
///
/// <para><c>WebApplicationFactory</c> was declined: it needs both flavours' <c>Program.cs</c> startable
/// without their runtimes, which is a refactor of the two most dangerous files in the repo.</para>
/// </summary>
public class AuthorizationCoverageTests
{
    private static IReadOnlyList<Endpoint> BuildEndpoints()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);
        RegisterHandlerDependencies(builder.Services);

        var app = builder.Build();
        // No protos dir, no docs file, no SPA dist: those three route groups are host-specific and
        // deliberately out of the authorization surface this test pins.
        app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-authz-spike-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        // The routes live on the WebApplication's own IEndpointRouteBuilder.DataSources until the
        // routing middleware folds them into the composite EndpointDataSource at Run() time — and Run()
        // is precisely what this test refuses to call. So read the data sources directly.
        return ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints).ToList();
    }

    /// <summary>Minimal-API parameter binding decides "service or request body?" by asking the service
    /// provider whether the type is registered — at MAP time, before any request exists. So every
    /// handler dependency has to be registered here or the endpoint fails to build; none of them is ever
    /// resolved, which is why a throwing factory is the honest registration (an accidental resolve
    /// becomes a loud failure rather than a null reference).</summary>
    private static void RegisterHandlerDependencies(IServiceCollection services)
    {
        var types = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamsForgeApiExtensions).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType))
            .Concat(typeof(IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)))
            .Distinct();

        foreach (var t in types)
        {
            services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this test builds endpoints, it never serves a request."));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // The table.
    // ---------------------------------------------------------------------------------------------

    /// <summary>The guard a route is expected to carry, as the canonical string
    /// <see cref="GuardOf"/> renders. Not an enum: the whole point is that the expected and the actual
    /// value are produced by two different mechanisms (a literal here, reflection over
    /// <see cref="EndpointDataSource"/> there) and compared as text, so a drift reads as a diff.</summary>
    private static class Guards
    {
        public const string Viewer = "Viewer";
        public const string Editor = "Editor";
        public const string Admin = "Admin";

        /// <summary>Explicit <c>.AllowAnonymous()</c>.</summary>
        public const string Anonymous = "anonymous";

        /// <summary>Scalar's own embedded static assets under a <c>RequireAuthorization</c>'d prefix:
        /// Scalar marks its asset routes AllowAnonymous, and AllowAnonymous wins, so the JS bundle is
        /// public while the page and the document it fetches are not. Recorded, not "fixed" — the
        /// assets are a stock UI bundle and carry nothing about this deployment.</summary>
        public const string AnonymousAndViewer = "anonymous+Viewer";

        /// <summary>Neither <c>IAuthorizeData</c> nor <c>IAllowAnonymous</c>. No fallback policy is
        /// registered, so this is anonymous in effect — indistinguishable, from the outside, from
        /// "somebody forgot". Only tolerated OUTSIDE <c>/api/</c>; inside it, the floor test below
        /// fails.</summary>
        public const string Unmarked = "(unmarked)";
    }

    /// <param name="Method">HTTP method(s), <c>|</c>-joined, or <c>(any)</c> for a route with no
    /// <see cref="HttpMethodMetadata"/> (the SignalR hub).</param>
    /// <param name="Pattern">The route pattern verbatim, trailing slash and all — <c>MapGroup("/api/x")
    /// + MapGet("/")</c> really does produce <c>/api/x/</c>, and pinning what is mapped rather than what
    /// is tidy is the point.</param>
    /// <param name="Note">Why this row is what it is, for the rows where that is not obvious.</param>
    private sealed record ExpectedRoute(string Method, string Pattern, string Guard, string? Note = null);

    /// <summary>
    /// Every route <see cref="StreamsForgeApiExtensions.MapStreamsForgeApi"/> maps, and the guard it is
    /// expected to carry. Read off the map sites — every <c>Map{Get,Post,Put,Delete}</c> and
    /// <c>MapGroup</c> under <c>shared/StreamsForge.Api/Endpoints/**</c> and
    /// <c>shared/StreamsForge.Api/Chat/</c>, plus the Scalar loop, the <c>/healthz</c> pair and the hub in
    /// <c>StreamsForgeApiExtensions.cs</c> — not from memory.
    ///
    /// <para>Where a route is <c>MapGroup</c>-gated (<c>/api/users</c> is <c>RequireAuthorization("Admin")</c>
    /// on the group) the group's policy is the row's guard, because that is what the endpoint's metadata
    /// ends up carrying.</para>
    ///
    /// <para>This table is the REST half of the same specification
    /// <c>StreamsForge.AppCore.Tests/Access/LegacyEquivalenceMatrixTests.cs</c> states in terms of
    /// entitlement actions. That one proves the evaluator answers what the policy answers; this one
    /// proves the policy is still attached to the route.</para>
    /// </summary>
    private static readonly ExpectedRoute[] Expected =
    [
        // ---- liveness + auth ------------------------------------------------------------------
        new("GET", "/healthz", Guards.Anonymous),
        new("GET", "/api/healthz", Guards.Anonymous),
        new("POST", "/api/auth/login", Guards.Anonymous),
        new("POST", "/api/auth/logout", Guards.Anonymous),
        new("GET", "/api/auth/me", Guards.Viewer),

        // ---- sources ---------------------------------------------------------------------------
        new("GET", "/api/sources/", Guards.Viewer),
        new("GET", "/api/sources/{name}", Guards.Viewer),
        new("GET", "/api/sources/{name}/proto", Guards.Viewer),
        new("GET", "/api/sources/{name}/status", Guards.Viewer),
        // Plan 026 wave 3 D6: replay-log status. Viewer, the same as every other read here — AccessGuard
        // still asks source.read at the resolved scope inside the handler.
        new("GET", "/api/sources/{name}/replay", Guards.Viewer),
        new("GET", "/api/sources/{name}/ingest", Guards.Viewer),
        new("POST", "/api/sources/", Guards.Editor),
        new("PUT", "/api/sources/{name}", Guards.Editor),
        new("DELETE", "/api/sources/{name}", Guards.Editor),
        new("POST", "/api/sources/{name}/ingest/keys", Guards.Editor),
        new("GET", "/api/sources/{name}/ingest/keys", Guards.Editor),
        new("DELETE", "/api/sources/{name}/ingest/keys/{id}", Guards.Editor),
        new("POST", "/api/sources/schema/mapping-validate", Guards.Editor),
        new("POST", "/api/sources/schema/derive-openapi", Guards.Editor),
        new("POST", "/api/sources/schema/from-remote", Guards.Editor),
        new("POST", "/api/sources/{name}/run", Guards.Editor),

        // Plan 020 wave B-2: the CRDT document intake route.
        new("POST", "/api/sources/{name}/crdt/updates", Guards.Editor),
        new("GET", "/api/sources/{name}/crdt", Guards.Viewer),
        // Plan 020 wave C: the replay route. Editor, not Viewer, deliberately — it merges nothing and
        // cannot change the document, but it PUBLISHES the whole projection onto the source's stream,
        // which is a write to every table and sink downstream.
        new("POST", "/api/sources/{name}/crdt/replay", Guards.Editor),
        // Plan 020 wave F: the escrow rebalance route. Editor, not Viewer — same "writes to the document
        // (or, here, to its escrow counter) even though it can never touch entity content" reasoning as
        // /crdt/replay above.
        new("POST", "/api/sources/{name}/crdt/escrow/rebalance", Guards.Editor),

        // The one REST route that is anonymous ON PURPOSE at the metadata layer. Plan 009 A1.2: a
        // telemetry producer holds an ingest key, not a JWT, so route-level authorization cannot express
        // the requirement. The REAL gate is the manual dual check at the top of the handler in
        // shared/StreamsForge.Api/Endpoints/SourcesEndpoints.cs — IsAuthorizedToPushAsync: an Editor JWT
        // (resolved through the REAL "Editor" policy via IAuthorizationService, so it cannot drift) OR a
        // valid X-SF-Ingest-Key for THAT source. Do not "fix" this row by adding RequireAuthorization:
        // that breaks every key-holding producer. See DualAuthPathsAreAnonymousAtTheMetadataLayer below,
        // which pins both halves of the same decision.
        new("POST", "/api/sources/{name}/events", Guards.Anonymous,
            "deliberate: manual Editor-JWT-or-ingest-key dual check inside the handler (plan 009 A1.2)"),

        // ---- pipelines -------------------------------------------------------------------------
        new("GET", "/api/pipelines/", Guards.Viewer),
        new("GET", "/api/pipelines/{id}", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/proto", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/plan", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/results", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/results.csv", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/metrics", Guards.Viewer),
        // Plan 026 wave 3 D6: replay-log status.
        new("GET", "/api/pipelines/{id}/replay", Guards.Viewer),
        new("POST", "/api/pipelines/", Guards.Editor),
        new("PUT", "/api/pipelines/{id}", Guards.Editor),
        new("DELETE", "/api/pipelines/{id}", Guards.Editor),
        new("POST", "/api/pipelines/{id}/start", Guards.Editor),
        new("POST", "/api/pipelines/{id}/stop", Guards.Editor),
        new("POST", "/api/pipelines/validate", Guards.Editor),

        // ---- tables ----------------------------------------------------------------------------
        new("GET", "/api/tables/", Guards.Viewer),
        new("GET", "/api/tables/{id}", Guards.Viewer),
        new("GET", "/api/tables/{id}/plan", Guards.Viewer),
        new("GET", "/api/tables/{id}/rows", Guards.Viewer),
        new("GET", "/api/tables/{id}/rows.csv", Guards.Viewer),
        new("GET", "/api/tables/{id}/metrics", Guards.Viewer),
        // Plan 026 wave 3 D6: replay-log status.
        new("GET", "/api/tables/{id}/replay", Guards.Viewer),
        new("GET", "/api/tables/{id}/proto", Guards.Viewer),
        new("GET", "/api/tables/{id}/search", Guards.Viewer),
        new("POST", "/api/tables/{id}/history/lookup", Guards.Viewer),
        new("GET", "/api/tables/{id}/history/stats", Guards.Viewer),
        new("POST", "/api/tables/{id}/shard/lookup", Guards.Viewer),
        new("GET", "/api/tables/{id}/shards", Guards.Viewer),
        new("GET", "/api/tables/{id}/shards/scan", Guards.Viewer),
        new("POST", "/api/tables/", Guards.Editor),
        new("PUT", "/api/tables/{id}", Guards.Editor),
        new("DELETE", "/api/tables/{id}", Guards.Editor),
        new("POST", "/api/tables/{id}/start", Guards.Editor),
        new("POST", "/api/tables/{id}/stop", Guards.Editor),
        new("POST", "/api/tables/validate", Guards.Editor),

        // ---- users (the WHOLE group is Admin, on the MapGroup) -----------------------------------
        new("GET", "/api/users/", Guards.Admin),
        new("POST", "/api/users/", Guards.Admin),
        new("PUT", "/api/users/{username}", Guards.Admin),
        new("DELETE", "/api/users/{username}", Guards.Admin),

        // Plan 015 wave 2-C. The Admin group policy is the COMPATIBILITY FLOOR, not the real check:
        // every handler additionally asks AccessGuard for `access.read` or `access.write` at the scope
        // named in its own route. That is the pattern wave 3 rolls out everywhere, and it is why these
        // rows will read `Admin` right up until wave 3 drops the group policy — at which point the
        // handlers already do the right thing and only these rows change.
        new("GET", "/api/access/", Guards.Admin),
        new("GET", "/api/access/effective/{username}", Guards.Admin),
        new("PUT", "/api/access/roles/{name}", Guards.Admin),
        new("DELETE", "/api/access/roles/{name}", Guards.Admin),
        new("PUT", "/api/access/groups/{name}", Guards.Admin),
        new("DELETE", "/api/access/groups/{name}", Guards.Admin),
        new("PUT", "/api/access/users/{username}", Guards.Admin),
        new("PUT", "/api/access/users/{username}/disabled", Guards.Admin),
        new("DELETE", "/api/access/users/{username}", Guards.Admin),
        new("PUT", "/api/access/approval-templates/{name}", Guards.Admin),
        new("DELETE", "/api/access/approval-templates/{name}", Guards.Admin),

        // Plan 015 wave 5-A. Approvals sit on the VIEWER floor and audit on the ADMIN one, and the
        // asymmetry is deliberate: the floor is the only control that survives Auth:Mode=legacy, where
        // the guard allows everything. Approvals can afford Viewer because the STORE's eligibility and
        // self-vote rules are mode-independent — and because an approver is by design an ordinary user
        // in a group, so an Admin floor would make the feature unusable by the people it is for. Audit
        // has no store-side control at all, so it fails closed and reproduces today's Admin-only reach.
        new("POST", "/api/approvals/", Guards.Viewer),
        new("GET", "/api/approvals/", Guards.Viewer),
        new("GET", "/api/approvals/{id}", Guards.Viewer),
        new("POST", "/api/approvals/{id}/approve", Guards.Viewer),
        new("POST", "/api/approvals/{id}/reject", Guards.Viewer),
        new("POST", "/api/approvals/{id}/cancel", Guards.Viewer),
        new("GET", "/api/audit/days", Guards.Admin),
        new("GET", "/api/audit/{day}", Guards.Admin),

        // ---- config, chat ------------------------------------------------------------------------
        new("GET", "/api/config/export", Guards.Viewer),
        new("POST", "/api/config/import", Guards.Editor),
        new("POST", "/api/chat/", Guards.Editor),

        // ---- platform metadata ---------------------------------------------------------------------
        new("GET", "/api/meta/protos/static", Guards.Viewer),
        new("GET", "/api/meta/grpc", Guards.Viewer),
        new("GET", "/api/meta/arrangements", Guards.Viewer),
        // Plan 016 wave 5. /instance is anonymous ON PURPOSE, for the same reason /healthz is: it is
        // what a peer probes and what an operator curls before they hold any credential. That is why
        // its body is counts and kind names — never entity names; DiscoveryEndpointsTests pins that.
        new("GET", "/api/meta/instance", Guards.Anonymous),
        new("GET", "/api/meta/peers", Guards.Viewer),
        // A POST that writes only this instance's own bookkeeping ABOUT a peer — not the peer, not this
        // catalog — so it carries the same catalog.read a caller already needed to list peers at all.
        new("POST", "/api/meta/peers/{name}/probe", Guards.Viewer),
        // Plan 016 wave 6. NOT anonymous, unlike /instance two lines up — see the comment on this route
        // in MetaEndpoints.cs for why the asymmetry is deliberate: this lists internal wiring
        // (hostnames/ports an operator put behind Endpoints:<name>), not a capability probe.
        new("GET", "/api/meta/endpoints", Guards.Viewer),
        // ---- plan 021: environments -------------------------------------------------------------
        // Create and delete are Admin twice over — the route policy AND an AccessGuard check — because
        // D7 calls force-delete the one genuinely destructive operation that plan adds: it erases the
        // catalog AND the runtime state of everything in the environment. There is deliberately no
        // rename route; the name is qualified into every key an entity ever produced.
        new("GET", "/api/environments/", Guards.Viewer),
        new("POST", "/api/environments/", Guards.Admin),
        new("DELETE", "/api/environments/{name}", Guards.Admin),

        new("GET", "/api/transports", Guards.Viewer),
        new("POST", "/api/transports/{kind}/probe", Guards.Editor),
        // Console UI plugins. Anonymous ON PURPOSE, the same way the SPA's own JS bundle is: the console
        // imports these modules before anyone has logged in, so a Viewer floor would make a plugin
        // unloadable on the login screen and every page after it. What they serve is front-end code an
        // operator installed into ui-plugins/ plus its filenames — never catalog data, never a value from
        // any entity. That is also the documented rule for the directory (TRANSPORTS.md): anything put
        // there is served to every browser that can reach the console.
        new("GET", "/api/ui-plugins", Guards.Anonymous,
            "deliberate: the SPA loads plugin modules before login, exactly like its own bundle"),
        new("GET", "/api/ui-plugins/{file}", Guards.Anonymous,
            "deliberate: serves operator-installed front-end code, not catalog data"),
        new("GET", "/api/sql/functions", Guards.Viewer),

        // ---- per-entity OpenAPI documents (EntityOpenApiEndpoints.RouteSuffix) ----------------------
        new("GET", "/api/tables/{id}/openapi.json", Guards.Viewer),
        new("GET", "/api/pipelines/{id}/openapi.json", Guards.Viewer),
        new("GET", "/api/sources/{name}/openapi.json", Guards.Viewer),

        // ---- SignalR ---------------------------------------------------------------------------------
        // [Authorize(Policy = "Viewer")] on StreamHub itself, which is why both the connection route and
        // the negotiate route carry it. No HttpMethodMetadata on either.
        new("(any)", "/hubs/stream", Guards.Viewer),
        new("(any)", "/hubs/stream/negotiate", Guards.Viewer),

        // ---- OpenAPI document + the whole-application Scalar page ------------------------------------
        // Unmarked, i.e. anonymous in effect. Outside /api/, so the floor test does not reach them; noted
        // rather than silently accepted, since the document does describe the whole surface.
        new("GET", "/openapi/{documentName}.json", Guards.Unmarked,
            "unmarked = anonymous in effect; the app-wide OpenAPI document is public"),
        new("GET", "/scalar/{documentName?}", Guards.Unmarked, "the app-wide Scalar page; public"),
        new("GET", "/scalar/favicon.svg", Guards.Anonymous),
        new("GET", "/scalar/scalar.js", Guards.Anonymous),
        new("GET", "/scalar/scalar.aspnetcore.js", Guards.Anonymous),

        // ---- the per-entity Scalar loop (3 iterations x 4 routes each) --------------------------------
        // The PAGE is Viewer-gated (satisfied by the DocsAuthCookie, since Scalar cannot send a header);
        // the three static assets Scalar maps under the same prefix are its own AllowAnonymous bundle
        // routes, and AllowAnonymous wins over the group's RequireAuthorization.
        new("GET", "/scalar/tables/{id}/{documentName?}", Guards.Viewer),
        new("GET", "/scalar/tables/{id}/favicon.svg", Guards.AnonymousAndViewer),
        new("GET", "/scalar/tables/{id}/scalar.js", Guards.AnonymousAndViewer),
        new("GET", "/scalar/tables/{id}/scalar.aspnetcore.js", Guards.AnonymousAndViewer),
        new("GET", "/scalar/pipelines/{id}/{documentName?}", Guards.Viewer),
        new("GET", "/scalar/pipelines/{id}/favicon.svg", Guards.AnonymousAndViewer),
        new("GET", "/scalar/pipelines/{id}/scalar.js", Guards.AnonymousAndViewer),
        new("GET", "/scalar/pipelines/{id}/scalar.aspnetcore.js", Guards.AnonymousAndViewer),
        new("GET", "/scalar/sources/{name}/{documentName?}", Guards.Viewer),
        new("GET", "/scalar/sources/{name}/favicon.svg", Guards.AnonymousAndViewer),
        new("GET", "/scalar/sources/{name}/scalar.js", Guards.AnonymousAndViewer),
        new("GET", "/scalar/sources/{name}/scalar.aspnetcore.js", Guards.AnonymousAndViewer),
    ];

    /// <summary>The canonical rendering of what a route's metadata actually says. Explicit
    /// <c>AllowAnonymous</c> first, then every authorize policy in metadata order — so a route that is
    /// BOTH (Scalar's asset routes under a gated prefix) renders as both rather than hiding one.</summary>
    private static string GuardOf(RouteEndpoint endpoint)
    {
        var parts = new List<string>();
        if (endpoint.Metadata.GetMetadata<IAllowAnonymous>() is not null)
        {
            parts.Add(Guards.Anonymous);
        }

        parts.AddRange(endpoint.Metadata.GetOrderedMetadata<IAuthorizeData>()
            .Select(a => string.IsNullOrEmpty(a.Policy) ? "authenticated" : a.Policy!));

        return parts.Count == 0 ? Guards.Unmarked : string.Join("+", parts);
    }

    private static string KeyOf(RouteEndpoint endpoint)
    {
        var methods = endpoint.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods;
        var method = methods is null || methods.Count == 0 ? "(any)" : string.Join("|", methods);
        return $"{method} /{endpoint.RoutePattern.RawText?.TrimStart('/')}";
    }

    [Fact]
    public void TheAuthorizationSurfaceMatchesThePinnedTableExactly()
    {
        var actual = BuildEndpoints().OfType<RouteEndpoint>()
            .ToDictionary(KeyOf, GuardOf, StringComparer.Ordinal);
        var expected = Expected.ToDictionary(
            r => $"{r.Method} {r.Pattern}", r => r, StringComparer.Ordinal);

        var problems = new List<string>();

        foreach (var (key, guard) in actual.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (!expected.TryGetValue(key, out var row))
            {
                problems.Add(
                    $"NEW ROUTE, no table row: '{key}' is mapped with guard '{guard}'. Nobody has decided " +
                    "what it should be guarded by. Add a row to AuthorizationCoverageTests.Expected " +
                    "stating the policy you intend, then make sure the map site agrees.");
            }
            else if (!string.Equals(row.Guard, guard, StringComparison.Ordinal))
            {
                problems.Add(
                    $"GUARD DRIFTED on '{key}': the table says '{row.Guard}', the route now carries " +
                    $"'{guard}'. Either the RequireAuthorization/AllowAnonymous at the map site changed " +
                    "and the table must follow, or the change was an accident and the map site must be " +
                    "put back." + (row.Note is null ? "" : $" Row note: {row.Note}"));
            }
        }

        foreach (var key in expected.Keys.Except(actual.Keys, StringComparer.Ordinal)
                     .OrderBy(k => k, StringComparer.Ordinal))
        {
            problems.Add(
                $"ROUTE GONE: the table expects '{key}' (guard '{expected[key].Guard}') but nothing maps " +
                "it. If it was renamed or removed on purpose, update the table; if it vanished by " +
                "accident, an authorization-relevant route just stopped existing.");
        }

        Assert.True(problems.Count == 0,
            $"The mapped authorization surface no longer matches the table pinned in " +
            $"{nameof(AuthorizationCoverageTests)}.{nameof(Expected)} " +
            $"({expected.Count} rows expected, {actual.Count} routes mapped):\n\n  "
            + string.Join("\n\n  ", problems));
    }

    /// <summary>
    /// The two ingest paths are AllowAnonymous at the metadata layer on purpose (plan 009 A1.2) and the
    /// real check is a manual dual check inside the handler. Pinned here so that "this route has no
    /// policy" reads as a decision and not as an omission, on both halves at once: the REST route's
    /// metadata, and the gRPC service's absent <c>[Authorize]</c>.
    /// </summary>
    [Fact]
    public void DualAuthPathsAreAnonymousAtTheMetadataLayer()
    {
        var events = BuildEndpoints().OfType<RouteEndpoint>()
            .Single(e => KeyOf(e) == "POST /api/sources/{name}/events");

        Assert.Equal(Guards.Anonymous, GuardOf(events));
        Assert.Empty(events.Metadata.GetOrderedMetadata<IAuthorizeData>());

        // The gRPC twin. It never reaches an EndpointDataSource here (this test maps no gRPC services),
        // so it is pinned by reflection instead: IngestGrpcService.Ingest must carry NO [Authorize],
        // because it authorizes PER MESSAGE — request.SourceName travels on every message and an ingest
        // key only ever authorizes one source. Its real gate is IsAuthorizedAsync in
        // shared/StreamsForge.Api/Grpc/IngestGrpcService.cs (plan 025 moved the gRPC services here,
        // namespace StreamsForge.Host.Grpc unchanged, served on both flavors), which resolves the same
        // "Editor" policy through IAuthorizationService, exactly as the REST route does.
        var grpc = typeof(StreamsForge.Host.Grpc.IngestGrpcService);
        Assert.Empty(grpc.GetCustomAttributes(typeof(AuthorizeAttribute), inherit: true));
        Assert.Empty(grpc.GetMethod(nameof(StreamsForge.Host.Grpc.IngestGrpcService.Ingest))!
            .GetCustomAttributes(typeof(AuthorizeAttribute), inherit: true));
    }

    [Fact]
    public void EveryApiEndpointIsEitherAuthorizedOrExplicitlyAnonymous()
    {
        var unguarded = BuildEndpoints()
            .OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText?.StartsWith("/api/", StringComparison.Ordinal) == true)
            .Where(e => e.Metadata.GetMetadata<IAuthorizeData>() is null
                        && e.Metadata.GetMetadata<IAllowAnonymous>() is null)
            .Select(e => $"{string.Join(",", e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? [])} {e.RoutePattern.RawText}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.True(unguarded.Count == 0,
            "Endpoints under /api/ with neither an authorize policy nor an explicit AllowAnonymous:\n  "
            + string.Join("\n  ", unguarded));
    }

    [Fact]
    public void TheApiSurfaceIsNotAccidentallyEmpty()
    {
        // Without this, the assertion above passes trivially the day MapStreamsForgeApi stops mapping.
        var apiRoutes = BuildEndpoints()
            .OfType<RouteEndpoint>()
            .Count(e => e.RoutePattern.RawText?.StartsWith("/api/", StringComparison.Ordinal) == true);

        Assert.True(apiRoutes > 50, $"expected the whole REST surface, saw {apiRoutes} routes under /api/");
    }
}
