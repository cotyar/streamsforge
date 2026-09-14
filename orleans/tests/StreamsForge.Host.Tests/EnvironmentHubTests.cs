using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Connections.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using StreamsForge.Api.Hubs;
using StreamsForge.AppCore.Access;
using StreamsForge.AppCore.Environments;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 021 wave 2 (track "shared REST/SignalR surface") — <see cref="StreamHub"/>'s group names are
/// qualified by the CONNECTION's environment (<c>EnvKeys.Qualify(ConnectionEnv, name)</c>), read off
/// <c>HttpContext.Items[EnvironmentSelectionMiddleware.HttpContextItemKey]</c> rather than off
/// <see cref="StreamsForge.AppCore.Environments.EnvironmentAmbient"/>, because a hub method invocation runs
/// over an already-established WebSocket and never passes back through the HTTP middleware pipeline — see
/// <see cref="StreamHub"/>'s own class remarks for the full argument.
///
/// <para>Two harnesses, because the two things worth pinning are different in kind:</para>
/// <list type="bullet">
/// <item><b><see cref="GroupNaming"/></b> drives <see cref="StreamHub"/> directly — the
/// <c>StreamHubEntitlementTests</c> pattern (<c>Context</c>/<c>Groups</c> are settable, no SignalR test
/// harness needed) — with a <see cref="HubCallerContext"/> whose <c>Features</c> carry an
/// <see cref="IHttpContextFeature"/> exposing an <see cref="HttpContext"/> pre-stamped with
/// <c>Items[HttpContextItemKey]</c>, exactly as <see cref="EnvironmentSelectionMiddleware"/> leaves it on
/// the connection-establishing request. This is what proves the group STRING is exactly
/// <c>EnvKeys.Qualify</c>, byte for byte, for both a named environment and the untouched default.</item>
/// <item><b><see cref="MiddlewareReachesTheHub"/></b> is a REAL Kestrel listener running the actual
/// <c>MapStreamsForgeApi</c> route table (the <see cref="EnvironmentAmbientTests"/>/
/// <see cref="EnvironmentEndpointsTests"/> pattern), and answers the question those unit tests cannot:
/// does the hub's own negotiate endpoint actually run through
/// <see cref="EnvironmentSelectionMiddleware"/> in this process, or only in theory because of where
/// <c>UseEnvironmentSelection()</c> happens to be called in <c>StreamsForgeApiExtensions</c>? An unknown
/// environment on <c>POST /hubs/stream/negotiate</c> coming back 404 — the SAME refusal shape every other
/// route gets — is the fact, not an inference from reading <c>Program.cs</c>. A second, test-only echo
/// route (mapped after <c>MapStreamsForgeApi</c>, exactly like <see cref="EnvironmentAmbientTests"/>'s own)
/// confirms <c>HttpContext.Items</c> carries the STAMPED value (not merely "truthy") for both the default
/// path and a named one — the fact <see cref="StreamHub.ConnectionEnv"/> depends on to tell "resolved to
/// default" apart from "never went through the middleware".</item>
/// </list>
/// </summary>
public sealed class EnvironmentHubTests
{
    // =================================================================================================
    // Part A — group-string qualification, hub driven directly (StreamHubEntitlementTests' pattern)
    // =================================================================================================

    public sealed class GroupNaming
    {
        private static (StreamHub Hub, RecordingGroups Groups) HubFor(string? stampedEnv)
        {
            // entitlementsEnabled: false — CheckAsync short-circuits to Allowed for everything, so this
            // harness is purely about which group STRING gets joined, not about who may join it (that is
            // StreamHubEntitlementTests' job, in a file this wave does not touch).
            var resolver = new PermissionResolver(
                new CountingAccessPolicyFacade(PermissionResolverTests.Doc(version: 1)),
                NullLogger<PermissionResolver>.Instance,
                policyCacheSeconds: 600);

            var httpContext = new DefaultHttpContext();
            if (stampedEnv is not null)
            {
                httpContext.Items[EnvironmentSelectionMiddleware.HttpContextItemKey] = stampedEnv;
            }

            var features = new FeatureCollection();
            features.Set<IHttpContextFeature>(new TestHttpContextFeature(httpContext));

            var groups = new RecordingGroups();
            var hub = new StreamHub(
                new AccessGuard(resolver, entitlementsEnabled: false),
                new SingleCatalogServiceProvider(new StubCatalog(
                    sources: [new SourceDefinition { Name = "s1" }],
                    pipelines: [new PipelineDefinition { Id = "p1" }],
                    tables: [new TableDefinition { Id = "t1", Name = "orders" }])),
                new NullEntityStreamFacade())
            {
                Context = new FakeCallerContext(PermissionResolverTests.Principal("alice"), features),
                Groups = groups,
            };

            return (hub, groups);
        }

        [Fact]
        public async Task No_stamped_env_joins_the_byte_identical_group_names_it_joins_today()
        {
            // Simulates a connection whose HttpContext never reached EnvironmentSelectionMiddleware —
            // ConnectionEnv falls back to EnvKeys.Default, and Qualify("", key) == key (D2), so nothing
            // about the group names changes for a deployment that never mentions an environment.
            var (hub, groups) = HubFor(stampedEnv: null);

            await hub.SubscribePipeline("p1");
            await hub.SubscribeSource("s1");
            await hub.SubscribeTable("orders");
            await hub.SubscribeMetrics();

            Assert.Equal(["pipeline:p1", "source:s1", "table:orders", "metrics"], groups.Added);
        }

        [Fact]
        public async Task Explicit_default_stamp_is_identical_to_no_stamp_at_all()
        {
            // EnvironmentSelectionMiddleware now stamps EnvKeys.Default explicitly on the untouched path
            // (wave 2) rather than leaving Items untouched — this proves that stamp is inert for group
            // naming, exactly as the D2 "byte-identical" invariant requires.
            var (hub, groups) = HubFor(stampedEnv: EnvKeys.Default);

            await hub.SubscribePipeline("p1");
            await hub.SubscribeSource("s1");
            await hub.SubscribeTable("orders");

            Assert.Equal(["pipeline:p1", "source:s1", "table:orders"], groups.Added);
        }

        [Fact]
        public async Task A_named_environment_joins_the_qualified_group_and_nothing_else()
        {
            var (hub, groups) = HubFor(stampedEnv: "staging");

            await hub.SubscribePipeline("p1");
            await hub.SubscribeSource("s1");
            await hub.SubscribeTable("orders");
            await hub.SubscribeMetrics();

            Assert.Equal(
                [
                    $"pipeline:{EnvKeys.Qualify("staging", "p1")}",
                    $"source:{EnvKeys.Qualify("staging", "s1")}",
                    $"table:{EnvKeys.Qualify("staging", "orders")}",
                    // metrics stays unqualified — it is cluster-wide, not per-entity (see StreamHub).
                    "metrics",
                ],
                groups.Added);

            // Spelled out once, byte for byte, so a change to EnvKeys.Separator cannot silently pass this
            // file by only ever going through EnvKeys.Qualify on both sides of the assertion.
            Assert.Equal("pipeline:staging.p1", groups.Added[0]);
            Assert.Equal("source:staging.s1", groups.Added[1]);
            Assert.Equal("table:staging.orders", groups.Added[2]);
        }

        [Fact]
        public async Task Unsubscribe_leaves_the_same_qualified_group_it_joined()
        {
            // A join/leave pair that disagreed about the group string would leak a subscription rather
            // than fail loudly — RemoveFromGroupAsync on the wrong name is a silent no-op in SignalR.
            var (hub, groups) = HubFor(stampedEnv: "staging");

            await hub.SubscribeTable("orders");
            await hub.UnsubscribeTable("orders");

            Assert.Equal(["table:staging.orders"], groups.Added);
            Assert.Equal(["table:staging.orders"], groups.Removed);
        }
    }

    // =================================================================================================
    // Part B — the hub's negotiate endpoint actually runs through EnvironmentSelectionMiddleware, and
    // HttpContext.Items carries the stamped value for both default and a named environment. Real
    // Kestrel, the EnvironmentAmbientTests/EnvironmentEndpointsTests pattern.
    // =================================================================================================

    public sealed class MiddlewareReachesTheHub : IAsyncDisposable
    {
        private readonly string _dataDir =
            Path.Combine(Path.GetTempPath(), "sf-env-hub-tests-" + Guid.NewGuid().ToString("n"));

        private WebApplication? _app;

        public async ValueTask DisposeAsync()
        {
            if (_app is not null)
            {
                await _app.StopAsync();
                await _app.DisposeAsync();
            }

            if (Directory.Exists(_dataDir))
            {
                Directory.Delete(_dataDir, recursive: true);
            }
        }

        private sealed record ItemsEcho(string? Value, bool Present);

        /// <summary>Same shape as <see cref="EnvironmentAmbientTests"/>'s own fake — answers <c>true</c>
        /// for default and for whatever names were passed to the constructor.</summary>
        private sealed class FakeEnvironmentFacade(params string[] known) : IEnvironmentFacade
        {
            private readonly HashSet<string> _known = new(known, StringComparer.Ordinal);

            public Task<List<EnvironmentRecord>> ListAsync() => Task.FromResult(new List<EnvironmentRecord>());

            public Task<bool> ExistsAsync(string name) =>
                Task.FromResult(name == EnvKeys.Default || _known.Contains(name));

            public Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy) =>
                throw new NotSupportedException("this file only drives the negotiate route and the echo route");

            public Task<bool> DeleteAsync(string name, bool force) =>
                throw new NotSupportedException("this file only drives the negotiate route and the echo route");
        }

        private sealed class FakeUserStoreFacade : IUserStoreFacade
        {
            public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(new List<UserRecord>());
            public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
            public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
            public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
            public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
        }

        /// <summary>Only the built-in roles, no per-user entries — the JWT's role claim resolves grants
        /// through the legacy-equivalence path, same as <see cref="EnvironmentEndpointsTests"/>.</summary>
        private sealed class FakeAccessPolicyFacade : IAccessPolicyFacade
        {
            private readonly AccessPolicyDocument _document = new() { Roles = BuiltInRoleCatalog.Create(), Version = 1 };

            public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(_document);
            public Task<long> GetVersionAsync() => Task.FromResult(_document.Version);
            public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
            public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
            public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
            public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
            public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotSupportedException();
            public Task<bool> DeleteUserAccessAsync(string username) => throw new NotSupportedException();
            public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
            public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
        }

        private async Task<HttpClient> StartAsync(FakeEnvironmentFacade environments)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Jwt:Key"] = new string('k', 64),
                ["Jwt:Issuer"] = "streamsforge-test",
                ["Jwt:Audience"] = "streamsforge-test",
            });
            builder.Services.AddStreamsForgeApi(builder.Configuration);

            // See EnvironmentAmbientTests.StartAsync / DiscoveryEndpointsTests.StartAsync: a real
            // StartAsync() makes minimal API ask, for EVERY mapped route, whether each handler-parameter
            // type is resolvable — so every facade/tracker interface needs a stub even though this file
            // only ever calls a handful of routes.
            var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
                .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
                .Concat(typeof(StreamsForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
                    .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                                && t.Name.EndsWith("Tracker", StringComparison.Ordinal)));
            foreach (var t in throwingStubTypes)
            {
                builder.Services.AddSingleton(t, _ => throw new InvalidOperationException(
                    $"{t.Name} was resolved — this file only drives /hubs/stream/negotiate and the test echo route."));
            }

            builder.Services.AddSingleton<IEnvironmentFacade>(environments);
            builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
            builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());

            _app = builder.Build();
            _app.MapStreamsForgeApi(new StreamsForgeApiOptions(
                ProtosDir: Path.Combine(Path.GetTempPath(), "sf-env-hub-tests-protos"),
                GrpcPort: 7299,
                GrpcStaticServices: [],
                DocsFilePath: null,
                SpaDistPath: null,
                Flavor: "test",
                DataDir: _dataDir));

            // Test-only echo route, mapped AFTER MapStreamsForgeApi so it still goes through
            // EnvironmentSelectionMiddleware (registered inside MapStreamsForgeApi) exactly like every
            // real route including /hubs/stream/negotiate — the same trick EnvironmentAmbientTests uses
            // for EnvironmentAmbient.Current, aimed at HttpContext.Items instead.
            _app.MapGet("/__test/items-echo", (HttpContext http) =>
            {
                var present = http.Items.TryGetValue(EnvironmentSelectionMiddleware.HttpContextItemKey, out var raw);
                return Results.Ok(new ItemsEcho(raw as string, present));
            }).AllowAnonymous();

            await _app.StartAsync();
            var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
            return new HttpClient { BaseAddress = new Uri(address) };
        }

        private HttpRequestMessage AuthedRequest(HttpMethod method, string url, string role = "Viewer")
        {
            var token = _app!.Services.GetRequiredService<JwtTokenService>()
                .CreateToken(new UserRecord { Username = role.ToLowerInvariant(), DisplayName = role, Role = role });
            return new HttpRequestMessage(method, url)
            {
                Headers = { Authorization = new AuthenticationHeaderValue("Bearer", token) },
            };
        }

        [Fact]
        public async Task HttpContext_Items_carries_the_empty_string_for_the_default_environment()
        {
            using var client = await StartAsync(new FakeEnvironmentFacade("staging"));

            var echo = await client.GetFromJsonAsync<ItemsEcho>("/__test/items-echo");

            // Present AND equal to EnvKeys.Default — not simply "falsy" — is the fact StreamHub.ConnectionEnv
            // relies on to tell "resolved to default" apart from "never reached the middleware".
            Assert.True(echo!.Present);
            Assert.Equal(EnvKeys.Default, echo.Value);
            Assert.Equal("", echo.Value);
        }

        [Fact]
        public async Task HttpContext_Items_carries_the_named_environment()
        {
            using var client = await StartAsync(new FakeEnvironmentFacade("staging"));

            var request = new HttpRequestMessage(HttpMethod.Get, "/__test/items-echo");
            request.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "staging");
            var response = await client.SendAsync(request);
            var echo = await response.Content.ReadFromJsonAsync<ItemsEcho>();

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(echo!.Present);
            Assert.Equal("staging", echo.Value);
        }

        [Fact]
        public async Task Negotiate_on_an_unknown_environment_is_the_same_404_every_other_route_gets()
        {
            // The fact this proves: /hubs/stream/negotiate is not special-cased out of
            // EnvironmentSelectionMiddleware — it hits the identical branch as /api/environments or any
            // other route, in THIS process, not merely "by reading where UseEnvironmentSelection() is
            // called in StreamsForgeApiExtensions".
            using var client = await StartAsync(new FakeEnvironmentFacade("staging"));

            var response = await client.SendAsync(
                AuthedRequest(HttpMethod.Post, "/hubs/stream/negotiate?env=nope"));

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync();
            Assert.Contains("nope", body, StringComparison.Ordinal);
        }

        [Fact]
        public async Task Negotiate_on_a_known_environment_succeeds_and_the_connection_gets_a_connection_id()
        {
            using var client = await StartAsync(new FakeEnvironmentFacade("staging"));

            var response = await client.SendAsync(
                AuthedRequest(HttpMethod.Post, "/hubs/stream/negotiate?env=staging"));

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync();
            Assert.Contains("connectionId", body, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task Negotiate_with_no_environment_also_succeeds_unauthenticated_callers_get_401_not_404()
        {
            // Placement check, the other half of D4/D7's ordering argument (see
            // EnvironmentSelectionMiddleware's class remarks): an ANONYMOUS caller must not be able to
            // learn "environment X exists" from a route it was never getting into anyway. An unauthenticated
            // negotiate hits [Authorize] first and gets 401 — never this middleware's 404 — regardless of
            // what ?env= names.
            using var client = await StartAsync(new FakeEnvironmentFacade("staging"));

            var anonymous = await client.PostAsync("/hubs/stream/negotiate?env=nope", content: null);
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);

            var authed = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/hubs/stream/negotiate"));
            Assert.Equal(HttpStatusCode.OK, authed.StatusCode);
        }
    }

    // =================================================================================================
    // Fakes shared by Part A (StreamHubEntitlementTests' own shapes, duplicated rather than shared across
    // files — that file is owned by the plan-015 wave and this one must not edit it).
    // =================================================================================================

    /// <summary>Plan 026 wave 1 — <see cref="StreamHub"/>'s new required <see cref="IEntityStreamFacade"/>
    /// dependency, unused by every test in this file (none of them call <c>SubscribeSourceFrom</c>): every
    /// member throws, so an accidental call is loud rather than silently returning nothing.</summary>
    private sealed class NullEntityStreamFacade : IEntityStreamFacade
    {
        public Task<IAsyncDisposable> SubscribeSourceAsync(
            string environment, string sourceName, Func<IReadOnlyDictionary<string, object?>, long, Task> onEvent) =>
            throw new NotImplementedException("not exercised by this test");

        public Task<IEntityReplaySubscription> SubscribeSourceAsync(
            string environment, string sourceName, ReplayFrom? from,
            Func<IReadOnlyDictionary<string, object?>, long, long, Task> onEvent) =>
            throw new NotImplementedException("not exercised by this test");

        public Task<IEntityReplaySubscription> SubscribePipelineAsync(
            string environment, string pipelineId, ReplayFrom? from,
            Func<IReadOnlyList<ResultEnvelope>, long, Task> onResults) =>
            throw new NotImplementedException("not exercised by this test");

        public Task<IEntityReplaySubscription> SubscribeTableAsync(
            string environment, string tableName, ReplayFrom? from,
            Func<IReadOnlyList<TableDeltaDto>, long, Task> onDeltas) =>
            throw new NotImplementedException("not exercised by this test");

        public Task<IAsyncDisposable> SubscribePipelineAsync(
            string environment, string pipelineId, Func<IReadOnlyList<ResultEnvelope>, Task> onResults) =>
            throw new NotImplementedException("not exercised by this test");

        public Task<IAsyncDisposable> SubscribeTableAsync(
            string environment, string tableName, Func<IReadOnlyList<TableDeltaDto>, Task> onDeltas) =>
            throw new NotImplementedException("not exercised by this test");
    }

    private sealed class RecordingGroups : IGroupManager
    {
        public List<string> Added { get; } = [];
        public List<string> Removed { get; } = [];

        public Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
        {
            Added.Add(groupName);
            return Task.CompletedTask;
        }

        public Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
        {
            Removed.Add(groupName);
            return Task.CompletedTask;
        }
    }

    /// <summary>Minimal <see cref="IHttpContextFeature"/> so a <see cref="HubCallerContext"/>'s
    /// <c>GetHttpContext()</c> extension (which reads <c>Features.Get&lt;IHttpContextFeature&gt;()</c>)
    /// resolves to a real, controllable <see cref="HttpContext"/> without depending on which concrete
    /// implementation ASP.NET Core ships this release — the interface is the only contract that matters
    /// here.</summary>
    private sealed class TestHttpContextFeature(HttpContext httpContext) : IHttpContextFeature
    {
        public HttpContext HttpContext { get; set; } = httpContext;
    }

    private sealed class FakeCallerContext(ClaimsPrincipal user, IFeatureCollection features) : HubCallerContext
    {
        public override string ConnectionId => "conn-1";
        public override string? UserIdentifier => user.Identity?.Name;
        public override ClaimsPrincipal? User => user;
        public override IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();
        public override IFeatureCollection Features { get; } = features;
        public override CancellationToken ConnectionAborted => CancellationToken.None;
        public override void Abort() { }
    }

    /// <summary>The three lookups the hub makes, and nothing else — every other member throws, matching
    /// <c>StreamHubEntitlementTests</c>' own convention.</summary>
    private sealed class StubCatalog(
        List<SourceDefinition> sources,
        List<PipelineDefinition> pipelines,
        List<TableDefinition> tables) : ICatalogFacade
    {
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(sources.FirstOrDefault(s => s.Name == name));

        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(pipelines.FirstOrDefault(p => p.Id == id));

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(tables);

        public Task<List<SourceDefinition>> GetSourcesAsync() => throw new NotImplementedException();
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotImplementedException();
        public Task<List<PipelineDefinition>> GetPipelinesAsync() => throw new NotImplementedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotImplementedException();
    }
}
