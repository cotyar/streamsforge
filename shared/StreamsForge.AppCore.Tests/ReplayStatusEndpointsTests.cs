using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 026 wave 3 — <c>GET /api/{sources|pipelines|tables}/{key}/replay</c>, exercised over real HTTP
/// (same "bare <c>WebApplication</c>, real Kestrel listener" shape <see cref="UiPluginsEndpointsTests"/>
/// uses) with a fake <see cref="IReplayStatusFacade"/> and a fake <see cref="ICatalogFacade"/> — unlike
/// that class, these three routes sit behind the real auth stack (<c>RequireAuthorization("Viewer")</c>
/// + <see cref="AccessGuard"/>), so this brings up <see cref="StreamsForgeApiExtensions.AddStreamsForgeApi"/>
/// for real rather than a bare route group.
///
/// <para><b>Why an empty <see cref="AccessPolicyDocument"/> gives both a 200 path and a 403 path for
/// free.</b> <c>StrictViewerHandler</c> succeeds the coarse "Viewer" policy outright when the document has
/// no roles at all (a pre-upgrade-catalog safety valve — see its own doc comment), so a JWT is always
/// enough to reach the handler. What happens INSIDE the handler then depends only on <c>Auth:Mode</c>:
/// <c>legacy</c> makes <see cref="AccessGuard.CheckAsync"/> unconditionally Allowed (no entitlement store
/// consulted at all), while the default <c>entitlements</c> mode resolves the JWT's role against the same
/// empty document, finds no grant, and denies — a 403, with no bespoke policy-document fixture needed for
/// either case.</para>
///
/// <para><c>IAccessPolicyFacade</c>/<c>IUserStoreFacade</c> are faked only because
/// <c>AccessBootstrapService</c> (a hosted service <c>AddStreamsForgeApi</c> always registers) takes them
/// as constructor dependencies — DI has to be able to construct it for <c>StartAsync</c> to succeed, even
/// though this test never waits on what it does.</para>
/// </summary>
public sealed class ReplayStatusEndpointsTests : IAsyncDisposable
{
    private const string JwtKey = "test-signing-key-at-least-32-bytes-long!!";
    private const string JwtIssuer = "sf-replay-endpoint-tests";
    private const string JwtAudience = "sf-replay-endpoint-tests";

    private WebApplication? _app;

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }

    // ------------------------------------------------------------------
    // Sources: name-only, the simplest of the three.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Source_replay_answers_200_with_the_facades_status()
    {
        var catalog = new FakeCatalogFacade { Sources = [new SourceDefinition { Name = "trades" }] };
        var replay = new FakeReplayStatusFacade
        {
            Source = _ => Task.FromResult<ReplayStatus?>(new ReplayStatus { FirstSeq = 1, LastSeq = 500, TotalSeen = 500 }),
        };
        using var client = await StartAsync(catalog, replay, authMode: "legacy");

        var response = await client.GetAsync("/api/sources/trades/replay");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var status = await response.Content.ReadFromJsonAsync<ReplayStatus>();
        Assert.Equal(1, status!.FirstSeq);
        Assert.Equal(500, status.LastSeq);
        Assert.Equal("trades", replay.LastSourceName); // the route segment, verbatim — sources have no id.
    }

    [Fact]
    public async Task Source_replay_answers_404_for_an_unknown_source()
    {
        var catalog = new FakeCatalogFacade();
        using var client = await StartAsync(catalog, new FakeReplayStatusFacade(), authMode: "legacy");

        var response = await client.GetAsync("/api/sources/nope/replay");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Source_replay_answers_501_when_the_facade_throws_NotSupported()
    {
        var catalog = new FakeCatalogFacade { Sources = [new SourceDefinition { Name = "trades" }] };
        var replay = new FakeReplayStatusFacade { Source = _ => throw new NotSupportedException("dapr/PARITY.md D11") };
        using var client = await StartAsync(catalog, replay, authMode: "legacy");

        var response = await client.GetAsync("/api/sources/trades/replay");

        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<ErrorResponse>();
        Assert.Equal("dapr/PARITY.md D11", body!.Error);
    }

    [Fact]
    public async Task Source_replay_answers_403_without_the_read_entitlement()
    {
        var catalog = new FakeCatalogFacade { Sources = [new SourceDefinition { Name = "trades" }] };
        // Default Auth:Mode ("entitlements") + an empty policy document: StrictViewerHandler still lets
        // the request past the coarse Viewer gate (no roles to be strict about), but AccessGuard then
        // resolves this JWT's role against the same empty document, finds no grant, and denies.
        using var client = await StartAsync(catalog, new FakeReplayStatusFacade(), authMode: null);

        var response = await client.GetAsync("/api/sources/trades/replay");

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    // ------------------------------------------------------------------
    // Pipelines: id-or-name resolution, and the facade is asked by ID (IReplayStatusFacade.GetPipelineAsync's
    // own doc says "pipelineId").
    // ------------------------------------------------------------------

    [Fact]
    public async Task Pipeline_replay_resolves_by_name_and_asks_the_facade_by_id()
    {
        var catalog = new FakeCatalogFacade { Pipelines = [new PipelineDefinition { Id = "p1", Name = "orders-out" }] };
        var replay = new FakeReplayStatusFacade
        {
            Pipeline = _ => Task.FromResult<ReplayStatus?>(new ReplayStatus { FirstSeq = 101, LastSeq = 300 }),
        };
        using var client = await StartAsync(catalog, replay, authMode: "legacy");

        var response = await client.GetAsync("/api/pipelines/orders-out/replay"); // by NAME, not id

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("p1", replay.LastPipelineId); // resolved to the id before reaching the facade.
    }

    [Fact]
    public async Task Pipeline_replay_answers_404_for_an_unknown_id_or_name()
    {
        var catalog = new FakeCatalogFacade();
        using var client = await StartAsync(catalog, new FakeReplayStatusFacade(), authMode: "legacy");

        var response = await client.GetAsync("/api/pipelines/nope/replay");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // ------------------------------------------------------------------
    // Tables: id-or-name resolution, and the facade is asked by NAME (IReplayStatusFacade.GetTableAsync's
    // own doc says "tableName" — tables have no separate grain/actor key).
    // ------------------------------------------------------------------

    [Fact]
    public async Task Table_replay_resolves_by_id_and_asks_the_facade_by_name()
    {
        var catalog = new FakeCatalogFacade { Tables = [new TableDefinition { Id = "t1", Name = "positions" }] };
        var replay = new FakeReplayStatusFacade
        {
            Table = _ => Task.FromResult<ReplayStatus?>(new ReplayStatus { Persisted = true, Segments = 3 }),
        };
        using var client = await StartAsync(catalog, replay, authMode: "legacy");

        var response = await client.GetAsync("/api/tables/t1/replay"); // by ID

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var status = await response.Content.ReadFromJsonAsync<ReplayStatus>();
        Assert.True(status!.Persisted);
        Assert.Equal("positions", replay.LastTableName); // resolved to the NAME before reaching the facade.
    }

    [Fact]
    public async Task Table_replay_answers_404_for_an_unknown_id_or_name()
    {
        var catalog = new FakeCatalogFacade();
        using var client = await StartAsync(catalog, new FakeReplayStatusFacade(), authMode: "legacy");

        var response = await client.GetAsync("/api/tables/nope/replay");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // ------------------------------------------------------------------
    // Plumbing.
    // ------------------------------------------------------------------

    /// <summary>Minimal-API parameter binding decides "service or request body?" by asking the service
    /// provider whether the type is registered — at MAP time, before any request exists (the same fact
    /// <c>AuthorizationCoverageTests.RegisterHandlerDependencies</c> is built on). Mapping THREE whole
    /// endpoint files pulls in every facade type any of their OTHER routes ask for too (ingest,
    /// history, shards, connector status, …) — routes these tests never call — and leaving even one
    /// unregistered makes ASP.NET misread some unrelated route's own complex parameter as an inferred
    /// request body and throw at map time, before a single test method runs. None of these are ever
    /// actually invoked, so a throwing factory is the honest registration.</summary>
    private static void RegisterUnusedHandlerDependencies(IServiceCollection services)
    {
        var facadeInterfaces = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Where(t => t != typeof(ICatalogFacade) && t != typeof(IReplayStatusFacade)
                        && t != typeof(IAccessPolicyFacade) && t != typeof(IUserStoreFacade));

        foreach (var t in facadeInterfaces)
        {
            services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this test never routes a request to a handler that needs it."));
        }

        services.AddSingleton<StreamsForge.AppCore.Ingest.IngestKeyUsageTracker>();
    }

    private async Task<HttpClient> StartAsync(ICatalogFacade catalog, IReplayStatusFacade replay, string? authMode)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var configValues = new Dictionary<string, string?>
        {
            ["Jwt:Key"] = JwtKey,
            ["Jwt:Issuer"] = JwtIssuer,
            ["Jwt:Audience"] = JwtAudience,
        };
        if (authMode is not null)
        {
            configValues["Auth:Mode"] = authMode;
        }

        builder.Configuration.AddInMemoryCollection(configValues);
        builder.Services.AddStreamsForgeApi(builder.Configuration);
        builder.Services.AddSingleton(catalog);
        builder.Services.AddSingleton(replay);
        builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());
        RegisterUnusedHandlerDependencies(builder.Services);

        _app = builder.Build();
        _app.UseAuthentication();
        _app.UseAuthorization();
        _app.MapSourcesEndpoints();
        _app.MapPipelinesEndpoints();
        _app.MapTablesEndpoints(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-replay-endpoint-tests-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();

        var token = new JwtTokenService(builder.Configuration).CreateToken(
            new UserRecord { Username = "viewer", DisplayName = "Viewer", Role = BuiltInRoles.Viewer });
        var client = new HttpClient { BaseAddress = new Uri(address) };
        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        return client;
    }

    private sealed class FakeReplayStatusFacade : IReplayStatusFacade
    {
        public Func<string, Task<ReplayStatus?>> Source { get; set; } = _ => Task.FromResult<ReplayStatus?>(null);
        public Func<string, Task<ReplayStatus?>> Pipeline { get; set; } = _ => Task.FromResult<ReplayStatus?>(null);
        public Func<string, Task<ReplayStatus?>> Table { get; set; } = _ => Task.FromResult<ReplayStatus?>(null);

        public string? LastSourceName { get; private set; }
        public string? LastPipelineId { get; private set; }
        public string? LastTableName { get; private set; }

        public Task<ReplayStatus?> GetSourceAsync(string sourceName) { LastSourceName = sourceName; return Source(sourceName); }
        public Task<ReplayStatus?> GetPipelineAsync(string pipelineId) { LastPipelineId = pipelineId; return Pipeline(pipelineId); }
        public Task<ReplayStatus?> GetTableAsync(string tableName) { LastTableName = tableName; return Table(tableName); }
    }

    /// <summary>Only the members these tests actually reach are implemented; every route this test file
    /// does not exercise throws loudly rather than silently returning a wrong empty answer.</summary>
    private sealed class FakeCatalogFacade : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; init; } = [];
        public List<PipelineDefinition> Pipelines { get; init; } = [];
        public List<TableDefinition> Tables { get; init; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotImplementedException();

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);
        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);
        public Task<TableDefinition?> GetTableAsync(string id) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotImplementedException();
    }

    private sealed class FakeAccessPolicyFacade : IAccessPolicyFacade
    {
        // Deliberately empty (no roles/groups/users): see the class doc above for what this buys —
        // StrictViewerHandler succeeds outright, and AccessGuard (entitlements mode) then denies
        // everything, which is exactly the pairing the 200-in-legacy / 403-in-entitlements tests need.
        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(new AccessPolicyDocument());
        public Task<long> GetVersionAsync() => Task.FromResult(0L);
        public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotImplementedException();
        public Task<bool> DeleteRoleAsync(string name) => throw new NotImplementedException();
        public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotImplementedException();
        public Task<bool> DeleteGroupAsync(string name) => throw new NotImplementedException();
        public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotImplementedException();
        public Task<bool> DeleteUserAccessAsync(string username) => throw new NotImplementedException();
        public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotImplementedException();
        public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotImplementedException();
    }

    /// <summary>Exists only so <c>AccessBootstrapService</c> (a hosted service <c>AddStreamsForgeApi</c>
    /// always registers) can be constructed; its migration never runs anything this test observes.</summary>
    private sealed class FakeUserStoreFacade : IUserStoreFacade
    {
        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => Task.FromResult<UserRecord?>(null);
        public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(new List<UserRecord>());
        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => Task.FromResult(false);
        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => Task.FromResult(false);
        public Task<bool> DeleteUserAsync(string username) => Task.FromResult(false);
    }
}
