using System.Security.Claims;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.Api.Hubs;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 3-B — <see cref="StreamHub"/> was gated exactly once, at <c>[Authorize("Viewer")]</c> on
/// the class, and its subscribe methods checked nothing: any authenticated user could join any pipeline's
/// or source's group and receive its frames. This pins the fix from both sides — a subscription inside the
/// caller's scope still joins the group, one outside it is refused — and pins the refusal SHAPE, because
/// "how does the client learn it was refused" is the part a test would otherwise never notice regressing.
///
/// <para><see cref="HubException"/> specifically: it is the only exception type whose message SignalR
/// relays to the caller verbatim without <c>EnableDetailedErrors</c>. Anything else the hub threw would
/// reach the SPA as "An unexpected error occurred invoking 'SubscribePipeline'", which is exactly the
/// uninformative refusal this plan exists to remove — hence the assertion on the type AND on the message
/// carrying the evaluator's own reason.</para>
///
/// <para>The hub is driven directly rather than over a SignalR connection: <see cref="Hub.Context"/> and
/// <see cref="Hub.Groups"/> are settable, so the whole decision is exercisable with a fake caller context
/// and a recording group manager. There is no SignalR test harness in this repo and this needs none.</para>
/// </summary>
public class StreamHubEntitlementTests
{
    // Alice may read the two dev-* entities and anything tagged finance; nothing else.
    private static AccessPolicyDocument Document()
    {
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry
        {
            Username = "alice",
            Grants =
            [
                new PermissionGrant { Action = Actions.PipelineRead, Scope = "dev-*" },
                new PermissionGrant { Action = Actions.SourceRead, Scope = "dev-*" },
                new PermissionGrant { Action = Actions.TableRead, Scope = "tag:finance" },
            ],
        });
        return document;
    }

    private static (StreamHub Hub, RecordingGroups Groups) HubFor(ClaimsPrincipal user, AccessPolicyDocument? document = null)
    {
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document ?? Document()),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);

        var groups = new RecordingGroups();
        var hub = new StreamHub(
            new AccessGuard(resolver, entitlementsEnabled: true),
            new SingleCatalogServiceProvider(new StubCatalog(
                sources: [new SourceDefinition { Name = "dev-trades" }, new SourceDefinition { Name = "prod-trades" }],
                pipelines: [new PipelineDefinition { Id = "dev-1" }, new PipelineDefinition { Id = "prod-1" }],
                tables:
                [
                    new TableDefinition { Id = "t-fin", Name = "positions", Tags = ["finance"] },
                    new TableDefinition { Id = "t-hr", Name = "salaries", Tags = ["hr"] },
                ])),
            new NullEntityStreamFacade())
        {
            Context = new FakeCallerContext(user),
            Groups = groups,
        };

        return (hub, groups);
    }

    // ---------------------------------------------------------------------------------------------
    // Inside the scope: unchanged behaviour, the group is joined
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ASubscriptionInsideTheCallersScopeStillJoinsTheGroup()
    {
        var (hub, groups) = HubFor(PermissionResolverTests.Principal("alice"));

        await hub.SubscribePipeline("dev-1");
        await hub.SubscribeSource("dev-trades");

        Assert.Equal(["pipeline:dev-1", "source:dev-trades"], groups.Added);
    }

    [Fact]
    public async Task ATagScopedGrantAdmitsTheTableItTagsAndNothingElse()
    {
        // The whole reason the hub reads the definition before checking: `tag:finance` can only be
        // answered against the entity's Tags, and SubscribeTable is keyed by NAME while the catalog is
        // keyed by id — the lookup that resolves that is the part most likely to rot silently.
        var (hub, groups) = HubFor(PermissionResolverTests.Principal("alice"));

        await hub.SubscribeTable("positions");
        Assert.Equal(["table:positions"], groups.Added);

        await Assert.ThrowsAsync<HubException>(() => hub.SubscribeTable("salaries"));
        Assert.Equal(["table:positions"], groups.Added);
    }

    // ---------------------------------------------------------------------------------------------
    // Outside it: refused, in a shape the client can act on
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ASubscriptionOutsideTheCallersScopeIsRefusedAndTheGroupIsNotJoined()
    {
        var (hub, groups) = HubFor(PermissionResolverTests.Principal("alice"));

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub.SubscribePipeline("prod-1"));

        // The evaluator's own sentence, not a generic "forbidden": the same text the REST 403 body
        // carries, so an operator reads one explanation whichever transport they are on.
        Assert.Contains(Actions.PipelineRead, refusal.Message, StringComparison.Ordinal);
        Assert.Contains("prod-1", refusal.Message, StringComparison.Ordinal);
        Assert.Empty(groups.Added);

        await Assert.ThrowsAsync<HubException>(() => hub.SubscribeSource("prod-trades"));
        Assert.Empty(groups.Added);
    }

    [Fact]
    public async Task AnAuthenticatedUserWithNoEntitlementsAtAllReachesNothing()
    {
        // The regression this wave closes, stated as bluntly as it can be: passing the hub's
        // [Authorize("Viewer")] floor is no longer enough to subscribe to somebody else's stream.
        var (hub, groups) = HubFor(PermissionResolverTests.Principal("mallory"));

        await Assert.ThrowsAsync<HubException>(() => hub.SubscribePipeline("dev-1"));
        await Assert.ThrowsAsync<HubException>(() => hub.SubscribeSource("dev-trades"));
        await Assert.ThrowsAsync<HubException>(() => hub.SubscribeTable("positions"));
        await Assert.ThrowsAsync<HubException>(hub.SubscribeMetrics);

        Assert.Empty(groups.Added);
    }

    [Fact]
    public async Task AViewerRoleStillReachesEverythingItReachedBeforeTheUpgrade()
    {
        // The other half of "nothing that works today stops working": a migrated catalog gives Viewer
        // the built-in role's global read grants, so every subscribe the SPA makes today still lands.
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "viewer", Roles = [BuiltInRoles.Viewer] });

        var (hub, groups) = HubFor(PermissionResolverTests.Principal("viewer"), document);

        await hub.SubscribePipeline("prod-1");
        await hub.SubscribeSource("prod-trades");
        await hub.SubscribeTable("salaries");
        await hub.SubscribeMetrics();

        Assert.Equal(["pipeline:prod-1", "source:prod-trades", "table:salaries", "metrics"], groups.Added);
    }

    [Fact]
    public async Task UnsubscribeIsNeverRefused()
    {
        // Leaving a group takes nothing away from anybody, and a caller whose grant was just revoked
        // must still be able to detach — refusing would strand the very subscription we want gone.
        var (hub, groups) = HubFor(PermissionResolverTests.Principal("mallory"));

        await hub.UnsubscribePipeline("prod-1");
        await hub.UnsubscribeSource("prod-trades");
        await hub.UnsubscribeTable("salaries");

        Assert.Equal(["pipeline:prod-1", "source:prod-trades", "table:salaries"], groups.Removed);
    }

    [Fact]
    public async Task InLegacyModeTheHubBehavesExactlyAsItDidBefore()
    {
        // Auth:Mode=legacy is the plan's one-flag rollback and it has to reach here too, or the flag
        // would roll back REST and leave the hub enforcing.
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(Document()), NullLogger<PermissionResolver>.Instance, 600);
        var groups = new RecordingGroups();
        var hub = new StreamHub(new AccessGuard(resolver, entitlementsEnabled: false), new SingleCatalogServiceProvider(new StubCatalog([], [], [])), new NullEntityStreamFacade())
        {
            Context = new FakeCallerContext(PermissionResolverTests.Principal("mallory")),
            Groups = groups,
        };

        await hub.SubscribePipeline("prod-1");
        await hub.SubscribeMetrics();

        Assert.Equal(["pipeline:prod-1", "metrics"], groups.Added);
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

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

    private sealed class FakeCallerContext(ClaimsPrincipal user) : HubCallerContext
    {
        public override string ConnectionId => "conn-1";
        public override string? UserIdentifier => user.Identity?.Name;
        public override ClaimsPrincipal? User => user;
        public override IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();
        public override IFeatureCollection Features { get; } = new FeatureCollection();
        public override CancellationToken ConnectionAborted => CancellationToken.None;
        public override void Abort() { }
    }

    /// <summary>The three lookups the hub makes, and nothing else — every other member throws, matching
    /// the "interface conformance only" convention the other catalog fakes in this project use.</summary>
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
