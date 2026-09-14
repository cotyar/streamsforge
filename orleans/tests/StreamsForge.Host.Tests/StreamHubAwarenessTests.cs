using System.Security.Claims;
using System.Threading;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.Api.Hubs;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 020 wave G — presence/liveness for CRDT documents, off by default, over <c>StreamHub</c> and
/// <c>AwarenessRegistry</c>. Driven directly, same idiom as <see cref="StreamHubEntitlementTests"/>: the
/// hub's <c>Context</c>/<c>Groups</c>/<c>Clients</c> are all settable, so no SignalR connection is needed
/// to pin the authorization decision, the cap refusal, or the TTL expiry.
///
/// <para>Covers: the guard runs at the same action/scope a REST read of the source would use (and refuses
/// before anything is joined); a source that is not crdt-kind, or crdt-kind with no
/// <see cref="CrdtAwarenessConfig"/>, refuses loudly rather than joining a group nothing will ever publish
/// to; the cap is enforced and named in the refusal; a heartbeat is ungated and can only refresh an entry
/// <c>SubscribeAwareness</c> already created, never create one; an entry expires without a heartbeat and a
/// LIVE peer's own heartbeat is what surfaces that to the group; unsubscribe and disconnect both broadcast
/// the peer that left.</para>
/// </summary>
public class StreamHubAwarenessTests
{
    private static AccessPolicyDocument Document() => Grant(PermissionResolverTests.Doc(version: 1), "alice", Actions.SourceRead, "doc-*");

    private static AccessPolicyDocument Grant(AccessPolicyDocument d, string user, string action, string scope)
    {
        d.Users.Add(new UserAccessEntry { Username = user, Grants = [new PermissionGrant { Action = action, Scope = scope }] });
        return d;
    }

    private static SourceDefinition CrdtSource(string name, CrdtAwarenessConfig? awareness) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Connector = new ConnectorConfig { Crdt = new CrdtSourceConfig { Awareness = awareness } },
    };

    private static (StreamHub Hub, RecordingGroups Groups, RecordingClients Clients, AwarenessRegistry Registry) HubFor(
        ClaimsPrincipal user, string connectionId, IReadOnlyList<SourceDefinition> sources,
        AccessPolicyDocument? document = null, Func<DateTimeOffset>? clock = null)
    {
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document ?? Document()),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);

        var registry = new AwarenessRegistry(clock);
        var groups = new RecordingGroups();
        var clients = new RecordingClients();
        var hub = new StreamHub(
            new AccessGuard(resolver, entitlementsEnabled: true),
            new TestServiceProvider(new StubCatalog(sources), registry),
            new NullEntityStreamFacade())
        {
            Context = new FakeCallerContext(user, connectionId),
            Groups = groups,
            Clients = clients,
        };

        return (hub, groups, clients, registry);
    }

    // ---------------------------------------------------------------------------------------------
    // Authorization: the same read action/scope a REST read of the source would ask for
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AGrantedCallerJoinsAndSeesItsOwnEntry()
    {
        var awareness = new CrdtAwarenessConfig { TtlSeconds = 30, MaxEntries = 50 };
        var (hub, groups, _, _) = HubFor(
            PermissionResolverTests.Principal("alice"), "conn-1", [CrdtSource("doc-orders", awareness)]);

        var snapshot = await hub.SubscribeAwareness("doc-orders", "tab-1", "cursor-red");

        Assert.Equal(30, snapshot.TtlSeconds);
        Assert.Equal(50, snapshot.MaxEntries);
        var entry = Assert.Single(snapshot.Peers);
        Assert.Equal("tab-1", entry.ClientId);
        Assert.Equal("alice", entry.Identity);
        Assert.Equal("cursor-red", entry.Label);
        Assert.Equal(["crdt-awareness:doc-orders"], groups.Added);
    }

    [Fact]
    public async Task ACallerWithNoGrantOnTheSourceIsRefusedBeforeAnythingIsJoined()
    {
        var awareness = new CrdtAwarenessConfig();
        var (hub, groups, _, _) = HubFor(
            PermissionResolverTests.Principal("mallory"), "conn-1", [CrdtSource("doc-orders", awareness)]);

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub.SubscribeAwareness("doc-orders", "tab-1", null));

        Assert.Contains(Actions.SourceRead, refusal.Message, StringComparison.Ordinal);
        Assert.Empty(groups.Added);
    }

    [Fact]
    public async Task LabelDoesNotSubstituteForIdentity()
    {
        // A client can claim any label it likes, but Identity always comes from the authenticated
        // principal, never from client input -- presence answers "who is working on this", and a
        // spoofable identity field would defeat that.
        var awareness = new CrdtAwarenessConfig();
        var (hub, _, _, _) = HubFor(
            PermissionResolverTests.Principal("alice"), "conn-1", [CrdtSource("doc-orders", awareness)]);

        var snapshot = await hub.SubscribeAwareness("doc-orders", "tab-1", "pretending-to-be-bob");

        Assert.Equal("alice", Assert.Single(snapshot.Peers).Identity);
    }

    // ---------------------------------------------------------------------------------------------
    // Loud refusals: unlike SubscribeSource/SubscribeTable, a misconfigured target never joins silently
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ANonCrdtSourceRefusesRatherThanJoiningAnEmptyGroup()
    {
        var (hub, groups, _, _) = HubFor(
            PermissionResolverTests.Principal("alice"), "conn-1",
            [new SourceDefinition { Name = "doc-plain", Kind = SourceKinds.Generator }]);

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub.SubscribeAwareness("doc-plain", "tab-1", null));

        Assert.Contains("not crdt-kind", refusal.Message, StringComparison.Ordinal);
        Assert.Empty(groups.Added);
    }

    [Fact]
    public async Task ACrdtSourceWithNoAwarenessConfigRefuses()
    {
        var (hub, groups, _, _) = HubFor(
            PermissionResolverTests.Principal("alice"), "conn-1", [CrdtSource("doc-orders", awareness: null)]);

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub.SubscribeAwareness("doc-orders", "tab-1", null));

        Assert.Contains("awareness is not enabled", refusal.Message, StringComparison.Ordinal);
        Assert.Empty(groups.Added);
    }

    [Fact]
    public async Task AnUnknownSourceRefusesAfterTheGuardRuns()
    {
        var (hub, groups, _, _) = HubFor(PermissionResolverTests.Principal("alice"), "conn-1", []);

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub.SubscribeAwareness("doc-ghost", "tab-1", null));

        Assert.Contains("not found", refusal.Message, StringComparison.Ordinal);
        Assert.Empty(groups.Added);
    }

    // ---------------------------------------------------------------------------------------------
    // The cap: visible, not silent
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheCapIsEnforcedAndNamedInTheRefusal()
    {
        var awareness = new CrdtAwarenessConfig { MaxEntries = 1 };
        var source = CrdtSource("doc-orders", awareness);
        var document = Grant(PermissionResolverTests.Doc(1), "alice", Actions.SourceRead, "*");

        var (hub1, _, _, registry) = HubFor(PermissionResolverTests.Principal("alice"), "conn-1", [source], document);
        await hub1.SubscribeAwareness("doc-orders", "tab-1", null);

        // A second CONNECTION (same registry, same document, different connectionId) hits the cap.
        var hub2 = new StreamHub(
            new AccessGuard(new PermissionResolver(new CountingAccessPolicyFacade(document), NullLogger<PermissionResolver>.Instance, 600), entitlementsEnabled: true),
            new TestServiceProvider(new StubCatalog([source]), registry),
            new NullEntityStreamFacade())
        {
            Context = new FakeCallerContext(PermissionResolverTests.Principal("alice"), "conn-2"),
            Groups = new RecordingGroups(),
            Clients = new RecordingClients(),
        };

        var refusal = await Assert.ThrowsAsync<HubException>(() => hub2.SubscribeAwareness("doc-orders", "tab-2", null));
        Assert.Contains("cap of 1", refusal.Message, StringComparison.Ordinal);

        // Re-subscribing the SAME connection never counts against its own cap slot.
        var again = await hub1.SubscribeAwareness("doc-orders", "tab-1", null);
        Assert.Single(again.Peers);
    }

    // ---------------------------------------------------------------------------------------------
    // Heartbeat: ungated, cannot create an entry, refreshes TTL, broadcasts only on real eviction
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task HeartbeatFromAConnectionThatNeverSubscribedCreatesNothing()
    {
        // No grant at all for "mallory" -- if Heartbeat could create a presence entry on its own, this
        // would be the back door around SubscribeAwareness's AccessGuard check.
        var (hub, _, clients, registry) = HubFor(PermissionResolverTests.Principal("mallory"), "conn-x", []);

        await hub.Heartbeat("doc-orders");

        Assert.Empty(registry.RemoveConnection("conn-x"));
        Assert.Empty(clients.GroupSends);
    }

    [Fact]
    public async Task AStaleEntryIsEvictedAndBroadcastByAnotherMembersHeartbeat()
    {
        var now = DateTimeOffset.UtcNow;
        DateTimeOffset Clock() => now;
        var awareness = new CrdtAwarenessConfig { TtlSeconds = 10, MaxEntries = 50 };
        var source = CrdtSource("doc-orders", awareness);
        var document = Grant(PermissionResolverTests.Doc(1), "alice", Actions.SourceRead, "*");
        document.Users.Add(new UserAccessEntry { Username = "bob", Grants = [new PermissionGrant { Action = Actions.SourceRead, Scope = "*" }] });

        var (hubA, _, clientsA, registry) = HubFor(PermissionResolverTests.Principal("alice"), "conn-a", [source], document, Clock);
        await hubA.SubscribeAwareness("doc-orders", "tab-a", null);

        // Bob joins 3s later than Alice, so his own expiry (t=13) outlives hers (t=10) even though both
        // carry the same 10s TTL -- the scenario needs that gap, or advancing time past Alice's expiry
        // would equally strand Bob's own entry before he ever gets to heartbeat.
        now = now.AddSeconds(3);
        var hubB = new StreamHub(
            new AccessGuard(new PermissionResolver(new CountingAccessPolicyFacade(document), NullLogger<PermissionResolver>.Instance, 600), entitlementsEnabled: true),
            new TestServiceProvider(new StubCatalog([source]), registry),
            new NullEntityStreamFacade())
        {
            Context = new FakeCallerContext(PermissionResolverTests.Principal("bob"), "conn-b"),
            Groups = new RecordingGroups(),
            Clients = new RecordingClients(),
        };
        var joined = await hubB.SubscribeAwareness("doc-orders", "tab-b", null);
        Assert.Equal(2, joined.Peers.Count);

        // Alice's tab goes dark: no more heartbeats. 8 more seconds put the clock at t=11 -- past
        // Alice's t=10 expiry but before Bob's t=13 one -- so Bob's own heartbeat's eviction pass finds
        // ONLY Alice's entry expired, and that is what surfaces her departure, with no timer anywhere in
        // this registry.
        now = now.AddSeconds(8);
        await hubB.Heartbeat("doc-orders");

        var (_, method, args) = Assert.Single(((RecordingClients)hubB.Clients).GroupSends);
        Assert.Equal("awarenessUpdate", method);
        var peers = Assert.IsAssignableFrom<IReadOnlyList<AwarenessEntry>>(args[1]);
        Assert.Equal(["tab-b"], peers.Select(p => p.ClientId));
    }

    // ---------------------------------------------------------------------------------------------
    // Unsubscribe and disconnect: both broadcast, neither needs a grant
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task UnsubscribeRemovesTheEntryAndBroadcastsWhoIsLeft()
    {
        var awareness = new CrdtAwarenessConfig();
        var source = CrdtSource("doc-orders", awareness);
        var document = Grant(PermissionResolverTests.Doc(1), "alice", Actions.SourceRead, "*");

        var (hub, groups, clients, _) = HubFor(PermissionResolverTests.Principal("alice"), "conn-1", [source], document);
        await hub.SubscribeAwareness("doc-orders", "tab-1", null);

        await hub.UnsubscribeAwareness("doc-orders");

        Assert.Equal(["crdt-awareness:doc-orders"], groups.Removed);
        var (_, method, args) = Assert.Single(clients.GroupSends);
        Assert.Equal("awarenessUpdate", method);
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<AwarenessEntry>>(args[1]));
    }

    [Fact]
    public async Task DisconnectCleansUpEveryDocumentTheConnectionJoined()
    {
        var awareness = new CrdtAwarenessConfig();
        var docA = CrdtSource("doc-a", awareness);
        var docB = CrdtSource("doc-b", awareness);
        var document = Grant(PermissionResolverTests.Doc(1), "alice", Actions.SourceRead, "*");

        var (hub, _, clients, registry) = HubFor(PermissionResolverTests.Principal("alice"), "conn-1", [docA, docB], document);
        await hub.SubscribeAwareness("doc-a", "tab-1", null);
        await hub.SubscribeAwareness("doc-b", "tab-1", null);

        await hub.OnDisconnectedAsync(null);

        var broadcastGroups = clients.GroupSends.Select(s => s.Group).OrderBy(g => g, StringComparer.Ordinal).ToList();
        Assert.Equal(["crdt-awareness:doc-a", "crdt-awareness:doc-b"], broadcastGroups);
        Assert.Empty(registry.RemoveConnection("conn-1"));
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

    private sealed class TestServiceProvider(ICatalogFacade catalog, AwarenessRegistry registry) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            serviceType == typeof(ICatalogFacade) ? catalog :
            serviceType == typeof(AwarenessRegistry) ? registry :
            null;
    }

    private sealed class StubCatalog(IReadOnlyList<SourceDefinition> sources) : ICatalogFacade
    {
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(sources.FirstOrDefault(s => s.Name == name));

        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotImplementedException();
        public Task<List<TableDefinition>> GetTablesAsync() => throw new NotImplementedException();
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

    /// <summary>Records every <c>Group(...)</c>/<c>OthersInGroup(...)</c> send. Every other member of
    /// <see cref="IHubCallerClients"/> throws — this hub never calls them.</summary>
    private sealed class RecordingClients : IHubCallerClients
    {
        public List<(string Group, string Method, object?[] Args)> GroupSends { get; } = [];
        public List<(string Group, string Method, object?[] Args)> OthersInGroupSends { get; } = [];

        public IClientProxy Group(string groupName) => new RecordingProxy(this, groupName, others: false);
        public IClientProxy OthersInGroup(string groupName) => new RecordingProxy(this, groupName, others: true);

        public IClientProxy All => throw new NotImplementedException();
        public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotImplementedException();
        public ISingleClientProxy Client(string connectionId) => throw new NotImplementedException();
        public IClientProxy Clients(IReadOnlyList<string> connectionIds) => throw new NotImplementedException();
        public IClientProxy Groups(IReadOnlyList<string> groupNames) => throw new NotImplementedException();
        public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotImplementedException();
        public IClientProxy User(string userId) => throw new NotImplementedException();
        public IClientProxy Users(IReadOnlyList<string> userIds) => throw new NotImplementedException();
        public ISingleClientProxy Caller => throw new NotImplementedException();
        public IClientProxy Others => throw new NotImplementedException();
        IClientProxy IHubClients<IClientProxy>.Client(string connectionId) => Client(connectionId);
        IClientProxy IHubCallerClients<IClientProxy>.Caller => Caller;

        private sealed class RecordingProxy(RecordingClients owner, string group, bool others) : IClientProxy
        {
            public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default)
            {
                (others ? owner.OthersInGroupSends : owner.GroupSends).Add((group, method, args));
                return Task.CompletedTask;
            }
        }
    }

    private sealed class FakeCallerContext(ClaimsPrincipal user, string connectionId) : HubCallerContext
    {
        public override string ConnectionId => connectionId;
        public override string? UserIdentifier => user.Identity?.Name;
        public override ClaimsPrincipal? User => user;
        public override IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();
        public override IFeatureCollection Features { get; } = new FeatureCollection();
        public override CancellationToken ConnectionAborted => CancellationToken.None;
        public override void Abort() { }
    }
}
