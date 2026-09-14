using System.Security.Claims;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.Api.Hubs;
using StreamsForge.AppCore.Environments;
using StreamsForge.Host.Facades;
using Xunit;
using V1 = StreamsForge.Host.Grpc.V1;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 026 wave 1 agent B — the replaying half of <see cref="IEntityStreamFacade.SubscribeSourceAsync(string, string, ReplayFrom?, System.Func{System.Collections.Generic.IReadOnlyDictionary{string, object?}, long, long, System.Threading.Tasks.Task})"/>,
/// <see cref="StreamsForge.Host.Grpc.StreamGrpcService.SubscribeSource"/>'s <c>from_seq</c>/<c>from_timestamp_ms</c>
/// handling, and <see cref="StreamHub.SubscribeSourceFrom"/> — proven against a real
/// <see cref="TestCluster"/>, the same fixture shape as <see cref="SourceLateConsumerClusterTests"/> (own
/// cluster, own scratch dir, <see cref="ConnectorTestSiloConfigurator"/>/<see cref="ConnectorTestClientConfigurator"/>
/// reused from <see cref="ConnectorGrainClusterTests"/> — both `internal`, same assembly).
///
/// <para>gRPC and hub call sites are driven directly (no ASP.NET Core host, no real SignalR connection),
/// the same idiom <see cref="IngestGrpcEntitlementTests"/> and <see cref="StreamHubEntitlementTests"/> use
/// — a fake <see cref="ServerCallContext"/>/<see cref="IServerStreamWriter{T}"/> pair for gRPC, a fake
/// <see cref="HubCallerContext"/>/<see cref="IHubCallerClients"/>/<see cref="IGroupManager"/> trio for the
/// hub.</para>
/// </summary>
public sealed class SourceReplayGrpcTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-replay-grpc-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_scratchDir, recursive: true); } catch { /* best-effort */ }
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    // =================================================================================================
    // Fixtures shared by every fact below — file source setup, borrowed verbatim from
    // SourceLateConsumerClusterTests (same file, same reasons for the settle delay / mtime touch; not
    // shared across files by this codebase's own convention).
    // =================================================================================================

    private static SourceDefinition FileSource(string name, string path) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = true,
        Fields =
        [
            new FieldDef("id", FieldType.Long),
            new FieldDef("value", FieldType.String),
        ],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                DedupKeyField = "id",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
                    new FieldMapEntry { Field = new FieldDef("value", FieldType.String) },
                ],
            },
        },
    };

    private static string Ndjson(int from, int count) =>
        string.Concat(Enumerable.Range(from, count).Select(i => $"{{\"id\":{i},\"value\":\"v{i}\"}}\n"));

    private static async Task AppendRowsAsync(string path, int from, int count)
    {
        await File.AppendAllTextAsync(path, Ndjson(from, count));
        await Task.Delay(1200);
        File.SetLastWriteTimeUtc(path, DateTime.UtcNow);
    }

    /// <summary>Creates the source already enabled, waits for every row to be published, then waits an
    /// extra 2s for the stream to quiesce — see <c>SourceLateConsumerClusterTests.StartedFileSourceWithAsync</c>'s
    /// own doc comment for why the settle delay is load-bearing rather than padding (a subscribe inside
    /// the in-flight window sees a row live AND replayed, at-least-once instead of exactly-once).</summary>
    private async Task<(string Name, string Path)> StartedFileSourceWithAsync(string label, int rows)
    {
        var name = label + "_" + Guid.NewGuid().ToString("n")[..8];
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(1, rows));

        await Registry.UpsertSourceAsync(FileSource(name, path));

        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var status = await PollUntilAsync(
            () => connector.GetStatusAsync(),
            s => s.EventsEmittedTotal >= rows,
            deadlineSeconds: 90);
        Assert.Equal(rows, status.EventsEmittedTotal);

        await Task.Delay(2000);

        return (name, path);
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        if (until(last)) return last;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }

    // =================================================================================================
    // Facade: OrleansEntityStreamFacade.SubscribeSourceAsync(from) directly over the cluster client
    // =================================================================================================

    [Fact]
    public async Task Facade_replays_from_a_position_then_continues_live_with_consecutive_positions()
    {
        var (source, path) = await StartedFileSourceWithAsync("replay_facade", 500);
        var facade = new OrleansEntityStreamFacade(_cluster.Client);

        var received = new List<(long Id, long Position)>();
        var handle = await facade.SubscribeSourceAsync(EnvKeys.Default, source, new ReplayFrom { Seq = 401 },
            (row, _, position) =>
            {
                lock (received)
                {
                    received.Add((Convert.ToInt64(row["id"]), position));
                }
                return Task.CompletedTask;
            });

        try
        {
            // The whole snapshot is fed through the handler before SubscribeSourceAsync returns — no
            // polling needed for the replayed half.
            Assert.Equal(100, received.Count);
            Assert.Equal(Enumerable.Range(401, 100).Select(i => (long)i), received.Select(r => r.Id));
            Assert.Equal(Enumerable.Range(401, 100).Select(i => (long)i), received.Select(r => r.Position));

            await AppendRowsAsync(path, 501, 20);

            await PollUntilAsync(
                () => Task.FromResult(received.Count),
                c => c >= 120,
                deadlineSeconds: 90);

            Assert.Equal(120, received.Count);
            Assert.Equal(Enumerable.Range(501, 20).Select(i => (long)i), received.Skip(100).Select(r => r.Id));
            Assert.Equal(Enumerable.Range(501, 20).Select(i => (long)i), received.Skip(100).Select(r => r.Position));

            Assert.Equal(1L, handle.FirstSeq);
            Assert.Equal(500L, handle.LastSeq);
            Assert.False(handle.Truncated);
        }
        finally
        {
            await handle.DisposeAsync();
            await Registry.DeleteSourceAsync(source);
        }
    }

    [Fact]
    public async Task Two_subscribers_see_the_same_position_for_the_same_row()
    {
        var (source, _) = await StartedFileSourceWithAsync("replay_two_subs", 200);
        var facade = new OrleansEntityStreamFacade(_cluster.Client);

        var byIdA = new Dictionary<long, long>();
        var byIdB = new Dictionary<long, long>();

        var handleA = await facade.SubscribeSourceAsync(EnvKeys.Default, source, new ReplayFrom { Seq = 1 },
            (row, _, position) => { byIdA[Convert.ToInt64(row["id"])] = position; return Task.CompletedTask; });
        var handleB = await facade.SubscribeSourceAsync(EnvKeys.Default, source, new ReplayFrom { Seq = 1 },
            (row, _, position) => { byIdB[Convert.ToInt64(row["id"])] = position; return Task.CompletedTask; });

        try
        {
            Assert.Equal(200, byIdA.Count);
            Assert.Equal(200, byIdB.Count);
            foreach (var (id, positionA) in byIdA)
            {
                Assert.Equal(positionA, byIdB[id]);
            }
        }
        finally
        {
            await handleA.DisposeAsync();
            await handleB.DisposeAsync();
            await Registry.DeleteSourceAsync(source);
        }
    }

    // =================================================================================================
    // gRPC: StreamGrpcService.SubscribeSource
    // =================================================================================================

    private static AccessGuard PermissiveGuard()
    {
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(PermissionResolverTests.Doc(version: 1)),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);
        // entitlementsEnabled: false — CheckAsync short-circuits to Allowed for everything, exactly like
        // EnvironmentHubTests.GroupNaming.HubFor: these facts are about replay/position wiring, not about
        // who may subscribe (StreamHubEntitlementTests' job).
        return new AccessGuard(resolver, entitlementsEnabled: false);
    }

    private StreamsForge.Host.Grpc.StreamGrpcService GrpcService() =>
        new(Registry, new OrleansEntityStreamFacade(_cluster.Client), PermissiveGuard());

    [Fact]
    public async Task Grpc_SubscribeSource_writes_position_and_replay_truncated()
    {
        var (source, _) = await StartedFileSourceWithAsync("replay_grpc", 500);
        var service = GrpcService();

        var writer = new RecordingStreamWriter<V1.SourceEvent>();
        using var cts = new CancellationTokenSource();
        var call = service.SubscribeSource(
            new V1.SubscribeSourceRequest { Name = source, FromSeq = 401 }, writer, FakeContext(cts.Token));

        await PollUntilAsync(() => Task.FromResult(writer.Written.Count), c => c >= 100, deadlineSeconds: 90);
        cts.Cancel();
        await call;

        Assert.Equal(100, writer.Written.Count);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal((long)(401 + i), writer.Written[i].Position);
            Assert.Equal((long)(i + 1), writer.Written[i].Seq);
        }
        Assert.False(writer.Written[0].ReplayTruncated);

        await Registry.DeleteSourceAsync(source);
    }

    [Fact]
    public async Task Grpc_SubscribeSource_surfaces_replay_truncated_on_the_first_event_past_retention()
    {
        // ReplayLog's default capacity is 10 000 (shared/StreamsForge.AppCore/Streaming/ReplayLog.cs) —
        // 10 005 rows retains positions 6..10005, so `from_seq: 1` is truncated and the first replayed
        // event carries position 6.
        var (source, _) = await StartedFileSourceWithAsync("replay_grpc_trunc", 10_005);
        var service = GrpcService();

        var writer = new RecordingStreamWriter<V1.SourceEvent>();
        using var cts = new CancellationTokenSource();
        var call = service.SubscribeSource(
            new V1.SubscribeSourceRequest { Name = source, FromSeq = 1 }, writer, FakeContext(cts.Token));

        await PollUntilAsync(() => Task.FromResult(writer.Written.Count), c => c >= 1, deadlineSeconds: 90);
        cts.Cancel();
        await call;

        Assert.True(writer.Written[0].ReplayTruncated);
        Assert.Equal(6L, writer.Written[0].Position);

        await Registry.DeleteSourceAsync(source);
    }

    [Fact]
    public async Task Grpc_SubscribeSource_refuses_a_crdt_kind_source_asked_to_replay_from_a_position()
    {
        var name = "replay_crdt_" + Guid.NewGuid().ToString("n")[..8];
        // Enabled: false — an enabled crdt source needs the crdt plugin loaded (RegistryGrain.UpsertSourceAsync),
        // which this bare TestCluster does not have; disabled is enough to put a crdt-kind definition in
        // the catalog for the facade to classify.
        await Registry.UpsertSourceAsync(new SourceDefinition { Name = name, Kind = SourceKinds.Crdt, Enabled = false });

        var service = GrpcService();
        var writer = new RecordingStreamWriter<V1.SourceEvent>();
        using var cts = new CancellationTokenSource();

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.SubscribeSource(new V1.SubscribeSourceRequest { Name = name, FromSeq = 1 }, writer, FakeContext(cts.Token)));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);

        await Registry.DeleteSourceAsync(name);
    }

    // =================================================================================================
    // Hub: StreamHub.SubscribeSourceFrom
    // =================================================================================================

    [Fact]
    public async Task Hub_SubscribeSourceFrom_replays_to_the_caller_then_joins_the_group()
    {
        var (source, _) = await StartedFileSourceWithAsync("replay_hub", 50);

        var groups = new RecordingGroups();
        var clients = new RecordingClients();
        var hub = new StreamHub(
            PermissiveGuard(),
            new SingleCatalogServiceProvider(Registry),
            new OrleansEntityStreamFacade(_cluster.Client))
        {
            Context = new FakeCallerContext(PermissionResolverTests.Principal("alice")),
            Groups = groups,
            Clients = clients,
        };

        await hub.SubscribeSourceFrom(source, fromSeq: 1, fromTimestampMs: null);

        Assert.Equal(50, clients.CallerSends.Count);
        for (var i = 0; i < 50; i++)
        {
            var (method, args) = clients.CallerSends[i];
            Assert.Equal("sourceEvent", method);
            Assert.Equal(source, args[0]);
            Assert.Equal((long)(i + 1), args[2]);
        }

        Assert.Single(groups.Added);
        Assert.Equal($"source:{source}", groups.Added[0]);

        await Registry.DeleteSourceAsync(source);
    }

    // =================================================================================================
    // Fakes
    // =================================================================================================

    private static ServerCallContext FakeContext(CancellationToken cancellationToken)
    {
        var ctx = new FakeServerCallContext(cancellationToken);
        ctx.UserState["__HttpContext"] = new DefaultHttpContext
        {
            User = PermissionResolverTests.Principal("alice"),
        };
        return ctx;
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore { get; } = new();
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore { get; } = new("test", new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken? CreatePropagationTokenCore(ContextPropagationOptions? options) => null;
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    private sealed class RecordingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Written { get; } = [];
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Written.Add(message);
            return Task.CompletedTask;
        }

        public Task WriteAsync(T message, CancellationToken cancellationToken) => WriteAsync(message);
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

    private sealed class RecordingGroups : IGroupManager
    {
        public List<string> Added { get; } = [];

        public Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
        {
            Added.Add(groupName);
            return Task.CompletedTask;
        }

        public Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    /// <summary>Only <see cref="Caller"/> does anything real — <see cref="StreamHub.SubscribeSourceFrom"/>
    /// never touches the rest of <see cref="IHubCallerClients"/>. The dual <c>Caller</c>/<c>Client</c>
    /// declarations (a narrowing one on the non-generic interface, a widening one on
    /// <see cref="IHubCallerClients{T}"/>) mirror <c>StreamHubAwarenessTests.RecordingClients</c> exactly.</summary>
    private sealed class RecordingClients : IHubCallerClients
    {
        public List<(string Method, object?[] Args)> CallerSends { get; } = [];

        public ISingleClientProxy Caller { get; }
        IClientProxy IHubCallerClients<IClientProxy>.Caller => Caller;

        public RecordingClients() => Caller = new RecordingProxy(this);

        public ISingleClientProxy Client(string connectionId) => throw new NotImplementedException("not exercised by this test");
        IClientProxy IHubClients<IClientProxy>.Client(string connectionId) => Client(connectionId);

        public IClientProxy All => throw new NotImplementedException("not exercised by this test");
        public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy Clients(IReadOnlyList<string> connectionIds) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy Group(string groupName) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy Groups(IReadOnlyList<string> groupNames) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy OthersInGroup(string groupName) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy User(string userId) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy Users(IReadOnlyList<string> userIds) => throw new NotImplementedException("not exercised by this test");
        public IClientProxy Others => throw new NotImplementedException("not exercised by this test");

        private sealed class RecordingProxy(RecordingClients owner) : ISingleClientProxy
        {
            public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default)
            {
                owner.CallerSends.Add((method, args));
                return Task.CompletedTask;
            }

            public Task<T> InvokeCoreAsync<T>(string method, object?[] args, CancellationToken cancellationToken = default) =>
                throw new NotImplementedException("not exercised by this test");
        }
    }
}
