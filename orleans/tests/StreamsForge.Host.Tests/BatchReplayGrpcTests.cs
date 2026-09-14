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
/// Plan 026 wave 2 agent A — the batch-grained replay half of
/// <see cref="IEntityStreamFacade.SubscribePipelineAsync(string, string, ReplayFrom?, System.Func{System.Collections.Generic.IReadOnlyList{ResultEnvelope}, long, System.Threading.Tasks.Task})"/>/
/// <see cref="IEntityStreamFacade.SubscribeTableAsync(string, string, ReplayFrom?, System.Func{System.Collections.Generic.IReadOnlyList{TableDeltaDto}, long, System.Threading.Tasks.Task})"/>,
/// <see cref="StreamsForge.Host.Grpc.StreamGrpcService.SubscribePipeline"/>/<c>SubscribeTable</c>'s
/// <c>from_seq</c>/<c>from_timestamp_ms</c> handling, and <see cref="StreamHub.SubscribePipelineFrom"/>/
/// <see cref="StreamHub.SubscribeTableFrom"/> — the exact model and fixture shape of wave 1's
/// <see cref="SourceReplayGrpcTests"/> (own <see cref="TestCluster"/>, own scratch dir, the same
/// <see cref="ConnectorTestSiloConfigurator"/>/<see cref="ConnectorTestClientConfigurator"/>, gRPC/hub call
/// sites driven directly with fakes rather than a real ASP.NET Core host or SignalR connection).
///
/// <para>Unlike a source, a pipeline or a table publishes one item PER BATCH rather than per row, so
/// "position" here is a batch position: <see cref="TableGrain.OnStreamEventAsync"/>/
/// <c>ApplyAndPublishAsync</c> and <see cref="PipelineGrain.OnSourceEventAsync"/>/<c>PublishRowsAsync</c>
/// both publish exactly one batch per input event they admit — a fact several assertions below rely on to
/// predict positions deterministically for a <c>LATEST BY</c> table (unique keys, no retractions) and a
/// windowless pass-through pipeline (no aggregation, one emitted row per input row).</para>
/// </summary>
public sealed class BatchReplayGrpcTests : IAsyncLifetime
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

        _scratchDir = Directory.CreateTempSubdirectory("sf-batch-replay-grpc-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_scratchDir, recursive: true); } catch { /* best-effort */ }
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    // =================================================================================================
    // Fixtures — file source setup, borrowed verbatim from SourceReplayGrpcTests/SourceLateConsumerClusterTests
    // (same file, same reasons for the settle delay / mtime touch; not shared across files by this
    // codebase's own convention).
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
    /// own doc comment for why the settle delay is load-bearing rather than padding.</summary>
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

    /// <summary>Writes the full NDJSON content up front and enables the source, but — unlike
    /// <see cref="StartedFileSourceWithAsync"/> — returns immediately, before the connector's first poll
    /// fires. A caller creates its consumer (table/pipeline) right after this returns, so the consumer's
    /// live subscription is already in place when that first (bulk) poll happens, and every row reaches it
    /// live rather than through a late-attach backfill. Two reasons a late attach is the wrong fixture here,
    /// both found live in this wave: (1) a windowless pass-through PIPELINE's <c>AttachToSourceAsync</c>
    /// subscribes to the live stream FIRST and then feeds the connector's retained rows through the SAME
    /// handler a second time — for a <c>LATEST BY</c> TABLE input that is invisible (a duplicate
    /// re-assertion of an unchanged key nets to a no-op the engine publishes nothing for), but a plain
    /// passthrough pipeline has no such idempotency and double-counts every backfilled row against its own
    /// replay log; (2) independently, a table that late-attaches to a source already holding MORE than the
    /// source's own 10 000-entry replay log can backfill only what THAT log still retains, capping the
    /// table's own log below what a truncation test needs to observe. Neither is something wave 2 owns
    /// fixing in <c>PipelineGrain.cs</c>/<c>TableGrain.cs</c> (agent B's files) — it is a test-fixture
    /// concern, sidestepped by never taking the late-attach path at all.</summary>
    private async Task<(string Name, string Path)> UpsertFileSourceAsync(string label, int rows)
    {
        var name = label + "_" + Guid.NewGuid().ToString("n")[..8];
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(1, rows));
        await Registry.UpsertSourceAsync(FileSource(name, path));
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

    /// <summary>Creates and starts a table via the registry (the sanctioned path — <c>ITableGrain</c>'s key
    /// is the table name, so this is what a gRPC/hub caller addresses too).</summary>
    private async Task<string> StartRunningTableAsync(string sql, int parallelism = 1)
    {
        var tableName = "tbl_" + Guid.NewGuid().ToString("n")[..8];
        var created = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = sql,
            Parallelism = parallelism,
        });
        await Registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return tableName;
    }

    // =================================================================================================
    // Facade: OrleansEntityStreamFacade.SubscribeTableAsync(from) directly over the cluster client
    // =================================================================================================

    [Fact]
    public async Task Table_delta_batches_replay_from_a_position_then_continue_live()
    {
        var (source, path) = await StartedFileSourceWithAsync("batch_replay_table", 200);
        var tableName = await StartRunningTableAsync($"SELECT id, value FROM {source} LATEST BY (id)");
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 200, deadlineSeconds: 90);
        Assert.Equal(200, await table.GetRowCountAsync());
        await Task.Delay(2000);

        // LATEST BY over 200 distinct ids never retracts, and TableGrain publishes one delta batch per
        // admitted input event (see class doc), so the gate's LastSeq is exactly 200.
        long lastSeq;
        var baseline = await table.BeginAttachAsync(null);
        try { lastSeq = baseline.LastSeq; }
        finally { await table.EndAttachAsync(); }
        Assert.Equal(200L, lastSeq);

        var facade = new OrleansEntityStreamFacade(_cluster.Client);
        var received = new List<long>();
        var handle = await facade.SubscribeTableAsync(EnvKeys.Default, tableName, new ReplayFrom { Seq = lastSeq - 4 },
            (deltas, position) =>
            {
                lock (received) received.Add(position);
                return Task.CompletedTask;
            });

        try
        {
            Assert.Equal(5, received.Count);
            Assert.Equal(Enumerable.Range((int)(lastSeq - 4), 5).Select(i => (long)i), received);

            await AppendRowsAsync(path, 201, 10);

            await PollUntilAsync(() => Task.FromResult(received.Count), c => c >= 15, deadlineSeconds: 90);
            Assert.Equal(15, received.Count);
            Assert.Equal(Enumerable.Range((int)lastSeq + 1, 10).Select(i => (long)i), received.Skip(5));

            Assert.False(handle.Truncated);
        }
        finally
        {
            await handle.DisposeAsync();
            await Registry.DeleteSourceAsync(source);
        }
    }

    /// <summary>D7 acceptance, proven live: <c>AttachSnapshotAsync().LastSeq</c> equals exactly the
    /// position a concurrent replaying subscriber last saw, never more, never less.</summary>
    [Fact]
    public async Task Attach_snapshot_LastSeq_matches_the_position_a_concurrent_subscriber_saw()
    {
        var (source, _) = await StartedFileSourceWithAsync("batch_replay_attach_seq", 150);
        var tableName = await StartRunningTableAsync($"SELECT id, value FROM {source} LATEST BY (id)");
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 150, deadlineSeconds: 90);
        Assert.Equal(150, await table.GetRowCountAsync());

        var facade = new OrleansEntityStreamFacade(_cluster.Client);
        long lastSeenPosition = 0;
        var handle = await facade.SubscribeTableAsync(EnvKeys.Default, tableName, new ReplayFrom { Seq = 1 },
            (_, position) =>
            {
                Interlocked.Exchange(ref lastSeenPosition, position);
                return Task.CompletedTask;
            });

        try
        {
            await PollUntilAsync(
                () => Task.FromResult(Interlocked.Read(ref lastSeenPosition)),
                p => p >= 150,
                deadlineSeconds: 90);

            // Quiesce — nothing else is published from here, so lastSeenPosition is stable.
            await Task.Delay(1000);

            var snapshot = await table.AttachSnapshotAsync();
            Assert.Equal(Interlocked.Read(ref lastSeenPosition), snapshot.LastSeq);
        }
        finally
        {
            await handle.DisposeAsync();
            await Registry.DeleteSourceAsync(source);
        }
    }

    /// <summary>A coordinator-mode (Parallelism &gt;= 2) table's gate never held anything for a replay
    /// request (wave 3 owes routing that path through a gate — see <c>TableGrain.BeginAttachAsync</c>'s
    /// own doc comment), so asking for a position gets nothing back, honestly flagged truncated — but the
    /// SAME underlying delta stream still carries live batches, which the facade subscribes to directly and
    /// numbers from 1 (the snapshot's LastSeq, 0 for coordinator mode, plus the live counter).</summary>
    [Fact]
    public async Task Coordinator_mode_table_replay_request_is_empty_and_truncated_but_live_batches_still_flow()
    {
        var (source, path) = await StartedFileSourceWithAsync("batch_replay_coordinator", 50);
        var tableName = await StartRunningTableAsync($"SELECT id, value FROM {source} LATEST BY (id)", parallelism: 2);
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 50, deadlineSeconds: 90);
        Assert.Equal(50, await table.GetRowCountAsync());
        await Task.Delay(2000);

        var facade = new OrleansEntityStreamFacade(_cluster.Client);
        var received = new List<long>();
        var handle = await facade.SubscribeTableAsync(EnvKeys.Default, tableName, new ReplayFrom { Seq = 1 },
            (_, position) =>
            {
                lock (received) received.Add(position);
                return Task.CompletedTask;
            });

        try
        {
            Assert.Empty(received);
            Assert.True(handle.Truncated);

            await AppendRowsAsync(path, 51, 10);

            await PollUntilAsync(() => Task.FromResult(received.Count), c => c >= 1, deadlineSeconds: 90);
            Assert.True(received.Count >= 1);
            Assert.Equal(Enumerable.Range(1, received.Count).Select(i => (long)i), received);
        }
        finally
        {
            await handle.DisposeAsync();
            await Registry.DeleteSourceAsync(source);
        }
    }

    // =================================================================================================
    // gRPC: StreamGrpcService.SubscribeTable / SubscribePipeline
    // =================================================================================================

    private static AccessGuard PermissiveGuard()
    {
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(PermissionResolverTests.Doc(version: 1)),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);
        // entitlementsEnabled: false — these facts are about replay/position wiring, not about who may
        // subscribe, same rationale as SourceReplayGrpcTests.PermissiveGuard.
        return new AccessGuard(resolver, entitlementsEnabled: false);
    }

    private StreamsForge.Host.Grpc.StreamGrpcService GrpcService() =>
        new(Registry, new OrleansEntityStreamFacade(_cluster.Client), PermissiveGuard());

    [Fact]
    public async Task Grpc_SubscribeTable_writes_position_and_flags_truncation_past_retention()
    {
        // Phase 1: an ordinary table, replaying the last 5 batches — Position 196..200, Seq 1..5.
        var (source, _) = await StartedFileSourceWithAsync("batch_replay_grpc_table", 200);
        var tableName = await StartRunningTableAsync($"SELECT id, value FROM {source} LATEST BY (id)");
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 200, deadlineSeconds: 90);
        await Task.Delay(2000);

        var service = GrpcService();
        var writer = new RecordingStreamWriter<V1.TableDeltaBatch>();
        using (var cts = new CancellationTokenSource())
        {
            var call = service.SubscribeTable(
                new V1.SubscribeTableRequest { Name = tableName, FromSeq = 196 }, writer, FakeContext(cts.Token));

            await PollUntilAsync(() => Task.FromResult(writer.Written.Count), c => c >= 5, deadlineSeconds: 90);
            cts.Cancel();
            await call;
        }

        Assert.Equal(5, writer.Written.Count);
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal((long)(196 + i), writer.Written[i].Position);
            Assert.Equal((long)(i + 1), writer.Written[i].Seq);
        }
        Assert.False(writer.Written[0].ReplayTruncated);

        await Registry.DeleteSourceAsync(source);

        // Phase 2: a 10 005-row table overflows the 10 000-capacity replay log, so from_seq: 1 is
        // truncated and the first replayed batch carries position 6 (positions 1..5 evicted) — mirrors
        // SourceReplayGrpcTests.Grpc_SubscribeSource_surfaces_replay_truncated_on_the_first_event_past_retention.
        // The table is started right after the source is registered but BEFORE its first poll fires (see
        // UpsertFileSourceAsync's own doc), so every one of the 10 005 rows reaches it LIVE — a late
        // attach here would backfill only what the SOURCE's own 10 000-entry log still retains, capping
        // the table's own log at 10 000 (no eviction, no truncation) rather than the 10 005 this fact
        // needs to overflow it.
        var (source2, _) = await UpsertFileSourceAsync("batch_replay_grpc_table_trunc", 10_005);
        var tableName2 = await StartRunningTableAsync($"SELECT id, value FROM {source2} LATEST BY (id)");
        var table2 = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName2);

        var connector2 = _cluster.GrainFactory.GetGrain<IConnectorGrain>(source2);
        await PollUntilAsync(() => connector2.GetStatusAsync(), s => s.EventsEmittedTotal >= 10_005, deadlineSeconds: 90);

        await PollUntilAsync(() => table2.GetRowCountAsync(), c => c >= 10_005, deadlineSeconds: 90);
        Assert.Equal(10_005, await table2.GetRowCountAsync());
        await Task.Delay(2000);

        var writer2 = new RecordingStreamWriter<V1.TableDeltaBatch>();
        using (var cts2 = new CancellationTokenSource())
        {
            var call2 = service.SubscribeTable(
                new V1.SubscribeTableRequest { Name = tableName2, FromSeq = 1 }, writer2, FakeContext(cts2.Token));

            await PollUntilAsync(() => Task.FromResult(writer2.Written.Count), c => c >= 1, deadlineSeconds: 90);
            cts2.Cancel();
            await call2;
        }

        Assert.True(writer2.Written[0].ReplayTruncated);
        Assert.Equal(6L, writer2.Written[0].Position);

        await Registry.DeleteSourceAsync(source2);
    }

    /// <summary>A windowless pass-through pipeline (<c>SELECT id, value FROM src</c>, no aggregation) emits
    /// exactly one row per input row and publishes exactly one batch per emitted row (see class doc). The
    /// pipeline is started right after the source is registered but BEFORE its first poll fires (see
    /// <see cref="UpsertFileSourceAsync"/>'s own doc for why: a passthrough pipeline has no idempotency
    /// guard, so a late-attach backfill racing its own live subscription's queue cache double-counts every
    /// backfilled row — found live in this wave, and a test-fixture concern rather than something wave 2
    /// owns fixing in <c>PipelineGrain.cs</c>), so every one of the 200 rows reaches it exactly once, live,
    /// publishing batches 1..200. Replaying from <c>FromSeq: 101</c> then yields exactly rows 101..200, one
    /// per envelope, sharing their batch's position with themselves (batch size 1 here) — proving every row
    /// of one batch carries that batch's position even in the batch-size-&gt;1 case
    /// <see cref="Table_delta_batches_replay_from_a_position_then_continue_live"/> does not exercise.</summary>
    [Fact]
    public async Task Grpc_SubscribePipeline_rows_of_one_batch_share_its_position()
    {
        var (source, _) = await UpsertFileSourceAsync("batch_replay_grpc_pipeline", 200);

        var pipelineId = Guid.NewGuid().ToString("n");
        var pipeline = _cluster.GrainFactory.GetGrain<IPipelineGrain>(pipelineId);
        await pipeline.StartAsync(new PipelineDefinition
        {
            Id = pipelineId,
            Name = "batch_replay_pipeline",
            Sql = $"SELECT id, value FROM {source}",
            Status = PipelineStatus.Running,
        });

        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(source);
        await PollUntilAsync(() => connector.GetStatusAsync(), s => s.EventsEmittedTotal >= 200, deadlineSeconds: 90);

        var lastSeq = await PollUntilAsync(async () =>
        {
            var snap = await pipeline.BeginAttachAsync(null);
            try { return snap.LastSeq; }
            finally { await pipeline.EndAttachAsync(); }
        }, seq => seq >= 200, deadlineSeconds: 90);
        Assert.Equal(200L, lastSeq);

        var service = GrpcService();
        var writer = new RecordingStreamWriter<V1.ResultEnvelope>();
        using var cts = new CancellationTokenSource();
        var call = service.SubscribePipeline(
            new V1.SubscribePipelineRequest { Id = pipelineId, FromSeq = 101 }, writer, FakeContext(cts.Token));

        await PollUntilAsync(() => Task.FromResult(writer.Written.Count), c => c >= 100, deadlineSeconds: 90);
        cts.Cancel();
        await call;

        Assert.Equal(100, writer.Written.Count);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal((long)(101 + i), writer.Written[i].Position);
            Assert.Equal((long)(i + 1), writer.Written[i].Seq);
        }
        // Non-decreasing, restated as its own assertion per the acceptance criterion's own wording.
        Assert.Equal(writer.Written.Select(w => w.Position).OrderBy(p => p), writer.Written.Select(w => w.Position));
        Assert.False(writer.Written[0].ReplayTruncated);

        await Registry.DeleteSourceAsync(source);
    }

    // =================================================================================================
    // Hub: StreamHub.SubscribeTableFrom
    // =================================================================================================

    [Fact]
    public async Task Hub_SubscribeTableFrom_replays_to_the_caller_then_joins_the_group()
    {
        var (source, _) = await StartedFileSourceWithAsync("batch_replay_hub_table", 50);
        var tableName = await StartRunningTableAsync($"SELECT id, value FROM {source} LATEST BY (id)");
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 50, deadlineSeconds: 90);
        await Task.Delay(2000);

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

        await hub.SubscribeTableFrom(tableName, fromSeq: 1, fromTimestampMs: null);

        Assert.Equal(50, clients.CallerSends.Count);
        for (var i = 0; i < 50; i++)
        {
            var (method, args) = clients.CallerSends[i];
            Assert.Equal("tableDelta", method);
            Assert.Equal(tableName, args[0]);
            Assert.Equal((long)(i + 1), args[2]); // this call's own per-batch seq counter
            Assert.Equal((long)(i + 1), args[3]); // producer position
        }

        Assert.Single(groups.Added);
        Assert.Equal($"table:{tableName}", groups.Added[0]);

        await Registry.DeleteSourceAsync(source);
    }

    // =================================================================================================
    // Fakes — copied from SourceReplayGrpcTests (private per class, per this codebase's own convention).
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

    /// <summary>Only <see cref="Caller"/> does anything real — mirrors
    /// <c>SourceReplayGrpcTests.RecordingClients</c> exactly.</summary>
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
