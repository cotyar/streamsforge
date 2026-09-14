using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;
using StreamsForge.Host.Storage;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Real <see cref="JsonFileGrainStorage"/> (not memory storage) plus a bound
/// <see cref="ReplayOptions"/> pointed at this test's own temp <c>DataDir</c> — the disk tier is the whole
/// subject here, so the silo has to be wired the way a real host is. Mirrors
/// <c>PersistenceModeTestSiloConfigurator</c>'s "pass DataDir through host configuration" shape.</summary>
internal sealed class ReplayPersistenceSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.ConfigureServices(services =>
        {
            services.AddGrainStorage(StreamConstants.StorageName, (sp, providerName) =>
            {
                var dataDir = sp.GetRequiredService<IConfiguration>()["DataDir"]
                    ?? throw new InvalidOperationException("DataDir not configured — see ReplayPersistenceSiloConfigurator.");
                return new JsonFileGrainStorage(providerName, dataDir);
            });
            services.AddOptions<ReplayOptions>().Configure<IConfiguration>((o, c) =>
                o.Directory = Path.Combine(
                    c["DataDir"] ?? throw new InvalidOperationException("DataDir not configured — see ReplayPersistenceSiloConfigurator."),
                    "replay"));
        });
    }
}

internal sealed class ReplayPersistenceClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Plan 026 wave 3 D6 — the disk tier as the five producer grains actually use it: a source that
/// opted in writes NDJSON segments under <c>DataDir/replay/&lt;ns&gt;/&lt;key&gt;/</c> on its write-behind
/// timer, a real deactivation loses the activation's ring but not the log, and the flag itself is what
/// switches the tier on and off (turning it off restarts positions at 1, which is the honest consequence
/// of "positions are durable only while the log is").</summary>
public sealed class ReplayPersistenceClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _dataDir = null!;

    public async Task InitializeAsync()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "sf-replay-persist-tests", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(_dataDir);

        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["DataDir"] = _dataDir,
        }));
        builder.AddSiloBuilderConfigurator<ReplayPersistenceSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ReplayPersistenceClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // The grain-storage factory above is lazy — force it (and the registry) to exist before any test runs.
        await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetTablesAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> read, Func<T, bool> done, int deadlineSeconds = 30)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await read();
        while (!done(last) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
            last = await read();
        }
        return last;
    }

    private string SegmentDirFor(string ns, string key) => Path.Combine(_dataDir, "replay", ns, key);

    private SourceDefinition MakeFileSource(string name, string path, ReplayConfig? replay) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = true,
        Replay = replay,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("n", FieldType.Long)],
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
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("n", FieldType.Long) },
                ],
            },
        },
    };

    private async Task<string> Write300RowsAsync(string name)
    {
        var path = Path.Combine(_dataDir, name + ".ndjson");
        await File.WriteAllLinesAsync(path, Enumerable.Range(1, 300).Select(i => $"{{\"id\":\"r{i}\",\"n\":{i}}}"));
        return path;
    }

    [Fact]
    public async Task A_persisted_source_writes_segments_and_replays_them_from_disk_after_a_deactivation()
    {
        var name = "replay_persist_" + Guid.NewGuid().ToString("n")[..8];
        var path = await Write300RowsAsync(name);
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        await grain.StartAsync(MakeFileSource(name, path, new ReplayConfig { Persist = true }));

        var status = await PollUntilAsync(() => grain.GetReplayStatusAsync(), s => s.LastSeq >= 300);
        Assert.True(status.Persisted, "the disk tier should be on for a source with replay.persist");
        Assert.Equal(300, status.LastSeq);

        // The write-behind timer runs every 2s; segments and RetainedBytes only exist once it has fired.
        var flushed = await PollUntilAsync(() => grain.GetReplayStatusAsync(), s => s.Segments >= 1, deadlineSeconds: 20);
        Assert.True(flushed.Segments >= 1, $"expected at least one segment after a flush tick, got {flushed.Segments}");
        Assert.True(flushed.RetainedBytes > 0, "a flushed segment is not empty");

        var segmentDir = SegmentDirFor(StreamConstants.SourcesNamespace, name);
        var files = Directory.GetFiles(segmentDir, "*.ndjson");
        Assert.NotEmpty(files);
        Assert.All(files, f => Assert.True(new FileInfo(f).Length > 0, $"segment {f} is empty"));

        // Stop (which flushes the tail), then deactivate for real: the activation's in-memory ring goes with
        // it, so anything the replay below returns came off disk.
        await grain.StopAsync();
        await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

        var replayed = await grain.BeginAttachAsync(new ReplayFrom { Seq = 1 });
        try
        {
            Assert.Equal(300, replayed.Rows.Count);
            Assert.Equal(Enumerable.Range(1, 300).Select(i => (long)i), replayed.Positions);
            Assert.Equal(1, replayed.FirstSeq);
            Assert.Equal(300, replayed.LastSeq);
            Assert.False(replayed.Truncated);
            Assert.Equal("r1", replayed.Rows[0]["id"]);
            Assert.Equal("r300", replayed.Rows[299]["id"]);
        }
        finally
        {
            await grain.EndAttachAsync();
        }
    }

    [Fact]
    public async Task Turning_replay_persist_off_drops_the_disk_tier_and_restarts_positions_at_one()
    {
        var name = "replay_toggle_" + Guid.NewGuid().ToString("n")[..8];
        var path = await Write300RowsAsync(name);
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        await grain.StartAsync(MakeFileSource(name, path, new ReplayConfig { Persist = true }));
        var on = await PollUntilAsync(() => grain.GetReplayStatusAsync(), s => s.LastSeq >= 300);
        Assert.True(on.Persisted);

        // A source restarts on every upsert, and this is the definition the restart hands the grain. The
        // dedup tracker is persisted, so the next poll re-parses the same file and emits nothing — which is
        // what makes "positions restarted at 1" observable as LastSeq == 0 rather than as a race.
        await grain.StartAsync(MakeFileSource(name, path, null));
        var off = await grain.GetReplayStatusAsync();
        Assert.False(off.Persisted, "clearing the replay group must drop the disk tier");
        Assert.Equal(0, off.LastSeq);

        await grain.StopAsync();
    }

    [Fact]
    public async Task A_memory_only_table_with_replay_persist_keeps_its_delta_log_on_disk_and_follows_the_flag()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "replay_tbl_src_" + Guid.NewGuid().ToString("n")[..8];
        var path = await Write300RowsAsync(sourceName);
        await registry.UpsertSourceAsync(MakeFileSource(sourceName, path, null));

        var tableName = "replay_tbl_" + Guid.NewGuid().ToString("n")[..8];
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = $"SELECT id, n FROM {sourceName} LATEST BY (id)",
            Persistence = TablePersistenceMode.MemoryOnly,
            Replay = new ReplayConfig { Persist = true },
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var status = await PollUntilAsync(() => table.GetReplayStatusAsync(), s => s.LastSeq > 0);
        Assert.True(status.Persisted, "a MemoryOnly table with replay.persist still keeps its delta log on disk");

        // Its own flush timer is separate from the persistence-mode one a MemoryOnly table never registers.
        var flushed = await PollUntilAsync(() => table.GetReplayStatusAsync(), s => s.Segments >= 1, deadlineSeconds: 20);
        Assert.True(flushed.Segments >= 1, $"expected at least one delta segment after a flush tick, got {flushed.Segments}");
        Assert.NotEmpty(Directory.GetFiles(SegmentDirFor(StreamConstants.TableDeltaNamespace, tableName), "*.ndjson"));

        // Plan 026 D6 — the registry restarts a Running table when its replay group changes, so clearing it
        // takes effect now rather than at the next manual stop/start.
        created.Replay = null;
        var updated = await registry.UpdateTableAsync(created);
        Assert.NotNull(updated);
        Assert.Equal(PipelineStatus.Running, updated!.Status);

        var after = await table.GetReplayStatusAsync();
        Assert.False(after.Persisted, "clearing the replay group must drop the disk tier");
    }
}
