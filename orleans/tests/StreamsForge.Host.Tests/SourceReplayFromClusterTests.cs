using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Facades;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring ConnectorTestSiloConfigurator/IngestTestSiloConfigurator — duplicated
/// per those files' own "xunit test classes shouldn't share cluster state" rationale.</summary>
internal sealed class ReplayFromTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class ReplayFromTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Plan 026 wave 1 acceptance: <see cref="ReplayFrom"/> on the attach gate
/// (<see cref="IReplayableSourceGrain.BeginAttachAsync(ReplayFrom?)"/>), proven at the grain level for
/// all three producer kinds that carry the gate today — connector (file), generator, and the new
/// <see cref="IngestSourceGrain"/> (D3). This is grain-level coverage, deliberately below the
/// table/pipeline attach protocol (that opt-in — <c>replayFrom</c> on a definition — is wave 2), so
/// each test drives <c>BeginAttachAsync</c>/<c>EndAttachAsync</c> directly, exactly like
/// <see cref="SourceLateConsumerClusterTests"/>'s own gate-contract fact does.</summary>
public sealed class SourceReplayFromClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;
    private IIngressFacade _ingress = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ReplayFromTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ReplayFromTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-replay-from-").FullName;

        // Real production wiring (OrleansFacadesExtensions.AddOrleansFacades) against this TestCluster's
        // IClusterClient — same shape as IngestFacadeClusterTests, so IngestSourceGrain is exercised
        // through the real OrleansIngressFacade.DrainAsync path, not a hand-built shortcut.
        var services = new ServiceCollection();
        services.AddSingleton<IClusterClient>(_cluster.Client);
        services.AddOrleansFacades();
        _ingress = services.BuildServiceProvider().GetRequiredService<IIngressFacade>();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_scratchDir, recursive: true); } catch { /* best-effort */ }
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

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
            Schedule = new ScheduleSpec { IntervalMs = 1000 }, // the floor — ScheduleCalc.MinIntervalMs
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

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        if (until(last)) return last;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }

    [Fact]
    public async Task File_source_replays_exactly_from_a_position_and_from_a_timestamp()
    {
        var name = "replay_file_" + Guid.NewGuid().ToString("n")[..8];
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(0, 500));

        await Registry.UpsertSourceAsync(FileSource(name, path));
        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        try
        {
            var status = await PollUntilAsync(() => connector.GetStatusAsync(), s => s.EventsEmittedTotal >= 500, deadlineSeconds: 90);
            Assert.Equal(500, status.EventsEmittedTotal);

            // Plan 023's quiescence window (see IConnectorGrain.BeginAttachAsync's "THE ONE GAP,
            // MEASURED" paragraph): the attach hold stops PUBLISHING, not the stream provider's own
            // pull-into-cache pipeline, so subscribing/snapshotting inside ~one pull period of the last
            // publish can see duplicates. Waiting here is what makes "exactly N" a legitimate assertion.
            await Task.Delay(2000);

            // A full snapshot (from = null) both proves plan 023's parameterless-equivalent behaviour is
            // unchanged and gives us the 300th row's own _ts to filter on below.
            SourceReplaySnapshot full;
            try
            {
                full = await connector.BeginAttachAsync(null);
            }
            finally
            {
                await connector.EndAttachAsync();
            }
            Assert.Equal(500, full.Rows.Count);
            Assert.Equal(1, full.FirstSeq);
            Assert.Equal(500, full.LastSeq);
            Assert.Equal(500, full.TotalSeen);
            Assert.False(full.Truncated);
            var row300Ts = Convert.ToInt64(full.Rows[299][EventRecord.TimestampField]);

            SourceReplaySnapshot byPosition;
            try
            {
                byPosition = await connector.BeginAttachAsync(new ReplayFrom { Seq = 401 });
            }
            finally
            {
                await connector.EndAttachAsync();
            }
            Assert.Equal(100, byPosition.Rows.Count);
            Assert.Equal(Enumerable.Range(401, 100).Select(i => (long)i).ToList(), byPosition.Positions);
            Assert.Equal(1, byPosition.FirstSeq);
            Assert.Equal(500, byPosition.LastSeq);
            Assert.Equal(500, byPosition.TotalSeen);
            Assert.False(byPosition.Truncated);
            // ids are 0-indexed (Ndjson(0, 500)) while positions are 1-based, so position 401 is id 400.
            Assert.Equal(400L, Convert.ToInt64(byPosition.Rows[0]["id"]));
            Assert.Equal(499L, Convert.ToInt64(byPosition.Rows[^1]["id"]));

            SourceReplaySnapshot byTimestamp;
            try
            {
                byTimestamp = await connector.BeginAttachAsync(new ReplayFrom { TimestampMs = row300Ts });
            }
            finally
            {
                await connector.EndAttachAsync();
            }
            Assert.True(byTimestamp.Rows.Count >= 200, $"expected >= 200 rows from row 300's timestamp, got {byTimestamp.Rows.Count}");
            Assert.All(byTimestamp.Rows, r => Assert.True(Convert.ToInt64(r[EventRecord.TimestampField]) >= row300Ts));

            SourceReplaySnapshot afterNull;
            try
            {
                afterNull = await connector.BeginAttachAsync(null);
            }
            finally
            {
                await connector.EndAttachAsync();
            }
            Assert.Equal(500, afterNull.Rows.Count);
        }
        finally
        {
            await Registry.DeleteSourceAsync(name);
        }
    }

    [Fact]
    public async Task Generator_source_replays_from_a_position()
    {
        var name = "replay_gen_" + Guid.NewGuid().ToString("n")[..8];
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = SourceKinds.Generator,
            GeneratorProfile = "trades",
            Enabled = true,
            EventsPerSecond = 100,
        });
        var generator = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);

        try
        {
            // 300 events at EventsPerSecond=100 is ~3s of ticks on an idle machine; widened to 90s
            // (matching this suite's other CPU-contention deadlines, e.g. SourceLateConsumerClusterTests)
            // because a whole-solution parallel run competes for the same CPU the grain timer ticks on —
            // observed reaching only seq 238/300 in 30s under load, passing in ~3s under --filter alone.
            long lastSeq = 0;
            var deadline = DateTime.UtcNow.AddSeconds(90);
            while (DateTime.UtcNow < deadline && lastSeq < 300)
            {
                SourceReplaySnapshot snap;
                try
                {
                    snap = await generator.BeginAttachAsync(null);
                }
                finally
                {
                    await generator.EndAttachAsync();
                }
                lastSeq = snap.LastSeq;
                if (lastSeq < 300)
                {
                    await Task.Delay(100);
                }
            }
            Assert.True(lastSeq >= 300, $"generator only reached seq {lastSeq} within the deadline");

            // Stop the timer before pinning the boundary — otherwise a tick landing between reading
            // LastSeq and issuing the position-based attach would make "exactly 50" a race rather than a
            // fact. Gate.ForceReleaseAsync (called by StopAsync) is a no-op here since no hold is
            // outstanding at this point.
            await generator.StopAsync();

            SourceReplaySnapshot finalSnap;
            try
            {
                finalSnap = await generator.BeginAttachAsync(null);
            }
            finally
            {
                await generator.EndAttachAsync();
            }
            lastSeq = finalSnap.LastSeq;
            Assert.True(lastSeq >= 300);

            SourceReplaySnapshot byPosition;
            try
            {
                byPosition = await generator.BeginAttachAsync(new ReplayFrom { Seq = lastSeq - 49 });
            }
            finally
            {
                await generator.EndAttachAsync();
            }
            Assert.Equal(50, byPosition.Rows.Count);
            Assert.Equal(
                Enumerable.Range(0, 50).Select(i => lastSeq - 49 + i).ToList(),
                byPosition.Positions);
        }
        finally
        {
            await Registry.DeleteSourceAsync(name);
        }
    }

    [Fact]
    public async Task Ingest_source_publishes_through_the_grain_and_loses_nothing()
    {
        var name = "replay_ing_" + Guid.NewGuid().ToString("n")[..8];
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = SourceKinds.Ingest,
            Enabled = true,
            Fields =
            [
                new FieldDef("id", FieldType.Long),
                new FieldDef("value", FieldType.String),
            ],
            // Inline: PushAsync publishes synchronously inside the call (SourceIngressBuffer.PushAsync),
            // through the real drain pump — OrleansIngressFacade.DrainAsync now a grain hop
            // (IIngestSourceGrain.PublishAsync, plan 026 D3) — so the table below fills deterministically
            // rather than depending on a background sweep this bare-DI test never starts.
            Ingest = new IngestConfig { Policy = IngressOverflowPolicy.Inline, CapacityRows = 1000, MaxBatchRows = 100 },
        });

        // Table FIRST — plan 023's rule still holds for ingest sources at wave 1 (no gate on the
        // consumer side yet, wave 2's job): a subscriber must exist before rows are published to see
        // them at all, memory streams having no replay of their own.
        var tableName = "replay_ing_tbl_" + Guid.NewGuid().ToString("n")[..8];
        var created = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = $"SELECT id, value FROM {name} LATEST BY (id)",
        });
        await Registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        try
        {
            const int total = 1000;
            const int batchSize = 100;
            for (var b = 0; b < total / batchSize; b++)
            {
                var rows = Enumerable.Range(b * batchSize, batchSize)
                    .Select(i => new Dictionary<string, object?> { ["id"] = (long)i, ["value"] = $"v{i}" })
                    .ToList();
                var result = await _ingress.PushAsync(name, rows, partial: false);
                Assert.Equal(IngestOutcome.Accepted, result.Outcome);
            }

            var count = await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= total, deadlineSeconds: 30);
            Assert.Equal(total, count);

            var status = await _ingress.GetStatusAsync(name);
            Assert.NotNull(status);
            Assert.Equal(0, status!.DownstreamDropped);

            var ingestGrain = _cluster.GrainFactory.GetGrain<IIngestSourceGrain>(name);
            SourceReplaySnapshot byPosition;
            try
            {
                byPosition = await ingestGrain.BeginAttachAsync(new ReplayFrom { Seq = 901 });
            }
            finally
            {
                await ingestGrain.EndAttachAsync();
            }
            Assert.Equal(100, byPosition.Rows.Count);
            Assert.Equal(Enumerable.Range(901, 100).Select(i => (long)i).ToList(), byPosition.Positions);
            Assert.Equal(1, byPosition.FirstSeq);
            Assert.Equal(1000, byPosition.LastSeq);
            Assert.Equal(1000, byPosition.TotalSeen);
        }
        finally
        {
            await Registry.DeleteTableAsync(created.Id);
            await Registry.DeleteSourceAsync(name);
        }
    }
}
