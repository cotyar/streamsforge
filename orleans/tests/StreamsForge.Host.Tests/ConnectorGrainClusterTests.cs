using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring LifecycleSeedClusterTests'/StreamBridgeServiceStartupRaceTests'
/// configurator (memory streams + memory grain storage) — duplicated here (not shared) since xunit
/// test classes shouldn't share cluster state.</summary>
internal sealed class ConnectorTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class ConnectorTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Plan 006 W3A: cluster tests for the Orleans connector driver (IConnectorGrain/ConnectorGrain)
/// and RegistryGrain's Kind dispatch. Exercises the FILE kind end to end (deterministic — no real HTTP
/// or gRPC dependency needed to prove the driver wiring: schedule -> AppCore.Connectors.ConnectorPollCycle
/// -> stream emission -> persisted ConnectorRuntimeStatus) against a real TestingHost cluster, the same
/// way LifecycleSeedClusterTests exercises RegistryGrain's boot path for real.</summary>
public sealed class ConnectorGrainClusterTests : IAsyncLifetime
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

        _scratchDir = Directory.CreateTempSubdirectory("sf-connector-test-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try
        {
            Directory.Delete(_scratchDir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    private static SourceDefinition MakeFileSource(string name, string path, int intervalMs = 1000) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = true,
        Fields =
        [
            new FieldDef("id", FieldType.String),
            new FieldDef("price", FieldType.Double),
        ],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = intervalMs },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                DedupKeyField = "id",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                ],
            },
        },
    };

    [Fact]
    public async Task File_kind_connector_emits_rows_stamped_with_source_and_dedups_across_polls()
    {
        var name = "file_conn_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, "{\"id\":\"a1\",\"price\":100.5}\n{\"id\":\"a2\",\"price\":200.5}\n");

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

        var received = new List<EventRecord>();
        var subHandle = await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        try
        {
            var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
            await grain.StartAsync(MakeFileSource(name, filePath));

            await PollUntilAsync(
                () => Task.FromResult(Count(received)),
                count => count >= 2,
                deadlineSeconds: 15);

            List<EventRecord> firstBatch;
            lock (received) firstBatch = [.. received];

            Assert.True(firstBatch.Count >= 2, $"expected at least 2 rows, got {firstBatch.Count}");
            Assert.All(firstBatch, evt => Assert.Equal(name, evt.Source));
            Assert.Contains(firstBatch, evt => (string?)evt.GetValueOrDefault("id") == "a1" && Convert.ToDouble(evt["price"]) == 100.5);
            Assert.Contains(firstBatch, evt => (string?)evt.GetValueOrDefault("id") == "a2" && Convert.ToDouble(evt["price"]) == 200.5);

            // Append one duplicate id (a1) + one new id (a3). The whole file re-parses on the next poll
            // (mtime changed) but the dedup tracker (persisted across cycles) must suppress a1/a2 while
            // letting a3 through — exactly once.
            await File.AppendAllTextAsync(filePath, "{\"id\":\"a1\",\"price\":999}\n{\"id\":\"a3\",\"price\":300.5}\n");

            await PollUntilAsync(
                () => Task.FromResult(Count(received)),
                count => count >= 3,
                deadlineSeconds: 15);

            // Give any further (incorrect) re-emission a moment to show up before asserting the final count.
            await Task.Delay(1500);

            List<EventRecord> finalBatch;
            lock (received) finalBatch = [.. received];

            Assert.Equal(3, finalBatch.Count); // a1, a2 (first poll) + a3 (second poll) — a1's duplicate suppressed
            Assert.Contains(finalBatch, evt => (string?)evt.GetValueOrDefault("id") == "a3" && Convert.ToDouble(evt["price"]) == 300.5);
            Assert.Single(finalBatch, evt => (string?)evt.GetValueOrDefault("id") == "a1"); // never re-emitted
        }
        finally
        {
            await subHandle.UnsubscribeAsync();
            await _cluster.GrainFactory.GetGrain<IConnectorGrain>(name).StopAsync();
        }
    }

    [Fact]
    public async Task GetStatusAsync_reflects_a_successful_run()
    {
        var name = "file_conn_status_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, "{\"id\":\"x1\",\"price\":1.5}\n");

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(MakeFileSource(name, filePath));

            var status = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.LastStatus == "ok",
                deadlineSeconds: 15);

            Assert.Equal("ok", status.LastStatus);
            Assert.Null(status.LastError);
            Assert.Equal(0, status.ConsecutiveFailures);
            Assert.True(status.EventsEmittedTotal >= 1, $"expected >= 1 emitted events, got {status.EventsEmittedTotal}");
            Assert.True(status.LastBatchCount >= 1);
            Assert.NotNull(status.LastRunMs);
            Assert.NotNull(status.NextRunMs);
            Assert.True(status.NextRunMs > status.LastRunMs);
            Assert.Equal(name, status.SourceName);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Trait("Category", "Slow")]
    [Fact]
    public async Task Missing_file_reports_error_status_with_growing_backoff()
    {
        var name = "file_conn_missing_" + Guid.NewGuid().ToString("n")[..8];
        var missingPath = Path.Combine(_scratchDir, "does-not-exist-" + Guid.NewGuid().ToString("n") + ".ndjson");

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            // Short interval so the first failure (and backoff arithmetic on top of it) resolves quickly;
            // D-E's backoff formula (min(30s * 2^(k-1), 15min)) then dominates the schedule regardless.
            await grain.StartAsync(MakeFileSource(name, missingPath, intervalMs: 1000));

            var afterFirstFailure = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.ConsecutiveFailures >= 1,
                deadlineSeconds: 15);

            Assert.Equal("error", afterFirstFailure.LastStatus);
            Assert.NotNull(afterFirstFailure.LastError);
            Assert.Contains("file not found", afterFirstFailure.LastError, StringComparison.OrdinalIgnoreCase);
            Assert.True(afterFirstFailure.ConsecutiveFailures >= 1);
            Assert.NotNull(afterFirstFailure.LastRunMs);
            Assert.NotNull(afterFirstFailure.NextRunMs);
            // D-E: k=1 -> 30s backoff, dwarfing the 1s schedule interval — NextRun must reflect that,
            // not just "last run + 1s".
            var gapMs = afterFirstFailure.NextRunMs!.Value - afterFirstFailure.LastRunMs!.Value;
            Assert.True(gapMs >= 25_000, $"expected backoff-pushed NextRunMs (>= 25s gap), got {gapMs}ms");

            // Wait for a SECOND failure to confirm ConsecutiveFailures actually grows across cycles (not
            // just "became 1 once") — the ~30s backoff from the first failure dominates this wait.
            var afterSecondFailure = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.ConsecutiveFailures >= 2,
                deadlineSeconds: 40);

            Assert.True(afterSecondFailure.ConsecutiveFailures >= 2, $"expected ConsecutiveFailures to grow past 1, got {afterSecondFailure.ConsecutiveFailures}");
            Assert.Equal("error", afterSecondFailure.LastStatus);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    /// <summary>The failure streak belongs to the definition that produced it. Before this was cleared on
    /// (re)start, fixing a broken source's config left it waiting out the BROKEN config's backoff — 30s
    /// after one failure, 15 minutes after five — and a PUT, a config import and a disable/enable cycle
    /// all behaved the same way, so restarting the host was the only prompt lever. The 15s deadline below
    /// is the assertion: it is shorter than the >= 30s gap the preceding failure had already scheduled.</summary>
    [Trait("Category", "Slow")]
    [Fact]
    public async Task Restarting_with_a_fixed_config_clears_the_failure_backoff()
    {
        var name = "file_conn_refix_" + Guid.NewGuid().ToString("n")[..8];
        var missingPath = Path.Combine(_scratchDir, "does-not-exist-" + Guid.NewGuid().ToString("n") + ".ndjson");
        var goodPath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(goodPath, "{\"id\":\"x1\",\"price\":1.5}\n");

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(MakeFileSource(name, missingPath, intervalMs: 1000));

            var failed = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.ConsecutiveFailures >= 1,
                deadlineSeconds: 15);
            var backoffGapMs = failed.NextRunMs!.Value - failed.LastRunMs!.Value;
            Assert.True(backoffGapMs >= 25_000, $"expected a backoff to be in effect before the fix, got {backoffGapMs}ms");

            // The fix an operator would make: same source, corrected path.
            await grain.StartAsync(MakeFileSource(name, goodPath, intervalMs: 1000));

            var recovered = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.LastStatus == "ok",
                deadlineSeconds: 15);

            // Asserted before the streak so a timeout here reads as "never recovered" rather than as a
            // confusing count mismatch — PollUntilAsync returns the last snapshot on timeout, it doesn't throw.
            Assert.Equal("ok", recovered.LastStatus);
            Assert.Equal(0, recovered.ConsecutiveFailures);
            Assert.Null(recovered.LastError);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task Registry_dispatches_file_kind_to_connector_and_leaves_generator_kind_seeds_ticking()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.EnsureInitializedAsync();

        // --- kind=file: UpsertSourceAsync must start the CONNECTOR grain, not a generator. ---
        var name = "file_conn_registry_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, "{\"id\":\"r1\",\"price\":42}\n");

        await registry.UpsertSourceAsync(MakeFileSource(name, filePath));

        var connectorStatus = await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<IConnectorGrain>(name).GetStatusAsync(),
            s => s.LastStatus == "ok",
            deadlineSeconds: 15);
        Assert.Equal("ok", connectorStatus.LastStatus);
        Assert.True(connectorStatus.EventsEmittedTotal >= 1);

        try
        {
            // --- kind default ("generator", pre-006 behavior) must still start IGeneratorGrain: the
            // seeded "trades" source (Kind unset -> "generator") should still be producing events after
            // EnsureInitializedAsync — unaffected by IConnectorGrain existing at all. ---
            var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
            var tradesStream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, "trades"));

            var gotTrade = false;
            var subHandle = await tradesStream.SubscribeAsync((_, _) =>
            {
                gotTrade = true;
                return Task.CompletedTask;
            });
            try
            {
                await PollUntilAsync(() => Task.FromResult(gotTrade), got => got, deadlineSeconds: 15);
                Assert.True(gotTrade, "seeded generator-kind source 'trades' never produced an event after registry dispatch changes");
            }
            finally
            {
                await subHandle.UnsubscribeAsync();
            }

            var sources = await registry.GetSourcesAsync();
            var tradesDef = sources.Single(s => s.Name == "trades");
            Assert.True(string.IsNullOrEmpty(tradesDef.Kind) || tradesDef.Kind == SourceKinds.Generator);
        }
        finally
        {
            await registry.DeleteSourceAsync(name);
        }
    }

    private static int Count(List<EventRecord> list)
    {
        lock (list) return list.Count;
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }
}
