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

/// <summary>Silo config mirroring LifecycleSeedClusterTests/HistoryGrainClusterTests' configurator (memory
/// streams + memory grain storage) — duplicated here (not shared) since xunit test classes shouldn't share
/// cluster state.</summary>
internal sealed class PartitionedTableTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class PartitionedTableTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 003 M2 acceptance at the cluster level: a real Orleans TestingHost cluster runs a Parallelism=4
/// table's full coordinator-deployed grain graph (TableIngestGrain + TableStageGrain x stages x partitions +
/// TableOutputGrain, orchestrated by TableGrain's coordinator mode — see TableGrain's class doc) side by
/// side with the classic Parallelism=1 path over the SAME deterministic event set published on a real
/// source stream (generator stopped, same trick LifecycleSeedClusterTests uses), and asserts the two
/// converge to byte-identical rows — the M2 acceptance criterion ("seeded tables byte-equivalent outputs vs
/// main"). Also exercises: per-partition metrics detail, search, the table's own (TableDeltaNamespace,
/// name) delta stream (the exact stream identity StreamBridgeService relays to SignalR and StreamGrpcService
/// relays to gRPC — subscribing to it directly here proves that surface is unchanged under Parallelism>=2),
/// row history, and a StopAsync/StartAsync cycle (including the documented restart-resume tradeoff: output
/// snapshot resets and rebuilds from live traffic — same behavior as the classic path, not new to M2).
/// </summary>
public sealed class PartitionedTableClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<PartitionedTableTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<PartitionedTableTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private async Task<string> SeedSourceAsync(IRegistryGrain registry)
    {
        var sourceName = "trades_p_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "partitioned-table cluster test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false, // no live generator — the test publishes a deterministic event set itself
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });
        return sourceName;
    }

    private async Task PublishTradeAsync(string sourceName, string symbol, double price, long qty)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
            ["qty"] = qty,
        });
    }

    private static readonly (string Symbol, double Price, long Qty)[] DeterministicEvents =
    [
        ("AAPL", 100.0, 10), ("MSFT", 200.0, 5), ("AAPL", 101.0, 7), ("GOOG", 50.0, 20),
        ("MSFT", 199.5, 3), ("AAPL", 99.0, 4), ("GOOG", 51.0, 6), ("MSFT", 201.0, 9),
        ("AAPL", 102.0, 2), ("GOOG", 49.5, 11),
    ];

    [Fact]
    public async Task Parallelism4_table_converges_to_same_rows_as_classic_and_exposes_partition_metrics()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);
        var sql = $"SELECT symbol, SUM(qty) AS total FROM {sourceName} GROUP BY symbol";

        var classicName = "classic_" + Guid.NewGuid().ToString("n")[..8];
        var classic = await registry.CreateTableAsync(new TableDefinition
        {
            Name = classicName,
            Sql = sql,
            Parallelism = 1,
            SearchEnabled = true,
            SearchMode = TableSearchMode.Exact,
        });
        await registry.SetTableStatusAsync(classic.Id, PipelineStatus.Running);

        var partName = "part4_" + Guid.NewGuid().ToString("n")[..8];
        var partitioned = await registry.CreateTableAsync(new TableDefinition
        {
            Name = partName,
            Sql = sql,
            Parallelism = 4,
            SearchEnabled = true,
            SearchMode = TableSearchMode.Exact,
        });
        await registry.SetTableStatusAsync(partitioned.Id, PipelineStatus.Running);

        // Both tables subscribe to the same source stream — this deterministic event set (generator
        // stopped) is the only thing driving either table's state, so any divergence between them is a
        // genuine partitioned-execution bug, not a race with a live generator.
        foreach (var (symbol, price, qty) in DeterministicEvents)
        {
            await PublishTradeAsync(sourceName, symbol, price, qty);
        }

        var classicGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(classicName);
        var partGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(partName);

        var expectedTotals = DeterministicEvents
            .GroupBy(e => e.Symbol)
            .ToDictionary(g => g.Key, g => g.Sum(e => e.Qty));

        async Task<Dictionary<string, long>> ReadTotalsAsync(ITableGrain grain)
        {
            var rows = await grain.GetRowsAsync(1000, 0);
            return rows.ToDictionary(r => (string)r.Row["symbol"]!, r => Convert.ToInt64(r.Row["total"]));
        }

        bool Converged(Dictionary<string, long> t) =>
            t.Count == expectedTotals.Count && expectedTotals.All(kv => t.TryGetValue(kv.Key, out var v) && v == kv.Value);

        var classicTotals = await PollUntilAsync(() => ReadTotalsAsync(classicGrain), Converged, deadlineSeconds: 15);
        var partTotals = await PollUntilAsync(() => ReadTotalsAsync(partGrain), Converged, deadlineSeconds: 15);

        Assert.Equal(expectedTotals, classicTotals);
        Assert.Equal(expectedTotals, partTotals); // byte-equivalent to classic — the M2 acceptance criterion

        // Metrics: per-partition detail is additive — present (non-null) only on the Parallelism>=2 table.
        var classicMetrics = await classicGrain.GetMetricsAsync();
        Assert.Null(classicMetrics.Partitions);

        var partMetrics = await PollUntilAsync(
            () => partGrain.GetMetricsAsync(),
            m => m.Partitions is { Count: > 0 } && m.Partitions.All(p => p.FrontierEpoch >= 0),
            deadlineSeconds: 15);
        Assert.NotNull(partMetrics.Partitions);
        Assert.True(partMetrics.Partitions!.Sum(p => p.DeltasIn) > 0);
        Assert.False(partMetrics.Rebuilding);
        Assert.Equal(expectedTotals.Count, partMetrics.RowCount);

        // Search: both tables index the same field values under the same SearchMode — a query for a known
        // symbol should hit exactly one row on both, with matching content.
        var classicHits = await classicGrain.SearchAsync("AAPL", 10);
        var partHits = await partGrain.SearchAsync("AAPL", 10);
        Assert.Single(classicHits);
        Assert.Single(partHits);
        Assert.Equal(expectedTotals["AAPL"], Convert.ToInt64(partHits[0].Row["total"]));
    }

    [Fact]
    public async Task Parallelism4_table_survives_stop_start_and_delta_stream_plus_history_still_work()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);
        var sql = $"SELECT symbol, SUM(qty) AS total FROM {sourceName} GROUP BY symbol";

        var tableName = "part4b_" + Guid.NewGuid().ToString("n")[..8];
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = sql,
            Parallelism = 4,
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        // Subscribe directly to the table's own (TableDeltaNamespace, name) delta stream — the exact stream
        // identity StreamBridgeService relays to SignalR ("tableDelta") and StreamGrpcService relays to
        // gRPC clients — to prove the coordinator's TableOutputGrain-published stream carries deltas
        // exactly like a classic (Parallelism==1) table's would, with no surface changes.
        var received = new List<List<TableDeltaDto>>();
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var deltaStream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, tableName));
        var subHandle = await deltaStream.SubscribeAsync((batch, _) => { received.Add(batch); return Task.CompletedTask; });

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        await PollUntilAsync(() => Task.FromResult(received.Count), c => c > 0, deadlineSeconds: 15);
        Assert.NotEmpty(received);

        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);
        var history = await PollUntilAsync(() => historyGrain.GetHistoryAsync(key, 0), r => r.KeyFound, deadlineSeconds: 15);
        Assert.True(history.KeyFound);
        Assert.Equal(10L, Convert.ToInt64(history.Versions[0].Row["total"]));

        // Stop then restart the table — the coordinator must tear down and redeploy the partitioned graph
        // (ingest + stage + output grains) cleanly.
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        var stoppedMetrics = await tableGrain.GetMetricsAsync();
        Assert.Equal(PipelineStatus.Stopped, stoppedMetrics.Status);

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        // Documented restart-resume tradeoff (TableGrain's class doc — applies identically to the classic
        // path): operator internal state can't be rebuilt from the persisted output snapshot alone, so a
        // resume marks Rebuilding and resets to empty, rebuilding purely from live traffic going forward.
        var justRestartedMetrics = await tableGrain.GetMetricsAsync();
        Assert.True(justRestartedMetrics.Rebuilding);
        Assert.Equal(0, justRestartedMetrics.RowCount);

        await PublishTradeAsync(sourceName, "MSFT", 50, 3);
        await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);

        var rows = await tableGrain.GetRowsAsync(10, 0);
        Assert.Contains(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "MSFT");
        Assert.DoesNotContain(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "AAPL"); // dropped by the reset above

        var afterMetrics = await PollUntilAsync(() => tableGrain.GetMetricsAsync(), m => !m.Rebuilding, deadlineSeconds: 15);
        Assert.False(afterMetrics.Rebuilding);

        await subHandle.UnsubscribeAsync();
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
