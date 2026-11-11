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

/// <summary>Silo config mirroring HistoryGrainClusterTests' configurator (memory streams + memory grain
/// storage) — duplicated here (not shared) since xunit test classes shouldn't share cluster state.</summary>
internal sealed class LifecycleTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class LifecycleTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>End-to-end smoke tests for Phase L3's seeded catalog entries ("order_states" table,
/// "fill-rate-5s" pipeline): a real Orleans TestingHost cluster runs the exact same
/// RegistryGrain.EnsureInitializedAsync boot path Program.cs runs for real, so these tests exercise the
/// actual seeded SQL strings (no duplicated copy in the test) end to end — compile, start, and (for
/// order_states) converge to one row per order with a working row-history stage trail.</summary>
public sealed class LifecycleSeedClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<LifecycleTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<LifecycleTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Seeded_order_states_table_and_fill_rate_pipeline_compile_and_start_running()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.EnsureInitializedAsync();

        var tables = await registry.GetTablesAsync();
        var orderStates = tables.Single(t => t.Name == "order_states");
        Assert.Equal(PipelineStatus.Running, orderStates.Status);
        Assert.Null(orderStates.Error);
        Assert.NotEmpty(orderStates.OutputFields);
        Assert.Contains(orderStates.OutputFields, f => f.Name == "order_id");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "stage_rank");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "stage");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "stage_ts");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "filled_qty");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "qty");
        Assert.Contains(orderStates.OutputFields, f => f.Name == "px");
        Assert.True(orderStates.HistoryEnabled);
        Assert.Equal(TableHistoryMode.LastN, orderStates.HistoryMode);
        Assert.Equal(8, orderStates.HistoryLimit);

        // The other new-grammar seeds must also have compiled and started clean.
        var legExposure = tables.Single(t => t.Name == "leg_exposure");
        Assert.Equal(PipelineStatus.Running, legExposure.Status);
        Assert.Null(legExposure.Error);
        Assert.Contains(legExposure.OutputFields, f => f.Name == "ccy");
        Assert.Contains(legExposure.OutputFields, f => f.Name == "notional");

        var pipelines = await registry.GetPipelinesAsync();
        var fillRate = pipelines.Single(p => p.Name == "fill-rate-5s");
        Assert.Equal(PipelineStatus.Running, fillRate.Status);
        Assert.Null(fillRate.Error);

        var nestedVwap = pipelines.Single(p => p.Name == "Hot symbol VWAP (nested)");
        Assert.Equal(PipelineStatus.Running, nestedVwap.Status);
        Assert.Null(nestedVwap.Error);
    }

    [Fact]
    public async Task Order_states_converges_to_one_row_per_order_and_history_shows_the_stage_trail()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.EnsureInitializedAsync();

        // Stop the real "order_events" generator so only the deterministic events published below drive
        // order_states/history for this test — avoids the assertions racing the live random generator.
        await _cluster.GrainFactory.GetGrain<IGeneratorGrain>("order_events").StopAsync();

        var orderId = "ORD-TEST" + Guid.NewGuid().ToString("n")[..4].ToUpperInvariant();
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, "order_events"));

        async Task PublishAsync(string stage, long stageRank, long filledQty, long qty, double px)
        {
            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await stream.OnNextAsync(new EventRecord
            {
                [EventRecord.TimestampField] = ts,
                [EventRecord.SourceField] = "order_events",
                ["order_id"] = orderId,
                ["symbol"] = "AAPL",
                ["side"] = "BUY",
                ["stage"] = stage,
                ["stage_rank"] = stageRank,
                ["stage_ts"] = ts,
                ["qty"] = qty,
                ["filled_qty"] = filledQty,
                ["px"] = px,
            });
            await Task.Delay(5); // keep stage_ts strictly increasing across the published sequence
        }

        // The documented lifecycle: NEW -> ACK -> PART_FILL -> PART_FILL -> FILLED.
        await PublishAsync("NEW", 1, 0, 1000, 0);
        await PublishAsync("ACK", 2, 0, 1000, 0);
        await PublishAsync("PART_FILL", 3, 400, 1000, 101.5);
        await PublishAsync("PART_FILL", 3, 750, 1000, 101.7);
        await PublishAsync("FILLED", 4, 1000, 1000, 101.9);

        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>("order_states");
        var row = await PollUntilAsync(
            async () => (await tableGrain.GetRowsAsync(500, 0))
                .FirstOrDefault(r => (string?)r.Row.GetValueOrDefault("order_id") == orderId),
            r => r is not null && Convert.ToInt64(r.Row["stage_rank"]) == 4,
            deadlineSeconds: 15);

        Assert.NotNull(row);
        Assert.Equal(4L, Convert.ToInt64(row!.Row["stage_rank"])); // converged to the latest (FILLED) stage
        Assert.Equal("FILLED", row.Row["stage"]); // LATEST BY carries the real stage string
        Assert.Equal(1000L, Convert.ToInt64(row.Row["filled_qty"]));
        Assert.Equal(1000L, Convert.ToInt64(row.Row["qty"]));
        Assert.Equal(101.9, Convert.ToDouble(row.Row["px"]), 3);
        Assert.Equal("AAPL", row.Row["symbol"]);

        // Derive the history-lookup key exactly the way TablesEndpoints.MapPost("/{id}/history/lookup")
        // does at runtime — from the table's *actual* seeded SQL, not a copy pasted into the test.
        var def = (await registry.GetTablesAsync()).Single(t => t.Name == "order_states");
        var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
        Assert.NotNull(identityColumns);
        Assert.Equal(["order_id"], identityColumns);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["order_id"] = orderId }, identityColumns);

        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>("order_states");
        var history = await PollUntilAsync(
            () => historyGrain.GetHistoryAsync(key, 0),
            r => r.KeyFound && r.TotalVersions >= 5,
            deadlineSeconds: 15);

        Assert.True(history.KeyFound);
        Assert.Equal(TableHistoryMode.LastN, history.Mode);
        Assert.Equal(5, history.TotalVersions); // 5 published events, under LastN(8)'s cap -> all retained
        // Newest-first ordering: the full stage trail for this order, most recent version first.
        var stageRanksNewestFirst = history.Versions.Select(v => Convert.ToInt64(v.Row["stage_rank"])).ToList();
        Assert.Equal([4L, 3L, 3L, 2L, 1L], stageRanksNewestFirst);
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
