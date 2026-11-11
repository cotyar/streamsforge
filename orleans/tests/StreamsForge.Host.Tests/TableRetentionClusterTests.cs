using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

internal sealed class RetentionTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class RetentionTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 011 wave C2 — the row retention policy end to end on a real cluster: RegistryGrain's guards, the
/// seeded <c>order_states</c> bound, and (the part that matters) that an evicted row is gone from every
/// derived surface at once — the consolidated rows, the reverse search index, the per-row history, and a
/// DOWNSTREAM table reading this one. Eviction emits an ordinary retraction precisely so all four follow
/// along without any of them knowing retention exists; these tests are what makes that claim checkable
/// rather than asserted.
///
/// The Engine-level proof that the OPERATOR's per-key state (and not just the output copy) is reclaimed
/// lives next door in StreamsForge.Engine.Tests/TableRetentionTests.cs.
/// </summary>
public sealed class TableRetentionClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<RetentionTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<RetentionTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private async Task<string> SeedOrderSourceAsync(IRegistryGrain registry)
    {
        var sourceName = "orderev_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "retention test source",
            GeneratorProfile = "orders",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("order_id", FieldType.String),
                new FieldDef("stage", FieldType.String),
            ],
        });
        return sourceName;
    }

    private async Task PublishAsync(string sourceName, long ts, string orderId, string stage)
    {
        var stream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = ts,
            [EventRecord.SourceField] = sourceName,
            ["order_id"] = orderId,
            ["stage"] = stage,
        });
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> read, Func<T, bool> done, int deadlineSeconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await read();
        while (!done(last) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
            last = await read();
        }
        return last;
    }

    // ------------------------------------------------------------------
    // Guards (409-style) — refusing beats accepting a bound that could not be honored.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RetentionOnAnAggregateTableIsRejected()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "agg_" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT order_id, COUNT(*) AS n FROM {sourceName} GROUP BY order_id",
            RetentionMaxRows = 10,
        }));
        Assert.Contains("not supported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RetentionWithParallelismAboveOneIsRejected()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "par_" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT order_id, stage FROM {sourceName} LATEST BY (order_id)",
            Parallelism = 4,
            RetentionMaxRows = 10,
        }));
        Assert.Contains("Parallelism = 1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NegativeRetentionBoundsAreRejected()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "neg_" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT order_id, stage FROM {sourceName} LATEST BY (order_id)",
            RetentionTtlMs = -1,
        }));
    }

    [Fact]
    public async Task RetentionIsAcceptedOnALatestByTableAndSurvivesTheRoundTrip()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "ok_" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT order_id, stage FROM {sourceName} LATEST BY (order_id)",
            RetentionMaxRows = 25,
            RetentionTtlMs = 60_000,
        });

        var reloaded = await registry.GetTableAsync(created.Id);
        Assert.Equal(25, reloaded!.RetentionMaxRows);
        Assert.Equal(60_000, reloaded.RetentionTtlMs);
    }

    // ------------------------------------------------------------------
    // The seed — the reason wave C2 exists at all.
    // ------------------------------------------------------------------

    [Fact]
    public void SeededOrderStatesIsBounded()
    {
        var orderStates = SeedCatalog.Tables().Single(t => t.Name == "order_states");

        Assert.Equal(PipelineStatus.Running, orderStates.Status); // still seeded Running, as its own tests assert
        Assert.True(orderStates.RetentionMaxRows > 0, "the one seeded table with an unbounded key space must carry a bound");
        Assert.Equal(2000, orderStates.RetentionMaxRows);

        // And the bound is one the runtime can actually honor for this SQL, not a number in a field.
        var streams = SeedCatalog.Sources().ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => f.Type switch
            {
                FieldType.String => FieldKind.String,
                FieldType.Double => FieldKind.Double,
                FieldType.Long => FieldKind.Long,
                FieldType.Bool => FieldKind.Bool,
                FieldType.Timestamp => FieldKind.Timestamp,
                _ => FieldKind.Json,
            })));
        var compiled = SqlCompiler.CompileTable(orderStates.Sql, streams, new Dictionary<string, SourceSchema>());
        Assert.True(compiled.Ok, string.Join("; ", compiled.Diagnostics.Select(d => d.Message)));
        Assert.True(compiled.Plan!.SupportsRetention);
    }

    // ------------------------------------------------------------------
    // End to end: an evicted row leaves EVERY derived surface at once.
    // ------------------------------------------------------------------

    [Fact]
    public async Task EvictedRowsLeaveRowsSearchHistoryAndADownstreamTableTogether()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        var upstreamName = "ret_up_" + Guid.NewGuid().ToString("n")[..8];
        var upstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT order_id, stage FROM {sourceName} LATEST BY (order_id)",
            SearchEnabled = true,
            SearchMode = TableSearchMode.Exact,
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
            RetentionMaxRows = 3,
        });
        await registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);

        var downstreamName = "ret_down_" + Guid.NewGuid().ToString("n")[..8];
        var downstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = downstreamName,
            Sql = $"SELECT order_id, stage FROM {upstreamName}",
        });
        await registry.SetTableStatusAsync(downstream.Id, PipelineStatus.Running);

        // Six distinct keys, strictly increasing event time: keys k0..k2 must be evicted, k3..k5 retained.
        for (int i = 0; i < 6; i++)
        {
            await PublishAsync(sourceName, 1_700_000_000_000 + i, $"k{i}", "NEW");
        }

        var upstreamGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(upstreamName);
        var metrics = await PollUntilAsync(() => upstreamGrain.GetMetricsAsync(), m => m.DeltasIn >= 6 && m.RowCount == 3);

        // 1) The consolidated rows plateau at the bound, and it is the OLDEST keys that went.
        Assert.Equal(3, metrics.RowCount);
        var rows = await upstreamGrain.GetRowsAsync(100, 0);
        var retained = rows.Select(r => (string)r.Row["order_id"]!).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "k3", "k4", "k5" }, retained);

        // 2) The reverse search index dropped them too — an evicted row that stayed searchable would be a
        //    row the table says it does not have.
        var evictedHits = await upstreamGrain.SearchAsync("k0", 10);
        Assert.Empty(evictedHits);
        var survivingHits = await upstreamGrain.SearchAsync("k5", 10);
        Assert.Single(survivingHits);

        // 3) Per-row history reclaimed the evicted keys and kept the surviving ones. A LATEST BY table's
        //    row identity IS its LATEST BY key (TableGroupKeyExtractor handles the no-GROUP-BY case), so
        //    every version of one order shares one entry — and an eviction takes that whole entry with it.
        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(upstreamName);
        var stats = await PollUntilAsync(() => historyGrain.GetStatsAsync(), s => s.KeyCount == 3);
        Assert.Equal(3, stats.KeyCount);

        List<string> identity = ["order_id"];
        var evictedHistory = await historyGrain.GetHistoryAsync(
            RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["order_id"] = "k0" }, identity), 0);
        Assert.False(evictedHistory.KeyFound);
        // ...and the surviving keys really are found under the SAME derivation, so the assertion above is
        // "the entry is gone", not "the key was never right".
        var survivingHistory = await historyGrain.GetHistoryAsync(
            RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["order_id"] = "k5" }, identity), 0);
        Assert.True(survivingHistory.KeyFound);

        // 4) A downstream table reading this one saw the retractions and agrees on the row set — the
        //    property that makes eviction safe at all.
        var downstreamGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(downstreamName);
        var downstreamMetrics = await PollUntilAsync(() => downstreamGrain.GetMetricsAsync(), m => m.RowCount == 3);
        Assert.Equal(3, downstreamMetrics.RowCount);
        var downstreamRows = await downstreamGrain.GetRowsAsync(100, 0);
        Assert.Equal(retained, downstreamRows.Select(r => (string)r.Row["order_id"]!).OrderBy(x => x, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task TighteningTheBoundOnARunningTableTakesEffectImmediately()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedOrderSourceAsync(registry);

        var name = "ret_upd_" + Guid.NewGuid().ToString("n")[..8];
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = name,
            Sql = $"SELECT order_id, stage FROM {sourceName} LATEST BY (order_id)",
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        for (int i = 0; i < 5; i++) await PublishAsync(sourceName, 1_700_000_100_000 + i, $"u{i}", "NEW");

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(name);
        await PollUntilAsync(() => grain.GetMetricsAsync(), m => m.RowCount == 5);

        // Turn a bound on. RegistryGrain restarts the table (the policy is installed at StartAsync), so the
        // executor starts empty and refills from live traffic — the same rebuild any SQL/persistence change
        // already causes. What must hold afterwards is that the bound is now enforced.
        var current = await registry.GetTableAsync(created.Id);
        current!.RetentionMaxRows = 2;
        var updated = await registry.UpdateTableAsync(current);
        Assert.Equal(PipelineStatus.Running, updated!.Status);

        for (int i = 0; i < 5; i++) await PublishAsync(sourceName, 1_700_000_200_000 + i, $"v{i}", "NEW");

        var after = await PollUntilAsync(() => grain.GetMetricsAsync(), m => m.RowCount == 2);
        Assert.Equal(2, after.RowCount);
    }
}
