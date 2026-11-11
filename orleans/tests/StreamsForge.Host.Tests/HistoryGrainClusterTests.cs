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

/// <summary>Silo config mirroring StreamsForge.Engine.Tests' TestSiloConfigurator (memory streams + memory
/// grain storage) — duplicated here (not shared) since it's a different test assembly.</summary>
internal sealed class HistoryTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class HistoryTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>End-to-end smoke tests for Feature B (ROW HISTORY): a real Orleans TestingHost cluster, a real
/// RegistryGrain → TableGrain → TableHistoryGrain wired together exactly as Program.cs wires them in the
/// real Host (grain discovery, no explicit registration needed), fed real trade events through the real
/// "table-delta" stream. Mirrors StreamsForge.Engine.Tests/TableGrainClusterSmokeTest.cs's pattern.</summary>
public sealed class HistoryGrainClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<HistoryTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<HistoryTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    /// <summary>Orleans grain calls serialize their arguments — mutations CreateTableAsync makes to the
    /// TableDefinition it's given (assigning Id, etc.) do NOT reflect back into the caller's own object, so
    /// callers must use the returned (post-create) TableDefinition, not the one they passed in.</summary>
    private async Task<(IRegistryGrain Registry, string SourceName, TableDefinition Created)> SeedTradesTableAsync(TableDefinition tableDef)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "trades_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });
        tableDef.Sql = tableDef.Sql.Replace("__SOURCE__", sourceName);
        var created = await registry.CreateTableAsync(tableDef);
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return (registry, sourceName, created);
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

    [Fact]
    public async Task LastN_History_AccumulatesVersionsForOneSymbol_CappedAtN()
    {
        var tableName = "hist_lastn_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM __SOURCE__ GROUP BY symbol",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.LastN,
            HistoryLimit = 3,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);

        // 5 successive trades for AAPL -> 5 group updates (retract-old/assert-new pairs after the first).
        for (var i = 1; i <= 5; i++)
        {
            await PublishTradeAsync(sourceName, "AAPL", 100 + i, 10);
        }

        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);

        // TotalVersions reflects what's currently RETAINED (post-LastN-retention), not the total ever
        // appended — RetractionCount is the monotonic counter to poll on instead (4 retract/assert pairs
        // out of 5 sequential updates to the same AAPL group: the very first insertion has no retraction).
        var result = await PollUntilAsync(
            () => historyGrain.GetHistoryAsync(key, limit: 0),
            r => r.KeyFound && r.RetractionCount >= 4,
            deadlineSeconds: 15);

        Assert.True(result.KeyFound);
        Assert.Equal(3, result.Versions.Count); // LastN(3) ring buffer
        Assert.Equal(3, result.TotalVersions);
        // Newest-first ordering: the last trade's "trades" count (5) should be in the first (newest) version.
        Assert.Equal(5L, result.Versions[0].Row["trades"]);
        Assert.Equal(4, result.RetractionCount); // 5 assertions, 4 preceding retractions

        var stats = await historyGrain.GetStatsAsync();
        Assert.True(stats.Enabled);
        Assert.Equal(TableHistoryMode.LastN, stats.Mode);
        Assert.Equal(1, stats.KeyCount);
    }

    [Fact]
    public async Task MinByMaxBy_ConfigChangeViaUpdateTable_ResetsHistoryAndTracksExtremePlusLatest()
    {
        var tableName = "hist_maxby_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades, AVG(price) AS avg_price FROM __SOURCE__ GROUP BY symbol",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.LastN,
            HistoryLimit = 10,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);
        await PollUntilAsync(() => historyGrain.GetHistoryAsync(key, 0), r => r.KeyFound, deadlineSeconds: 10);

        // Switch to MaxBy(avg_price) via UpdateTableAsync — RegistryGrain must detect the history-config
        // change and call ResetAsync, wiping the LastN history collected above.
        var current = await registry.GetTableAsync(created.Id);
        Assert.NotNull(current);
        current!.HistoryMode = TableHistoryMode.MaxBy;
        current.HistoryByField = "avg_price";
        var updated = await registry.UpdateTableAsync(current);
        Assert.Equal(PipelineStatus.Running, updated!.Status); // sql unchanged -> table itself wasn't restarted/broken

        var statsAfterReset = await historyGrain.GetStatsAsync();
        Assert.Equal(TableHistoryMode.MaxBy, statsAfterReset.Mode);
        Assert.Equal(0, statsAfterReset.KeyCount); // reset cleared prior LastN history

        // Feed a low, then a high, then a middling avg_price for AAPL (avg_price = running AVG(price)).
        await PublishTradeAsync(sourceName, "AAPL", 50, 10);   // avg after this trade batch, etc.
        await PublishTradeAsync(sourceName, "AAPL", 300, 10);
        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        var result = await PollUntilAsync(
            () => historyGrain.GetHistoryAsync(key, 0),
            r => r.KeyFound && r.TotalVersions >= 2,
            deadlineSeconds: 15);

        Assert.True(result.KeyFound);
        Assert.True(result.Versions.Count <= 2); // MinBy/MaxBy: extreme + latest, 2 entries max
        Assert.Equal(TableHistoryMode.MaxBy, result.Mode);
    }

    [Fact]
    public async Task CreateTable_MinByWithNonExistentByField_ThrowsClearError()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var tableName = "hist_bad_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM trades GROUP BY symbol",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.MinBy,
            HistoryByField = "does_not_exist",
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(def));
        Assert.Contains("does_not_exist", ex.Message);
    }

    [Fact]
    public async Task CreateTable_MinByWithNonNumericByField_ThrowsClearError()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "trades_for_kind_check",
            Enabled = false,
            EventsPerSecond = 0,
            Fields = [new FieldDef("symbol", FieldType.String)],
        });

        var tableName = "hist_badkind_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM trades_for_kind_check GROUP BY symbol",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.MaxBy,
            HistoryByField = "symbol", // exists, but is a String — not numeric/timestamp
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(def));
        Assert.Contains("numeric or timestamp", ex.Message);
    }

    [Fact]
    public async Task DeleteTable_DisablesHistoryGrain()
    {
        var tableName = "hist_del_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);
        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);
        await PollUntilAsync(() => historyGrain.GetHistoryAsync(key, 0), r => r.KeyFound, deadlineSeconds: 10);

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        await registry.DeleteTableAsync(created.Id);

        var statsAfterDelete = await historyGrain.GetStatsAsync();
        Assert.False(statsAfterDelete.Enabled);
        Assert.Equal(0, statsAfterDelete.KeyCount);
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
