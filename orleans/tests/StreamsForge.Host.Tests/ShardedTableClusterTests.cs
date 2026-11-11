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

internal sealed class ShardTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class ShardTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 011 wave D1 — SHARDED TABLES end to end on a real cluster.
///
/// Four of these tests are the wave's actual acceptance criteria, and they are the ones to read first:
///
///  * <see cref="ShardedAndUnshardedTables_OverTheSameInput_ProduceIdenticalRows"/> — the shard tier is a
///    FAITHFUL second materialization. Same SQL, same input, and the union of the shards equals the
///    unsharded table's rows. Without this everything else is measuring a tier that quietly disagrees
///    with the table it claims to mirror.
///  * <see cref="IdleShards_Deactivate_AndReactivateWithTheirStateIntact"/> — the memory win itself.
///  * <see cref="RepeatedKeylessRowsReads_DoNotWakeIdleShards"/> — the trap. The console polls
///    <c>/rows</c> every two seconds; if that woke shards, nothing would ever be swapped out and the
///    feature would be self-defeating while passing every functional test above.
///  * <see cref="ShardedTable_DisablesTheTableWideHistoryTier"/> — the two history tiers never run at
///    once, because running both would hold every version trail twice.
///
/// Deactivation is driven by <c>IManagementGrain.ForceActivationCollection</c> rather than by waiting out
/// a collection age: the production knob is <c>Shards:IdleSeconds</c> (Program.cs), and a test that slept
/// long enough for a real collection cycle would be a slow test measuring Orleans' timer rather than this
/// code's behavior. Forcing collection exercises the identical deactivation path.
/// </summary>
public sealed class ShardedTableClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ShardTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ShardTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private async Task<string> SeedInstrumentSourceAsync(IRegistryGrain registry)
    {
        var sourceName = "instr_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "sharding test source: a state-machine instrument with legs",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("instrument", FieldType.String),
                new FieldDef("leg", FieldType.Long),
                new FieldDef("stage", FieldType.String),
                new FieldDef("notional", FieldType.Double),
            ],
        });
        return sourceName;
    }

    private async Task PublishAsync(string sourceName, string instrument, long leg, string stage, double notional)
    {
        var stream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["instrument"] = instrument,
            ["leg"] = leg,
            ["stage"] = stage,
            ["notional"] = notional,
        });
    }

    /// <summary>The instrument shape the wave was built for: one row per (instrument, leg), replaced as
    /// the state machine advances, so a key accumulates a version trail.</summary>
    private const string LegSql =
        "SELECT instrument, leg, stage, notional FROM __SOURCE__ LATEST BY (instrument, leg)";

    private async Task<TableDefinition> CreateShardedTableAsync(
        IRegistryGrain registry, string sourceName, string namePrefix, List<string> shardBy, bool historyEnabled = true)
    {
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = namePrefix + "_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = shardBy,
            HistoryEnabled = historyEnabled,
            HistoryMode = TableHistoryMode.All,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return created;
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(150);
        }
        return last;
    }

    private Task<TableShardView> ShardViewAsync(string tableName, Dictionary<string, object?> keyRow, List<string> shardBy)
    {
        var key = TableShardKeys.EncodeShardKey(keyRow, shardBy);
        return _cluster.GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(tableName, key)).GetViewAsync(0);
    }

    /// <summary>Deactivates everything Orleans is willing to collect. Grains that pin themselves with
    /// DelayDeactivation (every other grain in the table path, including the shard ROUTER) are untouched —
    /// which is exactly the asymmetry under test.</summary>
    private Task CollectIdleActivationsAsync() =>
        _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

    private static string RowSignature(TableRowDto r) => TableShardKeys.CanonicalRowKey(r.Row);

    // ------------------------------------------------------------------
    // The acceptance criteria
    // ------------------------------------------------------------------

    [Fact]
    public async Task ShardedAndUnshardedTables_OverTheSameInput_ProduceIdenticalRows()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);

        // Two tables, same SQL, same source, differing only in ShardBy — the whole claim of the wave is
        // that the second one is the first one plus a per-key materialization, not a different table.
        var plain = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "plain_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            HistoryEnabled = true,
        });
        await registry.SetTableStatusAsync(plain.Id, PipelineStatus.Running);
        var sharded = await CreateShardedTableAsync(registry, sourceName, "sharded", ["instrument"]);

        var instruments = new[] { "XS100", "XS200", "XS300" };
        foreach (var instrument in instruments)
        {
            foreach (var leg in new long[] { 1, 2 })
            {
                foreach (var stage in new[] { "New", "Confirmed", "Settled" })
                {
                    await PublishAsync(sourceName, instrument, leg, stage, 1_000 * leg);
                }
            }
        }

        var plainGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(plain.Name);
        var plainRows = await PollUntilAsync(() => plainGrain.GetRowsAsync(1000, 0), r => r.Count == 6);
        Assert.Equal(6, plainRows.Count); // 3 instruments x 2 legs, LATEST BY collapsing the stages

        var directory = _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(sharded.Name);
        await PollUntilAsync(() => directory.GetCountAsync(), c => c == 3);

        var keys = await directory.GetKeysAsync(1000, 0);
        var shardRows = new List<TableRowDto>();
        foreach (var key in keys)
        {
            var view = await _cluster.GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(sharded.Name, key)).GetViewAsync(0);
            shardRows.AddRange(view.Rows);
        }

        Assert.Equal(
            plainRows.Select(RowSignature).OrderBy(s => s, StringComparer.Ordinal).ToList(),
            shardRows.Select(RowSignature).OrderBy(s => s, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task PerKeyLookup_ReturnsOnlyThatKeysRows_AndItsFullVersionTrail()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "perkey", ["instrument"]);

        // One instrument walks its state machine; a second one exists purely so "only that key's rows"
        // is a claim with something to be wrong about.
        foreach (var stage in new[] { "New", "Confirmed", "Allocated", "Settled" })
        {
            await PublishAsync(sourceName, "XS777", 1, stage, 5_000);
        }
        await PublishAsync(sourceName, "XS888", 1, "New", 9_000);

        var keyRow = new Dictionary<string, object?> { ["instrument"] = "XS777" };
        var view = await PollUntilAsync(
            () => ShardViewAsync(table.Name, keyRow, ["instrument"]),
            v => v.Found && v.History.Sum(h => h.TotalVersions) >= 4);

        Assert.True(view.Found);
        Assert.Single(view.Rows);
        Assert.Equal("XS777", view.Rows[0].Row["instrument"]);
        Assert.Equal("Settled", view.Rows[0].Row["stage"]);

        // FULL history for the key — the point of the use case: every version is kept, not just the last.
        Assert.Single(view.History);
        var trail = view.History[0];
        Assert.Equal(4, trail.TotalVersions);
        Assert.Equal("Settled", trail.Versions[0].Row["stage"]); // newest-first
        Assert.Equal(new[] { "Settled", "Allocated", "Confirmed", "New" }, trail.Versions.Select(v => (string)v.Row["stage"]!).ToArray());

        // The router's sequence stamp reached this shard (the mechanism a fenced scan will use in D2).
        Assert.True(view.AppliedSeq >= 0, $"expected a stamped sequence, got {view.AppliedSeq}");

        // A key nothing was ever routed to answers Found=false rather than an empty success.
        var absent = await ShardViewAsync(table.Name, new Dictionary<string, object?> { ["instrument"] = "NOPE" }, ["instrument"]);
        Assert.False(absent.Found);
        Assert.Empty(absent.Rows);
    }

    [Fact]
    public async Task CompositeShardKey_SeparatesLegsOfTheSameInstrument()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "composite", ["instrument", "leg"]);

        await PublishAsync(sourceName, "XS900", 1, "New", 100);
        await PublishAsync(sourceName, "XS900", 2, "New", 200);

        var directory = _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name);
        await PollUntilAsync(() => directory.GetCountAsync(), c => c == 2);

        var leg1 = await ShardViewAsync(table.Name, new Dictionary<string, object?> { ["instrument"] = "XS900", ["leg"] = 1L }, ["instrument", "leg"]);
        Assert.Single(leg1.Rows);
        Assert.Equal(1L, leg1.Rows[0].Row["leg"]);
        Assert.Equal(100d, leg1.Rows[0].Row["notional"]);
    }

    [Fact]
    public async Task IdleShards_Deactivate_AndReactivateWithTheirStateIntact()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "swap", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);

        for (var i = 0; i < 6; i++)
        {
            foreach (var stage in new[] { "New", "Confirmed" })
            {
                await PublishAsync(sourceName, $"XS{i:D3}", 1, stage, 1_000 + i);
            }
        }

        var loaded = await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 6 && i.ResidentShardCount == 6);
        Assert.Equal(6, loaded.ShardCount);
        Assert.Equal(6, loaded.ResidentShardCount);
        var activationsBefore = loaded.Activations;

        // Nothing is arriving any more, so every shard is idle. THIS is the wave's whole claim.
        await CollectIdleActivationsAsync();
        var collected = await PollUntilAsync(() => router.GetInfoAsync(), i => i.ResidentShardCount == 0);
        Assert.Equal(0, collected.ResidentShardCount);
        Assert.Equal(6, collected.ShardCount); // the directory still knows every key — they are on disk, not gone
        Assert.True(collected.Deactivations >= 6, $"expected >= 6 deactivations, got {collected.Deactivations}");

        // …and a lookup brings one back, with its rows and its full version trail, from storage alone.
        var view = await ShardViewAsync(table.Name, new Dictionary<string, object?> { ["instrument"] = "XS003" }, ["instrument"]);
        Assert.True(view.Found);
        Assert.Single(view.Rows);
        Assert.Equal("Confirmed", view.Rows[0].Row["stage"]);
        Assert.Equal(2, view.History.Single().TotalVersions);

        var after = await router.GetInfoAsync();
        Assert.Equal(1, after.ResidentShardCount); // exactly the one that was asked for, not all six
        Assert.True(after.Activations > activationsBefore, "a reactivation should have been counted");
    }

    [Fact]
    public async Task RepeatedKeylessRowsReads_DoNotWakeIdleShards()
    {
        // THE TRAP. The console's table page polls /rows every two seconds. If a keyless listing on a
        // sharded table fanned out across the directory, every shard would wake on every poll, nothing
        // would ever be swapped out, and the feature would be self-defeating while looking correct in
        // every other test in this file.
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "poll", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);
        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);

        for (var i = 0; i < 5; i++)
        {
            await PublishAsync(sourceName, $"XP{i:D3}", 1, "New", 100 + i);
        }
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 5);
        // Wait for the table's own write-behind flush before collecting: classic-mode GetRowsAsync serves
        // the flushed snapshot, not the live executor, so reading too early would measure the flush timer
        // rather than the shard tier.
        await PollUntilAsync(() => tableGrain.GetRowsAsync(100, 0), r => r.Count == 5);

        await CollectIdleActivationsAsync();
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.ResidentShardCount == 0);

        // Twenty polls of exactly what the console asks for on this page.
        for (var i = 0; i < 20; i++)
        {
            var rows = await tableGrain.GetRowsAsync(100, 0);
            Assert.Equal(5, rows.Count); // and the listing is still complete — served from the snapshot
            await tableGrain.GetRowCountAsync();
            await tableGrain.GetSeqAsync();
            await tableGrain.GetMetricsAsync();
            // The shard-metrics endpoint is on the same page and must be equally safe to poll.
            await router.GetInfoAsync();
        }

        var info = await router.GetInfoAsync();
        Assert.Equal(0, info.ResidentShardCount);
        Assert.Equal(5, info.ShardCount);

        // The explicit scan is the one call that DOES wake them — proving the distinction is real and not
        // an accident of nothing having been ingested.
        var keys = await _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name).GetKeysAsync(100, 0);
        foreach (var key in keys)
        {
            await _cluster.GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(table.Name, key)).GetStatsAsync();
        }
        Assert.Equal(5, (await router.GetInfoAsync()).ResidentShardCount);
    }

    [Fact]
    public async Task ShardedTable_DisablesTheTableWideHistoryTier()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "onetier", ["instrument"]);

        foreach (var stage in new[] { "New", "Confirmed", "Settled" })
        {
            await PublishAsync(sourceName, "XS555", 1, stage, 42);
        }

        var view = await PollUntilAsync(
            () => ShardViewAsync(table.Name, new Dictionary<string, object?> { ["instrument"] = "XS555" }, ["instrument"]),
            v => v.Found && v.History.Sum(h => h.TotalVersions) >= 3);
        Assert.Equal(3, view.History.Single().TotalVersions);

        // The table-wide tier holds NOTHING for this table: running both would keep every version trail
        // twice, and keep the second copy resident, which is the memory the shard tier exists to release.
        var stats = await _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(table.Name).GetStatsAsync();
        Assert.False(stats.Enabled);
        Assert.Equal(0, stats.KeyCount);
        Assert.Equal(0, stats.TotalVersions);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    [Fact]
    public async Task DeletingTheTable_RemovesEveryShard()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "del", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);

        await PublishAsync(sourceName, "XD001", 1, "New", 1);
        await PublishAsync(sourceName, "XD002", 1, "New", 2);
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 2);

        await registry.SetTableStatusAsync(table.Id, PipelineStatus.Stopped);
        Assert.True(await registry.DeleteTableAsync(table.Id));

        var info = await router.GetInfoAsync();
        Assert.False(info.Enabled);
        Assert.Equal(0, info.ShardCount);

        var view = await ShardViewAsync(table.Name, new Dictionary<string, object?> { ["instrument"] = "XD001" }, ["instrument"]);
        Assert.False(view.Found);
        Assert.Empty(view.Rows);
    }

    [Fact]
    public async Task ChangingShardBy_ReKeysTheTier_RatherThanStrandingTheOldShards()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var table = await CreateShardedTableAsync(registry, sourceName, "rekey", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);

        await PublishAsync(sourceName, "XR001", 1, "New", 1);
        await PublishAsync(sourceName, "XR001", 2, "New", 2);
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 1);

        var stored = await registry.GetTableAsync(table.Id);
        stored!.ShardBy = ["instrument", "leg"];
        await registry.UpdateTableAsync(stored);

        // Every old shard was filed under a rule that no longer holds, so the tier starts clean.
        var afterUpdate = await router.GetInfoAsync();
        Assert.Equal(0, afterUpdate.ShardCount);
        Assert.Equal(new[] { "instrument", "leg" }, afterUpdate.ShardBy.ToArray());

        await PublishAsync(sourceName, "XR001", 1, "Confirmed", 1);
        await PublishAsync(sourceName, "XR001", 2, "Confirmed", 2);
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 2);
        Assert.Equal(2, (await router.GetInfoAsync()).ShardCount);
    }

    [Fact]
    public async Task RetentionEviction_ReclaimsTheEvictedKeysShard()
    {
        // Retention and sharding COMPOSE, and this is the rule: retention bounds what the TABLE holds,
        // sharding bounds what is RESIDENT. A row the table has evicted takes its shard with it, exactly
        // as it already takes its row history with it (TableHistoryGrain's Evicted branch) — history is
        // derived from the table. A user who wants to keep everything simply leaves retention off, which
        // is its default.
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "ret_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT instrument, leg, stage, notional FROM " + sourceName,
            ShardBy = ["instrument"],
            HistoryEnabled = true,
            RetentionMaxRows = 3,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(created.Name);

        for (var i = 0; i < 6; i++)
        {
            await PublishAsync(sourceName, $"XE{i:D3}", 1, "New", i);
            await Task.Delay(5); // distinct event timestamps: eviction is oldest-first by event time
        }

        // The table is bounded at 3 rows, so the tier converges to 3 live shards — the evicted keys'
        // shards cleared themselves and dropped out of the directory rather than lingering empty.
        var info = await PollUntilAsync(() => router.GetInfoAsync(), i => i.ShardCount == 3);
        Assert.Equal(3, info.ShardCount);

        var evicted = await ShardViewAsync(created.Name, new Dictionary<string, object?> { ["instrument"] = "XE000" }, ["instrument"]);
        Assert.False(evicted.Found);
    }

    // ------------------------------------------------------------------
    // The refusals
    // ------------------------------------------------------------------

    [Fact]
    public async Task SearchEnabled_PlusShardBy_IsRefused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "bad_search_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = ["instrument"],
            SearchEnabled = true,
        }));
        Assert.Contains("searchEnabled", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ShardBy_ColumnNotInTheOutputSchema_IsRefused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "bad_col_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = ["not_a_column"],
        }));
        Assert.Contains("not_a_column", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShardBy_DuplicateColumns_AreRefused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "bad_dup_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = ["instrument", "instrument"],
        }));
        Assert.Contains("duplicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ShardBy_OnSqlThatDoesNotCompile_IsSavedAsADraft_NotRefused()
    {
        // Draft-friendly, exactly like ValidateHistoryConfig and ValidateRetention: a table whose SQL is
        // mid-edit still saves, and the column check re-runs on the next update that compiles.
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "draft_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT instrument FROM a_source_that_does_not_exist",
            ShardBy = ["instrument"],
        });
        Assert.Empty(created.OutputFields);
        Assert.Equal(new[] { "instrument" }, created.ShardBy.ToArray());
    }

    [Fact]
    public async Task UnshardedTable_IsCompletelyUntouchedByTheFeature()
    {
        // The opt-in discipline, asserted rather than assumed: no router, no directory, no shards, and the
        // table-wide history tier still running exactly as before.
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedInstrumentSourceAsync(registry);
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "plainold_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            HistoryEnabled = true,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        await PublishAsync(sourceName, "XU001", 1, "New", 7);

        var historyKey = RowKeyCodec.EncodeIdentity(
            new Dictionary<string, object?> { ["instrument"] = "XU001", ["leg"] = 1L }, ["instrument", "leg"]);
        var history = await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(created.Name).GetHistoryAsync(historyKey, 0),
            h => h.KeyFound);
        Assert.True(history.KeyFound);

        var info = await _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(created.Name).GetInfoAsync();
        Assert.False(info.Enabled);
        Assert.False(info.RouterActive);
        Assert.Equal(0, info.ShardCount);
        Assert.Equal(0, info.ResidentShardCount);
    }
}
