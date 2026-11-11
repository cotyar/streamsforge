using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Storage;
using Xunit;

namespace StreamsForge.Host.Tests;

internal sealed class EnvTableSourceSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class EnvTableSourceClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 021 wave 2 — the acceptance case D3/D6 exist to close: today (before this wave) a
/// <c>tablehistory_{name}.json</c>-shaped file, one physical <c>ITableGrain</c> and one physical delta
/// stream all serve TWO same-named tables in two different environments. This asserts the fix end to end
/// against a real Orleans cluster with real <c>JsonFileGrainStorage</c> — not the pure <c>EnvKeys.Qualify</c>
/// helper in isolation (see <c>EnvIsolationKeyGateTests</c>'s own doc comment for why that distinction
/// matters) — and against real stream delivery, mirroring the pattern
/// <c>EnvIsolationLifecycleStreamTests</c> already established for the lifecycle stream.
/// </summary>
public sealed class EnvIsolationTableAndSourceTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-env-table-source-tests", Guid.NewGuid().ToString("n"));
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dataDir);
        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["DataDir"] = _dataDir,
        }));
        builder.AddSiloBuilderConfigurator<EnvTableSourceSiloConfigurator>();
        builder.AddClientBuilderConfigurator<EnvTableSourceClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Hyphenated — for environment names (<see cref="EnvKeys.IsValidName"/> allows
    /// <c>[a-z0-9-]</c>, no underscore).</summary>
    private static string Unique(string prefix) => prefix + "-" + Guid.NewGuid().ToString("n")[..8];

    /// <summary>Underscored — for source/table names that appear inside SQL text (the tokenizer's
    /// identifier grammar is <c>[letter|digit|_]+</c>; a hyphen would not parse as part of the FROM clause).</summary>
    private static string UniqueIdent(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..8];

    private async Task<IRegistryGrain> RegistryForAsync(string env)
    {
        if (env != EnvKeys.Default)
        {
            await _cluster.GrainFactory.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey)
                .CreateAsync(env, "", "tester");
        }
        return _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));
    }

    private async Task PublishTickAsync(string env, string sourceName, string symbol, double price)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(
            StreamId.Create(StreamConstants.SourcesNamespace, EnvKeys.Qualify(env, sourceName)));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
        });
    }

    private static async Task<int> PollRowCountAsync(ITableGrain grain, int atLeast, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        int last = 0;
        while (DateTime.UtcNow < deadline)
        {
            last = await grain.GetRowCountAsync();
            if (last >= atLeast) return last;
            await Task.Delay(50);
        }
        return last;
    }

    private static async Task<bool> PollFileExistsAsync(string path, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (File.Exists(path)) return true;
            await Task.Delay(100);
        }
        return File.Exists(path);
    }

    /// <summary>THE bug this wave closes, asserted directly: two environments' identically-named table are
    /// two grains with two persisted state files and two independent row sets — rows written into one never
    /// appear in the other. Uses a short FlushMs so the Batched-mode periodic flush (default 2000ms) writes
    /// the state file within the test's own timeout instead of racing it.</summary>
    [Fact]
    public async Task Same_named_tables_in_two_environments_are_isolated_grains_with_two_state_files_and_isolated_rows()
    {
        var env = Unique("tblenv");
        var sourceName = UniqueIdent("tbl_src");
        var tableName = UniqueIdent("tbl_tbl");
        var sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)";

        var defaultRegistry = await RegistryForAsync(EnvKeys.Default);
        var envRegistry = await RegistryForAsync(env);

        foreach (var registry in new[] { defaultRegistry, envRegistry })
        {
            await registry.UpsertSourceAsync(new SourceDefinition
            {
                Name = sourceName,
                Enabled = false, // deterministic — the test publishes events itself
                Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
            });
            var created = await registry.CreateTableAsync(new TableDefinition
            {
                Name = tableName,
                Sql = sql,
                FlushMs = 200,
            });
            var status = await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
            Assert.True(status!.Status == PipelineStatus.Running, status.Error);
        }

        var defaultGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var envGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(EnvKeys.Qualify(env, tableName));

        // Different content per environment, same symbol — proves isolation, not merely non-interference.
        await PublishTickAsync(EnvKeys.Default, sourceName, "AAPL", 100.0);
        await PublishTickAsync(env, sourceName, "AAPL", 999.0);

        Assert.Equal(1, await PollRowCountAsync(defaultGrain, atLeast: 1, TimeSpan.FromSeconds(10)));
        Assert.Equal(1, await PollRowCountAsync(envGrain, atLeast: 1, TimeSpan.FromSeconds(10)));

        var defaultRows = await defaultGrain.GetRowsAsync(10, 0);
        var envRows = await envGrain.GetRowsAsync(10, 0);
        Assert.Equal(100.0, (double)defaultRows.Single().Row["price"]!);
        Assert.Equal(999.0, (double)envRows.Single().Row["price"]!);

        // Two DISTINCT state files on disk — the exact bug wave 1's own report named
        // ("a single tablehistory_{name}.json serves both", the ITableGrain equivalent of it).
        var stateDir = Path.Combine(_dataDir, "state");
        var defaultFile = Path.Combine(stateDir, $"table.table_{tableName}.json");
        var envFile = Path.Combine(stateDir, $"table.table_{env}.{tableName}.json");
        Assert.True(await PollFileExistsAsync(defaultFile, TimeSpan.FromSeconds(10)),
            $"expected the DEFAULT environment's table state file at '{defaultFile}' (byte-identical pre-plan-021 name, D2)");
        Assert.True(await PollFileExistsAsync(envFile, TimeSpan.FromSeconds(10)),
            $"expected '{env}''s table state file at '{envFile}', distinct from the default's");
        Assert.NotEqual(defaultFile, envFile);
    }

    /// <summary>Item 3 of this wave's scope: a source's own stream is qualified with its environment, so a
    /// subscriber camped on one environment's copy of a source never hears the other's traffic — the same
    /// property <c>EnvIsolationLifecycleStreamTests</c> already proved for the lifecycle stream, here for
    /// the per-source <c>SourcesNamespace</c> stream every generator/connector/ingest source publishes on
    /// (see GeneratorGrain/ConnectorGrain/OrleansIngressFacade's own <c>this.GetPrimaryKeyString()</c>/
    /// <c>EnvKeys.Qualify</c> publish sites).</summary>
    [Fact]
    public async Task A_sources_stream_delivers_only_to_its_own_environments_subscribers()
    {
        var env = Unique("srcenv");
        var sourceName = UniqueIdent("iso_src");
        await RegistryForAsync(env); // just to create the environment; the source itself needs no registry entry for this stream-level test.

        var defaultReceived = new List<EventRecord>();
        var envReceived = new List<EventRecord>();

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var defaultStream = streamProvider.GetStream<EventRecord>(
            StreamId.Create(StreamConstants.SourcesNamespace, EnvKeys.Qualify(EnvKeys.Default, sourceName)));
        await defaultStream.SubscribeAsync((evt, _) => { defaultReceived.Add(evt); return Task.CompletedTask; });

        var envStream = streamProvider.GetStream<EventRecord>(
            StreamId.Create(StreamConstants.SourcesNamespace, EnvKeys.Qualify(env, sourceName)));
        await envStream.SubscribeAsync((evt, _) => { envReceived.Add(evt); return Task.CompletedTask; });

        // Publish ONLY on env's qualified stream.
        await PublishTickAsync(env, sourceName, "ONLY_ENV", 1.0);

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (envReceived.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }
        Assert.Single(envReceived);
        Assert.Empty(defaultReceived); // proves isolation, not just "the env subscription works"

        // Sanity the other direction: default's own traffic reaches only default's subscriber.
        await PublishTickAsync(EnvKeys.Default, sourceName, "ONLY_DEFAULT", 2.0);
        deadline = DateTime.UtcNow.AddSeconds(10);
        while (defaultReceived.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }
        Assert.Single(defaultReceived);
        Assert.Single(envReceived); // unchanged — still just the one from before
    }

    /// <summary>The D2 gate, extended to this wave's own scope: with no environment ever mentioned, a table
    /// grain's key and its persisted state file name are the EXACT pre-plan-021 strings — proven above
    /// (the default half of <see cref="Same_named_tables_in_two_environments_are_isolated_grains_with_two_state_files_and_isolated_rows"/>)
    /// against a real grain and a real file. This test adds the shard grain key half: activating
    /// <c>ITableShardRouterGrain</c>/<c>ITableShardGrain</c> at a bare table name in the DEFAULT environment
    /// produces the exact same <c>GetPrimaryKeyString()</c> the pre-plan-021 code always did — proving the
    /// RegistryGrain/TableShardRouterGrain call sites this wave qualified still compose an UNqualified key
    /// when there is nothing to qualify with.</summary>
    [Fact]
    public void Default_environments_shard_grain_keys_are_byte_identical_to_pre_plan021()
    {
        var tableName = UniqueIdent("shard_tbl");
        var routerGrain = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(tableName);
        Assert.Equal(tableName, routerGrain.GetPrimaryKeyString());

        var directoryGrain = _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(tableName);
        Assert.Equal(tableName, directoryGrain.GetPrimaryKeyString());

        // TableShardKeys.GrainKey's own format ("{table}|{token}") is untouched by this wave — only the
        // `table` component is ever qualified, and Qualify("", table) == table (D2).
        var shardKey = TableShardKeys.GrainKey(tableName, "AAPL");
        var shardGrain = _cluster.GrainFactory.GetGrain<ITableShardGrain>(shardKey);
        Assert.Equal(shardKey, shardGrain.GetPrimaryKeyString());
        Assert.StartsWith(tableName + "|", shardGrain.GetPrimaryKeyString(), StringComparison.Ordinal);
    }
}
