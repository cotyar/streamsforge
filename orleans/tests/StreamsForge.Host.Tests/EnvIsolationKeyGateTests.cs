using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Host.Storage;
using Xunit;

namespace StreamsForge.Host.Tests;

internal sealed class EnvKeyGateSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class EnvKeyGateClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 021 D2 — "the acceptance criterion, stated here so a wave cannot quietly trade it away": with the
/// feature merged and no environment ever mentioned, every grain key and state file name is byte-identical
/// to what it was before this plan. This is asserted on the ACTUAL composed key/file name, against a real
/// <c>JsonFileGrainStorage</c>-backed silo — the same shape <c>PipelineLineageBackfillTests</c> uses to
/// assert on persisted file names elsewhere in this assembly — not merely on the pure <c>EnvKeys.Qualify</c>
/// function in isolation (that alone would not catch a bug in how a grain composes the key it activates
/// at).
///
/// Also covers the D3/write-path guard (item 4): a source or table name containing <c>EnvKeys.Separator</c>
/// is refused going forward.
/// </summary>
public sealed class EnvIsolationKeyGateTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-env-key-gate-tests", Guid.NewGuid().ToString("n"));
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dataDir);
        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["DataDir"] = _dataDir,
        }));
        builder.AddSiloBuilderConfigurator<EnvKeyGateSiloConfigurator>();
        builder.AddClientBuilderConfigurator<EnvKeyGateClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Qualify_of_the_default_environment_is_the_identity_function()
    {
        Assert.Equal(StreamConstants.RegistryKey, EnvKeys.Qualify(EnvKeys.Default, StreamConstants.RegistryKey));
        Assert.Equal("", EnvKeys.Default);
    }

    /// <summary>The D2 gate itself: with no environment ever mentioned, the default RegistryGrain's
    /// persisted state file has the EXACT pre-plan-021 name — <c>catalog.registry_catalog.json</c> — the
    /// same literal name already shipping in every existing <c>data/</c> directory
    /// (orleans/src/StreamsForge.Host/data/state/catalog.registry_catalog.json in this repo).</summary>
    [Fact]
    public async Task Default_environments_registry_grain_produces_the_pre_plan021_state_file_name()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.EnsureInitializedAsync(); // forces a write, hence a file, on first activation.

        var stateDir = Path.Combine(_dataDir, "state");
        var files = Directory.GetFiles(stateDir, "catalog.*.json");
        var file = Assert.Single(files);
        Assert.Equal("catalog.registry_catalog.json", Path.GetFileName(file));
    }

    [Fact]
    public async Task A_named_environments_registry_grain_produces_a_qualified_state_file_name()
    {
        var env = "sf-key-gate-env";
        await _cluster.GrainFactory.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey)
            .CreateAsync(env, "", "tester");

        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));
        // A real write, not EnsureInitializedAsync: seeding is DEFAULT-ENVIRONMENT-ONLY (see
        // RegistryGrain.EnsureInitializedAsync's own comment — otherwise creating an empty environment and
        // restarting would fill it with the demo catalog), so a fresh environment's registry has nothing
        // to persist and produces no state file at all until something is actually created in it. The
        // property under test is the FILE NAME, so the test has to make a file exist on purpose.
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "keygate_src",
            Enabled = false,
            Fields = [new FieldDef("a", FieldType.String)],
        });

        var stateDir = Path.Combine(_dataDir, "state");
        var files = Directory.GetFiles(stateDir, "catalog.*.json");
        // The default's file plus this environment's — never the same file.
        Assert.Contains(files, f => Path.GetFileName(f) == $"catalog.registry_{env}.catalog.json");
    }

    // =============================================================================================
    // D3/write-path guard: a name containing the qualification separator is refused.
    // =============================================================================================

    [Fact]
    public async Task Creating_a_source_with_a_dotted_name_is_refused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "bad.name",
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.Double)],
        }));
        Assert.Contains($"'{EnvKeys.Separator}'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Creating_a_table_with_a_dotted_name_is_refused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "kg-src-" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.Double)],
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "bad.table.name",
            Sql = $"SELECT x FROM {sourceName} LATEST BY (x)",
        }));
        Assert.Contains($"'{EnvKeys.Separator}'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Renaming_a_table_onto_a_dotted_name_is_refused()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "kg-src2-" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.Double)],
        });
        var table = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "kg-tbl-" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT x FROM {sourceName} LATEST BY (x)",
        });

        table.Name = "renamed.badly";
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.UpdateTableAsync(table));
        Assert.Contains($"'{EnvKeys.Separator}'", ex.Message, StringComparison.Ordinal);
    }
}
