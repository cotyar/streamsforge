using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Memory streams + memory grain storage, same shape as every other cluster test in this
/// assembly (duplicated rather than shared — per-file convention here, see e.g.
/// CatalogNamePolicyClusterTests' identical configurator).</summary>
internal sealed class EnvIsolationSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class EnvIsolationClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 021 wave 1, track A — the environment directory's own invariants (D7), and the fact that two
/// environments' registries hold genuinely disjoint catalogs (D1) once <c>RegistryGrain</c> is activated at
/// a qualified key. Against a real cluster (<c>EnvironmentRegistryGrain</c> and <c>RegistryGrain</c>), not
/// fakes — the invariants this pins are about grain KEYS, which a fake facade has no notion of.
/// </summary>
public sealed class EnvIsolationRegistryTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<EnvIsolationSiloConfigurator>();
        builder.AddClientBuilderConfigurator<EnvIsolationClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private IEnvironmentRegistryGrain Environments =>
        _cluster.GrainFactory.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey);

    private static string Unique(string prefix) => prefix + "-" + Guid.NewGuid().ToString("n")[..8];

    // =============================================================================================
    // D7 invariants
    // =============================================================================================

    /// <summary>Plan 021 — seeding is DEFAULT-ONLY. Boot calls EnsureInitializedAsync on every
    /// environment's registry so already-Running entities resume everywhere, and the three seed blocks in
    /// that method fire on an EMPTY catalog — so without the guard, creating an empty environment and
    /// restarting would fill it with the demo catalog, and force-deleting an environment's contents would
    /// re-seed them on the next boot. Both silent, neither asked for.</summary>
    [Fact]
    public async Task EnsureInitialized_seeds_the_default_environment_and_never_any_other()
    {
        var env = Unique("seedgate");
        await Environments.CreateAsync(env, "", "test");

        var seeded = _cluster.GrainFactory.GetGrain<IRegistryGrain>(
            EnvKeys.Qualify(EnvKeys.Default, StreamConstants.RegistryKey));
        await seeded.EnsureInitializedAsync();
        Assert.NotEmpty(await seeded.GetSourcesAsync());

        var fresh = _cluster.GrainFactory.GetGrain<IRegistryGrain>(
            EnvKeys.Qualify(env, StreamConstants.RegistryKey));
        await fresh.EnsureInitializedAsync();
        Assert.Empty(await fresh.GetSourcesAsync());
        Assert.Empty(await fresh.GetPipelinesAsync());
        Assert.Empty(await fresh.GetTablesAsync());

        // And it stays empty across a second boot — the case that would bite an operator who created an
        // environment, restarted, and found someone else's demo catalog in it.
        await fresh.EnsureInitializedAsync();
        Assert.Empty(await fresh.GetSourcesAsync());
    }

    [Fact]
    public async Task Default_environment_always_exists_even_with_nothing_ever_created()
    {
        Assert.True(await Environments.ExistsAsync(EnvKeys.DefaultDisplayName));
        Assert.True(await Environments.ExistsAsync(""));

        var list = await Environments.ListAsync();
        Assert.Equal(EnvKeys.DefaultDisplayName, list[0].Name);
    }

    [Fact]
    public async Task Default_environment_cannot_be_created()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            Environments.CreateAsync(EnvKeys.DefaultDisplayName, "", "tester"));
        Assert.Contains("not a valid environment name", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Default_environment_cannot_be_deleted()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Environments.DeleteAsync(EnvKeys.DefaultDisplayName, force: true));
        Assert.Contains("default environment cannot be deleted", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Has-Upper")]
    [InlineData("has a space")]
    [InlineData("has.dot")]
    [InlineData("catalog")] // reserved
    [InlineData("access")] // reserved
    public async Task An_invalid_name_is_refused_on_create(string name)
    {
        await Assert.ThrowsAsync<ArgumentException>(() => Environments.CreateAsync(name, "", "tester"));
    }

    [Fact]
    public async Task A_duplicate_name_is_refused_on_create()
    {
        var name = Unique("dup");
        await Environments.CreateAsync(name, "first", "tester");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Environments.CreateAsync(name, "second", "tester"));
        Assert.Contains("already exists", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_created_environment_is_listed_and_empty()
    {
        var name = Unique("fresh");
        var created = await Environments.CreateAsync(name, "a fresh one", "tester");
        Assert.Equal(name, created.Name);

        var list = await Environments.ListAsync();
        var entry = Assert.Single(list, e => e.Name == name);
        Assert.Equal(0, entry.EntityCount);
    }

    [Fact]
    public async Task A_nonempty_environment_needs_force_to_delete()
    {
        var name = Unique("busy");
        await Environments.CreateAsync(name, "", "tester");

        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(name, StreamConstants.RegistryKey));
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = Unique("src"),
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.Double)],
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Environments.DeleteAsync(name, force: false));
        Assert.Contains("not empty", ex.Message, StringComparison.Ordinal);
        Assert.Contains("force=true", ex.Message, StringComparison.Ordinal);

        // …and still exists afterward — a refused delete must not have removed anything.
        Assert.True(await Environments.ExistsAsync(name));
    }

    [Fact]
    public async Task Force_deletes_a_nonempty_environment_and_its_catalog()
    {
        var name = Unique("wipe");
        await Environments.CreateAsync(name, "", "tester");

        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(name, StreamConstants.RegistryKey));
        var sourceName = Unique("wsrc");
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.Double)],
        });
        await registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("wtbl"),
            Sql = $"SELECT x FROM {sourceName} LATEST BY (x)",
        });

        var deleted = await Environments.DeleteAsync(name, force: true);
        Assert.True(deleted);
        Assert.False(await Environments.ExistsAsync(name));

        // The environment's own registry grain is now an empty catalog (its persisted state file is not
        // physically removed — see EnvironmentRegistryGrain.DeleteAsync's own doc comment on what
        // force-delete does not clean up), not an error to keep reading from.
        Assert.Empty(await registry.GetSourcesAsync());
        Assert.Empty(await registry.GetTablesAsync());
    }

    [Fact]
    public async Task Deleting_an_unknown_environment_returns_false_rather_than_throwing()
    {
        Assert.False(await Environments.DeleteAsync(Unique("ghost"), force: true));
    }

    // =============================================================================================
    // D1: two environments' catalogs are genuinely disjoint.
    // =============================================================================================

    [Fact]
    public async Task Two_environments_hold_disjoint_catalogs_for_a_table_with_the_same_name()
    {
        var envA = Unique("env-a");
        var envB = Unique("env-b");
        await Environments.CreateAsync(envA, "", "tester");
        await Environments.CreateAsync(envB, "", "tester");

        var registryA = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(envA, StreamConstants.RegistryKey));
        var registryB = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(envB, StreamConstants.RegistryKey));

        const string sharedSourceName = "orders";
        var fields = new List<FieldDef> { new("symbol", FieldType.String) };
        await registryA.UpsertSourceAsync(new SourceDefinition { Name = sharedSourceName, Enabled = false, Fields = fields });
        await registryB.UpsertSourceAsync(new SourceDefinition { Name = sharedSourceName, Enabled = false, Fields = fields });

        var tableA = await registryA.CreateTableAsync(new TableDefinition
        {
            Name = "orders_view",
            Sql = $"SELECT symbol FROM {sharedSourceName} LATEST BY (symbol)",
        });
        var tableB = await registryB.CreateTableAsync(new TableDefinition
        {
            Name = "orders_view",
            Sql = $"SELECT symbol FROM {sharedSourceName} LATEST BY (symbol)",
        });

        // Different ids: two genuinely separate entities, not one shared one.
        Assert.NotEqual(tableA.Id, tableB.Id);

        // Each environment's own definition remembers which environment it belongs to.
        Assert.Equal(envA, tableA.Environment);
        Assert.Equal(envB, tableB.Environment);

        // Neither environment's list contains the other's entity.
        var tablesA = await registryA.GetTablesAsync();
        var tablesB = await registryB.GetTablesAsync();
        Assert.Single(tablesA);
        Assert.Single(tablesB);
        Assert.DoesNotContain(tablesA, t => t.Id == tableB.Id);
        Assert.DoesNotContain(tablesB, t => t.Id == tableA.Id);

        // …and the default environment's own catalog is untouched by either.
        var defaultRegistry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        Assert.DoesNotContain(await defaultRegistry.GetTablesAsync(), t => t.Id == tableA.Id || t.Id == tableB.Id);
    }

    [Fact]
    public async Task An_entitys_environment_is_never_moved_by_an_update()
    {
        var env = Unique("sticky");
        await Environments.CreateAsync(env, "", "tester");
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));

        var pipeline = await registry.CreatePipelineAsync(new PipelineDefinition { Name = Unique("pipe"), Sql = "SELECT 1" });
        Assert.Equal(env, pipeline.Environment);

        // Even if a caller's payload claims a different (or empty/default) environment, an update must
        // keep the stored value — plan 021 D5.
        pipeline.Environment = "";
        pipeline.Description = "edited";
        var updated = await registry.UpdatePipelineAsync(pipeline);

        Assert.Equal(env, updated!.Environment);
    }
}
