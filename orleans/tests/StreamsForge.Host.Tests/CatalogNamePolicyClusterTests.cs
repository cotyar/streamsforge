using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Memory streams + memory grain storage, same shape as every other cluster test in this
/// assembly (duplicated rather than shared — they are per-file by convention here).</summary>
internal sealed class NamePolicyTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class NamePolicyTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 016 wave 1-C — the write path's name policy, against a real <c>RegistryGrain</c>:
///
/// <list type="bullet">
/// <item>a pipeline name is unique among PIPELINES (and only among pipelines);</item>
/// <item>a table rename is allowed IFF Stopped, IFF unsharded, IFF nothing lists it in
/// <c>TableInputs</c> — and when it is allowed, the OLD name's history tier is torn down;</item>
/// <item>a source cannot be renamed, and the catalog has no operation that would.</item>
/// </list>
///
/// <para><b>Why the source case is a test and not a guard.</b> A source's name is simultaneously its
/// REST route, its grain key, its Orleans stream key, its entry in the SQL namespace, its
/// <c>EntitySchemas.SourceKey</c> field-number key, a member of every
/// <c>PipelineDefinition.SourceNames</c> and every federated peer's <c>EntityKey</c>. Nothing renames
/// one today — <c>SourcesEndpoints</c>' PUT force-overwrites the body's name with the route segment, and
/// the catalog offers only upsert-by-name. That is precisely what makes name-keying safe everywhere
/// else, so it is pinned here rather than left as an accident nobody wrote down.</para>
/// </summary>
public sealed class CatalogNamePolicyClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<NamePolicyTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<NamePolicyTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static string Unique(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..8];

    private async Task<string> SeedSourceAsync()
    {
        var name = Unique("np_src");
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
        });
        return name;
    }

    // =============================================================================================
    // Pipelines: unique among pipelines — and, since table-over-pipeline made a pipeline name a
    // relation name a table's SQL can write, unique against source and table names too.
    // =============================================================================================

    [Fact]
    public async Task CreatePipeline_refuses_a_name_another_pipeline_already_uses()
    {
        var name = Unique("np_pipe");
        await Registry.CreatePipelineAsync(new PipelineDefinition { Name = name, Sql = "SELECT 1" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Registry.CreatePipelineAsync(new PipelineDefinition { Name = name, Sql = "SELECT 1" }));
        Assert.Contains("already used by another pipeline", ex.Message, StringComparison.Ordinal);

        Assert.Single(await Registry.GetPipelinesAsync(), p => p.Name == name);
    }

    [Fact]
    public async Task UpdatePipeline_refuses_renaming_onto_another_pipelines_name()
    {
        var taken = Unique("np_taken");
        await Registry.CreatePipelineAsync(new PipelineDefinition { Name = taken, Sql = "SELECT 1" });
        var mine = await Registry.CreatePipelineAsync(new PipelineDefinition { Name = Unique("np_mine"), Sql = "SELECT 1" });

        mine.Name = taken;
        await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdatePipelineAsync(mine));
    }

    /// <summary>The 95% case stays free: a pipeline renames to any unused name, running or not — it is
    /// id-keyed everywhere (its grain key is <c>def.Id</c>), which is exactly why it is the one entity
    /// this wave leaves freely renameable.</summary>
    [Fact]
    public async Task UpdatePipeline_allows_renaming_to_a_free_name_and_keeps_its_own()
    {
        var created = await Registry.CreatePipelineAsync(new PipelineDefinition { Name = Unique("np_old"), Sql = "SELECT 1" });

        var newName = Unique("np_new");
        created.Name = newName;
        var updated = await Registry.UpdatePipelineAsync(created);
        Assert.Equal(newName, updated!.Name);

        // …and re-saving it unchanged is not mistaken for a self-collision.
        Assert.NotNull(await Registry.UpdatePipelineAsync(updated));
    }

    /// <summary><b>INVERTED by table-over-pipeline, deliberately — this is a behaviour change, not a
    /// weakened assertion.</b> This test used to assert the opposite, on the premise that "pipelines are
    /// not in the SQL namespace". That premise is exactly what the feature removed: a table's SQL may now
    /// write <c>FROM &lt;pipeline&gt;</c>, so a pipeline name and a source name are two spellings in ONE
    /// namespace, and a catalog holding both would make <c>FROM foo</c> resolve by dictionary-insertion
    /// order inside <c>PipelineInputs.BuildStreamSchemas</c>. The old test's parenthetical ("the shape the
    /// seed data ships") does not hold either — verified: no seeded pipeline name collides with a seeded
    /// source name, so nothing in the demo catalog depended on the permission being granted.
    ///
    /// <para>Enforced going forward only, like every other name rule in this file: nothing scans the
    /// catalog at boot and an existing colliding pair keeps working until one of the two is next
    /// written.</para></summary>
    [Fact]
    public async Task A_pipeline_may_no_longer_share_its_name_with_a_source_it_reads_from()
    {
        var sourceName = await SeedSourceAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Registry.CreatePipelineAsync(new PipelineDefinition
            {
                Name = sourceName,
                Sql = $"SELECT symbol, price FROM {sourceName}",
            }));
        Assert.Contains("already used by a stream source", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(await Registry.GetPipelinesAsync(), p => p.Name == sourceName);

        // …and the reverse direction, which has no prior test at all: a SOURCE may not take a pipeline's
        // name either. Sources cannot be renamed, so this is the only moment the rule can be applied.
        var pipelineName = Unique("np_pipe_owns");
        await Registry.CreatePipelineAsync(new PipelineDefinition { Name = pipelineName, Sql = "SELECT 1" });
        var clash = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Registry.UpsertSourceAsync(new SourceDefinition
            {
                Name = pipelineName,
                Fields = [new FieldDef("symbol", FieldType.String)],
            }));
        Assert.Contains("already used by a pipeline", clash.Message, StringComparison.Ordinal);
    }

    // =============================================================================================
    // Tables: the three-condition rename.
    // =============================================================================================

    private async Task<(string Source, TableDefinition Table)> SeedStoppedTableAsync(bool history = false)
    {
        var sourceName = await SeedSourceAsync();
        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("np_tbl"),
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            HistoryEnabled = history,
        });
        Assert.Equal(PipelineStatus.Stopped, table.Status);
        return (sourceName, table);
    }

    [Fact]
    public async Task A_stopped_unsharded_table_with_no_dependents_renames()
    {
        var (_, table) = await SeedStoppedTableAsync();
        var newName = Unique("np_renamed");

        table.Name = newName;
        var updated = await Registry.UpdateTableAsync(table);

        Assert.Equal(newName, updated!.Name);
        Assert.Equal(table.Id, updated.Id);
        Assert.Single(await Registry.GetTablesAsync(), t => t.Id == table.Id);
    }

    /// <summary>The half that makes the rename safe rather than merely permitted: the old name's history
    /// tier is disabled and cleared BEFORE the new name is stored, so a table later created with the old
    /// name does not inherit its predecessor's version trails.</summary>
    [Fact]
    public async Task Renaming_releases_the_old_names_history_tier()
    {
        var (_, table) = await SeedStoppedTableAsync(history: true);
        var oldName = table.Name;
        Assert.True((await _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(oldName).GetStatsAsync()).Enabled);

        table.Name = Unique("np_renamed");
        var updated = await Registry.UpdateTableAsync(table);

        Assert.False((await _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(oldName).GetStatsAsync()).Enabled);
        Assert.True((await _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(updated!.Name).GetStatsAsync()).Enabled);
    }

    [Fact]
    public async Task A_running_table_is_not_renameable()
    {
        var (_, table) = await SeedStoppedTableAsync();
        var running = await Registry.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        Assert.Equal(PipelineStatus.Running, running!.Status);

        running.Name = Unique("np_renamed");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdateTableAsync(running));
        Assert.Contains("cannot be renamed", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Stop it first", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_sharded_table_is_not_renameable()
    {
        var sourceName = await SeedSourceAsync();
        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("np_shard"),
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            ShardBy = ["symbol"],
        });

        table.Name = Unique("np_renamed");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdateTableAsync(table));
        Assert.Contains("is sharded by", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>A dependent is refused at ANY status, unlike the stop/delete guard which only fences off
    /// Running ones: a stopped dependent still says <c>FROM oldname</c> in its SQL and still holds the old
    /// name in its persisted <c>TableInputs</c>, and nothing recompiles it.</summary>
    [Fact]
    public async Task A_table_another_stopped_table_reads_from_is_not_renameable()
    {
        var (_, upstream) = await SeedStoppedTableAsync();
        var downstream = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("np_down"),
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstream.Name} GROUP BY symbol",
        });
        Assert.Contains(upstream.Name, downstream.TableInputs);
        Assert.Equal(PipelineStatus.Stopped, downstream.Status);

        upstream.Name = Unique("np_renamed");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdateTableAsync(upstream));
        Assert.Contains(downstream.Name, ex.Message, StringComparison.Ordinal);
        Assert.Contains("read from it by name", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Everything above is about the NAME changing. An ordinary edit to a Running table with
    /// dependents is untouched by this wave — pinned here because a guard placed one line higher would
    /// have broken it.</summary>
    [Fact]
    public async Task A_non_rename_edit_is_unaffected_by_the_rename_policy()
    {
        var (_, upstream) = await SeedStoppedTableAsync();
        await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("np_down"),
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstream.Name} GROUP BY symbol",
        });
        var running = await Registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);

        running!.Description = "edited while running, with a dependent, without renaming";
        var updated = await Registry.UpdateTableAsync(running);

        Assert.Equal("edited while running, with a dependent, without renaming", updated!.Description);
        Assert.Equal(upstream.Name, updated.Name);
    }

    // =============================================================================================
    // Sources: no rename, by construction.
    // =============================================================================================

    /// <summary>There is no rename operation on the catalog at all — <c>UpsertSourceAsync</c> is keyed by
    /// name, so "renaming" through it would FORK the source: the old name keeps its grain, its stream,
    /// its field numbers and every <c>SourceNames</c> reference, while a second, empty source appears
    /// under the new one. This test pins that forking behaviour so that anyone who later tries to build a
    /// rename on top of upsert sees what it actually does.</summary>
    [Fact]
    public async Task Upserting_a_source_under_a_new_name_forks_it_rather_than_renaming_it()
    {
        var original = await SeedSourceAsync();
        var stored = await Registry.GetSourceAsync(original);

        stored!.Name = Unique("np_src_renamed");
        await Registry.UpsertSourceAsync(stored);

        Assert.NotNull(await Registry.GetSourceAsync(original));
        Assert.NotNull(await Registry.GetSourceAsync(stored.Name));
    }

    /// <summary>And no facade member offers one: the source surface is get/upsert/delete, all keyed by
    /// name. A method whose name suggests otherwise appearing on <see cref="ICatalogFacade"/> is exactly
    /// the change this test exists to catch.</summary>
    [Fact]
    public void No_catalog_operation_renames_a_source()
    {
        var suspects = typeof(ICatalogFacade).GetMethods()
            .Select(m => m.Name)
            .Where(n => n.Contains("Rename", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.Empty(suspects);
    }
}
