using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Memory streams + memory grain storage, same shape as every other cluster test in this
/// assembly (duplicated rather than shared — they are per-file by convention here).</summary>
internal sealed class RevisionTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class RevisionTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 016 wave 2 against a real <c>RegistryGrain</c>: the two counters actually move (and, more
/// importantly, actually DON'T move on a knob edit), a source schema change refreshes what dependent
/// tables publish without restarting them, and <c>StaleReason</c> is written by the upstream change and
/// cleared when the pin is re-satisfied.
///
/// <para>The unit-level rules live in <c>CatalogRevisionsTests</c> (shared project, both flavours). This
/// file is for the parts only the real write path can prove: that the bump is wired into every mutation
/// site, that the refresh sweep reaches transitively, and that a refreshed table's field NUMBERS come out
/// of the registry rather than being invented per request.</para>
/// </summary>
public sealed class CatalogRevisionClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<RevisionTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<RevisionTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static string Unique(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..8];

    private async Task<SourceDefinition> UpsertSourceAsync(string name, params FieldDef[] fields)
    {
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [.. fields],
        });

        return (await Registry.GetSourceAsync(name))!;
    }

    // =============================================================================================
    // The counters.
    // =============================================================================================

    [Fact]
    public async Task ANewSourceStartsAtRevisionOneNotZero()
    {
        // 0 means "written before plan 016" (and, on a pin, "no compatibility claim"), so a freshly
        // created entity must be distinguishable from one whose counters were never assigned.
        var src = await UpsertSourceAsync(Unique("rev_src"), new FieldDef("symbol", FieldType.String));

        Assert.Equal(1, src.Revision);
        Assert.Equal(1, src.SchemaRevision);
    }

    [Fact]
    public async Task AKnobOnlyEditDoesNotBumpSchemaRevision()
    {
        var name = Unique("rev_src");
        await UpsertSourceAsync(name, new FieldDef("symbol", FieldType.String));

        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            EventsPerSecond = 42,   // the knob
            Enabled = false,
            Fields = [new FieldDef("symbol", FieldType.String)],
        });

        var after = (await Registry.GetSourceAsync(name))!;
        Assert.Equal(2, after.Revision);
        Assert.Equal(1, after.SchemaRevision);
    }

    [Fact]
    public async Task ReUpsertingAnIdenticalSourceMovesNothing()
    {
        var name = Unique("rev_src");
        var first = await UpsertSourceAsync(name, new FieldDef("symbol", FieldType.String));
        var second = await UpsertSourceAsync(name, new FieldDef("symbol", FieldType.String));

        Assert.Equal(first.Revision, second.Revision);
        Assert.Equal(first.SchemaRevision, second.SchemaRevision);
    }

    [Fact]
    public async Task ACallerCannotChooseItsOwnRevision()
    {
        var name = Unique("rev_src");
        await UpsertSourceAsync(name, new FieldDef("symbol", FieldType.String));

        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("symbol", FieldType.String)],
            Revision = 5000,
            SchemaRevision = 5000,
        });

        var after = (await Registry.GetSourceAsync(name))!;
        Assert.Equal(1, after.Revision);
        Assert.Equal(1, after.SchemaRevision);
    }

    [Fact]
    public async Task StartingAndStoppingAPipelineIsNotADefinitionChange()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));
        var pipe = await Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = Unique("rev_pipe"), Sql = $"SELECT symbol FROM {src}",
        });

        Assert.Equal(1, pipe.Revision);

        await Registry.SetPipelineStatusAsync(pipe.Id, PipelineStatus.Running);
        await Registry.SetPipelineStatusAsync(pipe.Id, PipelineStatus.Stopped);

        Assert.Equal(1, (await Registry.GetPipelineAsync(pipe.Id))!.Revision);
    }

    // =============================================================================================
    // The highest-value line: a dependent table's PUBLISHED schema stops being stale.
    // =============================================================================================

    [Fact]
    public async Task ASourceSchemaChangeRefreshesADependentTablesPersistedOutputFieldsAndFieldNumbers()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"), Sql = $"SELECT symbol, price FROM {src}",
        });
        Assert.Equal(FieldType.Double, table.OutputFields.Single(f => f.Name == "price").Type);
        var schemaRevisionBefore = table.SchemaRevision;

        // The edit: price becomes a Long. This is the shape that used to be invisible — the table goes on
        // producing the new type at runtime while /proto keeps describing the old one.
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Long));

        var after = (await Registry.GetTableAsync(table.Id))!;
        Assert.Equal(FieldType.Long, after.OutputFields.Single(f => f.Name == "price").Type);
        Assert.Equal(schemaRevisionBefore + 1, after.SchemaRevision);

        // …and the field numbers behind /proto were refreshed at the write, not left to be discovered at
        // the next read: `price` keeps its number (a type change does not retire one).
        var numbers = EntitySchemas.ParseMap(
            await Registry.EnsureFieldNumbersAsync(EntitySchemas.TableKey(after.Id), after.OutputFields));
        Assert.True(numbers.Active.ContainsKey("price"));
    }

    [Fact]
    public async Task AnAddedSourceFieldReachesAStarSelectingTablesPublishedSchema()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"), Sql = $"SELECT * FROM {src}",
        });
        Assert.DoesNotContain(table.OutputFields, f => f.Name == "qty");

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long));

        var after = (await Registry.GetTableAsync(table.Id))!;
        Assert.Contains(after.OutputFields, f => f.Name == "qty");
    }

    [Fact]
    public async Task TheRefreshReachesTheSecondHopOfATableOverTableChain()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double));

        var upstream = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_up"), Sql = $"SELECT symbol, price FROM {src}",
        });
        var downstream = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_down"), Sql = $"SELECT symbol, price FROM {upstream.Name}",
        });
        Assert.Equal(FieldType.Double, downstream.OutputFields.Single(f => f.Name == "price").Type);

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Long));

        // One pass, both hops — the sweep runs in TableInputs topological order for exactly this reason.
        Assert.Equal(FieldType.Long, (await Registry.GetTableAsync(upstream.Id))!.OutputFields.Single(f => f.Name == "price").Type);
        Assert.Equal(FieldType.Long, (await Registry.GetTableAsync(downstream.Id))!.OutputFields.Single(f => f.Name == "price").Type);
    }

    [Fact]
    public async Task ADependentIsNotRestartedByAnUpstreamSchemaChange()
    {
        // Cascading auto-restart is explicitly NOT done: the restart-on-change machinery is for SELF
        // edits. A dependent keeps running on its compiled plan, which is what it does today.
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"), Sql = $"SELECT symbol, price FROM {src}",
        });
        await Registry.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        Assert.Equal(PipelineStatus.Running, (await Registry.GetTableAsync(table.Id))!.Status);

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Long));

        var after = (await Registry.GetTableAsync(table.Id))!;
        Assert.Equal(PipelineStatus.Running, after.Status);
        Assert.Null(after.Error);
    }

    [Fact]
    public async Task ATableWhoseSqlNoLongerCompilesKeepsItsStoredSchemaRatherThanLosingIt()
    {
        // "Keep them running either way": the refresh may only ever improve what is stored. Emptying
        // OutputFields because an unrelated edit invalidated the query would take a live table's /proto
        // from stale to absent.
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"), Sql = $"SELECT symbol, price FROM {src}",
        });
        Assert.Equal(2, table.OutputFields.Count);

        // `price` disappears from the source, so the table's SELECT no longer resolves.
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));

        var after = (await Registry.GetTableAsync(table.Id))!;
        Assert.Equal(2, after.OutputFields.Count);
        Assert.Equal(table.SchemaRevision, after.SchemaRevision);
    }

    // =============================================================================================
    // StaleReason — set by the upstream change, cleared when the pin is re-satisfied.
    // =============================================================================================

    [Fact]
    public async Task AnUpstreamSchemaChangeSetsTheDependantsStaleReasonAndNamesWhatMoved()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"),
            Sql = $"SELECT symbol FROM {src}",
            DependsOn = [new EntityPin { Kind = "source", Name = src, SchemaRevision = 1 }],
        });
        Assert.Null(table.StaleReason);

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long));

        var stale = (await Registry.GetTableAsync(table.Id))!;
        Assert.NotNull(stale.StaleReason);
        Assert.Contains(src, stale.StaleReason);
        Assert.Equal(PipelineStatus.Stopped, stale.Status); // still saved, still startable — only badged
    }

    [Fact]
    public async Task ReSatisfyingThePinClearsTheStaleReason()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"),
            Sql = $"SELECT symbol FROM {src}",
            DependsOn = [new EntityPin { Kind = "source", Name = src, SchemaRevision = 1 }],
        });

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long));
        Assert.NotNull((await Registry.GetTableAsync(table.Id))!.StaleReason);

        var current = (await Registry.GetSourceAsync(src))!;
        var refreshed = (await Registry.GetTableAsync(table.Id))!;
        await Registry.UpdateTableAsync(new TableDefinition
        {
            Id = table.Id,
            Name = refreshed.Name,
            Sql = refreshed.Sql,
            DependsOn = [new EntityPin { Kind = "source", Name = src, SchemaRevision = current.SchemaRevision }],
        });

        Assert.Null((await Registry.GetTableAsync(table.Id))!.StaleReason);
    }

    [Fact]
    public async Task DeletingAPinnedSourceMakesTheDependantStale()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));

        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"),
            Sql = $"SELECT symbol FROM {src}",
            DependsOn = [new EntityPin { Kind = "source", Name = src, SchemaRevision = 1 }],
        });
        Assert.Null(table.StaleReason);

        await Registry.DeleteSourceAsync(src);

        var stale = (await Registry.GetTableAsync(table.Id))!;
        Assert.NotNull(stale.StaleReason);
        Assert.Contains(src, stale.StaleReason);
    }

    [Fact]
    public async Task AnEntityWithNoPinsIsNeverBadged()
    {
        var src = Unique("rev_src");
        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String));
        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = Unique("rev_tbl"), Sql = $"SELECT symbol FROM {src}",
        });

        await UpsertSourceAsync(src, new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long));

        Assert.Null((await Registry.GetTableAsync(table.Id))!.StaleReason);
    }
}
