using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring StreamsForge.Engine.Tests' TestSiloConfigurator (memory streams + memory
/// grain storage) — duplicated here since it's a different test assembly than Engine.Tests.</summary>
internal sealed class MetadataTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class MetadataTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Feature A (metadata) round-trip tests against a real RegistryGrain in a real Orleans
/// TestingHost cluster (mirrors StreamsForge.Engine.Tests/ClusterSmokeTest.cs's pattern) — proves Tags +
/// Metadata survive every registry code path: source upsert (create AND update), pipeline create +
/// update, and table create + update.</summary>
public sealed class MetadataRegistryRoundTripTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private IRegistryGrain _registry = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<MetadataTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<MetadataTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Source_TagsAndMetadata_SurviveUpsertCreateAndUpdate()
    {
        var name = "meta_src_" + Guid.NewGuid().ToString("n")[..8];

        await _registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Description = "test",
            EventsPerSecond = 1,
            Enabled = false,
            Fields = [new FieldDef("x", FieldType.String)],
            Tags = ["risk", "demo"],
            Metadata = new Dictionary<string, string> { ["owner"] = "desk-a" },
        });

        var afterCreate = await _registry.GetSourceAsync(name);
        Assert.NotNull(afterCreate);
        Assert.Equal(["risk", "demo"], afterCreate!.Tags);
        Assert.Equal("desk-a", afterCreate.Metadata["owner"]);

        // Full-replace update (matches the real PUT /api/sources/{name} semantics) with different tags.
        afterCreate.Tags = ["risk"];
        afterCreate.Metadata["reviewed"] = "true";
        await _registry.UpsertSourceAsync(afterCreate);

        var afterUpdate = await _registry.GetSourceAsync(name);
        Assert.Equal(["risk"], afterUpdate!.Tags);
        Assert.Equal("desk-a", afterUpdate.Metadata["owner"]);
        Assert.Equal("true", afterUpdate.Metadata["reviewed"]);
    }

    [Fact]
    public async Task Pipeline_TagsAndMetadata_SurviveCreateAndUpdate()
    {
        var created = await _registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "meta_pipeline_" + Guid.NewGuid().ToString("n")[..8],
            Description = "test",
            Sql = "SELECT symbol FROM trades",
            Tags = ["risk", "demo"],
            Metadata = new Dictionary<string, string> { ["owner"] = "desk-a" },
        });

        Assert.Equal(["risk", "demo"], created.Tags);
        Assert.Equal("desk-a", created.Metadata["owner"]);

        created.Tags = ["demo"];
        created.Metadata["stage"] = "prod";
        var updated = await _registry.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Equal(["demo"], updated!.Tags);
        Assert.Equal("desk-a", updated.Metadata["owner"]);
        Assert.Equal("prod", updated.Metadata["stage"]);

        // Re-fetch independently to confirm it's actually persisted in registry state, not just the
        // mutated in-memory reference returned by UpdatePipelineAsync.
        var refetched = await _registry.GetPipelineAsync(created.Id);
        Assert.Equal(["demo"], refetched!.Tags);
        Assert.Equal("prod", refetched.Metadata["stage"]);
    }

    [Fact]
    public async Task Table_TagsAndMetadata_SurviveCreateAndUpdate()
    {
        var created = await _registry.CreateTableAsync(new TableDefinition
        {
            Name = "meta_table_" + Guid.NewGuid().ToString("n")[..8],
            Description = "test",
            Sql = "SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol",
            Tags = ["risk", "demo"],
            Metadata = new Dictionary<string, string> { ["owner"] = "desk-a" },
        });

        Assert.Equal(["risk", "demo"], created.Tags);
        Assert.Equal("desk-a", created.Metadata["owner"]);

        created.Tags = ["demo"];
        created.Metadata["stage"] = "prod";
        var updated = await _registry.UpdateTableAsync(created);

        Assert.NotNull(updated);
        Assert.Equal(["demo"], updated!.Tags);
        Assert.Equal("desk-a", updated.Metadata["owner"]);
        Assert.Equal("prod", updated.Metadata["stage"]);

        var refetched = await _registry.GetTableAsync(created.Id);
        Assert.Equal(["demo"], refetched!.Tags);
        Assert.Equal("prod", refetched.Metadata["stage"]);
    }
}
