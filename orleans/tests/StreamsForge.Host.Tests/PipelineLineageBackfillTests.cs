using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Storage;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config using the REAL JsonFileGrainStorage (not memory) — same
/// StreamConstants.StorageName wiring Program.cs uses — so a catalog can be persisted to an on-disk data
/// dir, hand-edited between two separate cluster lifetimes, and re-read on the second cluster's first
/// RegistryGrain activation. Mirrors TablePersistenceModeClusterTests' PersistenceModeTestSiloConfigurator
/// but without its DelayableGrainStorage wrapper — this test only needs real files on disk, not
/// write-timing control.</summary>
internal sealed class BackfillTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class BackfillTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 011 Wave A: the backfill in RegistryGrain.EnsureInitializedAsync must repair pipelines that were
/// durably persisted with an empty SourceNames BEFORE this backfill existed — not just freshly-seeded
/// ones (SeededPipelineLineageTests covers that path). "Existing install with SourceNames: [] on disk" is
/// reproduced literally here: a first cluster seeds+persists a real catalog to a temp data dir, the
/// persisted JSON file is then hand-edited to blank out one pipeline's SourceNames (simulating the
/// pre-Wave-A shape that pipeline would actually have shipped with), and a SECOND cluster — a fresh
/// RegistryGrain activation reading that same data dir — must repair it via EnsureInitializedAsync without
/// re-seeding (the persisted Pipelines list is already non-empty, so the seed block itself is a no-op;
/// only the new backfill loop, which is driven off SourceNames.Count == 0 rather than "did we just seed",
/// can fix it).
/// </summary>
public sealed class PipelineLineageBackfillTests
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    [Fact]
    public async Task EnsureInitializedAsync_RestoredCatalogWithEmptySourceNames_RepairsOnNextActivation()
    {
        var dataDir = Path.Combine(Path.GetTempPath(), "sf-lineage-backfill-tests", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataDir);

        try
        {
            string pipelineId;

            // Phase 1: seed a real catalog to disk via a first cluster/activation.
            {
                var builder = new TestClusterBuilder(1);
                builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DataDir"] = dataDir,
                }));
                builder.AddSiloBuilderConfigurator<BackfillTestSiloConfigurator>();
                builder.AddClientBuilderConfigurator<BackfillTestClientConfigurator>();
                var cluster = builder.Build();
                await cluster.DeployAsync();

                var registry = cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
                await registry.EnsureInitializedAsync();

                var pipelines = await registry.GetPipelinesAsync();
                var orderBursts = pipelines.Single(p => p.Name == "Order bursts (session)");
                // Sanity: the seed-time backfill already populated this — we're about to undo that on disk
                // to reproduce a pre-Wave-A persisted shape.
                Assert.Equal(["orders"], orderBursts.SourceNames);
                pipelineId = orderBursts.Id;

                await cluster.DisposeAsync();
            }

            // Hand-edit the persisted catalog file: blank out the one pipeline's SourceNames, exactly the
            // shape every pipeline had before this backfill existed.
            var stateDir = Path.Combine(dataDir, "state");
            var catalogFile = Directory.GetFiles(stateDir, "catalog.*.json").Single();
            var state = JsonSerializer.Deserialize<RegistryState>(File.ReadAllText(catalogFile))!;
            var toBreak = state.Pipelines.Single(p => p.Id == pipelineId);
            Assert.NotEmpty(toBreak.SourceNames); // sanity before mutating
            toBreak.SourceNames = [];
            await File.WriteAllTextAsync(catalogFile, JsonSerializer.Serialize(state, JsonOptions));

            // Phase 2: a fresh cluster (fresh RegistryGrain activation) pointed at the SAME data dir must
            // read the corrupted-on-disk catalog and repair it, without re-seeding.
            {
                var builder = new TestClusterBuilder(1);
                builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DataDir"] = dataDir,
                }));
                builder.AddSiloBuilderConfigurator<BackfillTestSiloConfigurator>();
                builder.AddClientBuilderConfigurator<BackfillTestClientConfigurator>();
                var cluster = builder.Build();
                await cluster.DeployAsync();

                var registry = cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
                await registry.EnsureInitializedAsync();

                var pipelines = await registry.GetPipelinesAsync();
                Assert.Equal(7, pipelines.Count); // no re-seed happened — still exactly the original seed set
                var repaired = pipelines.Single(p => p.Id == pipelineId);
                Assert.Equal(["orders"], repaired.SourceNames);

                // Independently re-read from a second grain call to confirm it's real persisted state, not
                // just an in-memory artifact of the same EnsureInitializedAsync call.
                var refetched = await registry.GetPipelineAsync(pipelineId);
                Assert.Equal(["orders"], refetched!.SourceNames);

                await cluster.DisposeAsync();
            }
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Table-over-pipeline: the SAME boot backfill, driven by the other half of its predicate.
    /// <c>PipelineDefinition.OutputFields</c> is newer than every durably persisted pipeline record, so a
    /// restored catalog carries it EMPTY — and a pipeline with no output schema offers no relation, which
    /// would make every table reading it fail to compile at boot. This reproduces that shape on disk
    /// (SourceNames left INTACT, so only the new disjunct can repair it) and proves the next activation
    /// fills it back in without re-seeding.</summary>
    [Fact]
    public async Task EnsureInitializedAsync_RestoredCatalogWithEmptyOutputFields_RepairsOnNextActivation()
    {
        var dataDir = Path.Combine(Path.GetTempPath(), "sf-lineage-backfill-tests", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataDir);

        try
        {
            string pipelineId;

            {
                var builder = new TestClusterBuilder(1);
                builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DataDir"] = dataDir,
                }));
                builder.AddSiloBuilderConfigurator<BackfillTestSiloConfigurator>();
                builder.AddClientBuilderConfigurator<BackfillTestClientConfigurator>();
                var cluster = builder.Build();
                await cluster.DeployAsync();

                var registry = cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
                await registry.EnsureInitializedAsync();

                var orderBursts = (await registry.GetPipelinesAsync()).Single(p => p.Name == "Order bursts (session)");
                Assert.NotEmpty(orderBursts.OutputFields);
                pipelineId = orderBursts.Id;

                await cluster.DisposeAsync();
            }

            var stateDir = Path.Combine(dataDir, "state");
            var catalogFile = Directory.GetFiles(stateDir, "catalog.*.json").Single();
            var state = JsonSerializer.Deserialize<RegistryState>(File.ReadAllText(catalogFile))!;
            var toBreak = state.Pipelines.Single(p => p.Id == pipelineId);
            toBreak.OutputFields = [];
            Assert.NotEmpty(toBreak.SourceNames); // the OTHER disjunct must NOT be what repairs this
            await File.WriteAllTextAsync(catalogFile, JsonSerializer.Serialize(state, JsonOptions));

            {
                var builder = new TestClusterBuilder(1);
                builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DataDir"] = dataDir,
                }));
                builder.AddSiloBuilderConfigurator<BackfillTestSiloConfigurator>();
                builder.AddClientBuilderConfigurator<BackfillTestClientConfigurator>();
                var cluster = builder.Build();
                await cluster.DeployAsync();

                var registry = cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
                await registry.EnsureInitializedAsync();

                var pipelines = await registry.GetPipelinesAsync();
                Assert.Equal(7, pipelines.Count); // no re-seed happened
                Assert.NotEmpty(pipelines.Single(p => p.Id == pipelineId).OutputFields);

                var refetched = await registry.GetPipelineAsync(pipelineId);
                Assert.NotEmpty(refetched!.OutputFields);

                await cluster.DisposeAsync();
            }
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
