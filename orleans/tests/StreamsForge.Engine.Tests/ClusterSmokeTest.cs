using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Engine.Tests;

/// <summary>Silo config mirroring the real Host (memory streams instead of the Host's memory-streams
/// provider is identical; memory grain storage instead of the Host's JSON-file storage — no files
/// touched by this test).</summary>
internal sealed class TestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

/// <summary>The test client needs its own memory-streams provider registration to resolve
/// IClusterClient.GetStreamProvider — it isn't inherited from the silo side.</summary>
internal sealed class TestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>End-to-end smoke test: a real Orleans TestingHost cluster, a real compiled
/// PipelineGrain subscribed to a real memory stream, fed real events from a stream-provider
/// client — verifying the Phase B wiring (RegistryGrain → PipelineGrain → Engine → streams)
/// works together, not just each piece in isolation.</summary>
public sealed class ClusterSmokeTest : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<TestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<TestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task PipelineGrain_SubscribesAndEmitsRowsFromRealStream()
    {
        // Arrange the "trades" source schema directly (skip EnsureInitializedAsync's full demo
        // seed — that would also spin up synthetic generators and two other demo pipelines,
        // which would race noisy unrelated rows into this test).
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "trades",
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false, // no synthetic generator noise
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });

        var pipelineId = Guid.NewGuid().ToString("n");
        var def = new PipelineDefinition
        {
            Id = pipelineId,
            Name = "passthrough",
            Description = "smoke test",
            Sql = "SELECT symbol, price FROM trades",
            Status = PipelineStatus.Stopped,
        };

        var pipeline = _cluster.GrainFactory.GetGrain<IPipelineGrain>(pipelineId);
        await pipeline.StartAsync(def);

        // Act: publish 3 hand-made events onto the "trades" source stream from the test client.
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, "trades"));

        var symbols = new[] { "AAPL", "MSFT", "GOOG" };
        for (var i = 0; i < symbols.Length; i++)
        {
            await stream.OnNextAsync(new EventRecord
            {
                [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                [EventRecord.SourceField] = "trades",
                ["symbol"] = symbols[i],
                ["price"] = 100.0 + i,
                ["qty"] = 10L,
            });
        }

        // Assert: poll GetRecentResultsAsync until all 3 rows show up, or time out.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<ResultEnvelope> results = [];
        while (DateTime.UtcNow < deadline)
        {
            results = await pipeline.GetRecentResultsAsync(10);
            if (results.Count >= 3)
            {
                break;
            }

            await Task.Delay(200);
        }

        Assert.True(results.Count >= 3, $"Expected at least 3 result rows within 10s, got {results.Count}.");
        foreach (var symbol in symbols)
        {
            Assert.Contains(results, r => Equals(r.Row.GetValueOrDefault("symbol"), symbol) && r.Row.ContainsKey("price"));
        }

        await pipeline.StopAsync();
    }
}
