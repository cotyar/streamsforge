using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Generators;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Exercise the real idle collector without supervisor pings masking source lifetime bugs.</summary>
public sealed class SourceIdleCollectionTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task Stopped_generator_and_connector_remain_collectible()
    {
        var generator = _cluster.GrainFactory.GetGrain<IGeneratorGrain>("stopped_generator_" + Guid.NewGuid().ToString("n"));
        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>("stopped_connector_" + Guid.NewGuid().ToString("n"));
        await generator.StartAsync(new SourceDefinition { Name = "idle", EventsPerSecond = 0 });
        await generator.StopAsync();
        await connector.StartAsync(new SourceDefinition { Name = "idle", Kind = SourceKinds.File });
        await connector.StopAsync();
        await CollectIdleAsync();
        var management = _cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        Assert.Equal(0, await management.GetGrainActivationCount((GrainReference)generator));
        Assert.Equal(0, await management.GetGrainActivationCount((GrainReference)connector));
    }

    [Fact]
    public async Task Idle_loopback_generator_delivers_new_data_without_restart()
    {
        var name = "idle_generator_" + Guid.NewGuid().ToString("n");
        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);
        var delivered = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handle = await SourceStream(name).SubscribeAsync((row, _) =>
        {
            delivered.TrySetResult(row);
            return Task.CompletedTask;
        });
        try
        {
            await grain.StartAsync(new SourceDefinition { Name = name, EventsPerSecond = 0, Enabled = true });
            await CollectIdleAsync();
            Assert.True(LoopbackHub.TryPublish(name, new() { ["value"] = 42L }));
            var row = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(42L, Convert.ToInt64(row["value"]));
        }
        finally
        {
            await grain.StopAsync();
            await handle.UnsubscribeAsync();
        }
    }

    [Fact]
    public async Task Idle_file_connector_observes_new_file_data_without_status_poll_or_restart()
    {
        var name = "idle_connector_" + Guid.NewGuid().ToString("n");
        var path = Path.Combine(Path.GetTempPath(), name + ".ndjson");
        await File.WriteAllTextAsync(path, "");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var delivered = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handle = await SourceStream(name).SubscribeAsync((row, _) =>
        {
            delivered.TrySetResult(row);
            return Task.CompletedTask;
        });
        try
        {
            await grain.StartAsync(new SourceDefinition
            {
                Name = name, Kind = SourceKinds.File, Enabled = true,
                Fields = [new FieldDef("value", FieldType.Long)],
                Connector = new ConnectorConfig
                {
                    Schedule = new ScheduleSpec { IntervalMs = 1000 },
                    File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
                    Mapping = new MappingSpec { ItemsPath = "$", Fields = [new FieldMapEntry { Field = new FieldDef("value", FieldType.Long) }] },
                },
            });
            await CollectIdleAsync();
            await File.WriteAllTextAsync(path, "{\"value\":42}\n");
            var row = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(42L, Convert.ToInt64(row["value"]));
        }
        finally
        {
            await grain.StopAsync();
            await handle.UnsubscribeAsync();
            File.Delete(path);
        }
    }

    private IAsyncStream<EventRecord> SourceStream(string name) =>
        _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

    private async Task CollectIdleAsync()
    {
        await Task.Delay(1000);
        await _cluster.GrainFactory.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);
    }
}
