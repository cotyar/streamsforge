using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Host.Streaming;
using Xunit;

namespace StreamsForge.Host.Tests;

internal sealed class IdleConsumerSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddPushStreams(StreamConstants.ProviderName, 10_000);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.StorageName);
        siloBuilder.Configure<GrainCollectionOptions>(options =>
        {
            options.CollectionQuantum = TimeSpan.FromSeconds(1);
            options.CollectionAge = TimeSpan.FromSeconds(2);
        });
    }
}

internal sealed class IdlePullConsumerSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName, streams =>
            streams.ConfigurePullingAgent(options => options.Configure(agent =>
            {
                agent.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(20);
                agent.StreamInactivityPeriod = TimeSpan.FromSeconds(2);
            })));
        siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.StorageName);
        siloBuilder.Configure<GrainCollectionOptions>(options =>
        {
            options.CollectionQuantum = TimeSpan.FromSeconds(1);
            options.CollectionAge = TimeSpan.FromSeconds(2);
        });
    }
}

/// <summary>Neither REST-style reads nor incoming calls keep the consumers busy during the idle
/// interval. The cluster has no supervisor: running consumers must preserve their own lifetime.</summary>
public sealed class IdleConsumerClusterTests
{
    [Theory]
    [InlineData(true, 1, TablePersistenceMode.Batched)]
    [InlineData(true, 1, TablePersistenceMode.MemoryOnly)]
    [InlineData(true, 2, TablePersistenceMode.Batched)]
    [InlineData(false, 1, TablePersistenceMode.Batched)]
    [InlineData(false, 1, TablePersistenceMode.MemoryOnly)]
    [InlineData(false, 2, TablePersistenceMode.Batched)]
    public async Task Idle_consumers_keep_readable_rows_and_process_new_data_without_restart(
        bool push, int parallelism, TablePersistenceMode persistence)
    {
        var builder = new TestClusterBuilder(1);
        if (push) builder.AddSiloBuilderConfigurator<IdleConsumerSiloConfigurator>();
        else builder.AddSiloBuilderConfigurator<IdlePullConsumerSiloConfigurator>();
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        var registry = cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "idle_input", Kind = SourceKinds.Ingest, Enabled = true,
            Fields = [new FieldDef("id", FieldType.Long)],
        });
        var pipeline = await registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "idle_pipeline", Sql = "SELECT id FROM idle_input",
        });
        await registry.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);
        var table = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "idle_table", Sql = "SELECT id FROM idle_pipeline LATEST BY (id)",
            Parallelism = parallelism, Persistence = persistence,
        });
        await registry.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        var producer = cluster.GrainFactory.GetGrain<IIngestSourceGrain>("idle_input");
        var consumer = cluster.GrainFactory.GetGrain<IPipelineGrain>(pipeline.Id);
        var materialized = cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await producer.PublishAsync([Row(1)]);
        await WaitForRowsAsync(materialized, 1);

        // Several complete collection cycles, without polling either consumer.
        await Task.Delay(TimeSpan.FromSeconds(8));
        Assert.Single(await consumer.GetRecentResultsAsync(100));
        Assert.Single(await materialized.GetRowsAsync(100, 0));
        Assert.Equal(PipelineStatus.Running, (await consumer.GetMetricsAsync()).Status);

        // An ingest producer can itself have been collected; a new publish must wake it and
        // still reach both consumers. No Start/Stop calls occur after setup.
        await producer.PublishAsync([Row(2)]);
        await WaitForRowsAsync(materialized, 2);
        Assert.Equal(2, (await consumer.GetRecentResultsAsync(100)).Count);
        Assert.Equal(new long[] { 1, 2 }, (await materialized.GetRowsAsync(100, 0))
            .Select(row => Convert.ToInt64(row.Row["id"])).Order());
    }

    private static Dictionary<string, object?> Row(long id) => new()
    {
        ["id"] = id, ["_source"] = "idle_input",
        ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    };

    private static async Task WaitForRowsAsync(ITableGrain table, int count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (await table.GetRowCountAsync() != count && DateTime.UtcNow < deadline)
            await Task.Delay(100);
        Assert.Equal(count, await table.GetRowCountAsync());
    }
}
