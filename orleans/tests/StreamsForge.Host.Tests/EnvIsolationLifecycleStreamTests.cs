using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

internal sealed class EnvLifecycleSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class EnvLifecycleClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 021 D6 — the lifecycle stream (<c>RegistryGrain.PublishLifecycleAsync</c>) becomes one stream PER
/// ENVIRONMENT, keyed <c>EnvKeys.Qualify(env, StreamConstants.LifecycleEventsKey)</c>: a subscriber on the
/// DEFAULT environment's stream must see NOTHING when an entity is created in a different environment, and
/// must still see its own environment's events exactly as it always did (the D2 byte-identical half of the
/// same guarantee).
/// </summary>
public sealed class EnvIsolationLifecycleStreamTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<EnvLifecycleSiloConfigurator>();
        builder.AddClientBuilderConfigurator<EnvLifecycleClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static string Unique(string prefix) => prefix + "-" + Guid.NewGuid().ToString("n")[..8];

    [Fact]
    public async Task A_default_environment_subscriber_never_sees_a_staging_environments_events()
    {
        var staging = Unique("staging");
        await _cluster.GrainFactory.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey)
            .CreateAsync(staging, "", "tester");

        var received = new List<LifecycleEvent>();
        var defaultStream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<LifecycleEvent>(StreamId.Create(
                StreamConstants.LifecycleNamespace, EnvKeys.Qualify(EnvKeys.Default, StreamConstants.LifecycleEventsKey)));
        await defaultStream.SubscribeAsync((evt, _) =>
        {
            received.Add(evt);
            return Task.CompletedTask;
        });

        // Create a pipeline in `staging` — RegistryGrain.CreatePipelineAsync publishes a "created" lifecycle
        // event on ITS OWN (staging's) qualified stream, not the default one.
        var stagingRegistry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(staging, StreamConstants.RegistryKey));
        await stagingRegistry.CreatePipelineAsync(new PipelineDefinition { Name = Unique("pipe"), Sql = "SELECT 1" });

        // Give the memory-stream pulling agent a moment to have delivered anything there was to deliver.
        await Task.Delay(500);
        Assert.Empty(received);

        // Sanity: the SAME subscription DOES see an event when something happens in `default` — proves the
        // subscription itself is live and the stream identity match is the reason staging's event didn't
        // arrive, not a broken test.
        var defaultRegistry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await defaultRegistry.CreatePipelineAsync(new PipelineDefinition { Name = Unique("pipe"), Sql = "SELECT 1" });

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (received.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
        }
        Assert.Single(received);
        Assert.Equal("created", received[0].Kind);
    }
}
