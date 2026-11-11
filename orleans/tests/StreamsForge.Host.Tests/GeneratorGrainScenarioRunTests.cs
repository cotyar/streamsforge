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

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring ConnectorGrainClusterTests'/ClusterSmokeTest's own configurator (memory
/// streams + memory grain storage) — duplicated here rather than shared, same reasoning as those files'
/// own comment: xunit test classes shouldn't share cluster state.</summary>
internal sealed class GeneratorRunTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class GeneratorRunTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Wishlist #8: cluster-level coverage of IGeneratorGrain.RunAsync — the real grain, the real
/// Orleans memory stream, a real client-side subscriber, proving RunAsync both RETURNS the deterministic
/// batch (unit-tested in isolation by ScenarioGeneratorTests) AND PUBLISHES it onto the exact same
/// (StreamConstants.SourcesNamespace, source name) stream a tick would — the property a pure unit test on
/// ScenarioGenerator alone cannot prove.</summary>
public sealed class GeneratorGrainScenarioRunTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<GeneratorRunTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<GeneratorRunTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition ScenarioSource(string name, ScenarioSpec spec) => new()
    {
        Name = name,
        Description = "scenario run test source",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0, // wishlist: ignored/0 for this kind — rows only ever come from RunAsync
        Enabled = true,
        Scenario = spec,
    };

    private static ScenarioSpec SmallSpec(int paths = 4, int days = 2) => new()
    {
        Paths = paths,
        Days = days,
        Rho = 0.5,
        Seed = 7,
        Distribution = new ScenarioDistributionSpec { Kind = "normal" },
        Instruments =
        [
            new ScenarioInstrumentSpec { Id = "AAPL", Base = 100, Vol = 1, Group = "tech" },
            new ScenarioInstrumentSpec { Id = "MSFT", Base = 200, Vol = 2, Group = "tech" },
        ],
    };

    [Fact]
    public async Task RunAsync_publishes_the_whole_deterministic_batch_onto_the_sources_real_stream()
    {
        var name = "scenario_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        // UpsertSourceAsync starts the IGeneratorGrain for an enabled generator-kind source (same
        // dispatch every other generator-kind source goes through — see PushStreamClusterTests'
        // identical precedent for "trades"), so this activation already has _def cached before RunAsync
        // is ever called, exactly like it would after a real catalog create.
        await registry.UpsertSourceAsync(ScenarioSource(name, SmallSpec(paths: 4, days: 2)));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);
        var result = await grain.RunAsync(new ScenarioRunRequest { RunId = "cluster-run-1", Seed = 123 });

        Assert.Equal(ScenarioRunOutcome.Accepted, result.Outcome);
        Assert.Equal(4 * 2 * 2, result.Accepted); // paths * instruments * days
        Assert.Equal(result.Accepted, result.Rows.Count);

        var publishedCount = await PollUntilAsync(
            () => Task.FromResult(Count(received)),
            count => count >= result.Accepted,
            deadlineSeconds: 15);
        Assert.True(publishedCount >= result.Accepted, $"expected >= {result.Accepted} published events, got {publishedCount}");

        List<EventRecord> snapshot;
        lock (received) snapshot = [.. received];

        Assert.All(snapshot, evt => Assert.Equal(name, evt.Source));
        Assert.All(snapshot, evt => Assert.Equal("cluster-run-1", evt["run_id"]));
        // Every returned row must also have been published, with the identical field values — proves
        // RunAsync's return value and its stream side-effect describe the SAME batch, not two different
        // computations that happen to agree on row count.
        foreach (var row in result.Rows)
        {
            Assert.Contains(snapshot, evt =>
                Equals(evt["path_id"], row.PathId) &&
                Equals(evt["instrument_id"], row.InstrumentId) &&
                Equals(evt["day"], row.Day) &&
                Equals(evt["value"], row.Value));
        }
    }

    [Fact]
    public async Task RunAsync_on_an_activation_that_was_never_started_returns_NotFound_without_throwing()
    {
        var name = "scenario_unstarted_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name); // never StartAsync'd

        var result = await grain.RunAsync(new ScenarioRunRequest { RunId = "r" });

        Assert.Equal(ScenarioRunOutcome.NotFound, result.Outcome);
        Assert.Empty(result.Rows);
    }

    [Fact]
    public async Task RunAsync_with_a_spec_that_exceeds_MaxBatchRows_returns_ValidationError_and_publishes_nothing()
    {
        var name = "scenario_toolarge_" + Guid.NewGuid().ToString("n")[..8];
        var spec = SmallSpec(paths: 100, days: 1); // 100*2*1 = 200 rows
        spec.MaxBatchRows = 10;
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(ScenarioSource(name, spec));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);
        var result = await grain.RunAsync(new ScenarioRunRequest { RunId = "r" });

        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.NotEmpty(result.Errors);
        Assert.Empty(result.Rows);

        // Give any (wrongly) published events a moment to arrive, then confirm none did.
        await Task.Delay(300);
        lock (received) Assert.Empty(received);
    }

    private static int Count(List<EventRecord> list)
    {
        lock (list) return list.Count;
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var value = await poll();
        while (!until(value) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            value = await poll();
        }

        return value;
    }
}
