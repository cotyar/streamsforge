using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Host.Streaming;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Silo config for the PUSH transport: same shape as the other cluster tests' configurators, with
/// <c>AddPushStreams</c> in place of <c>AddMemoryStreams</c> under the identical provider name — exactly
/// what Program.cs does when <c>Streams:Transport=push</c>. No client-side stream provider is registered:
/// a TestCluster's client has its OWN service container, so it cannot see the silo's in-process bus (in
/// the real co-hosted Host they share one container — see PushStreamHostingExtensions' doc). Everything
/// this fixture exercises therefore flows grain → bus → grain, which is the path that matters.
/// </summary>
internal sealed class PushStreamTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddPushStreams(StreamConstants.ProviderName, 10_000);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

/// <summary>
/// End-to-end cluster coverage of the push transport with the REAL grains and the REAL, unmodified
/// producer/consumer code (GeneratorGrain publishing onto ("sources", name); PipelineGrain subscribing to
/// it and running the compiled SQL). Proves the two properties a unit test on the bus cannot: that a
/// grain's subscription callback is actually reached (i.e. the grain-extension turn hop works), and that
/// unsubscribing on StopAsync really detaches.
/// </summary>
public sealed class PushStreamClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<PushStreamTestSiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private async Task<string> SeedRunningSourceAsync(int eventsPerSecond = 50)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "trades_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "push transport test source",
            GeneratorProfile = "trades",
            EventsPerSecond = eventsPerSecond,
            Enabled = true, // UpsertSourceAsync starts the IGeneratorGrain for an enabled generator source
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });
        return sourceName;
    }

    private async Task<IPipelineGrain> StartPipelineAsync(string sourceName)
    {
        var id = Guid.NewGuid().ToString("n");
        var pipeline = _cluster.GrainFactory.GetGrain<IPipelineGrain>(id);
        await pipeline.StartAsync(new PipelineDefinition
        {
            Id = id,
            Name = "push-passthrough",
            Description = "push transport test",
            Sql = $"SELECT symbol, price FROM {sourceName}",
            Status = PipelineStatus.Stopped,
        });
        return pipeline;
    }

    private static async Task<int> WaitForResultsAsync(IPipelineGrain pipeline, int atLeast, int timeoutMs = 20_000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        var count = 0;
        while (DateTime.UtcNow < deadline)
        {
            count = (await pipeline.GetRecentResultsAsync(100)).Count;
            if (count >= atLeast) return count;
            await Task.Delay(100);
        }
        return count;
    }

    /// <summary>The core claim: with no pulling agents anywhere, a generator grain's published events still
    /// reach a subscribing pipeline grain — and reach TWO of them (fan-out on the same stream key).</summary>
    [Fact]
    public async Task PushTransport_DeliversSourceEventsToEverySubscribingGrain()
    {
        var sourceName = await SeedRunningSourceAsync();
        var a = await StartPipelineAsync(sourceName);
        var b = await StartPipelineAsync(sourceName);

        var countA = await WaitForResultsAsync(a, 5);
        var countB = await WaitForResultsAsync(b, 5);

        Assert.True(countA >= 5, $"pipeline A saw only {countA} rows over the push transport");
        Assert.True(countB >= 5, $"pipeline B saw only {countB} rows over the push transport");

        var rows = await a.GetRecentResultsAsync(5);
        Assert.All(rows, r => Assert.True(r.Row.ContainsKey("symbol") && r.Row.ContainsKey("price")));
    }

    /// <summary>StopAsync unsubscribes through PushStreamSubscriptionHandle — after it, the generator keeps
    /// publishing but this pipeline must stop accumulating.</summary>
    [Fact]
    public async Task PushTransport_StopUnsubscribesFromTheStream()
    {
        var sourceName = await SeedRunningSourceAsync();
        var pipeline = await StartPipelineAsync(sourceName);

        Assert.True(await WaitForResultsAsync(pipeline, 5) >= 5, "pipeline never received rows to begin with");

        await pipeline.StopAsync();
        var afterStop = (await pipeline.GetRecentResultsAsync(100)).Count;

        await Task.Delay(1_000); // ~50 more generated events would land here if the unsubscribe leaked
        Assert.Equal(afterStop, (await pipeline.GetRecentResultsAsync(100)).Count);
    }
}
