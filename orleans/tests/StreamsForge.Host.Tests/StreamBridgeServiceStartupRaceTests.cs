using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Api.Hubs;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Services;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring LifecycleSeedClusterTests' configurator (memory streams + memory grain
/// storage) — duplicated here (not shared) since xunit test classes shouldn't share cluster state.</summary>
internal sealed class BridgeRaceSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class BridgeRaceClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>A no-op <see cref="IHostApplicationLifetime"/> whose ApplicationStarted token is already
/// signaled — makes StartupSignal.WaitForApplicationStartedAsync return immediately, the same as it
/// would for real once the host has actually started.</summary>
internal sealed class AlreadyStartedLifetime : IHostApplicationLifetime
{
    public CancellationToken ApplicationStarted { get; } = new(canceled: true);
    public CancellationToken ApplicationStopping { get; } = CancellationToken.None;
    public CancellationToken ApplicationStopped { get; } = CancellationToken.None;
    public void StopApplication() { }
}

/// <summary>Captures every SignalR hub invocation StreamBridgeService makes so tests can assert on
/// method name + arguments without a real SignalR connection.</summary>
internal sealed class RecordingClientProxy(List<(string Method, object?[] Args)> sink) : IClientProxy
{
    public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default)
    {
        lock (sink) sink.Add((method, args));
        return Task.CompletedTask;
    }
}

internal sealed class RecordingHubClients(List<(string Method, object?[] Args)> sink) : IHubClients
{
    private readonly IClientProxy _proxy = new RecordingClientProxy(sink);
    public IClientProxy All => _proxy;
    public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => _proxy;
    public IClientProxy Client(string connectionId) => _proxy;
    public IClientProxy Clients(IReadOnlyList<string> connectionIds) => _proxy;
    public IClientProxy Group(string groupName) => _proxy;
    public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => _proxy;
    public IClientProxy Groups(IReadOnlyList<string> groupNames) => _proxy;
    public IClientProxy User(string userId) => _proxy;
    public IClientProxy Users(IReadOnlyList<string> userIds) => _proxy;
}

internal sealed class NoopGroupManager : IGroupManager
{
    public Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default) => Task.CompletedTask;
}

internal sealed class RecordingHubContext(List<(string Method, object?[] Args)> sink) : IHubContext<StreamHub>
{
    public IHubClients Clients { get; } = new RecordingHubClients(sink);
    public IGroupManager Groups { get; } = new NoopGroupManager();
}

/// <summary>Regression coverage for the Orleans-flavor SignalR relay bug: on boot, Program.cs kicks off
/// registry seeding (RegistryGrain.EnsureInitializedAsync, which also starts every already-"Running"
/// pipeline/table grain) from a fire-and-forget ApplicationStarted callback — a callback that races
/// StreamBridgeService.ExecuteAsync's own one-time enumeration of Running pipelines/tables. Unlike
/// source subscriptions (refreshed every 30s, so a lost race self-heals), pipeline/table subscriptions
/// are only ever established once at that startup enumeration — so if the registry hadn't been seeded
/// yet when the bridge asked "which pipelines/tables are Running?", it saw an empty catalog and
/// permanently never subscribed to those streams, even though the grains (once someone else eventually
/// seeded them) went on producing tableDelta/pipelineResult forever after. This test reproduces exactly
/// that ordering — nothing seeds the registry before StreamBridgeService starts — which is precisely
/// what a fresh boot looks like when the race is lost. The fix makes StreamBridgeService await
/// registry.EnsureInitializedAsync() itself before enumerating, so it no longer depends on winning (or
/// losing) that race with Program.cs.</summary>
public sealed class StreamBridgeServiceStartupRaceTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<BridgeRaceSiloConfigurator>();
        builder.AddClientBuilderConfigurator<BridgeRaceClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task TableDelta_and_pipelineResult_relay_even_when_bridge_starts_before_anything_seeds_the_registry()
    {
        var sink = new List<(string Method, object?[] Args)>();
        var hub = new RecordingHubContext(sink);
        var lifetime = new AlreadyStartedLifetime();

        // Nothing has called RegistryGrain.EnsureInitializedAsync yet — this is the losing side of the
        // real race: StreamBridgeService is the FIRST thing to touch the registry, exactly like it would
        // be if Program.cs's fire-and-forget InitializeGrainsAsync hadn't won the race yet.
        IHostedService bridge = new StreamBridgeService(_cluster.Client, hub, lifetime);
        await bridge.StartAsync(CancellationToken.None);
        try
        {
            // Give the bridge's ExecuteAsync a moment to run its startup enumeration and (post-fix)
            // its own EnsureInitializedAsync call, then confirm the seeded catalog really did activate —
            // the "positions"-equivalent seeded table ("order_states") and a seeded Running pipeline
            // ("fill-rate-5s") should both be Running by then.
            var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
            var tables = await PollUntilAsync(
                () => registry.GetTablesAsync(),
                ts => ts.Any(t => t.Name == "order_states" && t.Status == PipelineStatus.Running),
                deadlineSeconds: 15);
            Assert.Contains(tables, t => t.Name == "order_states" && t.Status == PipelineStatus.Running);

            // Publish a delta directly onto the table's delta stream — the same stream TableGrain
            // publishes onto in ApplyAndPublishAsync — and confirm the bridge relays it as "tableDelta".
            // (Waiting on the real generator would work too, but is slower and less deterministic.)
            var deltaStream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<List<TableDeltaDto>>(Orleans.Runtime.StreamId.Create(StreamConstants.TableDeltaNamespace, "order_states"));
            var probe = new TableDeltaDto { Row = new Dictionary<string, object?> { ["probe"] = true }, Weight = 1 };
            await deltaStream.OnNextAsync([probe]);

            var gotTableDelta = await PollUntilAsync(
                () => Task.FromResult(sink.Any(s => s.Method == "tableDelta" && (string)s.Args[0]! == "order_states")),
                found => found,
                deadlineSeconds: 15);
            Assert.True(gotTableDelta, "tableDelta was never relayed to the hub — the bridge never subscribed to the table's delta stream.");

            // Same check for pipelineResult on the seeded Running "fill-rate-5s" pipeline.
            var pipelines = await registry.GetPipelinesAsync();
            var fillRate = pipelines.Single(p => p.Name == "fill-rate-5s");
            var resultStream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<List<ResultEnvelope>>(Orleans.Runtime.StreamId.Create(StreamConstants.OutputNamespace, fillRate.Id));
            await resultStream.OnNextAsync([new ResultEnvelope { PipelineId = fillRate.Id, Seq = 1, TimestampMs = 0, Row = new Dictionary<string, object?> { ["probe"] = true } }]);

            var gotPipelineResult = await PollUntilAsync(
                () => Task.FromResult(sink.Any(s => s.Method == "pipelineResult" && (string)s.Args[0]! == fillRate.Id)),
                found => found,
                deadlineSeconds: 15);
            Assert.True(gotPipelineResult, "pipelineResult was never relayed to the hub — the bridge never subscribed to the pipeline's output stream.");
        }
        finally
        {
            await bridge.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }
}
