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

/// <summary>Silo config mirroring GeneratorGrainScenarioRunTests'/ConnectorGrainClusterTests' own
/// configurator — duplicated rather than shared, same reasoning as those files' own comment: xunit test
/// classes shouldn't share cluster state.</summary>
internal sealed class GeneratorStepTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class GeneratorStepTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Wishlist #9(b): cluster-level coverage of <c>IGeneratorGrain.RunAsync</c>'s <c>Step</c> branch —
/// the real grain, the real Orleans memory stream, proving (1) day-by-day stepping publishes onto the
/// SAME stream a whole run would and produces THE SAME rows (the pure-logic equivalence already proven by
/// <c>ScenarioGeneratorSteppingTests</c>, here proven through the actual grain/stream path), (2) stepping
/// past the end of a run is a no-op, not an error, and (3) step state is per-RunId and per-activation.</summary>
public sealed class GeneratorGrainStepRunTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<GeneratorStepTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<GeneratorStepTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition ScenarioSource(string name, ScenarioSpec spec) => new()
    {
        Name = name,
        Description = "scenario step test source",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0,
        Enabled = true,
        Scenario = spec,
    };

    private static ScenarioSpec SmallSpec(int paths = 4, int days = 3) => new()
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
    public async Task Stepping_dayByDay_publishes_the_same_rows_a_whole_run_would()
    {
        var name = "scenario_step_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(ScenarioSource(name, SmallSpec(paths: 4, days: 3)));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);

        // Step day 1.
        var step1 = await grain.RunAsync(new ScenarioRunRequest { RunId = "step-run", Seed = 55, Step = true });
        Assert.Equal(ScenarioRunOutcome.Accepted, step1.Outcome);
        Assert.Equal(4 * 2, step1.Accepted); // one day, all paths x instruments
        Assert.All(step1.Rows, r => Assert.Equal(1, r.Day));

        // Step day 2 — SAME RunId, no Seed/Overrides needed (ignored past the first step call).
        var step2 = await grain.RunAsync(new ScenarioRunRequest { RunId = "step-run", Step = true });
        Assert.Equal(ScenarioRunOutcome.Accepted, step2.Outcome);
        Assert.All(step2.Rows, r => Assert.Equal(2, r.Day));

        // Step day 3 — the last day.
        var step3 = await grain.RunAsync(new ScenarioRunRequest { RunId = "step-run", Step = true });
        Assert.All(step3.Rows, r => Assert.Equal(3, r.Day));

        // Step past the end: Accepted, 0 rows, NOT an error.
        var step4 = await grain.RunAsync(new ScenarioRunRequest { RunId = "step-run", Step = true });
        Assert.Equal(ScenarioRunOutcome.Accepted, step4.Outcome);
        Assert.Equal(0, step4.Accepted);
        Assert.Empty(step4.Rows);

        var steppedRows = new List<ScenarioRow>();
        steppedRows.AddRange(step1.Rows);
        steppedRows.AddRange(step2.Rows);
        steppedRows.AddRange(step3.Rows);

        // A whole run with the SAME RunId/Seed, from a DIFFERENT source (so it doesn't collide with the
        // stepped run's state or stream), must produce byte-identical rows.
        var wholeName = "scenario_whole_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(ScenarioSource(wholeName, SmallSpec(paths: 4, days: 3)));
        var wholeGrain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(wholeName);
        var whole = await wholeGrain.RunAsync(new ScenarioRunRequest { RunId = "step-run", Seed = 55 });

        Assert.Equal(ScenarioRunOutcome.Accepted, whole.Outcome);
        Assert.Equal(whole.Rows.Count, steppedRows.Count);
        for (var i = 0; i < whole.Rows.Count; i++)
        {
            Assert.Equal(whole.Rows[i].PathId, steppedRows[i].PathId);
            Assert.Equal(whole.Rows[i].InstrumentId, steppedRows[i].InstrumentId);
            Assert.Equal(whole.Rows[i].Day, steppedRows[i].Day);
            Assert.Equal(whole.Rows[i].Value, steppedRows[i].Value);
            Assert.Equal(whole.Rows[i].Shock, steppedRows[i].Shock);
        }

        // And everything RunAsync returned for the stepped run was genuinely published onto the stream —
        // same "return value and stream side-effect describe the SAME batch" property
        // GeneratorGrainScenarioRunTests pins for the whole-batch path.
        var publishedCount = await PollUntilAsync(
            () => Task.FromResult(Count(received)),
            count => count >= steppedRows.Count,
            deadlineSeconds: 15);
        Assert.True(publishedCount >= steppedRows.Count, $"expected >= {steppedRows.Count} published events, got {publishedCount}");

        List<EventRecord> snapshot;
        lock (received) snapshot = [.. received];
        foreach (var row in steppedRows)
        {
            Assert.Contains(snapshot, evt =>
                Equals(evt["path_id"], row.PathId) &&
                Equals(evt["instrument_id"], row.InstrumentId) &&
                Equals(evt["day"], row.Day) &&
                Equals(evt["value"], row.Value));
        }
    }

    [Fact]
    public async Task Two_different_RunIds_step_independently_on_the_same_activation()
    {
        var name = "scenario_step_multi_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(ScenarioSource(name, SmallSpec(paths: 2, days: 2)));

        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);

        var runA1 = await grain.RunAsync(new ScenarioRunRequest { RunId = "A", Seed = 1, Step = true });
        var runB1 = await grain.RunAsync(new ScenarioRunRequest { RunId = "B", Seed = 2, Step = true });
        var runA2 = await grain.RunAsync(new ScenarioRunRequest { RunId = "A", Step = true });
        var runB2 = await grain.RunAsync(new ScenarioRunRequest { RunId = "B", Step = true });

        Assert.All(runA1.Rows, r => Assert.Equal("A", r.RunId));
        Assert.All(runA1.Rows, r => Assert.Equal(1, r.Day));
        Assert.All(runA2.Rows, r => Assert.Equal(2, r.Day));
        Assert.All(runB1.Rows, r => Assert.Equal("B", r.RunId));
        Assert.All(runB1.Rows, r => Assert.Equal(1, r.Day));
        Assert.All(runB2.Rows, r => Assert.Equal(2, r.Day));

        // Different seeds -> different values for the same (path, instrument, day).
        Assert.NotEqual(runA1.Rows[0].Value, runB1.Rows[0].Value);
    }

    [Fact]
    public async Task Step_with_an_invalid_spec_fails_ValidationError_on_the_first_step_and_publishes_nothing()
    {
        var name = "scenario_step_bad_" + Guid.NewGuid().ToString("n")[..8];
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
        var result = await grain.RunAsync(new ScenarioRunRequest { RunId = "r", Step = true });

        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.NotEmpty(result.Errors);
        Assert.Empty(result.Rows);

        await Task.Delay(300);
        lock (received) Assert.Empty(received);
    }

    [Fact]
    public async Task Step_on_an_activation_that_was_never_started_returns_NotFound_without_throwing()
    {
        var name = "scenario_step_unstarted_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name); // never StartAsync'd

        var result = await grain.RunAsync(new ScenarioRunRequest { RunId = "r", Step = true });

        Assert.Equal(ScenarioRunOutcome.NotFound, result.Outcome);
        Assert.Empty(result.Rows);
    }

    [Fact]
    public async Task StopAsync_clears_step_state_so_a_restarted_run_starts_over_at_day_one()
    {
        var name = "scenario_step_restart_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var spec = SmallSpec(paths: 2, days: 3);
        await registry.UpsertSourceAsync(ScenarioSource(name, spec));

        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);
        var first = await grain.RunAsync(new ScenarioRunRequest { RunId = "restart-run", Seed = 9, Step = true });
        Assert.All(first.Rows, r => Assert.Equal(1, r.Day));
        var second = await grain.RunAsync(new ScenarioRunRequest { RunId = "restart-run", Step = true });
        Assert.All(second.Rows, r => Assert.Equal(2, r.Day)); // advanced to day 2

        // A source restart (StopAsync then StartAsync, e.g. from a config PUT) clears in-progress step
        // state — see GeneratorGrain.StopAsync's doc comment.
        await grain.StopAsync();
        await grain.StartAsync(ScenarioSource(name, spec));

        var afterRestart = await grain.RunAsync(new ScenarioRunRequest { RunId = "restart-run", Seed = 9, Step = true });
        Assert.Equal(ScenarioRunOutcome.Accepted, afterRestart.Outcome);
        Assert.All(afterRestart.Rows, r => Assert.Equal(1, r.Day)); // back to day 1, not day 3
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
