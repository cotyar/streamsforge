using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using StreamsForge.Engine;
using StreamsForge.Host.Generators;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring GeneratorGrainScenarioRunTests'/GeneratorGrainStepRunTests' own
/// configurator — duplicated rather than shared, same reasoning as those files' own comment: xunit test
/// classes shouldn't share cluster state.</summary>
internal sealed class LoopbackCycleTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class LoopbackCycleTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Wishlist #9(b): end-to-end proof of the native loopback pair against a REAL Orleans <c>GeneratorGrain</c>
/// — <see cref="LoopbackSinkClient"/> writing into <see cref="LoopbackHub"/>, the grain's own drain timer
/// picking rows up and republishing them onto the SAME real Orleans stream a tick would use. This file
/// closes the loop entirely with production code (<see cref="LoopbackSinkClient"/>, <see cref="LoopbackHub"/>,
/// <c>GeneratorGrain.DrainLoopbackAsync</c>) — the ONLY thing standing in for a real table+sink is this
/// test's own stream subscription callback, which republishes each received row back through a real
/// <see cref="LoopbackSinkClient"/> exactly as a table's own loopback sink would from
/// <c>SinkFanout.PublishAllAsync</c>. Table/sink-attachment machinery itself
/// (<c>TableGrain</c>/<c>TableExecutorImpl</c>/<c>NatsPublisherService</c>) is outside this wave's file
/// ownership, so this is the highest-fidelity proof achievable without it — the cycle-breaking mechanics
/// under test (the hub write/drain split — see <see cref="LoopbackHub"/>'s class doc) are entirely
/// independent of what triggers a publish.
///
/// <para><b>The unbounded case is the whole point of this file.</b>
/// <see cref="A_tight_unbounded_loopback_cycle_runs_for_hundreds_of_laps_without_crashing_or_hanging"/>
/// builds a cycle with <c>MaxDepth = 0</c> (the guard OFF) and NO other termination condition, and proves
/// it does not deadlock and does not overflow the stack — see that test's own comment for exactly how
/// "no StackOverflowException" is established (a StackOverflowException cannot be caught in .NET;
/// reaching this test's assertions at all, from a process that is still alive, is part of the proof) — and
/// that it keeps making genuine forward progress until explicitly stopped, at which point it stops
/// growing. <see cref="MaxDepth_bounds_a_loopback_cycle_to_an_exact_row_count"/> is the paired bounded
/// case, proving the SAME guard (<see cref="SinkStepGuard"/>, shared with <see cref="HttpSinkClient"/>)
/// terminates the identical cycle deterministically.</para>
/// </summary>
public sealed class LoopbackCycleTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<LoopbackCycleTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<LoopbackCycleTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition CycleSource(string name) => new()
    {
        Name = name,
        Description = "loopback cycle test source",
        GeneratorProfile = "generic",
        EventsPerSecond = 0, // no independent ticking — every row here comes from the loopback cycle
        Enabled = true,
    };

    [Fact]
    public async Task A_row_published_through_the_loopback_sink_arrives_on_the_targets_real_stream()
    {
        var name = "loop_basic_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(CycleSource(name));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        await using var sink = new LoopbackSinkClient(new LoopbackSinkConfig { TargetSourceName = name }, "table", "t1");
        await sink.PublishAsync(
            new NatsTableDeltaMessage { Table = "t1", Row = new() { ["step"] = 0L, ["marker"] = "hello" }, Weight = 1 },
            CancellationToken.None);

        Assert.Equal(1, sink.Counters.Published);

        var count = await PollUntilAsync(() => Task.FromResult(Count(received)), c => c >= 1, deadlineSeconds: 10);
        Assert.True(count >= 1, "expected the loopback-drained row to arrive on the target source's stream");

        List<EventRecord> snapshot;
        lock (received) snapshot = [.. received];
        Assert.Contains(snapshot, evt => Equals(evt["marker"], "hello") && Equals(evt["step"], 0L));
        Assert.All(snapshot, evt => Assert.Equal(name, evt.Source)); // stamped fresh by DrainLoopbackAsync
    }

    /// <summary>The bounded twin of the unbounded test below: the exact same shared-guard mechanism
    /// (wishlist's own requirement — "the maxDepth guard must work exactly as it does in the HTTP sink")
    /// deterministically caps a real, grain-driven loopback cycle at an exact row count.</summary>
    [Fact]
    public async Task MaxDepth_bounds_a_loopback_cycle_to_an_exact_row_count()
    {
        var name = "loop_bounded_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(CycleSource(name));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var observedSteps = new List<long>();

        const int maxDepth = 5;
        await using var sink = new LoopbackSinkClient(
            new LoopbackSinkConfig { TargetSourceName = name, MaxDepth = maxDepth }, "table", "cycle_table");

        // Every received row is a "table delta" that would, in production, come out of a table whose SQL
        // reads this same source and whose own loopback sink feeds it right back — simulated here (see
        // this class's doc comment) by re-publishing THROUGH THE SAME LoopbackSinkClient from this
        // subscription callback, with step + 1.
        await stream.SubscribeAsync((evt, token) =>
        {
            var step = (long)evt["step"]!;
            lock (observedSteps) observedSteps.Add(step);
            // Fire-and-forget, exactly like NatsPublisherService/NatsSinkPublisherService call
            // ISinkClient.PublishAsync with no try/catch around it (SinkFanout's own doc comment) — this
            // client's contract is that it never throws.
            var republish = sink.PublishAsync(
                new NatsTableDeltaMessage { Table = "cycle_table", Row = new() { ["step"] = step + 1 }, Weight = 1 },
                CancellationToken.None);
            _ = republish;
            return Task.CompletedTask;
        });

        // Seed the cycle at step 0, through the SAME sink, exactly like every subsequent lap.
        await sink.PublishAsync(new NatsTableDeltaMessage { Table = "cycle_table", Row = new() { ["step"] = 0L }, Weight = 1 }, CancellationToken.None);

        // The guard drops step 5 (>= maxDepth) BEFORE it is ever written to the hub, so the cycle
        // naturally stops producing new stream events once steps 0..4 have all round-tripped — poll until
        // the sink has recorded exactly one Failed (the dropped step-5 attempt) or a deadline elapses.
        await PollUntilAsync(
            () => Task.FromResult(sink.Counters.Failed),
            failed => failed >= 1,
            deadlineSeconds: 10);

        // Give any (wrongly) further laps a moment to arrive, then assert the exact, deterministic bound.
        await Task.Delay(300);

        List<long> snapshot;
        lock (observedSteps) snapshot = [.. observedSteps];
        Assert.Equal([0L, 1L, 2L, 3L, 4L], snapshot.OrderBy(s => s));

        Assert.Equal(maxDepth, sink.Counters.Published); // steps 0,1,2,3,4
        Assert.Equal(1, sink.Counters.Failed); // step 5, dropped
        Assert.Contains("maxDepth", sink.Counters.LastError, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>THE test the wishlist asks for: "the engine must not deadlock or stack-overflow if someone
    /// builds a tight cycle [with no bound]" — proved against the real hub + a real grain's drain timer,
    /// with the guard OFF (<c>MaxDepth = 0</c>) and no SQL-level bound either (there is no SQL here at
    /// all — see this class's doc comment for why that is the highest-fidelity substitute available
    /// within this wave's file ownership).
    ///
    /// <para><b>How "no StackOverflowException" is established.</b> A StackOverflowException cannot be
    /// caught by managed code — the CLR tears down the process immediately when the native stack is
    /// exhausted. There is therefore no <c>try/catch</c> that could ever prove its absence directly; the
    /// proof IS that this test's own assertions run to completion, in a process that is still alive,
    /// after hundreds of laps of the cycle. If <see cref="LoopbackHub"/>'s design (see its class doc:
    /// <see cref="LoopbackHub.TryPublish"/> never calls back into the reader, and the reader drains only
    /// from an independently-scheduled grain timer tick, so no synchronous call chain ever spans a lap of
    /// the cycle) were wrong and a lap actually recursed synchronously, this test — and every other test
    /// in this file — would simply never finish (the test host process would crash before xUnit could
    /// report anything). Reaching the final assertions below is the only kind of evidence this claim
    /// admits of.</para>
    ///
    /// <para><b>How "no deadlock" is established.</b> The row count is polled to grow past a threshold
    /// within a generous deadline; a deadlock would mean the count stops growing and the poll times out —
    /// asserted explicitly below, not merely assumed from "the test finished".</para>
    /// </summary>
    [Fact]
    public async Task A_tight_unbounded_loopback_cycle_runs_for_hundreds_of_laps_without_crashing_or_hanging()
    {
        var name = "loop_unbounded_" + Guid.NewGuid().ToString("n")[..8];
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(CycleSource(name));

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

        long receivedCount = 0;
        long maxStepSeen = -1;

        // MaxDepth = 0 (the default): the guard is OFF. Nothing here bounds the cycle except this test
        // eventually calling StopAsync — exactly the "termination is the user's job" contract the
        // wishlist describes for a loop with no WHERE-clause bound.
        await using var sink = new LoopbackSinkClient(new LoopbackSinkConfig { TargetSourceName = name }, "table", "unbounded_table");

        var subscriptionHandle = await stream.SubscribeAsync((evt, token) =>
        {
            var step = (long)evt["step"]!;
            Interlocked.Increment(ref receivedCount);
            InterlockedMax(ref maxStepSeen, step);
            // Same fire-and-forget re-publish as the bounded test — see this class's doc comment.
            var republish = sink.PublishAsync(
                new NatsTableDeltaMessage { Table = "unbounded_table", Row = new() { ["step"] = step + 1 }, Weight = 1 },
                CancellationToken.None);
            _ = republish;
            return Task.CompletedTask;
        });

        // Seed the cycle.
        await sink.PublishAsync(new NatsTableDeltaMessage { Table = "unbounded_table", Row = new() { ["step"] = 0L }, Weight = 1 }, CancellationToken.None);

        // Progress bar: wait until the cycle has genuinely completed several hundred laps. If this hangs,
        // the test framework's own timeout fails it — which is exactly the "must not deadlock" property
        // under test, made observable.
        const int lapsToObserve = 200;
        var reached = await PollUntilAsync(
            () => Task.FromResult(Interlocked.Read(ref receivedCount)),
            count => count >= lapsToObserve,
            deadlineSeconds: 60);
        Assert.True(reached >= lapsToObserve, $"expected >= {lapsToObserve} laps of genuine forward progress, got {reached} within the deadline — looks like a deadlock, not an unbounded loop");
        Assert.True(Interlocked.Read(ref maxStepSeen) >= lapsToObserve - 1, "step counter did not advance in lockstep with received-row count — something is re-delivering instead of advancing");

        // Stop the cycle the ONLY documented way it ends short of a bound: an operator action. StopAsync
        // detaches the hub AND the subscription is disposed, so no further row can complete a lap.
        await subscriptionHandle.UnsubscribeAsync();
        var grain = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);
        await grain.StopAsync();

        var countAtStop = Interlocked.Read(ref receivedCount);
        await Task.Delay(500); // grace period — long enough for the drain timer to have fired several times
        var countAfterGrace = Interlocked.Read(ref receivedCount);

        // Growth stopped once the cycle was actually torn down — proving this was a genuine live loop
        // that only a stop (never a crash, never a self-termination) ends, not a fluke that happened to
        // stop on its own.
        Assert.True(
            countAfterGrace - countAtStop <= 1, // at most one in-flight publish that was already queued
            $"expected growth to stop after StopAsync/Unsubscribe (was {countAtStop}, now {countAfterGrace}) — the cycle kept running after being told to stop");
    }

    private static void InterlockedMax(ref long location, long candidate)
    {
        long initial, computed;
        do
        {
            initial = Volatile.Read(ref location);
            computed = Math.Max(initial, candidate);
        }
        while (Interlocked.CompareExchange(ref location, computed, initial) != initial);
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
