using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 019 (wave C): does <see cref="ConnectorGrain"/> need ANY production code to arm a duplex-kind
/// source correctly? The brief's own framing — "this may shrink your wave, and that is a good outcome" —
/// turned out to be exactly right for the arming path itself: <c>ConnectorGrain.ArmForKind</c> already
/// routes any kind found in <see cref="InboundTransports"/> into <c>StartTransportSubscriber</c>, and
/// <see cref="DuplexTransports.Register"/> co-registers a duplex kind into THAT SAME registry (wave A) — so
/// a duplex source rides <see cref="SubscriberCore"/> unmodified, exactly like nats/grpc do today.
///
/// <para>What genuinely needed a look, and is what this file proves:</para>
/// <list type="bullet">
/// <item>Exactly one live session per source at any instant — <see cref="OneLiveSession_PublishedOnStart_WithdrawnOnStop"/>
/// and <see cref="OneLiveSession_AcrossACleanReconnect"/>.</item>
/// <item>The <c>_generation</c> race <see cref="DuplexSessions.Withdraw"/> exists to guard: a stale
/// generation's delayed <c>DisposeAsync</c> must NOT unpublish a newer live session —
/// <see cref="StaleGenerationDispose_DoesNotUnpublishTheNewerSession"/>, the wave's pinned failure mode.</item>
/// <item><c>ConnectorRuntimeStatus.DuplexReady</c>, read off <see cref="DuplexSessions"/> rather than
/// tracked locally — <see cref="DuplexReady_IsNullForNonDuplexKinds_FalseWhenDown_TrueWhenLive"/>.</item>
/// </list>
///
/// <para><b>Wave B2 closed the gap this file used to pin as open.</b> <see cref="IDuplexSession"/> now
/// exposes <c>SentTotal</c>/<c>FailedTotal</c>/<c>LastFailure</c>, and <c>ConnectorGrain.GetStatusAsync</c>
/// reads them straight off the live session the same read-through way it already read
/// <c>DuplexReady</c> — see <see cref="Status_DuplexCountersReflectTheLiveSessionsSendHistory"/>, which
/// replaces the old "stays at zero" pin now that there is something for the grain to read.</para>
///
/// <para>Named "duplexgrain" — distinct from <c>DuplexTransportRegistryTests</c>'s "flux"/"flux-collision"
/// (wave A) and whatever wave B/D register — same static-registry discipline
/// (<see cref="DuplexTransportRegistryTests"/>'s own doc: the registries are process-global and permanent,
/// so a fake kind is registered exactly once, distinctively named).</para>
/// </summary>
internal sealed class DuplexGrainTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class DuplexGrainTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

public sealed class DuplexConnectorGrainTests : IAsyncLifetime
{
    private const string DuplexGrainKind = "duplexgrain";

    private static readonly DuplexGrainFakeTransport Transport = new();

    static DuplexConnectorGrainTests()
    {
        DuplexTransports.Register(Transport);
    }

    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<DuplexGrainTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<DuplexGrainTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    // ------------------------------------------------------------------
    // The fake duplex transport — one static instance, per-source-name control blocks so parallel test
    // runs (each test uses a fresh Guid-suffixed source name) never interfere with each other.
    // ------------------------------------------------------------------

    /// <summary>Mirrors <c>DuplexTransportRegistryTests.FluxTransport</c>'s shape but adds the hooks this
    /// file's race tests need: a per-source open counter, a way to hand back the most recently opened
    /// session, and an optional gate that delays exactly the FIRST session's <c>DisposeAsync</c> so a test
    /// can force the generation race deterministically instead of hoping for a timing accident.</summary>
    private sealed class DuplexGrainFakeTransport : IDuplexTransport
    {
        public string Kind => DuplexGrainKind;

        public string FormatOf(SourceDefinition def) => FileFormats.JsonArray;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            // No required config — this fake needs none, unlike FluxTransport's connector.nats check.
        }

        // Pinned by IDuplexTransport's own doc comment: Open() delegates to OpenDuplex() so both entry
        // points return the same live object.
        public IInboundSubscription Open(SourceDefinition def) => OpenDuplex(def);

        public IDuplexSession OpenDuplex(SourceDefinition def)
        {
            var control = ControlFor(def.Name);
            var openIndex = Interlocked.Increment(ref control.OpenCount) - 1;
            // Only the FIRST session opened for a given source name is ever gated/forced-clean — later
            // opens (a reconnect, or a second StartAsync) always behave normally, exactly like a
            // well-behaved real session would.
            var disposeGate = openIndex == 0 ? control.GateFirstDispose : null;
            var cleanComplete = openIndex == 0 ? control.CleanCompleteFirstSession : null;
            var session = new Session(def.Name, control, disposeGate, cleanComplete);

            // Contractually required of every duplex transport (DuplexSessions' own doc: "every duplex
            // transport owes this pair") — publish here, in OpenDuplex, not in ConnectorGrain.
            DuplexSessions.Publish(def.Name, session);
            control.LastOpened = session;
            return session;
        }

        public TransportDescriptor Describe() => new()
        {
            Kind = DuplexGrainKind,
            Label = "DuplexGrain fake",
            ConfigProperty = "nats",
            Duplex = true,
            Fields = [new TransportField { Key = "url", Label = "Server URL", Required = true }],
        };

        public sealed class SessionControl
        {
            public int OpenCount;
            public IDuplexSession? LastOpened;

            /// <summary>Set by a test BEFORE calling StartAsync to delay the first session's DisposeAsync
            /// until the test releases it — the deterministic hook for the generation-race test.</summary>
            public TaskCompletionSource<bool>? GateFirstDispose;

            /// <summary>Set by a test BEFORE calling StartAsync to make the FIRST session's SubscribeAsync
            /// complete normally (no exception) once signaled — SubscriberCore's "clean disconnect"
            /// branch, which reconnects immediately with no backoff. The deterministic hook for the
            /// reconnect test, instead of forcing a throw (which would exercise backoff/error status
            /// instead of the clean-reconnect path).</summary>
            public TaskCompletionSource<bool>? CleanCompleteFirstSession;

            public bool Ready { get; set; } = true;

            /// <summary>Plan 019 wave B2: when set, every <c>SendAsync</c> call on a session for this
            /// source reports every row as failed rather than accepted — the deterministic hook the
            /// counters test uses to exercise <c>DuplexFailedTotal</c>/<c>LastDuplexFailure</c>, not just
            /// the (already-covered-elsewhere) accepted path.</summary>
            public bool FailSends { get; set; }
        }

        private static readonly ConcurrentDictionary<string, SessionControl> Controls = new(StringComparer.Ordinal);

        public static SessionControl ControlFor(string sourceName) =>
            Controls.GetOrAdd(sourceName, _ => new SessionControl());

        /// <summary>Never yields a message — this file only exercises open/dispose/reconnect plumbing, not
        /// row mapping (that is <c>DuplexTransportRegistryTests</c>'s job). Blocks on <paramref name="ct"/>
        /// so only an explicit cancel (StopAsync, or a restart bumping <c>_generation</c>) ends the
        /// subscribe loop — matching <c>FluxTransport.Session</c>'s "block rather than complete" discipline
        /// for the same reason.</summary>
        private sealed class Session(
            string sourceName, SessionControl control, TaskCompletionSource<bool>? disposeGate,
            TaskCompletionSource<bool>? cleanCompleteSignal)
            : IDuplexSession
        {
            // Plan 019 wave B2: mechanical implementation of IDuplexSession's new counters. Accumulated per
            // session INSTANCE (not per source) — see the interface's own doc for why a reconnect (a fresh
            // Session object from OpenDuplex) is expected to start these back at zero.
            private long _sent;
            private long _failed;
            private DuplexSendFailure? _lastFailure;

            public bool IsReady => control.Ready;

            public long SentTotal => Interlocked.Read(ref _sent);

            public long FailedTotal => Interlocked.Read(ref _failed);

            public DuplexSendFailure? LastFailure => Volatile.Read(ref _lastFailure);

            public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
            {
                if (cleanCompleteSignal is not null)
                {
                    var completed = await Task.WhenAny(Task.Delay(Timeout.Infinite, ct), cleanCompleteSignal.Task);
                    if (ReferenceEquals(completed, cleanCompleteSignal.Task))
                    {
                        // No exception — SubscriberCore's "clean disconnect" branch: resets the failure
                        // streak and reconnects on its very next loop iteration with NO backoff delay.
                        yield break;
                    }
                    // Otherwise ct fired first; fall through to the throw below so the outer loop sees the
                    // cancellation via its normal exception path.
                }

                await Task.Delay(Timeout.Infinite, ct);
                yield break; // unreachable — Task.Delay above only returns by throwing on cancellation.
            }

            public async ValueTask DisposeAsync()
            {
                if (disposeGate is not null)
                {
                    await disposeGate.Task;
                }

                // The load-bearing contract DuplexSessions.Withdraw exists for: compare-and-remove by
                // reference identity, so a STALE session's dispose (this one, if gated) cannot unpublish
                // whatever session is live now.
                DuplexSessions.Withdraw(sourceName, this);
            }

            public Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
            {
                if (control.FailSends)
                {
                    var failures = rows
                        .Select(r => new DuplexSendFailure(r.GetValueOrDefault("id")?.ToString(), null, "simulated rejection"))
                        .ToList();
                    Interlocked.Add(ref _failed, failures.Count);
                    Volatile.Write(ref _lastFailure, failures[^1]);
                    return Task.FromResult(new DuplexSendOutcome(0, failures.Count, failures));
                }

                Interlocked.Add(ref _sent, rows.Count);
                return Task.FromResult(new DuplexSendOutcome(rows.Count, 0, []));
            }
        }
    }

    private static SourceDefinition MakeDuplexSource(string name) => new()
    {
        Name = name,
        Kind = DuplexGrainKind,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "duplexgrain://localhost", Subject = "dg.subject" },
        },
    };

    private static string FreshName(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..8];

    // ------------------------------------------------------------------
    // 1. Exactly one live session per source.
    // ------------------------------------------------------------------

    [Fact]
    public async Task OneLiveSession_PublishedOnStart_WithdrawnOnStop()
    {
        var name = FreshName("dg_startstop");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        await grain.StartAsync(MakeDuplexSource(name));

        var session = await PollUntilNotNullAsync(() => DuplexSessions.Find(name), deadlineSeconds: 20);
        Assert.NotNull(session);
        Assert.True(session!.IsReady);
        Assert.Equal(1, DuplexGrainFakeTransport.ControlFor(name).OpenCount);

        await grain.StopAsync();

        // StopAsync cancels the transport CTS -> SubscriberCore's finally disposes the subscription ->
        // the session's own DisposeAsync withdraws it. No ConnectorGrain code does this explicitly; it is
        // an emergent property of the existing cancel-then-let-SubscriberCore's-finally-run plumbing.
        var withdrawn = await PollUntilAsync(() => Task.FromResult(DuplexSessions.Find(name)), s => s is null, deadlineSeconds: 20);
        Assert.Null(withdrawn);
    }

    [Fact]
    public async Task OneLiveSession_AcrossACleanReconnect()
    {
        var name = FreshName("dg_reconnect");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var control = DuplexGrainFakeTransport.ControlFor(name);

        // Signals the FIRST session's SubscribeAsync to end without throwing — SubscriberCore's "clean
        // disconnect" branch (RunAsync: "reached here without throwing -> clean disconnect, reconnect with
        // no backoff"), which loops straight back to _transport.Open(def) with no Task.Delay in between.
        control.CleanCompleteFirstSession = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            await grain.StartAsync(MakeDuplexSource(name));

            var first = await PollUntilNotNullAsync(() => DuplexSessions.Find(name), deadlineSeconds: 20);
            Assert.NotNull(first);
            Assert.Equal(1, control.OpenCount);

            // Trigger the clean disconnect. SubscriberCore's finally disposes session A (withdrawing it),
            // then its next loop iteration opens session B for the SAME source — no operator action, no
            // StartAsync/StopAsync, purely the reconnect loop.
            control.CleanCompleteFirstSession.SetResult(true);

            var second = await PollUntilAsync(
                () => Task.FromResult(DuplexSessions.Find(name)),
                s => s is not null && !ReferenceEquals(s, first),
                deadlineSeconds: 20);

            Assert.NotNull(second);
            Assert.NotSame(first, second); // a genuinely new session object, not the old one resurrected
            Assert.Equal(2, control.OpenCount);

            // At every point observed above, DuplexSessions.Find(name) resolved to AT MOST one session —
            // the dictionary is keyed by source name, so two live entries under one name is structurally
            // impossible; what this test adds is proof the reconnect really did open a fresh session
            // (session A got withdrawn BEFORE session B was published, per SubscriberCore's
            // finally-then-loop ordering) rather than leaking or duplicating.
        }
        finally
        {
            control.CleanCompleteFirstSession?.TrySetResult(true);
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 2. The _generation race: a stale session's delayed dispose must not unpublish the newer session.
    // ------------------------------------------------------------------

    [Fact]
    public async Task StaleGenerationDispose_DoesNotUnpublishTheNewerSession()
    {
        var name = FreshName("dg_race");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var control = DuplexGrainFakeTransport.ControlFor(name);

        // Gate the FIRST session's DisposeAsync so it does not complete (and therefore does not withdraw)
        // until this test releases it below — deterministically reproducing "a stale generation's dispose
        // runs AFTER the next connection attempt already published its session" (DuplexSessions.Withdraw's
        // own doc comment names this exact race).
        control.GateFirstDispose = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            await grain.StartAsync(MakeDuplexSource(name)); // generation 1 -> session A opens
            var sessionA = await PollUntilNotNullAsync(() => DuplexSessions.Find(name), deadlineSeconds: 20);
            Assert.NotNull(sessionA);

            // A second StartAsync bumps _generation and cancels the OLD _transportCts, which will
            // eventually (once the gate opens) run session A's DisposeAsync — but generation 2's
            // StartTransportSubscriber call opens session B on an independent task RIGHT NOW, synchronously
            // within this call's background Task.Run, unblocked by session A's stuck dispose.
            await grain.StartAsync(MakeDuplexSource(name)); // generation 2 -> session B opens

            var sessionB = await PollUntilAsync(
                () => Task.FromResult(DuplexSessions.Find(name)),
                s => s is not null && !ReferenceEquals(s, sessionA),
                deadlineSeconds: 20);
            Assert.NotNull(sessionB);
            Assert.NotSame(sessionA, sessionB);
            Assert.Equal(2, control.OpenCount);

            // Now let session A's (stale, generation-1) DisposeAsync finally run.
            control.GateFirstDispose.SetResult(true);

            // Give the stale dispose a moment to actually execute (it runs on the old background task, off
            // this test's control) and assert it did NOT clobber session B.
            await Task.Delay(500);
            var afterStaleDispose = DuplexSessions.Find(name);
            Assert.NotNull(afterStaleDispose);
            Assert.Same(sessionB, afterStaleDispose); // the load-bearing assertion: still B, never null, never A
        }
        finally
        {
            control.GateFirstDispose?.TrySetResult(true);
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 3. ConnectorRuntimeStatus.DuplexReady.
    // ------------------------------------------------------------------

    [Fact]
    public async Task DuplexReady_IsNullForNonDuplexKinds_FalseWhenDown_TrueWhenLive()
    {
        // Non-duplex kind (nats): DuplexReady must stay null, never false — proves the grain does not
        // treat "no session at all" and "wrong kind entirely" the same way.
        var natsName = FreshName("dg_nats_control");
        var natsGrain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(natsName);
        try
        {
            await natsGrain.StartAsync(new SourceDefinition
            {
                Name = natsName,
                Kind = SourceKinds.Nats,
                Enabled = true,
                Fields = [new FieldDef("id", FieldType.String)],
                Connector = new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://127.0.0.1:1", Subject = "x" } },
            });
            await Task.Delay(500); // let it attempt at least one connect
            var natsStatus = await natsGrain.GetStatusAsync();
            Assert.Null(natsStatus.DuplexReady);
        }
        finally
        {
            await natsGrain.StopAsync();
        }

        // Duplex kind, never started: null (no Def at all yet -> DuplexReadyForCurrentDef's def-is-null arm).
        var name = FreshName("dg_ready");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var neverStarted = await grain.GetStatusAsync();
        Assert.Null(neverStarted.DuplexReady);

        try
        {
            // Duplex kind, session live and ready -> true.
            await grain.StartAsync(MakeDuplexSource(name));
            await PollUntilNotNullAsync(() => DuplexSessions.Find(name), deadlineSeconds: 20);

            var live = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.DuplexReady == true, deadlineSeconds: 20);
            Assert.True(live.DuplexReady);

            // Flip the fake session to "not ready" without tearing it down -> false, not null (the session
            // still exists, per DuplexSessions.Find, it is just down).
            DuplexGrainFakeTransport.ControlFor(name).Ready = false;
            var down = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.DuplexReady == false, deadlineSeconds: 20);
            Assert.False(down.DuplexReady);
        }
        finally
        {
            await grain.StopAsync();
        }

        // Stopped: back to null? No — per this field's own doc, false means "there is one and it is
        // down", and a stopped duplex-kind source has no live session at all, same as "down". Either way
        // the important invariant is DuplexReady must NEVER read true once the session is withdrawn.
        var afterStop = await grain.GetStatusAsync();
        Assert.NotEqual(true, afterStop.DuplexReady);
    }

    // ------------------------------------------------------------------
    // 4. The documented gap: Sent/Failed/LastFailure are not populated by this grain.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Status_DuplexCountersReflectTheLiveSessionsSendHistory()
    {
        var name = FreshName("dg_counters");
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        try
        {
            await grain.StartAsync(MakeDuplexSource(name));
            var session = await PollUntilNotNullAsync(() => DuplexSessions.Find(name), deadlineSeconds: 20);
            Assert.NotNull(session);

            // Before any send, a live-but-untouched session reads as zero/null — not "no data available".
            var beforeSend = await grain.GetStatusAsync();
            Assert.Equal(0, beforeSend.DuplexSentTotal);
            Assert.Equal(0, beforeSend.DuplexFailedTotal);
            Assert.Null(beforeSend.LastDuplexFailure);

            // Send directly against the live session, the same way the wave-B proxy sink does (DuplexSessions.Find
            // + SendAsync, bypassing the grain entirely — see DuplexSessions' own doc on why: "the upgrade path is
            // to route the send through the grain/actor by key ... which is why it is not the first thing built").
            // Wave B2: IDuplexSession now carries its own cumulative counters, so GetStatusAsync CAN reflect a
            // send it never touched — it reads them straight off the same live session object.
            var outcome = await session!.SendAsync(
                [new Dictionary<string, object?> { ["id"] = "ord-1" }], CancellationToken.None);
            Assert.Equal(1, outcome.Sent);

            var afterAccepted = await grain.GetStatusAsync();
            Assert.Equal(1, afterAccepted.DuplexSentTotal);
            Assert.Equal(0, afterAccepted.DuplexFailedTotal);
            Assert.Null(afterAccepted.LastDuplexFailure);

            // Now force a rejection and confirm it shows up identified, not just counted — plan 019 D3's
            // whole point for a NewOrderSingle.
            DuplexGrainFakeTransport.ControlFor(name).FailSends = true;
            var failedOutcome = await session.SendAsync(
                [new Dictionary<string, object?> { ["id"] = "ord-2" }], CancellationToken.None);
            Assert.Equal(1, failedOutcome.Failed);

            var afterFailure = await grain.GetStatusAsync();
            Assert.Equal(1, afterFailure.DuplexSentTotal);     // the earlier accepted send still counts
            Assert.Equal(1, afterFailure.DuplexFailedTotal);
            Assert.NotNull(afterFailure.LastDuplexFailure);
            Assert.Contains("ord-2", afterFailure.LastDuplexFailure);
            Assert.Contains("simulated rejection", afterFailure.LastDuplexFailure);
        }
        finally
        {
            DuplexGrainFakeTransport.ControlFor(name).FailSends = false;
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // Polling helpers (same shape as ConnectorGrainNatsClusterTests.PollUntilAsync).
    // ------------------------------------------------------------------

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

    private static Task<IDuplexSession?> PollUntilNotNullAsync(Func<IDuplexSession?> poll, int deadlineSeconds) =>
        PollUntilAsync(() => Task.FromResult(poll()), s => s is not null, deadlineSeconds);
}
