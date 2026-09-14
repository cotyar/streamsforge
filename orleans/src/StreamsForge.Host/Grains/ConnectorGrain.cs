using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Grpc;
using StreamsForge.AppCore.Connectors.Polling;
using StreamsForge.AppCore.Connectors.Scheduling;
using StreamsForge.AppCore.Discovery;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Net;
using StreamsForge.AppCore.Transports;
using StreamsForge.Engine;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Grains;

public sealed class ConnectorGrainState
{
    public SourceDefinition? Def { get; set; }
    public bool Running { get; set; }
    public List<string> DedupKeys { get; set; } = [];
    public Dictionary<string, long> Ledger { get; set; } = [];
    public int ConsecutiveFailures { get; set; }
    public long? LastRunMs { get; set; }
    public long? NextRunMs { get; set; }
    /// <summary>"never" | "ok" | "error" | "connecting" (gRPC/NATS-kind only, transient — see
    /// GrpcSubscriberCore/NatsSubscriberCore's onStatus contract).</summary>
    public string LastStatus { get; set; } = "never";
    public string? LastError { get; set; }
    /// <summary>Plan 014: the polled transport's high-water mark, opaque to everything here. Persisted on
    /// the cycle's own WriteStateAsync (there is no second persistence path) and — the load-bearing part —
    /// written back UNCHANGED after a failed cycle, because PolledSourceCore hands back the cursor it was
    /// given rather than a partially-advanced one. This state class is a plain POCO, not a
    /// [GenerateSerializer] contract, so the field costs no [Id(n)] and breaks no contract test.</summary>
    public string? Cursor { get; set; }
    public long EventsEmittedTotal { get; set; }
    /// <summary>Plan 009 C2: cumulative field coercion failures — see ConnectorRuntimeStatus's own doc.</summary>
    public long CoercionFailuresTotal { get; set; }
    /// <summary>Plan 014: cumulative envelope-unwrap skips — see ConnectorRuntimeStatus's own doc.</summary>
    public long EnvelopeSkippedTotal { get; set; }
    public int LastBatchCount { get; set; }
}

/// <summary>Grain-internal companion to <see cref="IConnectorGrain"/>: the gRPC subscriber's onStatus
/// callback (plan 006 D-G, GrpcSubscriberCore) runs off this grain's turn — a background Task, not a
/// grain-context thread — and must not touch grain state directly, exactly like the onRows callback
/// that reaches <see cref="IConnectorGrain.EmitRowsAsync"/> via a captured self-reference. Status
/// transitions ("connecting"/"ok"/"error") route back the same way, through this second self-reference,
/// so both callbacks re-enter the grain's normal single-turn message queue instead of racing it.
/// Deliberately NOT folded into IConnectorGrain itself (that interface's public surface is pinned by
/// plan 006 W3A) — this is purely an internal wiring detail of ConnectorGrain's gRPC path, so it lives
/// here rather than in the frozen GrainInterfaces.cs.</summary>
public interface IConnectorStatusSink : IGrainWithStringKey
{
    Task ReportStatusAsync(string status, string? error);

    /// <summary>Plan 009 C2: accumulate coercion failures WITHOUT touching status or LastError. Separate
    /// from ReportStatusAsync on purpose — the two would otherwise race for LastError and the counting
    /// call, arriving second, would erase the very note the status call had just written.</summary>
    Task ReportCoercionFailuresAsync(int count);
}

/// <summary>Key = source name. The Orleans driver for connector-kind sources (plan 006 W3A) — mirrors
/// GeneratorGrain's shape (StartAsync/StopAsync/PingAsync on a grain timer) but composes the shared,
/// pure AppCore.Connectors cores (ConnectorPollCycle, ScheduleCalc/BackoffPolicy, DedupTracker/
/// FileLedger, GrpcSubscriberCore) instead of MarketDataProfiles. RegistryGrain dispatches "generator"
/// -kind sources to IGeneratorGrain and everything else here (SourceKinds.Url/File/Folder/Grpc) — this
/// grain never sees Kind == "generator".
///
/// url/file/folder: a ONE-SHOT grain timer (dueTime = next-run delay, period = Infinite; each fire runs
/// one cycle then re-arms) — cron correctness requires recomputing the next occurrence after every fire
/// rather than a fixed period. grpc: a persistent background subscription (GrpcSubscriberCore.RunAsync)
/// started on StartAsync and cancelled on StopAsync; its Schedule is ignored (D-B). Plan 014 adds a THIRD
/// arm to the first of those: a kind registered in PolledTransports rides the IDENTICAL one-shot timer,
/// driven through PolledSourceCore, and additionally persists an opaque cursor on the state write the
/// cycle already performs — so a silo recycle resumes such a source exactly where it stopped, including
/// mid-snapshot. Nothing here knows what any of those kinds are.
///
/// Emission goes through the same door GeneratorGrain uses: one EventRecord per row onto
/// (StreamConstants.SourcesNamespace, sourceName) — pipelines/tables/SignalR/SPA all work unchanged
/// for a connector-kind source's events. Inside this grain that door is exactly one method, PublishAsync:
/// both publish sites (the poll cycle's emission loop and EmitRowsAsync's subscriber path) go through it,
/// which is what lets the late-consumer attach gate (BeginAttachAsync/EndAttachAsync — see
/// IConnectorGrain's own doc for the protocol) be total rather than best-effort, and what feeds the
/// bounded replay log (plan 026: SourceReplayGate) a late-subscribing table/pipeline replays from.
///
/// State ([PersistentState("connector", ...)]) is written after every completed cycle (poll rates are
/// low — D-E's 1s floor) so status/dedup/ledger survive a silo recycle; OnActivateAsync self-resumes
/// (re-arms the timer / restarts the subscriber) when persisted Running is true, mirroring
/// RegistryGrain.EnsureInitializedAsync's boot-resume for generators/pipelines/tables.</summary>
public sealed class ConnectorGrain(
    [PersistentState("connector", StreamConstants.StorageName)] IPersistentState<ConnectorGrainState> state)
    : Grain, IConnectorGrain, IConnectorStatusSink
{
    /// <summary>D-D fallback schedule for a connector with no explicit ScheduleSpec.</summary>
    private static readonly ScheduleSpec DefaultSchedule = new() { IntervalMs = 30_000 };

    /// <summary>10 MB response cap (design D-D/W3A pin) — checked against Content-Length up front, and
    /// again against the actually-read body as a belt-and-braces fallback for chunked responses that
    /// carry no Content-Length header at all.</summary>
    private const long MaxUrlResponseBytes = 10 * 1024 * 1024;

    /// <summary>Built lazily through <see cref="OutboundTls.NewHandler"/> so this client honours the
    /// host's outbound TLS trust configuration (<c>Tls:TrustedCaPath</c> /
    /// <c>Tls:AcceptAnyCertificate</c>). Lazy, not eager: a static field initialiser would run at type
    /// load, which for a type touched during startup can precede <c>OutboundTls.Configure</c> and would
    /// then capture the default trust silently. First real dial is what forces it.</summary>
    private static readonly Lazy<HttpClient> SharedHttpClientLazy =
        new(() => new HttpClient(OutboundTls.NewHandler()) { Timeout = TimeSpan.FromSeconds(30) });

    private static HttpClient SharedHttpClient => SharedHttpClientLazy.Value;

    private IGrainTimer? _timer;

    /// <summary>Bumped by every StartAsync/StopAsync. A poll cycle captures it on entry and abandons its
    /// result if it no longer matches — see <see cref="RunCycleAsync"/>. Grains are turn-based, not
    /// serialized end-to-end: a cycle that yields at its `await` (writing state, pushing to the stream,
    /// an out-of-process poll) lets a StartAsync run in between, and without this the stale cycle then
    /// bumped the failure streak the restart had just cleared and re-armed the timer at the OLD backoff
    /// — reintroducing exactly the "fixing the config doesn't make it poll" symptom, but only under
    /// enough load to widen the window.</summary>
    private int _generation;

    private CancellationTokenSource? _grpcCts;
    private Task? _grpcTask;
    private CancellationTokenSource? _transportCts;
    private Task? _transportTask;
    private DedupTracker? _dedup;
    private FileLedger? _ledger;

    // ------------------------------------------------------------------
    // Late-consumer replay (plan: creation-time window). See IConnectorGrain.BeginAttachAsync's own doc
    // for the protocol these three fields implement; PublishAsync below is the single door every row
    // leaves this grain through, which is what makes the gate total rather than best-effort.
    // ------------------------------------------------------------------

    /// <summary>Plan 026: the late-consumer attach gate + replay log (plan 023's private fields, lifted
    /// into <see cref="SourceReplayGate"/> so generators and ingest sources own the identical mechanism).
    /// Created on first use because the safety timer needs <c>this</c>.</summary>
    private SourceReplayGate? _gate;
    private SourceReplayGate Gate => _gate ??= new SourceReplayGate(
        SourceStream,
        release => this.RegisterGrainTimer(release, SourceReplayGate.SafetyRelease, Timeout.InfiniteTimeSpan));

    // Emission-counter persist throttle for the gRPC/NATS path (EmitRowsAsync can fire once per remote
    // frame/message — far more often than a poll cycle's natural "persist once per cycle" cadence):
    // persist at most every 50 batches or 5 seconds, whichever comes first (design pin).
    private int _batchesSincePersist;
    private DateTime _lastPersistUtc = DateTime.MinValue;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.Running && state.State.Def is not null)
        {
            EnsureTrackers();
            ArmForKind(state.State.Def, persistNextRun: false);
        }
        return base.OnActivateAsync(cancellationToken);
    }

    public async Task StartAsync(SourceDefinition def)
    {
        // Rows already produced are rows already owed: flush anything an attach hold is sitting on BEFORE
        // the generation bump below makes this activation disown the cycle that produced them. Dropping
        // them here would reintroduce, on the restart path, exactly the loss the gate exists to prevent.
        await Gate.ForceReleaseAsync();

        state.State.Def = def;
        state.State.Running = true;
        // A (re)start is the operator saying "try this definition now". Carrying the old failure streak
        // across it meant a source that had backed off to minutes kept waiting out that backoff even
        // after the config that caused the failures was fixed — a PUT, a config import and a
        // disable/enable cycle all landed here and all left the streak intact, so "restart the host" was
        // the only way to get a prompt retry. The streak describes the PREVIOUS definition's health;
        // ArmForKind below reads it to pick the next run, so it has to be cleared before that call.
        state.State.ConsecutiveFailures = 0;
        _generation++;
        EnsureTrackers();

        _timer?.Dispose();
        _timer = null;
        CancelGrpc();
        CancelTransport();

        ArmForKind(def, persistNextRun: true);
        await state.WriteStateAsync();

        if (IsScheduled(def.Kind))
        {
            ArmTimer(NextRunDelay(def));
        }
    }

    public async Task StopAsync()
    {
        // Same reasoning as StartAsync's identical call: a stop must not eat rows this source had already
        // produced and merely deferred on a consumer's behalf.
        await Gate.ForceReleaseAsync();

        state.State.Running = false;
        _generation++;
        _timer?.Dispose();
        _timer = null;
        CancelGrpc();
        CancelTransport();
        await state.WriteStateAsync();
    }

    public Task PingAsync() => Task.CompletedTask;

    public Task<ConnectorRuntimeStatus> GetStatusAsync()
    {
        var (ready, session) = DuplexStateForCurrentDef();
        return Task.FromResult(new ConnectorRuntimeStatus
        {
            SourceName = state.State.Def?.Name ?? this.GetPrimaryKeyString(),
            NextRunMs = state.State.NextRunMs,
            LastRunMs = state.State.LastRunMs,
            LastStatus = state.State.LastStatus,
            LastError = state.State.LastError,
            ConsecutiveFailures = state.State.ConsecutiveFailures,
            EventsEmittedTotal = state.State.EventsEmittedTotal,
            CoercionFailuresTotal = state.State.CoercionFailuresTotal,
            EnvelopeSkippedTotal = state.State.EnvelopeSkippedTotal,
            LastBatchCount = state.State.LastBatchCount,
            Cursor = state.State.Cursor,
            DuplexReady = ready,
            // Plan 019 wave B2: IDuplexSession now exposes SentTotal/FailedTotal/LastFailure (the seam
            // both halves touch — see that interface's own doc for why the counters live there rather than
            // in the sink layer's SinkPublishCounters, which answers a different question and stays
            // unmodified). Read straight off the live session, same "no local accumulation" shape
            // DuplexReady already used — null session (not started, not duplex, or another process holds
            // it) reads as the record's own zero/null defaults, which is the correct reading for "nothing
            // to report" exactly like DuplexReady's false does.
            DuplexSentTotal = session?.SentTotal ?? 0,
            DuplexFailedTotal = session?.FailedTotal ?? 0,
            LastDuplexFailure = session?.LastFailure.Format(),
        });
    }

    /// <summary>Plan 019 D3: resolves the live duplex session for the currently-armed definition ONCE, so
    /// <see cref="GetStatusAsync"/> reads <c>IsReady</c> and the three counters off the SAME session object
    /// rather than risking two independent <see cref="DuplexSessions.Find"/> calls racing a reconnect
    /// between them. Returns <c>(null, null)</c> for every kind that is not a registered duplex kind at
    /// all — <c>ConnectorRuntimeStatus.DuplexReady</c>'s own doc: null and false mean genuinely different
    /// things — and <c>(false, null)</c> when the kind IS duplex but nothing is currently published (session
    /// down, mid-reconnect, or never started). The session is opened by
    /// <see cref="IDuplexTransport.OpenDuplex"/>, called from inside <see cref="SubscriberCore"/>'s
    /// reconnect loop (via <c>IInboundTransport.Open</c> delegating to it), never by this grain — so this
    /// grain has no reference of its own to hold, and querying the process-local rendezvous map is the
    /// only way to answer "is it ready right now".</summary>
    private (bool? Ready, IDuplexSession? Session) DuplexStateForCurrentDef()
    {
        var def = state.State.Def;
        if (def is null || DuplexTransports.Find(def.Kind) is null)
        {
            return (null, null);
        }

        // Plan 021 wave 2: same key FixDuplexTransport.OpenDuplex published under — see its comment.
        var session = DuplexSessions.Find(EnvKeys.Qualify(def.Environment, def.Name));
        return (session?.IsReady ?? false, session);
    }

    /// <summary>gRPC onRows callback entry (reached via a captured self-reference — see
    /// IConnectorStatusSink's class doc). Decoded rows carry no _source stamp (unlike poll-cycle rows,
    /// which ConnectorPollCycle already stamps) so it is applied here; _ts is stamped only if absent
    /// (a table-kind remote's decoded delta rows may already carry one from the source schema).</summary>
    public async Task EmitRowsAsync(List<Dictionary<string, object?>> rows, long remoteSeq)
    {
        var def = state.State.Def;
        if (def is null)
        {
            return;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (rows.Count > 0)
        {
            foreach (var row in rows)
            {
                row.TryAdd("_source", def.Name);
                row.TryAdd("_ts", nowMs);
                await PublishAsync(new EventRecord(row));
            }

            state.State.EventsEmittedTotal += rows.Count;
            state.State.LastBatchCount = rows.Count;
        }

        state.State.LastRunMs = nowMs;
        state.State.LastStatus = "ok";
        state.State.LastError = null;
        state.State.ConsecutiveFailures = 0;

        _batchesSincePersist++;
        var elapsed = DateTime.UtcNow - _lastPersistUtc;
        if (_batchesSincePersist >= 50 || elapsed >= TimeSpan.FromSeconds(5))
        {
            // Plan 009 B1: the nats path's MappingSpec.DedupKeyField dedup runs on _dedup (via
            // ConnectorPollCycle.Emit inside NatsSubscriberCore) — persist it on the same throttle as
            // everything else here so it survives a silo recycle. Harmless no-op for the grpc path,
            // which never touches _dedup.
            if (_dedup is not null)
            {
                state.State.DedupKeys = _dedup.ToPersistable();
            }

            await state.WriteStateAsync();
            _batchesSincePersist = 0;
            _lastPersistUtc = DateTime.UtcNow;
        }
    }

    /// <summary>gRPC onStatus callback entry (reached via a captured self-reference — see this
    /// interface's class doc on IConnectorStatusSink). "connecting"/"ok" carry no ConsecutiveFailures
    /// change on their own beyond what EmitRowsAsync already does for "ok"; "error" increments it so
    /// GetStatusAsync reflects a struggling subscription even when no rows ever arrive to drive
    /// EmitRowsAsync. Always persists immediately — status transitions are inherently low-frequency
    /// (once per reconnect attempt), unlike EmitRowsAsync's per-frame throttling.</summary>
    public async Task ReportStatusAsync(string status, string? error)
    {
        if (state.State.Def is null)
        {
            return;
        }

        state.State.LastStatus = status;
        state.State.LastError = error;
        if (string.Equals(status, "error", StringComparison.Ordinal))
        {
            state.State.ConsecutiveFailures++;
        }
        else if (string.Equals(status, "ok", StringComparison.Ordinal))
        {
            state.State.ConsecutiveFailures = 0;
        }

        await state.WriteStateAsync();
    }

    /// <summary>Plan 009 C2: the queryable half of "counted and surfaced". The subscriber kinds (grpc,
    /// nats) reach ConnectorRuntimeStatus only through the status channel, so without this their
    /// coercion failures would exist as a LastError string and nowhere countable, while the polled kinds
    /// — which report through EmitRowsAsync's cycle handler — had a real counter. A counter that is
    /// accurate for some source kinds and silently zero for others is worse than no counter.</summary>
    /// <summary>Plan 009 C2 + plan 014: the "clean cycle, but not silent" note. Both coercion failures
    /// (under Null/DropRow) and envelope skips leave <see cref="PollCycleResult.Error"/> null on purpose —
    /// an error drops the whole batch — so LastError is the only line an operator reading the status sees
    /// them on. Composed rather than either-or, because a CDC source can hit both in one cycle and the
    /// second one silently winning would be exactly the kind of quiet this note exists to prevent.</summary>
    private static string? CycleNote(PollCycleResult result, CoercionFailurePolicy policy)
    {
        List<string> notes = [];
        if (result.CoercionFailures > 0)
        {
            notes.Add($"{result.CoercionFailures} field coercion failure(s) this cycle; policy={policy}");
        }
        if (result.EnvelopeSkipped > 0)
        {
            notes.Add($"{result.EnvelopeSkipped} message(s) skipped: the envelope carried no representable row");
        }
        // The cycle core's own "clean cycle, something to say" channel (PollCycleResult.Note) — today the
        // folder kind's per-file parse failures. Same reason as the two above: it is not an Error precisely
        // so the rows beside it survive, which leaves this status line the only place it shows up.
        if (result.Note is not null)
        {
            notes.Add(result.Note);
        }
        return notes.Count == 0 ? null : string.Join("; ", notes);
    }

    public async Task ReportCoercionFailuresAsync(int count)
    {
        if (count <= 0 || state.State.Def is null)
        {
            return;
        }

        state.State.CoercionFailuresTotal += count;
        await state.WriteStateAsync();
    }

    // ------------------------------------------------------------------
    // Publishing + the late-consumer attach gate
    // ------------------------------------------------------------------

    private IAsyncStream<EventRecord> SourceStream() =>
        this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString()));

    /// <summary>THE single door every row leaves this grain through — both publish sites (the poll cycle's
    /// emission loop and <see cref="EmitRowsAsync"/>'s subscriber path) call it, and a third one must too.
    /// See <see cref="SourceReplayGate.PublishAsync"/> for the hold/replay rule.</summary>
    private Task PublishAsync(EventRecord evt) => Gate.PublishAsync(evt);

    /// <summary>See <see cref="IConnectorGrain.BeginAttachAsync()"/> for the protocol and why it is correct.</summary>
    public Task<SourceReplaySnapshot> BeginAttachAsync() => BeginAttachAsync(null);

    public Task<SourceReplaySnapshot> BeginAttachAsync(ReplayFrom? from) => Task.FromResult(Gate.Begin(from));

    public Task EndAttachAsync() => Gate.EndAsync();

    // ------------------------------------------------------------------
    // Arming
    // ------------------------------------------------------------------

    /// <summary>Common StartAsync/OnActivateAsync-resume logic: for url/file/folder computes and stores
    /// NextRunMs (persisted by the caller); for grpc launches the background subscriber. Does NOT arm
    /// the grain timer itself for the url/file/folder branch when <paramref name="persistNextRun"/> is
    /// false — OnActivateAsync's resume path arms directly off the already-persisted NextRunMs via
    /// <see cref="NextRunDelay"/> after this returns, so a resumed activation doesn't recompute a
    /// (possibly different, now-past) schedule twice.</summary>
    private void ArmForKind(SourceDefinition def, bool persistNextRun)
    {
        if (def.Kind == SourceKinds.Grpc)
        {
            StartGrpcSubscriber(def);
            return;
        }

        // Plan 010: any registered message transport, not a hardcoded nats branch.
        if (InboundTransports.Find(def.Kind) is { } transport)
        {
            StartTransportSubscriber(def, transport);
            return;
        }

        // Plan 014: any registered POLLED transport. Deliberately the SAME arm as url/file/folder rather
        // than one of its own — a polled kind is a scheduled cycle, so it wants exactly the timer,
        // NextRunMs and backoff that already exist here. The only thing the new seam adds is a cursor,
        // and a cursor rides on the state write the cycle already performs. The two registries are
        // disjoint by construction (PolledTransportRegistryTests pins it), so this lookup cannot steal a
        // kind from the branch above or from the built-ins below.
        if (IsScheduled(def.Kind))
        {
            if (persistNextRun)
            {
                var schedule = def.Connector?.Schedule ?? DefaultSchedule;
                var nowUtc = DateTimeOffset.UtcNow;
                var nextRun = BackoffPolicy.NextRun(schedule, nowUtc, state.State.ConsecutiveFailures);
                state.State.NextRunMs = nextRun?.ToUnixTimeMilliseconds();
            }
            else
            {
                ArmTimer(NextRunDelay(def));
            }
            return;
        }

        // Kind == "generator" (or an unrecognized value) never reaches ConnectorGrain in normal
        // operation — RegistryGrain dispatches those to IGeneratorGrain instead. Defensive no-op.
    }

    /// <summary>Kinds this grain drives off the one-shot timer: the three built-in poll kinds, plus
    /// whatever <see cref="PolledTransports"/> has been told about. A registry lookup rather than a
    /// hardcoded array is the whole point of the seam — a kind this assembly has never heard of gets a
    /// schedule because it registered, not because someone remembered to extend a list.</summary>
    private static bool IsScheduled(string? kind) =>
        kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder || PolledTransports.Find(kind) is not null;

    /// <summary>Delay until the persisted NextRunMs (falling back to a 30 s retry if unset/invalid —
    /// e.g. a schedule that failed ScheduleCalc.Validate at some point).</summary>
    private TimeSpan NextRunDelay(SourceDefinition def)
    {
        if (state.State.NextRunMs is long nextMs)
        {
            var delay = DateTimeOffset.FromUnixTimeMilliseconds(nextMs) - DateTimeOffset.UtcNow;
            return delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        }
        return TimeSpan.FromSeconds(30);
    }

    private void ArmTimer(TimeSpan dueTime)
    {
        _timer?.Dispose();
        _timer = this.RegisterGrainTimer(RunCycleAsync, dueTime, Timeout.InfiniteTimeSpan);
    }

    private void CancelGrpc()
    {
        if (_grpcCts is null)
        {
            return;
        }
        _grpcCts.Cancel();
        _grpcCts.Dispose();
        _grpcCts = null;
        _grpcTask = null;
    }

    private void StartGrpcSubscriber(SourceDefinition def)
    {
        var cfg = def.Connector?.Grpc
            ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'grpc' but no grpc config");

        _grpcCts?.Cancel();
        _grpcCts?.Dispose();
        _grpcCts = new CancellationTokenSource();
        var ct = _grpcCts.Token;

        // Self-references: the subscriber's callbacks run off this grain's turn (a background Task, not
        // grain-context) — see IConnectorStatusSink's class doc for why both callbacks route through a
        // captured grain reference (a normal, safely-serialized grain call) instead of touching `state`
        // directly.
        var self = this.AsReference<IConnectorGrain>();
        var statusSink = this.AsReference<IConnectorStatusSink>();

        var core = new GrpcSubscriberCore(
            cfg,
            (rows, seq) => HandleDecodedRowsAsync(def, self, statusSink, rows, seq),
            (status, error) => _ = SafeReportStatusAsync(statusSink, status, error));

        _grpcTask = Task.Run(async () =>
        {
            try
            {
                await core.RunAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Expected on StopAsync.
            }
            catch
            {
                // Best-effort — GrpcSubscriberCore.RunAsync already catches and status-reports every
                // failure it can attribute to a specific reconnect attempt; anything escaping here is
                // unexpected but must not crash the background task's thread.
            }
        }, CancellationToken.None);
    }

    /// <summary>Plan 009 C2: gRPC's onRows callback entry point. Unlike NATS (whose rows are already
    /// coerced — <see cref="ConnectorPollCycle.Emit"/> runs inside <see cref="NatsSubscriberCore"/> via
    /// the shared mapping path), a gRPC-decoded row never passes through
    /// <see cref="ConnectorPollCycle"/> at all, so coercion happens HERE, once per callback, before the
    /// rows ever reach <see cref="IConnectorGrain.EmitRowsAsync"/> — "coerce before admission" applies
    /// here exactly as it does for the poll-cycle kinds. A RejectBatch rejection never calls
    /// EmitRowsAsync at all (nothing left behind); Null/DropRow's non-zero failure count is reported
    /// AFTER emission (never before — see <see cref="EmitRowsAsync"/>'s own unconditional
    /// <c>LastError = null</c> on a successful batch, which would otherwise clobber this note if the
    /// order were reversed).</summary>
    private static async Task HandleDecodedRowsAsync(
        SourceDefinition def, IConnectorGrain self, IConnectorStatusSink statusSink,
        IReadOnlyList<Dictionary<string, object?>> rows, long seq)
    {
        var coercion = ConnectorRowCoercion.Apply(def.Fields, rows.ToList(), def.OnCoercionFailure);
        if (coercion.BatchRejected)
        {
            await SafeReportStatusAsync(statusSink, "error", $"coercion rejected batch: {coercion.RejectReason}");
            return;
        }

        await self.EmitRowsAsync(coercion.Rows, seq);

        if (coercion.FailureCount > 0)
        {
            await SafeReportStatusAsync(
                statusSink, "ok", $"{coercion.FailureCount} field coercion failure(s) this batch; policy={def.OnCoercionFailure}");
            try { await statusSink.ReportCoercionFailuresAsync(coercion.FailureCount); } catch { /* best-effort, same as the status report above */ }
        }
    }

    /// <summary>Plan 010: the driver half of a message-transport source, for ANY registered transport —
    /// previously a nats-shaped copy of this method. <see cref="SubscriberCore"/> already runs rows through
    /// ConnectorPollCycle (parse/extract/coerce/dedup/stamp) internally, so — unlike gRPC above — the onRows
    /// callback here just forwards already-finished rows straight to EmitRowsAsync (its own "_source"/"_ts"
    /// TryAdd stamping is a harmless no-op on rows that already carry both).</summary>
    private void StartTransportSubscriber(SourceDefinition def, IInboundTransport transport)
    {
        _transportCts?.Cancel();
        _transportCts?.Dispose();
        _transportCts = new CancellationTokenSource();
        var ct = _transportCts.Token;

        var self = this.AsReference<IConnectorGrain>();
        var statusSink = this.AsReference<IConnectorStatusSink>();

        var core = new SubscriberCore(
            def, transport, _dedup!,
            (rows, seq) => self.EmitRowsAsync(rows.ToList(), seq),
            (status, error) => _ = SafeReportStatusAsync(statusSink, status, error),
            onCoercionFailures: n => _ = statusSink.ReportCoercionFailuresAsync(n));

        _transportTask = Task.Run(async () =>
        {
            try
            {
                await core.RunAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Expected on StopAsync.
            }
            catch
            {
                // Best-effort — SubscriberCore.RunAsync already catches and status-reports every failure it
                // can attribute to a specific reconnect attempt; anything escaping here is unexpected but
                // must not crash the background task's thread.
            }
        }, CancellationToken.None);
    }

    private void CancelTransport()
    {
        if (_transportCts is null)
        {
            return;
        }
        _transportCts.Cancel();
        _transportCts.Dispose();
        _transportCts = null;
        _transportTask = null;
    }

    private static async Task SafeReportStatusAsync(IConnectorStatusSink sink, string status, string? error)
    {
        try
        {
            await sink.ReportStatusAsync(status, error);
        }
        catch
        {
            // Best-effort — a lost status update self-heals on the next transition (or the next
            // EmitRowsAsync call, for "ok").
        }
    }

    private void EnsureTrackers()
    {
        _dedup = new DedupTracker(state.State.DedupKeys);
        _ledger = new FileLedger(state.State.Ledger);
    }

    // ------------------------------------------------------------------
    // Poll cycle (url/file/folder and any registered IPolledTransport — grpc and the message transports
    // are persistent subscriptions, not polls)
    // ------------------------------------------------------------------

    private async Task RunCycleAsync()
    {
        var def = state.State.Def;
        if (def is null || !state.State.Running)
        {
            return;
        }

        EnsureTrackers();
        var generation = _generation;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Plan 014: set by a polled cycle that says there is more waiting RIGHT NOW — see the re-arm at
        // the bottom of this method. Always false for url/file/folder, which have no notion of a page.
        var hasMore = false;

        // Staged rather than written straight to state.State: everything below the cycle's awaits has to
        // be discardable, because the staleness check after them may find this whole cycle obsolete.
        // Seeded with the current cursor so a throw leaves it unchanged, matching the "a failed cycle
        // keeps the old cursor" rule PolledSourceCore is built around.
        var nextCursor = state.State.Cursor;

        PollCycleResult result;
        try
        {
            if (PolledTransports.Find(def.Kind) is { } polled)
            {
                // The cursor rules — never advance past a cycle that emitted nothing, null means
                // "unchanged" rather than "start over" — live in PolledSourceCore, not here. A
                // driver-local copy of them is exactly the copy that drifts from the other flavour's.
                // The dedup column comes from the kind's own config because a polled row source has no
                // mapping document to read a DedupKeyField out of.
                var outcome = await PolledSourceCore.RunCycleAsync(
                    polled, def, state.State.Cursor, _dedup!, nowMs, CancellationToken.None,
                    dedupKeyField: def.Connector?.Db?.DedupKeyColumn is { Length: > 0 } key ? key : null);

                result = outcome.Result;
                // Assigned unconditionally and persisted by the WriteStateAsync this cycle already does.
                // Safe precisely because a failed outcome carries the cursor that went IN — so "persist
                // whatever I was handed" and "never skip data on failure" are the same statement.
                nextCursor = outcome.Cursor;
                hasMore = outcome.HasMore;
            }
            else
            {
                result = def.Kind switch
                {
                    SourceKinds.Url => await ExecuteUrlCycleAsync(def, nowMs),
                    SourceKinds.File => ConnectorPollCycle.ExecuteFile(def, _ledger!, _dedup!, nowMs),
                    SourceKinds.Folder => ConnectorPollCycle.ExecuteFolder(def, _ledger!, _dedup!, nowMs),
                    _ => new PollCycleResult([], null),
                };
            }
        }
        catch (Exception ex)
        {
            result = new PollCycleResult([], $"{ex.GetType().Name}: {ex.Message}");
        }

        // A StartAsync/StopAsync ran while this cycle was awaiting: its definition is gone, its result
        // describes a source that no longer exists in this shape, and whoever replaced it has already
        // written the status and armed the timer they want. Drop everything rather than overwrite them.
        // The tracker state this cycle mutated in memory is rebuilt from persisted state by the
        // EnsureTrackers() that StartAsync and the next cycle both call, so nothing leaks forward.
        if (generation != _generation)
        {
            return;
        }

        // RE-ARM IS A `finally`, NOT A TAIL STATEMENT. The grain timer is ONE-SHOT (cron correctness — see
        // the class doc), so the ONLY thing that keeps a scheduled source alive is the next ArmTimer call.
        // Everything below can throw — the emission loop (a stream provider failure), WriteStateAsync (a
        // storage failure) — and a throw that skipped the re-arm stopped the source dead until the
        // activation was recycled, with nothing but a swallowed exception to say so. Set inside the try,
        // read in the finally, with a 30 s fallback when the cycle never got as far as computing one.
        //
        // The `generation == _generation` half of the finally's guard is load-bearing, not defensive: a
        // StartAsync that interleaved at one of those awaits has ALREADY armed the timer it wants, and
        // re-arming here on top of it would replace that timer with this stale cycle's (possibly
        // backed-off) delay — the exact staleness the generation check at the top of this method exists to
        // prevent, reintroduced one line from the end.
        TimeSpan? rearmDelay = null;
        try
        {
            // Driver-level emit policy: a cycle either succeeds (emit everything it produced, reset the
            // failure streak) or fails (emit nothing). What CHANGED is where the boundary of "failed" sits:
            // a cycle core that partially succeeded — ExecuteFolder parsing four files and choking on a
            // fifth — no longer reports that as an Error at all. It returns the good rows plus a
            // PollCycleResult.Note, and leaves the bad file OUT of the ledger so the next cycle retries it.
            // The old shape ledgered the good files AND dropped their rows, which is not "emit nothing this
            // cycle": those rows were never coming back. Persistence of the cursor/dedup/ledger is gated on
            // emission below for the same reason from the other direction — if the emit itself failed, the
            // trackers must NOT record progress the stream never saw.
            var emitFailed = false;
            var produced = result.Rows.Count;
            if (result.Error is null && produced > 0)
            {
                var emitted = 0;
                try
                {
                    foreach (var row in result.Rows)
                    {
                        await PublishAsync(new EventRecord(row));
                        emitted++;
                    }
                }
                catch (Exception ex)
                {
                    // Folded into the ordinary error path rather than escaping: the status line, the
                    // failure streak and the backoff are all things an operator already knows how to read,
                    // and an escaping exception here would additionally have skipped the re-arm.
                    result = new PollCycleResult([], $"emit failed after {emitted}/{produced} row(s): {ex.GetType().Name}: {ex.Message}");
                    emitFailed = true;
                }
            }

            if (result.Error is null)
            {
                state.State.ConsecutiveFailures = 0;
                state.State.LastStatus = "ok";
                // Plan 009 C2: a coercion failure under Null/DropRow never produces a non-null Error above
                // (only RejectBatch does, via ConnectorPollCycle.Emit). It is counted in
                // CoercionFailuresTotal — the queryable channel — and ALSO restated in LastError so an
                // operator reading the status line sees it without knowing to look at a counter.
                state.State.CoercionFailuresTotal += result.CoercionFailures;
                // Plan 014: an envelope skip reaches an operator the same way and for the same reason — it is
                // never an Error (that would drop the good rows beside it), so without this line a CDC source
                // quietly discarding every delete looks identical to one with no deletes.
                state.State.EnvelopeSkippedTotal += result.EnvelopeSkipped;
                state.State.LastError = CycleNote(result, def.OnCoercionFailure);
            }
            else
            {
                state.State.ConsecutiveFailures++;
                state.State.LastStatus = "error";
                state.State.LastError = result.Error;
            }

            state.State.LastRunMs = nowMs;
            state.State.LastBatchCount = result.Error is null ? result.Rows.Count : 0;
            state.State.EventsEmittedTotal += state.State.LastBatchCount;

            if (!emitFailed)
            {
                state.State.Cursor = nextCursor;
                state.State.DedupKeys = _dedup!.ToPersistable();
                state.State.Ledger = _ledger!.ToPersistable();
            }
            // else: leave all three at their persisted values. The in-memory trackers this cycle mutated are
            // rebuilt from that persisted state by the next cycle's EnsureTrackers(), so the file/page is
            // re-read and re-emitted — at-least-once, with the dedup key (when one is configured)
            // suppressing whatever DID get out before the failure.

            var schedule = def.Connector?.Schedule ?? DefaultSchedule;
            var nowUtc = DateTimeOffset.UtcNow;
            var nextRun = BackoffPolicy.NextRun(schedule, nowUtc, state.State.ConsecutiveFailures);

            // Plan 014: HasMore means "there is more waiting right now", and the whole reason a snapshot pages
            // across DRIVER cycles instead of inside one PollAsync is that every page's cursor gets persisted
            // before the next page is read — so a restart resumes mid-snapshot. Waiting out the schedule
            // between pages would make a million-row snapshot take (pages x interval) to land for no benefit,
            // so the next run is now. Same one-shot timer, just due immediately; PolledSourceCore never
            // returns HasMore on a failed cycle (and `!emitFailed` covers the one failure it cannot know
            // about), so this cannot spin against a failure. Overwriting NextRunMs (rather than arming behind
            // its back) keeps the status honest about when the next cycle runs.
            if (hasMore && !emitFailed)
            {
                nextRun = nowUtc;
            }

            state.State.NextRunMs = nextRun?.ToUnixTimeMilliseconds();

            await state.WriteStateAsync();

            var delay = nextRun.HasValue ? nextRun.Value - nowUtc : TimeSpan.FromSeconds(30);
            rearmDelay = delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        }
        finally
        {
            if (state.State.Running && generation == _generation)
            {
                ArmTimer(rearmDelay ?? TimeSpan.FromSeconds(30));
            }
        }
    }

    private async Task<PollCycleResult> ExecuteUrlCycleAsync(SourceDefinition def, long nowMs)
    {
        var cfg = def.Connector?.Url
            ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'url' but no url config");

        // Plan 016 wave 6: resolved fresh on every poll cycle — this method is invoked once per cycle
        // (ArmTimer re-arms it), so there is no caching to go stale. NamedEndpoints.Resolve throws for an
        // unresolvable @name; the exception propagates out of this method uncaught, same as a malformed
        // literal URL already would, and lands on this grain's existing poll-cycle failure path (see the
        // caller, which folds any exception here into the source's own LastError/status).
        var url = NamedEndpoints.Resolve(cfg.Url);

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        foreach (var (name, value) in cfg.Headers)
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        using var response = await SharedHttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        if (response.Content.Headers.ContentLength is long contentLength && contentLength > MaxUrlResponseBytes)
        {
            return new PollCycleResult([], $"response too large ({contentLength} bytes, cap {MaxUrlResponseBytes})");
        }
        if (!response.IsSuccessStatusCode)
        {
            return new PollCycleResult([], $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
        }

        var body = await response.Content.ReadAsStringAsync();
        if (body.Length > MaxUrlResponseBytes)
        {
            return new PollCycleResult([], $"response too large ({body.Length} bytes, cap {MaxUrlResponseBytes})");
        }

        return ConnectorPollCycle.ExecuteUrl(def, body, _dedup!, nowMs);
    }
}
