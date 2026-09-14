using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Generators;
using StreamsForge.Host.Streaming;

namespace StreamsForge.Host.Grains;

/// <summary>Key = source name. Publishes one synthetic event per tick on a grain timer.
///
/// <para><b>Wishlist #9(b): also the loopback target.</b> Regardless of profile/EventsPerSecond, every
/// StartAsync attaches this activation to <see cref="LoopbackHub"/> (see that class's doc comment for the
/// whole design) and arms a second, always-on timer that drains whatever a <c>LoopbackSinkClient</c> has
/// written for this source name and republishes it exactly like a tick would. This is what lets a
/// <c>scenario</c>-profile source (EventsPerSecond == 0 by convention — see
/// <see cref="ScenarioSpec"/>) or any other generator-kind source be a loopback target: nothing about
/// being loopback-fed depends on the tick timer being armed at all.</para>
/// </summary>
public sealed class GeneratorGrain : Grain, IGeneratorGrain
{
    /// <summary>Wishlist #9(b): how often this activation checks <see cref="LoopbackHub"/> for rows a
    /// loopback sink has written for it. Short — the "scenario clock" use case (wishlist #9's own framing)
    /// wants step t+1 to start soon after step t's downstream table finishes, and there is no tick-rate
    /// budget concern here the way <c>GeneratorActor</c>'s Dapr-sidecar-hop reasoning has: this is a plain
    /// in-process timer with no round trip behind it.</summary>
    private static readonly TimeSpan LoopbackDrainPeriod = TimeSpan.FromMilliseconds(20);

    /// <summary>Caps how many rows one drain tick will publish, so a burst (or a tight unbounded cycle —
    /// see <see cref="LoopbackHub"/>'s doc comment on that case) cannot hold this grain's turn indefinitely;
    /// anything left over is picked up on the next tick, not lost.</summary>
    private const int LoopbackDrainBatchCap = 2000;

    private SourceDefinition? _def;
    private IGrainTimer? _timer;
    private IGrainTimer? _loopbackDrainTimer;

    /// <summary>Wishlist #9(b): per-RunId continuation state for <c>step: true</c> — see
    /// <see cref="ScenarioRunState"/>'s doc comment for its in-memory-only lifecycle. Cleared on every
    /// StartAsync/StopAsync (see below) — a restart or stop is a fresh start for step sequences too, same
    /// as it already is for <see cref="_timer"/>.</summary>
    private readonly Dictionary<string, ScenarioRunState> _runStates = new(StringComparer.Ordinal);

    /// <summary>Plan 026 wave 1: every publish site below goes through this gate, so a gRPC/SignalR
    /// subscriber can attach from a position (<see cref="IReplayableSourceGrain"/>). Tables/pipelines
    /// still subscribe to a generator WITHOUT the gate (a continuous source has no "missed first poll"),
    /// until a definition opts in via <c>replayFrom</c> in wave 2.</summary>
    private SourceReplayGate? _gate;
    private SourceReplayGate Gate => _gate ??= new SourceReplayGate(
        SourceStream,
        release => this.RegisterGrainTimer(release, SourceReplayGate.SafetyRelease, Timeout.InfiniteTimeSpan));

    private IAsyncStream<EventRecord> SourceStream() =>
        this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString()));

    public Task<SourceReplaySnapshot> BeginAttachAsync(ReplayFrom? from) => Task.FromResult(Gate.Begin(from));

    public Task EndAttachAsync() => Gate.EndAsync();

    public async Task StartAsync(SourceDefinition def)
    {
        // Rows already produced are rows already owed — same rule as ConnectorGrain.StartAsync.
        await Gate.ForceReleaseAsync();
        _def = def;
        _timer?.Dispose();
        _timer = null;
        _loopbackDrainTimer?.Dispose();
        _loopbackDrainTimer = null;
        _runStates.Clear();

        // Wishlist #9(b): attach + arm the drain loop BEFORE the early EventsPerSecond<=0 return below —
        // a scenario/loopback-target source with EventsPerSecond == 0 must still be reachable.
        var sourceName = this.GetPrimaryKeyString();
        LoopbackHub.Attach(sourceName);
        _loopbackDrainTimer = this.RegisterGrainTimer(DrainLoopbackAsync, LoopbackDrainPeriod, LoopbackDrainPeriod);

        if (def.EventsPerSecond <= 0)
        {
            return;
        }

        var intervalMs = Math.Clamp(1000.0 / def.EventsPerSecond, 1, 10_000);
        var period = TimeSpan.FromMilliseconds(intervalMs);
        _timer = this.RegisterGrainTimer(TickAsync, period, period);
    }

    public async Task StopAsync()
    {
        await Gate.ForceReleaseAsync();
        _timer?.Dispose();
        _timer = null;
        _loopbackDrainTimer?.Dispose();
        _loopbackDrainTimer = null;
        // Detach LAST: any LoopbackSinkClient still writing after this returns false, reported as a
        // failure by the caller — see LoopbackHub.Detach's doc comment.
        LoopbackHub.Detach(this.GetPrimaryKeyString());
        _runStates.Clear();
    }

    public Task PingAsync() => Task.CompletedTask;

    /// <summary>Wishlist #8 (whole-batch) and #9(b) (<c>request.Step</c> — see
    /// <see cref="ScenarioRunRequest.Step"/>'s doc comment for the full stepping contract) — see
    /// IGeneratorGrain.RunAsync's doc comment for the base contract. NotFound when this activation has
    /// never been StartAsync'd (no _def on file yet); otherwise delegates the whole spec/request-
    /// validation + row-math decision to the pure, TOTAL <see cref="ScenarioGenerator"/> and — only for
    /// <see cref="ScenarioRunOutcome.Accepted"/> — publishes every row.
    ///
    /// <para><b>Step mode</b> (<c>request.Step == true</c>): the FIRST such call for a given
    /// <see cref="ScenarioRunRequest.RunId"/> calls <see cref="ScenarioGenerator.BeginRun"/> (running the
    /// exact same TOTAL validation a whole-batch run does) and caches the resulting
    /// <see cref="ScenarioRunState"/> in <see cref="_runStates"/>; every subsequent step call for the same
    /// RunId reuses it, IGNORING that call's <see cref="ScenarioRunRequest.Seed"/>/<see cref="ScenarioRunRequest.Overrides"/>
    /// (locked in at the first step, exactly like a whole-batch run's are locked in for its one call).
    /// Each call emits exactly ONE day via <see cref="ScenarioGenerator.GenerateDay"/> — empty once the
    /// run's <see cref="ScenarioRunState.IsComplete"/>, which is Accepted with 0 rows, never an error (see
    /// <see cref="ScenarioRunRequest.Step"/>'s doc comment for why no new outcome value exists for this).
    /// A non-step (whole) request for the SAME RunId does NOT read or touch <see cref="_runStates"/> —
    /// the two modes are independent; mixing them for one RunId is a caller footgun this method does not
    /// attempt to prevent, documented rather than silently reconciled.</para>
    ///
    /// <para><b>"Honouring MaxBatchRows/backpressure" (wishlist wording), as implemented here.</b>
    /// MaxBatchRows is enforced as a hard config-validation cap (ScenarioSpec.MaxBatchRows / Validate) —
    /// a run either emits the WHOLE batch or none of it, never a partial one, which is the same "never a
    /// partial admit" shape <see cref="IngestConfig.MaxBatchRows"/> already uses for push ingress
    /// (IngestModels.cs's header comment). Backpressure: rows are published ONE AT A TIME with `await
    /// stream.OnNextAsync(...)` in a loop, exactly like TickAsync below — Orleans' stream provider applies
    /// its own admission/queueing under that await, so a slow consumer genuinely holds this loop up rather
    /// than this method firing N*K*D publishes without ever yielding for one. This is a narrower claim
    /// than IngestConfig's own buffer+overflow-policy machinery (IngestModels.cs's header note on why
    /// there is no true end-to-end backpressure in this architecture); a shared, observable admission
    /// buffer for run-on-demand batches — mirroring SourceIngressBuffer — is out of scope for this
    /// change (would require extending the Ingest facade seam, which is intentionally untouched here).</para>
    /// </summary>
    public async Task<ScenarioRunResult> RunAsync(ScenarioRunRequest request)
    {
        if (_def is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        List<ScenarioRow> rows;
        if (request.Step)
        {
            if (!_runStates.TryGetValue(request.RunId, out var state))
            {
                if (!ScenarioGenerator.BeginRun(_def, request, out state, out var failure))
                {
                    return failure!;
                }

                _runStates[request.RunId] = state!;
            }

            rows = ScenarioGenerator.GenerateDay(state!, nowMs);
            if (rows.Count == 0 && state!.IsComplete)
            {
                // Wishlist #9(b): stepping past the end of a run is a no-op, not an error.
                return new ScenarioRunResult { Outcome = ScenarioRunOutcome.Accepted, Accepted = 0, Rows = [] };
            }
        }
        else
        {
            var result = ScenarioGenerator.GenerateBatch(_def, request, nowMs);
            if (result.Outcome != ScenarioRunOutcome.Accepted)
            {
                return result;
            }

            rows = result.Rows;
        }

        foreach (var row in rows)
        {
            await Gate.PublishAsync(ScenarioGenerator.ToEventRecord(row, _def.Name));
        }

        return new ScenarioRunResult { Outcome = ScenarioRunOutcome.Accepted, Accepted = rows.Count, Rows = rows };
    }

    private async Task TickAsync()
    {
        if (_def is null)
        {
            return;
        }

        var evt = MarketDataProfiles.GenerateEvent(_def);
        await Gate.PublishAsync(evt);
    }

    /// <summary>Wishlist #9(b): the loopback drain tick — see this class's doc comment and
    /// <see cref="LoopbackHub"/>'s for the full design, in particular why this MUST be a scheduled timer
    /// callback and never a continuation chained off a <c>LoopbackSinkClient</c> write (that is what keeps
    /// a feedback cycle from overflowing the stack). Runs on this grain's own turn like every other
    /// callback here — no reentrancy, no race with StartAsync/StopAsync swapping <see cref="_def"/>.
    ///
    /// <para><b>KNOWN GAP, stated rather than left to be found:</b> unlike <c>POST /api/sources/{name}/events</c>
    /// (<c>IngressRowAcceptance</c>, via <c>IIngressFacade</c>), this path does NOT run
    /// <see cref="SourceDefinition.OnCoercionFailure"/> field-type coercion or ingest dedup against the
    /// drained row — it republishes exactly what the sink handed <see cref="LoopbackHub"/>. This is the
    /// same tradeoff the HTTP sink's own gap notes make elsewhere in this wave: a loopback row already
    /// comes from a live table's own already-typed Z-set output, so a mismatch only arises if the TARGET
    /// source's declared <see cref="SourceDefinition.Fields"/> disagree with the origin table's column
    /// types — plausible, but not the common case, and wiring the shared coercion path here would mean
    /// reaching into <c>StreamsForge.AppCore.Ingest</c> call sites this wave does not own.</para></summary>
    private async Task DrainLoopbackAsync()
    {
        if (_def is null)
        {
            return;
        }

        // Plan 021 wave 2: the SAME key Attach/Detach use — this grain's own (environment-qualified)
        // primary key. It read _def.Name before, which is byte-identical in the default environment and
        // silently wrong in every other one: attach registers the channel under `staging.feed` while the
        // drain loop reads `feed`, so the loop runs forever draining nothing and the channel (unbounded)
        // grows without limit.
        var rows = LoopbackHub.Drain(this.GetPrimaryKeyString(), LoopbackDrainBatchCap);
        if (rows.Count == 0)
        {
            return;
        }

        // Fresh _source/_ts on arrival — a row drained here is a NEW event at THIS source, same as a
        // ScenarioGenerator/MarketDataProfiles row would be, regardless of whatever stamped values (if
        // any) the upstream table's own row happened to carry.
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var row in rows)
        {
            var evt = new EventRecord(row)
            {
                [EventRecord.SourceField] = _def.Name,
                [EventRecord.TimestampField] = nowMs,
            };
            await Gate.PublishAsync(evt);
        }
    }
}
