using Microsoft.Extensions.Logging;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;

namespace StreamsForge.Host.Streaming;

/// <summary>Plan 026 wave 2 — the attach gate + replay log for ANY stream payload: what wave 1's
/// <see cref="SourceReplayGate"/> did for <c>EventRecord</c>, generalized so a pipeline's result batches
/// and a table's delta batches get the identical hold / snapshot / flush mechanism. The protocol and every
/// reason it is correct are on <see cref="IConnectorGrain.BeginAttachAsync()"/>; nothing here changes it.
/// Owned by exactly one grain activation and called only from its turns (a grain turn is the unit of
/// atomicity — no lock needed, none would help).</summary>
/// <param name="stream">The stream every item is published on.</param>
/// <param name="armSafetyTimer">The owner's <c>RegisterGrainTimer</c> — the release has to run ON the
/// grain's turn because it publishes.</param>
/// <param name="timestampOf">The item's event time (a row's <c>_ts</c>, a batch's first envelope's
/// timestamp, or the wall clock for a payload with no event time of its own).</param>
/// <param name="copy">Defensive copy applied on append and on snapshot.</param>
internal sealed class ReplayGate<T>(
    Func<IAsyncStream<T>> stream,
    Func<Func<Task>, IDisposable> armSafetyTimer,
    Func<T, long> timestampOf,
    Func<T, T> copy)
{
    public static readonly TimeSpan SafetyRelease = TimeSpan.FromSeconds(10);

    private readonly ReplayLog<T> _log = new(timestampOf, copy);
    private int _holds;
    private readonly List<T> _pending = [];
    private IDisposable? _safetyTimer;

    public int Holds => _holds;
    public long LastSeq => _log.LastSeq;
    public long FirstSeq => _log.FirstSeq;

    /// <summary>THE single door every item leaves the owning grain through — deferred while a hold is
    /// outstanding, else published then remembered. Appended AFTER the publish deliberately: an item that
    /// failed to publish is not one a late consumer should be told it missed.</summary>
    public async Task PublishAsync(T item)
    {
        if (_holds > 0)
        {
            _pending.Add(item);
            return;
        }

        await stream().OnNextAsync(item);
        _log.Append(item);
    }

    /// <summary>Synchronous by construction — no await between taking the hold and reading the snapshot.</summary>
    public StreamReplaySnapshot<T> Begin(ReplayFrom? from)
    {
        _holds++;
        _safetyTimer?.Dispose();
        _safetyTimer = armSafetyTimer(ForceReleaseAsync);

        var s = _log.Snapshot(from is { IsEmpty: false } ? from : null);
        return new StreamReplaySnapshot<T>
        {
            Items = s.Entries.Select(e => e.Item).ToList(),
            Positions = s.Entries.Select(e => e.Seq).ToList(),
            TotalSeen = s.TotalSeen,
            FirstSeq = s.FirstSeq,
            LastSeq = s.LastSeq,
            Truncated = s.Truncated,
        };
    }

    public async Task EndAsync()
    {
        if (_holds > 0)
        {
            _holds--;
        }

        if (_holds == 0)
        {
            _safetyTimer?.Dispose();
            _safetyTimer = null;
            await FlushPendingAsync();
        }
    }

    /// <summary>Releases EVERY hold: the safety timer ("somebody is not coming back") and the owner's own
    /// Start/Stop (items already produced are items already owed).</summary>
    public async Task ForceReleaseAsync()
    {
        _holds = 0;
        _safetyTimer?.Dispose();
        _safetyTimer = null;
        await FlushPendingAsync();
    }

    private async Task FlushPendingAsync()
    {
        if (_pending.Count == 0)
        {
            return;
        }

        var pending = _pending.ToList();
        _pending.Clear();

        var s = stream();
        foreach (var item in pending)
        {
            await s.OnNextAsync(item);
            _log.Append(item);
        }
    }
}

/// <summary>Plan 026 D5 — the CONSUMER half of <c>replayFrom</c>, shared by the three grains that attach to
/// an input (<see cref="Grains.TableGrain"/>, <see cref="Grains.PipelineGrain"/>,
/// <see cref="Grains.TableIngestGrain"/>): which position an input starts from, and which driver grain to
/// ask for it.</summary>
internal static class ReplayInputs
{
    /// <summary>The position declared for <paramref name="inputName"/>, or null when the definition names
    /// no position for it — which is what keeps an unnamed input byte-for-byte on its pre-026 attach path.
    /// A present-but-empty entry counts as unnamed (nothing was actually asked for).</summary>
    public static ReplayFrom? For(IReadOnlyDictionary<string, ReplayFrom> replayFrom, string inputName) =>
        replayFrom.TryGetValue(inputName, out var from) && from is { IsEmpty: false } ? from : null;

    /// <summary>The source driver that owns the replay log for <paramref name="kind"/> — connector,
    /// generator AND ingest, unlike the default attach path, which is connector-only by decision (a new
    /// table over a seeded generator must not suddenly receive its whole ring). <c>crdt</c> and any
    /// unrecognized kind answer null: <c>RegistryGrain</c> refuses <c>replayFrom</c> on those, so null here
    /// only covers a catalog edited around that guard, and the caller then subscribes live rather than
    /// refusing to start.</summary>
    public static IReplayableSourceGrain? DriverFor(IGrainFactory grains, string? kind, string qualifiedName) =>
        SourceKindDispatch.Classify(kind) switch
        {
            SourceKindDispatch.ActorKind.Connector => grains.GetGrain<IConnectorGrain>(qualifiedName),
            SourceKindDispatch.ActorKind.Generator => grains.GetGrain<IGeneratorGrain>(qualifiedName),
            SourceKindDispatch.ActorKind.Ingest => grains.GetGrain<IIngestSourceGrain>(qualifiedName),
            _ => null,
        };

    /// <summary>The one warning shape for "you asked for a position the producer no longer retains" — never
    /// silence, the same rule the <c>WarmUpstreamDiagnostic</c> warning follows. Each placeholder name
    /// appears exactly once (the structured-logging formatter binds positionally).</summary>
    public static void WarnTruncated(ILogger logger, string entity, string inputName, long firstSeq) =>
        logger.LogWarning(
            "'{Entity}': replayFrom for input '{Input}' reached past what the producer retains; replayed from position {FirstSeq}.",
            entity, inputName, firstSeq);
}
