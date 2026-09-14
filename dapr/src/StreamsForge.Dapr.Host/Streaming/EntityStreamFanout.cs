using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 025 G2 — the Dapr half of <see cref="IEntityStreamFacade"/>, and the reason the shared gRPC
/// streaming services can run on this flavor at all.
///
/// <para><b>The shape difference, stated once.</b> Orleans addresses a stream per entity, so
/// "subscribe to table X" is literally a subscription. This flavor has five FIXED pub/sub topics
/// (decision D-D) whose endpoints fan every arriving envelope out to the registered sinks — there is
/// nothing to subscribe TO per entity. So this class IS the per-entity index: one handler list per
/// qualified key, consulted when an envelope for that key arrives, and the "subscription" is an entry
/// in it. That is the same trick <c>DaprStreamBridge</c> plays for SignalR (relay whatever arrives,
/// filtered by group name); this one filters by key instead of by SignalR group, because a gRPC caller
/// has no group to join.</para>
///
/// <para><b>Registration:</b> <see cref="StreamingRuntimeSetup.AddServices"/> registers it once as a
/// singleton, forwards <see cref="ISourceEventsSink"/>, <see cref="ITableDeltaSink"/> and
/// <see cref="IPipelineResultsSink"/> to that instance (so all three raw-ingress topics reach it through
/// the ordinary <c>IEnumerable&lt;T&gt;</c> fan-out, with no special case at the endpoint — plan 025
/// made sf-pipeline-out a generic sink because this class and the table-over-pipeline router both
/// consume it), and registers it as the flavor's <see cref="IEntityStreamFacade"/>.</para>
///
/// <para><b>Keys are QUALIFIED, exactly as the envelopes carry them.</b> <c>SourceEventsEnvelope.Source</c>,
/// <c>TableDeltaEnvelope.Table</c> and <c>PipelineResultsEnvelope.PipelineId</c> are already the
/// publishing actor's own id, which is <c>EnvKeys.Qualify(env, name)</c> — see <c>DaprStreamBridge</c>,
/// which composes its SignalR group names straight from them. So a subscription qualifies its
/// (environment, name) pair the same way and the two meet with no translation. Ordinal comparison, like
/// every other key comparison in this project.</para>
///
/// <para><b>Handler failures are contained here, and they have to be.</b> Dispatch runs inside an HTTP
/// topic endpoint: one throwing handler would abort the endpoint, turn the POST into a 500, and make
/// Dapr redeliver the message — to EVERY subscriber of that topic, not just the broken one. A gRPC
/// handler throws for an entirely ordinary reason (the client disconnected mid-write), so this is not a
/// theoretical case. Each handler is therefore invoked inside its own try/catch and a failure removes
/// nothing: the RPC's own cancellation path is what unsubscribes, and pre-emptively evicting a handler
/// here would race it.</para>
///
/// <para><b>Concurrency:</b> copy-on-write. Subscribe/unsubscribe take a lock and replace the whole list
/// for that key; dispatch reads the current list with no lock and iterates a snapshot that can never be
/// mutated under it. Subscriptions are rare (one per open server-streaming RPC) and dispatch is per
/// envelope, so the copy cost lands in the right place. An <c>IAsyncDisposable</c> handle removes
/// exactly the one entry it holds, by reference identity — two subscriptions to the same key with the
/// same delegate instance are two entries and disposing one leaves the other.</para>
/// </summary>
public sealed class EntityStreamFanout : ISourceEventsSink, ITableDeltaSink, IPipelineResultsSink, IEntityStreamFacade
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, IReadOnlyList<Subscription>> _sources = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<Subscription>> _tables = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<Subscription>> _pipelines = new(StringComparer.Ordinal);

    // ---- IEntityStreamFacade (the gRPC services' side) -------------------------------------------

    public Task<IAsyncDisposable> SubscribeSourceAsync(
        string environment, string sourceName, Func<IReadOnlyDictionary<string, object?>, long, Task> onEvent)
        => Task.FromResult(Add(_sources, EnvKeys.Qualify(environment, sourceName), payload =>
        {
            var (row, tsMs) = ((IReadOnlyDictionary<string, object?>, long))payload;
            return onEvent(row, tsMs);
        }));

    /// <summary>Plan 026 wave 1 — Dapr accepts and IGNORES <paramref name="from"/> (owed: dapr/PARITY.md).
    /// Positions are counted per subscription from 1, so a client sees the same shape as Orleans; the
    /// handle reports nothing retained and Truncated whenever a replay was actually asked for.</summary>
    public async Task<IEntityReplaySubscription> SubscribeSourceAsync(
        string environment, string sourceName, ReplayFrom? from,
        Func<IReadOnlyDictionary<string, object?>, long, long, Task> onEvent)
    {
        long position = 0;
        var inner = await SubscribeSourceAsync(environment, sourceName, (row, ts) => onEvent(row, ts, ++position));
        return new ReplayHandle(inner, truncated: from is { IsEmpty: false });
    }

    private sealed class ReplayHandle(IAsyncDisposable inner, bool truncated) : IEntityReplaySubscription
    {
        public long FirstSeq => 1;
        public long LastSeq => 0;
        public bool Truncated => truncated;
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    public Task<IAsyncDisposable> SubscribePipelineAsync(
        string environment, string pipelineId, Func<IReadOnlyList<ResultEnvelope>, Task> onResults)
        // Keyed by the BARE pipeline id, not EnvKeys.Qualify(environment, id): PipelineActor publishes
        // PipelineResultsEnvelope.PipelineId unqualified (a pipeline id is a GUID, already globally
        // unique — the same reason TableEventRouter.RegisterPipelineInputs keys bare). In the default
        // environment Qualify("", id) == id, which is why this was invisible live; in any other
        // environment a qualified key would never match an envelope.
        => Task.FromResult(Add(_pipelines, pipelineId,
            payload => onResults((IReadOnlyList<ResultEnvelope>)payload)));

    public Task<IAsyncDisposable> SubscribeTableAsync(
        string environment, string tableName, Func<IReadOnlyList<TableDeltaDto>, Task> onDeltas)
        => Task.FromResult(Add(_tables, EnvKeys.Qualify(environment, tableName),
            payload => onDeltas((IReadOnlyList<TableDeltaDto>)payload)));

    // ---- Topic side (the sinks / the direct call from MapTopicEndpoints) -------------------------

    /// <summary>sf-sources. One dispatch per EVENT, not per envelope — a gRPC <c>SourceEvent</c> frame
    /// carries a single row, exactly like the Orleans stream item does, so a batch fans out as N frames
    /// and the subscription's own counter numbers them. Unlike <c>DaprStreamBridge</c>, there is no
    /// sampling here: <c>SourceRateSampler</c> exists to protect a browser's SignalR connection from a
    /// firehose, and a gRPC subscriber asked for the raw stream and gets HTTP/2 flow control instead.</summary>
    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        var handlers = Snapshot(_sources, envelope.Source);
        if (handlers.Count == 0)
        {
            return;
        }

        foreach (var evt in envelope.Events)
        {
            await DispatchAsync(handlers, ((IReadOnlyDictionary<string, object?>)evt, TimestampOf(evt)));
        }
    }

    /// <summary>sf-table-delta. The envelope's own <c>Seq</c> is deliberately dropped: the gRPC
    /// <c>TableDeltaBatch.Seq</c> has always been a counter local to the subscription on the Orleans
    /// side, and plan 025 keeps the two flavors' wire output identical rather than making one of them
    /// authoritative. See <see cref="IEntityStreamFacade"/>'s own note.</summary>
    public Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        var handlers = Snapshot(_tables, envelope.Table);
        return handlers.Count == 0 ? Task.CompletedTask : DispatchAsync(handlers, envelope.Deltas);
    }

    /// <summary>sf-pipeline-out (<see cref="IPipelineResultsSink"/>).</summary>
    public Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        var handlers = Snapshot(_pipelines, envelope.PipelineId);
        return handlers.Count == 0 ? Task.CompletedTask : DispatchAsync(handlers, envelope.Results);
    }

    // ---- internals -------------------------------------------------------------------------------

    /// <summary>The reserved <c>"_ts"</c> key, read the way <see cref="EventRecord.Timestamp"/> reads it
    /// (long, else 0). It is a plain <c>long</c> by the time this runs: every topic endpoint calls
    /// <c>JsonValueNormalizer</c> before dispatching, which turns an integral <c>JsonElement</c> into a
    /// <c>long</c>. An <c>int</c> branch is included anyway for an in-process publisher that never went
    /// through JSON at all.</summary>
    private static long TimestampOf(IReadOnlyDictionary<string, object?> row) =>
        row.TryGetValue(EventRecord.TimestampField, out var v)
            ? v switch { long l => l, int i => i, _ => 0L }
            : 0L;

    private IAsyncDisposable Add(Dictionary<string, IReadOnlyList<Subscription>> index, string key, Func<object, Task> handler)
    {
        var subscription = new Subscription(handler);
        lock (_gate)
        {
            index[key] = index.TryGetValue(key, out var existing) ? [.. existing, subscription] : [subscription];
        }

        return new Handle(this, index, key, subscription);
    }

    private void Remove(Dictionary<string, IReadOnlyList<Subscription>> index, string key, Subscription subscription)
    {
        lock (_gate)
        {
            if (!index.TryGetValue(key, out var existing))
            {
                return;
            }

            var remaining = existing.Where(s => !ReferenceEquals(s, subscription)).ToArray();
            // The key is dropped when the last subscriber leaves, so an instance that has served a
            // million short-lived RPCs holds no more memory than one that has served none.
            if (remaining.Length == 0)
            {
                index.Remove(key);
            }
            else
            {
                index[key] = remaining;
            }
        }
    }

    private IReadOnlyList<Subscription> Snapshot(Dictionary<string, IReadOnlyList<Subscription>> index, string key)
    {
        lock (_gate)
        {
            return index.TryGetValue(key, out var existing) ? existing : [];
        }
    }

    private static async Task DispatchAsync(IReadOnlyList<Subscription> handlers, object payload)
    {
        foreach (var subscription in handlers)
        {
            try
            {
                await subscription.Handler(payload);
            }
            catch
            {
                // Contained on purpose — see the class doc. A gRPC handler throwing because its client
                // vanished must not fail the topic POST and trigger a redelivery to everyone else.
            }
        }
    }

    /// <summary>Identity wrapper: two subscriptions created from the same delegate instance must still
    /// be two removable entries, which a bare <c>Func</c> in a list could not give us.</summary>
    private sealed class Subscription(Func<object, Task> handler)
    {
        public Func<object, Task> Handler { get; } = handler;
    }

    private sealed class Handle(
        EntityStreamFanout owner,
        Dictionary<string, IReadOnlyList<Subscription>> index,
        string key,
        Subscription subscription) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            owner.Remove(index, key, subscription);
            return ValueTask.CompletedTask;
        }
    }
}
