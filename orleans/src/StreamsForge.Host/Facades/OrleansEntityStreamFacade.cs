using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;

namespace StreamsForge.Host.Facades;

/// <summary>
/// Plan 025 G1 — the Orleans half of <see cref="IEntityStreamFacade"/>: the exact
/// <c>GetStreamProvider(...).GetStream&lt;T&gt;(StreamId.Create(ns, EnvKeys.Qualify(env, key)))
/// .SubscribeAsync(...)</c> calls the gRPC streaming services used to make inline, lifted out of
/// <c>shared/StreamsForge.Api/Grpc/**</c> when those services moved there so they could serve both
/// runtimes.
///
/// <para><b>Nothing here is new behaviour.</b> Namespaces, key composition and payload types are
/// character-for-character what <c>StreamGrpcService</c>/<c>DynamicStreamService</c> passed before the
/// move — <see cref="StreamConstants.SourcesNamespace"/> keyed by qualified source NAME carrying
/// <see cref="EventRecord"/>, <see cref="StreamConstants.OutputNamespace"/> keyed by qualified pipeline
/// ID carrying <c>List&lt;ResultEnvelope&gt;</c>, <see cref="StreamConstants.TableDeltaNamespace"/> keyed
/// by qualified table NAME carrying <c>List&lt;TableDeltaDto&gt;</c>. The same three streams
/// <c>StreamBridgeService</c> relays to SignalR.</para>
///
/// <para><b>Registered as a singleton, and it holds no per-request state.</b> Every method takes the
/// environment explicitly (the caller passes the entity's own <c>Environment</c>, or
/// <c>EnvironmentAmbient.Current</c> where there is no entity), so this never reads the ambient itself —
/// which matters because an <c>IAsyncDisposable</c> returned from here outlives the request scope that
/// created it, for as long as the server-streaming RPC runs.</para>
///
/// <para><b>Why the returned handle is a struct-free little class rather than the Orleans handle
/// itself.</b> <see cref="StreamSubscriptionHandle{T}"/> is generic and not
/// <see cref="IAsyncDisposable"/>; wrapping it is one allocation per subscription and lets all three
/// methods return the one non-generic type the shared services await. The wrapper's DisposeAsync is a
/// bare <c>UnsubscribeAsync</c> — the swallow-everything rule stays at the call site, where it always
/// was (StreamGrpcService.WaitForCancellationThenUnsubscribeAsync), so this class cannot silently hide a
/// failure the caller decided to tolerate.</para>
/// </summary>
// Public, unlike the rest of this Facades/ directory, for the same reason GrpcEntityRef is (see that
// class's own comment): the Host test project has no InternalsVisibleTo, and
// SourceReplayGrpcTests constructs this directly over a TestCluster's IClusterClient rather than
// through DI. Adding an InternalsVisibleTo to the whole assembly to keep one class internal is the
// more expensive of the two options.
public sealed class OrleansEntityStreamFacade(IClusterClient client) : IEntityStreamFacade
{
    public async Task<IAsyncDisposable> SubscribeSourceAsync(
        string environment, string sourceName, Func<IReadOnlyDictionary<string, object?>, long, Task> onEvent)
    {
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(
                StreamConstants.SourcesNamespace, EnvKeys.Qualify(environment, sourceName)));

        // evt.Timestamp is EventRecord's typed read of the reserved "_ts" key — handed over separately
        // so IEntityStreamFacade (in Contracts) needs no dependency on StreamsForge.Engine.
        return new Handle<EventRecord>(await stream.SubscribeAsync((evt, _) => onEvent(evt, evt.Timestamp)));
    }

    /// <summary>Plan 026 wave 1 — the replaying half of <see cref="IEntityStreamFacade.SubscribeSourceAsync(string, string, ReplayFrom?, Func{IReadOnlyDictionary{string, object?}, long, long, Task})"/>,
    /// implementing plan 023's attach protocol verbatim (see <see cref="IConnectorGrain.BeginAttachAsync()"/>'s
    /// long doc for why the order below is correct and exactly-once): resolve the source's driver grain by
    /// kind, <c>BeginAttachAsync(from)</c> → subscribe to its stream → feed the snapshot through the SAME
    /// handler the live subscription uses → <c>EndAttachAsync</c> in a <c>finally</c>. The snapshot is fully
    /// delivered BEFORE this method returns — <see cref="StreamsForge.Api.Hubs.StreamHub.SubscribeSourceFrom"/>
    /// and <see cref="StreamsForge.Host.Grpc.StreamGrpcService"/> both rely on that to know when to stop
    /// treating callbacks as "replay" and start treating them as "live".
    ///
    /// <para><c>from == null</c> skips the gate entirely (nothing to replay) and subscribes live only —
    /// byte-identical to the older <see cref="SubscribeSourceAsync(string, string, Func{IReadOnlyDictionary{string, object?}, long, Task})"/>
    /// overload, just with a per-subscription position counted from 1 attached to every callback and a
    /// handle reporting <c>FirstSeq=1/LastSeq=0/Truncated=false</c> (there was nothing to retain-and-report
    /// because nothing was asked for).</para>
    ///
    /// <para>A <c>crdt</c>-kind source refuses with <see cref="NotSupportedException"/> — CRDT documents
    /// have their own replay (plan 020's <c>ReplayAsync</c>), never this log. A source the catalog does not
    /// know at all also refuses: unlike the plain <see cref="SubscribeSourceAsync(string, string, Func{IReadOnlyDictionary{string, object?}, long, Task})"/>
    /// overload (which has always tolerated subscribing before an entity exists), asking for a POSITION
    /// needs a driver grain to ask, and there is no way to pick one for a kind nobody has declared yet.</para>
    ///
    /// <para><b>ponytail: the one known drift, from plan 023's "ONE GAP, MEASURED" paragraph on
    /// <see cref="IConnectorGrain.BeginAttachAsync()"/>.</b> The attach hold stops the driver PUBLISHING;
    /// it has no reach into the stream provider's own delivery pipeline. A row already handed to
    /// <c>OnNextAsync</c> may still be sitting in the memory stream's queue, not yet pulled into the cache
    /// a brand-new subscriber is served from (default pull period 100 ms) — so a subscription that lands
    /// inside that window can receive such a row live AND see it in the replayed snapshot, with live
    /// positions then running one ahead of what the wire eventually settles on. Positions for live rows
    /// are counted locally from <c>snapshot.LastSeq</c> rather than stamped by the producer at publish
    /// time, which is the actual fix and the upgrade path once it is worth taking.</para></summary>
    public async Task<IEntityReplaySubscription> SubscribeSourceAsync(
        string environment, string sourceName, ReplayFrom? from,
        Func<IReadOnlyDictionary<string, object?>, long, long, Task> onEvent)
    {
        var qualified = EnvKeys.Qualify(environment, sourceName);
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, qualified));

        if (from is null)
        {
            long live = 0;
            var liveHandle = await stream.SubscribeAsync((evt, _) => onEvent(evt, evt.Timestamp, ++live));
            return new ReplaySubscriptionHandle(new Handle<EventRecord>(liveHandle), firstSeq: 1, lastSeq: 0, truncated: false);
        }

        var def = await client.RegistryFor(environment).GetSourceAsync(sourceName)
            ?? throw new NotSupportedException(
                $"source '{sourceName}' is not in the catalog — replay needs a driver grain to ask, and there is no kind to resolve one for");

        var kind = SourceKindDispatch.Classify(def.Kind);
        IReplayableSourceGrain grain = kind switch
        {
            SourceKindDispatch.ActorKind.Connector => client.GetGrain<IConnectorGrain>(qualified),
            SourceKindDispatch.ActorKind.Generator => client.GetGrain<IGeneratorGrain>(qualified),
            SourceKindDispatch.ActorKind.Ingest => client.GetGrain<IIngestSourceGrain>(qualified),
            SourceKindDispatch.ActorKind.Crdt => throw new NotSupportedException(
                $"source '{sourceName}' is crdt-kind — it has its own replay (plan 020's ReplayAsync), not this one"),
            _ => throw new NotSupportedException($"source '{sourceName}' has an unrecognized kind '{def.Kind}'"),
        };

        var snapshot = await grain.BeginAttachAsync(from);
        try
        {
            long live = 0;
            var handle = await stream.SubscribeAsync((evt, _) =>
                onEvent(evt, evt.Timestamp, snapshot.LastSeq + ++live));

            for (var i = 0; i < snapshot.Rows.Count; i++)
            {
                var row = snapshot.Rows[i];
                var ts = row.TryGetValue(EventRecord.TimestampField, out var v) && v is long l ? l : 0L;
                await onEvent(row, ts, snapshot.Positions[i]);
            }

            return new ReplaySubscriptionHandle(new Handle<EventRecord>(handle), snapshot.FirstSeq, snapshot.LastSeq, snapshot.Truncated);
        }
        finally
        {
            // Begin succeeded (we are past it), so the hold IS ours to drop — always release it, even if
            // subscribing or feeding the snapshot above threw, or the deferred rows the gate queued behind
            // this hold would never be flushed.
            await grain.EndAttachAsync();
        }
    }

    public async Task<IAsyncDisposable> SubscribePipelineAsync(
        string environment, string pipelineId, Func<IReadOnlyList<ResultEnvelope>, Task> onResults)
    {
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(
                StreamConstants.OutputNamespace, EnvKeys.Qualify(environment, pipelineId)));

        return new Handle<List<ResultEnvelope>>(await stream.SubscribeAsync((rows, _) => onResults(rows)));
    }

    public async Task<IAsyncDisposable> SubscribeTableAsync(
        string environment, string tableName, Func<IReadOnlyList<TableDeltaDto>, Task> onDeltas)
    {
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(
                StreamConstants.TableDeltaNamespace, EnvKeys.Qualify(environment, tableName)));

        return new Handle<List<TableDeltaDto>>(await stream.SubscribeAsync((deltas, _) => onDeltas(deltas)));
    }

    /// <summary>Plan 026 wave 2 — the batch-grained twin of the source overload above; see its doc for the
    /// full protocol (Begin → subscribe → feed the retained snapshot → End in a <c>finally</c>, live
    /// positions counted from the snapshot's <c>LastSeq</c>, the same one-pull-period drift note). A batch
    /// IS the stream item here (no per-row position — <see cref="ResultEnvelope.Seq"/> stays the
    /// per-subscription row counter the caller assigns), and the key is always
    /// <c>IPipelineGrain</c>/qualified pipeline ID — no catalog lookup, unlike the source overload, because
    /// there is exactly one grain kind behind a pipeline.</summary>
    public async Task<IEntityReplaySubscription> SubscribePipelineAsync(
        string environment, string pipelineId, ReplayFrom? from,
        Func<IReadOnlyList<ResultEnvelope>, long, Task> onResults)
    {
        var qualified = EnvKeys.Qualify(environment, pipelineId);
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, qualified));

        if (from is null)
        {
            long live = 0;
            var liveHandle = await stream.SubscribeAsync((rows, _) => onResults(rows, ++live));
            return new ReplaySubscriptionHandle(new Handle<List<ResultEnvelope>>(liveHandle), firstSeq: 1, lastSeq: 0, truncated: false);
        }

        var grain = client.GetGrain<IPipelineGrain>(qualified);
        var snapshot = await grain.BeginAttachAsync(from);
        try
        {
            long live = 0;
            var handle = await stream.SubscribeAsync((rows, _) => onResults(rows, snapshot.LastSeq + ++live));

            for (var i = 0; i < snapshot.Items.Count; i++)
            {
                await onResults(snapshot.Items[i], snapshot.Positions[i]);
            }

            return new ReplaySubscriptionHandle(new Handle<List<ResultEnvelope>>(handle), snapshot.FirstSeq, snapshot.LastSeq, snapshot.Truncated);
        }
        finally
        {
            await grain.EndAttachAsync();
        }
    }

    /// <summary>Plan 026 wave 2 — the batch-grained twin of the source overload above, over
    /// <see cref="ITableGrain"/>/qualified table NAME; see that overload's doc for the full protocol. A
    /// coordinator-mode table's <c>BeginAttachAsync</c> returns an empty, <c>Truncated</c> snapshot when a
    /// position was asked for (see <see cref="ITableGrain.EndAttachAsync"/>'s doc) — this method has no
    /// special case for that, it just feeds whatever the gate hands back, which for that case is
    /// nothing.</summary>
    public async Task<IEntityReplaySubscription> SubscribeTableAsync(
        string environment, string tableName, ReplayFrom? from,
        Func<IReadOnlyList<TableDeltaDto>, long, Task> onDeltas)
    {
        var qualified = EnvKeys.Qualify(environment, tableName);
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, qualified));

        if (from is null)
        {
            long live = 0;
            var liveHandle = await stream.SubscribeAsync((deltas, _) => onDeltas(deltas, ++live));
            return new ReplaySubscriptionHandle(new Handle<List<TableDeltaDto>>(liveHandle), firstSeq: 1, lastSeq: 0, truncated: false);
        }

        var grain = client.GetGrain<ITableGrain>(qualified);
        var snapshot = await grain.BeginAttachAsync(from);
        try
        {
            long live = 0;
            var handle = await stream.SubscribeAsync((deltas, _) => onDeltas(deltas, snapshot.LastSeq + ++live));

            for (var i = 0; i < snapshot.Items.Count; i++)
            {
                await onDeltas(snapshot.Items[i], snapshot.Positions[i]);
            }

            return new ReplaySubscriptionHandle(new Handle<List<TableDeltaDto>>(handle), snapshot.FirstSeq, snapshot.LastSeq, snapshot.Truncated);
        }
        finally
        {
            await grain.EndAttachAsync();
        }
    }

    private sealed class Handle<T>(StreamSubscriptionHandle<T> handle) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await handle.UnsubscribeAsync();
    }

    /// <summary>Plan 026 wave 1 — wraps the underlying stream <see cref="IAsyncDisposable"/> with the
    /// replay bookkeeping <see cref="IEntityReplaySubscription"/> promises. Disposing unsubscribes exactly
    /// the stream subscription, same as <see cref="Handle{T}"/>.</summary>
    private sealed class ReplaySubscriptionHandle(IAsyncDisposable inner, long firstSeq, long lastSeq, bool truncated)
        : IEntityReplaySubscription
    {
        public long FirstSeq { get; } = firstSeq;
        public long LastSeq { get; } = lastSeq;
        public bool Truncated { get; } = truncated;

        public async ValueTask DisposeAsync() => await inner.DisposeAsync();
    }
}
