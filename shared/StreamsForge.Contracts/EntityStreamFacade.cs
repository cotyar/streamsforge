namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 025 (Dapr parity), G1: the ONE runtime-specific primitive the shared gRPC streaming services
// still needed after everything else in Grpc/** had been retargeted onto ICatalogFacade and friends.
//
// StreamGrpcService and DynamicStreamService are the only two of the seven services that do not merely
// read the catalog: they open a live subscription for the lifetime of one server-streaming RPC. On
// Orleans that is `client.GetStreamProvider(...).GetStream<T>(StreamId.Create(ns, key))
// .SubscribeAsync(...)` plus an unsubscribe on cancel; on Dapr there is no per-entity stream to
// subscribe to at all — the flavor has five FIXED pub/sub topics (decision D-D) whose endpoints fan an
// arriving envelope out to every registered sink, so "subscribe to table X" is a per-key handler-list
// entry that the sf-table-delta fan-out consults. The two shapes have nothing in common except this
// interface, which is why it exists rather than the services taking a runtime type.
//
// SHAPE NOTES, each one load-bearing:
//
//  - The KEY is (environment, bare name/id), not a pre-composed string, because the two runtimes
//    compose it identically today (EnvKeys.Qualify) but the composition belongs to the implementation,
//    not to the caller. Both implementations qualify; a caller never sees a qualified key.
//
//  - A source event arrives as (row, timestampMs) rather than as StreamsForge.Engine's EventRecord.
//    Contracts deliberately has no dependency on Engine (decision D-A), and EventRecord is a
//    Dictionary subclass whose Timestamp property is just a typed read of the reserved "_ts" key — so
//    handing the caller the dictionary plus the already-extracted timestamp loses nothing and keeps
//    this file Engine-free. The Orleans implementation passes `evt` and `evt.Timestamp`; the Dapr one
//    passes the envelope's plain dictionary and the "_ts" value it carries.
//
//  - A table subscription hands over the delta LIST only, with no sequence number, even though the
//    Dapr TableDeltaEnvelope carries one. That is not an oversight: the Orleans gRPC surface has always
//    numbered TableDeltaBatch.Seq with a counter local to the subscription (see StreamGrpcService), and
//    plan 025 is a behaviour-preserving move — inventing a way to pass Dapr's own Seq through would
//    change what an Orleans client sees. Both flavors therefore number batches per-subscription.
//
//  - Subscribing returns IAsyncDisposable rather than taking a CancellationToken, because unsubscribe
//    is genuinely asynchronous on Orleans (StreamSubscriptionHandle.UnsubscribeAsync) and because the
//    call sites already own a cancellation wait — they await the RPC's own token and then dispose. A
//    token parameter would have made the implementations own the wait, and then the Orleans one would
//    have had to reproduce the "swallow the unsubscribe failure" rule in two places.
//
//  - Handler exceptions are the IMPLEMENTATION's problem, not the caller's, and the two answers
//    differ: Orleans' stream runtime already isolates a subscriber's failure to that subscriber, while
//    the Dapr fan-out is a plain foreach over a shared handler list inside an HTTP topic endpoint —
//    one throwing handler there would abort the endpoint, turn the topic POST into a 500 and make Dapr
//    redeliver the message to EVERY other subscriber. EntityStreamFanout therefore catches per
//    handler. Stated here so a third runtime cannot get it wrong by reading only the interface.
// ============================================================================

/// <summary>Live per-entity subscriptions for the shared gRPC streaming services — the one seam under
/// <c>shared/StreamsForge.Api/Grpc/**</c> whose implementation is genuinely runtime-specific. See the
/// block comment above this interface for the shape decisions.</summary>
public interface IEntityStreamFacade
{
    /// <summary>Raw events published by one source. <paramref name="onEvent"/> receives the event row
    /// and its epoch-millisecond timestamp (the row's reserved <c>"_ts"</c> key, already extracted).</summary>
    /// <param name="environment">Environment key — the empty string for the default environment
    /// (StreamsForge.AppCore.Environments.EnvKeys.Default). The implementation qualifies; the caller never does.</param>
    /// <param name="sourceName">Bare source name, unqualified.</param>
    /// <returns>Disposing unsubscribes exactly this subscription and nothing else.</returns>
    Task<IAsyncDisposable> SubscribeSourceAsync(
        string environment, string sourceName, Func<IReadOnlyDictionary<string, object?>, long, Task> onEvent);

    /// <summary>Plan 026 wave 1 — the same subscription, started from a position or an event time.
    /// Retained entries matching <paramref name="from"/> are delivered through <paramref name="onEvent"/>
    /// FIRST (oldest first, each with its producer position), then live events follow with consecutive
    /// positions; the returned handle says what the producer retained and whether the request reached
    /// past it. <paramref name="from"/> null = live only, from the current position (a plain
    /// <see cref="SubscribeSourceAsync(string, string, Func{IReadOnlyDictionary{string, object?}, long, Task})"/>
    /// with positions). The callback's third argument is the position. Orleans runs plan 023's attach
    /// gate around the subscribe so nothing is both replayed and delivered live; Dapr accepts and
    /// ignores <paramref name="from"/> until its port (dapr/PARITY.md).</summary>
    Task<IEntityReplaySubscription> SubscribeSourceAsync(
        string environment, string sourceName, ReplayFrom? from,
        Func<IReadOnlyDictionary<string, object?>, long, long, Task> onEvent);

    /// <summary>Result batches emitted by one running pipeline, addressed by pipeline ID (the key both
    /// runtimes publish under — a name-addressed subscription resolves to an id before calling this).</summary>
    Task<IAsyncDisposable> SubscribePipelineAsync(
        string environment, string pipelineId, Func<IReadOnlyList<ResultEnvelope>, Task> onResults);

    /// <summary>Z-set delta batches for one table, addressed by table NAME (the key both runtimes
    /// publish under). No sequence number — see the block comment.</summary>
    Task<IAsyncDisposable> SubscribeTableAsync(
        string environment, string tableName, Func<IReadOnlyList<TableDeltaDto>, Task> onDeltas);
}

/// <summary>Plan 026 — a replaying subscription's handle. Disposing unsubscribes exactly this subscription.
/// <see cref="FirstSeq"/>/<see cref="LastSeq"/> are what the producer retained at attach time (not the
/// selection); <see cref="Truncated"/> is true when the request reached past the oldest retained entry
/// — the honest "you got the last N of M", never a silent gap.</summary>
public interface IEntityReplaySubscription : IAsyncDisposable
{
    long FirstSeq { get; }
    long LastSeq { get; }
    bool Truncated { get; }
}
