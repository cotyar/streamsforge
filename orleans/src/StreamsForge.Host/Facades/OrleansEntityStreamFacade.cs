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
internal sealed class OrleansEntityStreamFacade(IClusterClient client) : IEntityStreamFacade
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

    /// <summary>Plan 026 wave 1 — STUB pinned by the orchestrator so both hosts build; wave 1 agent B
    /// replaces it with the gated implementation (BeginAttachAsync(from) on the source's driver grain →
    /// subscribe → replay → EndAttachAsync, positions continuing from LastSeq).</summary>
    public Task<IEntityReplaySubscription> SubscribeSourceAsync(
        string environment, string sourceName, ReplayFrom? from,
        Func<IReadOnlyDictionary<string, object?>, long, long, Task> onEvent) =>
        throw new NotImplementedException("plan 026 wave 1 agent B");

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

    private sealed class Handle<T>(StreamSubscriptionHandle<T> handle) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await handle.UnsubscribeAsync();
    }
}
