using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore;
using System.Threading;
using V1 = StreamsForge.Host.Grpc.V1;

namespace StreamsForge.Host.Grpc;

/// <summary>gRPC server-streaming mirror of the SignalR StreamHub/StreamBridgeService relays: raw
/// source events, pipeline results, and table deltas. Subscribes at the same seam the SignalR bridge
/// relays from — <see cref="IEntityStreamFacade"/>, which is Orleans streams on one flavor and the Dapr
/// topic fan-out on the other (plan 025 G1) — for the lifetime of the gRPC call; client disconnect
/// cancels ServerCallContext.CancellationToken, which unsubscribes.
///
/// <para>Plan 015 wave 3-B: each subscription keeps its <c>[Authorize(Policy = "Viewer")]</c> floor and
/// additionally asks <see cref="AccessGuard"/> for the READ action on the entity being subscribed to —
/// the same action the REST route that returns that entity's rows asks for. Subscribing to a stream is
/// reading the entity, continuously, so anything weaker than <c>{source,pipeline,table}.read</c> would
/// make the streaming surface the way around every read entitlement.</para>
///
/// <para><b>The entity is looked up only for its Tags, and a miss is not a 404.</b> Subscribing to a
/// name that does not exist has always been legal here (the Orleans stream simply never fires; a client
/// may subscribe before the entity is created), and turning that into a NotFound would be a behaviour
/// change this wave has no business making. So the lookup is best-effort: found → check with its tags,
/// absent → check with none, which can only ever narrow the answer.</para>
///
/// <para><b>Revocation does not reach a live subscription.</b> The check happens at subscribe time and
/// nothing re-checks per frame — see the identical note on <c>StreamHub</c>, which states the ceiling and
/// the upgrade path once for both transports.</para></summary>
public sealed class StreamGrpcService(ICatalogFacade catalog, IEntityStreamFacade streams, AccessGuard guard)
    : V1.StreamService.StreamServiceBase
{
    // Plan 025 G1 — see SourceGrpcService for why the injected ICatalogFacade is the same environment-
    // scoped catalog the removed `client.RegistryFor(EnvironmentAmbient.Current)` property produced.
    private ICatalogFacade Registry => catalog;

    /// <summary>Plan 026 wave 1 — now always goes through <see cref="IEntityStreamFacade"/>'s replaying
    /// overload (with <c>from: null</c> when neither request field is set), so <c>Position</c> is written
    /// on every event including the pre-026 live-only path; <c>Seq</c> stays the per-subscription counter
    /// it always was, so nothing an existing client asserts on changes.
    ///
    /// <para><b>Buffering, and why.</b> The facade's replaying overload feeds the ENTIRE retained snapshot
    /// through this callback before it returns the subscription handle (see
    /// <c>OrleansEntityStreamFacade.SubscribeSourceAsync</c>'s own doc) — so at the moment the first
    /// replayed row arrives, <c>handle.Truncated</c> (needed for <see cref="V1.SourceEvent.ReplayTruncated"/>
    /// on the FIRST event) is not known yet, and it must not be known: writing untruncated-or-not before
    /// the whole snapshot is accounted for would be guessing. Every event is therefore buffered under
    /// <c>writeGate</c> until the awaited call returns and the flag is known, then flushed in the order it
    /// arrived (a live row that races the handoff and gets buffered too is written in its correct position,
    /// not out of order) — after which the callback writes straight through. <c>writeGate</c> also
    /// serializes every write against <see cref="IServerStreamWriter{T}.WriteAsync"/>'s own "no concurrent
    /// calls" rule, which the buffer-vs-direct-write split would otherwise be free to violate.</para>
    ///
    /// <para>A <see cref="NotSupportedException"/> from the facade (a <c>crdt</c>-kind or unknown source
    /// asked to replay from a position) becomes <see cref="StatusCode.FailedPrecondition"/> — the caller
    /// asked for something this source cannot do, not a not-found or a permission problem.</para></summary>
    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeSource(
        V1.SubscribeSourceRequest request,
        IServerStreamWriter<V1.SourceEvent> responseStream,
        ServerCallContext context)
    {
        await GrpcAccess.EnsureAsync(
            guard, context, Actions.SourceRead, request.Name,
            (await Registry.GetSourceAsync(request.Name))?.Tags);

        ReplayFrom? from = request.FromSeq > 0
            ? new ReplayFrom { Seq = request.FromSeq }
            : request.FromTimestampMs > 0
                ? new ReplayFrom { TimestampMs = request.FromTimestampMs }
                : null;

        long seq = 0;
        var writeGate = new SemaphoreSlim(1, 1);
        var buffered = new List<V1.SourceEvent>();
        var flushed = false;
        var truncateNextLive = false;

        IEntityReplaySubscription handle;
        try
        {
            handle = await streams.SubscribeSourceAsync(
                EnvironmentAmbient.Current, request.Name, from, async (row, tsMs, position) =>
                {
                    var evt = new V1.SourceEvent
                    {
                        SourceName = request.Name,
                        Seq = ++seq,
                        TimestampMs = tsMs,
                        Row = GrpcValueConverter.ToStruct(row),
                        Position = position,
                    };

                    await writeGate.WaitAsync();
                    try
                    {
                        if (!flushed)
                        {
                            buffered.Add(evt);
                            return;
                        }

                        if (truncateNextLive)
                        {
                            evt.ReplayTruncated = true;
                            truncateNextLive = false;
                        }
                        await responseStream.WriteAsync(evt);
                    }
                    finally
                    {
                        writeGate.Release();
                    }
                });
        }
        catch (NotSupportedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }

        await writeGate.WaitAsync();
        try
        {
            if (buffered.Count > 0)
            {
                buffered[0].ReplayTruncated = handle.Truncated;
                foreach (var evt in buffered)
                {
                    await responseStream.WriteAsync(evt);
                }
            }
            else if (handle.Truncated)
            {
                // Nothing retained matched the request — surface the truncation on the first LIVE event
                // rather than lose it silently.
                truncateNextLive = true;
            }
            flushed = true;
        }
        finally
        {
            writeGate.Release();
        }

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    /// <summary>Plan 026 wave 2 — same replaying-overload-always shape as <see cref="SubscribeSource"/>
    /// above: <c>from: null</c> when neither wire field is set, so <c>Position</c> is written on every
    /// batch's rows including the pre-026 live-only path. One BATCH is one <see cref="ReplayFrom"/>
    /// position; every row it flattens to shares that <see cref="V1.ResultEnvelope.Position"/> while
    /// <see cref="V1.ResultEnvelope.Seq"/> stays the per-subscription, per-ROW counter it always was.
    /// <see cref="SubscribeWithReplayBufferAsync{T}"/> carries the buffer-until-truncation-known rule and
    /// the crdt/unsupported-source refusal mapping identically to <see cref="SubscribeSource"/> — see
    /// that method's doc for why both exist.</summary>
    [Authorize(Policy = "Viewer")]
    public override async Task SubscribePipeline(
        V1.SubscribePipelineRequest request,
        IServerStreamWriter<V1.ResultEnvelope> responseStream,
        ServerCallContext context)
    {
        // Scope is the pipeline's NAME, not the id in the request: an id is a Guid("n") the registry
        // minted, so a `prod-*` scope written by an operator would match nothing at all. Same rule as
        // the REST routes and the chat tools — a grant has to mean one thing on every transport.
        //
        // Plan 016 wave 1: the request field takes an id OR a name. This RPC deliberately does NOT fail
        // on an unknown id (an unknown key just yields a silent stream), so that stays — only an
        // AMBIGUOUS name is answered, and only after the guard, so the candidate ids are not an
        // enumeration oracle for a caller entitled to read neither.
        var hit = await GrpcEntityRef.PipelineAsync(Registry, request.Id);
        var subscribed = hit.Value;
        await GrpcAccess.EnsureAsync(
            guard, context, Actions.PipelineRead, subscribed?.Name ?? request.Id, subscribed?.Tags);
        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, hit.Message));
        }

        // The output stream is keyed by pipeline ID, so a name-addressed subscription resolves to one.
        var pipelineId = subscribed?.Id ?? request.Id;
        var environment = subscribed?.Environment ?? EnvironmentAmbient.Current;

        ReplayFrom? from = request.FromSeq > 0
            ? new ReplayFrom { Seq = request.FromSeq }
            : request.FromTimestampMs > 0
                ? new ReplayFrom { TimestampMs = request.FromTimestampMs }
                : null;

        long seq = 0;
        IEntityReplaySubscription handle;
        try
        {
            handle = await SubscribeWithReplayBufferAsync<IReadOnlyList<ResultEnvelope>>(
                onBatch => streams.SubscribePipelineAsync(environment, pipelineId, from, onBatch),
                async (rows, position, truncated) =>
                {
                    var firstRow = true;
                    foreach (var row in rows)
                    {
                        await responseStream.WriteAsync(new V1.ResultEnvelope
                        {
                            PipelineId = row.PipelineId,
                            Seq = ++seq,
                            TimestampMs = row.TimestampMs,
                            Row = GrpcValueConverter.ToStruct(row.Row),
                            Position = position,
                            ReplayTruncated = firstRow && truncated,
                        });
                        firstRow = false;
                    }
                });
        }
        catch (NotSupportedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    /// <summary>Plan 026 wave 2 — the table-delta twin of <see cref="SubscribePipeline"/> above: one
    /// <see cref="V1.TableDeltaBatch"/> per published batch, carrying its producer
    /// <see cref="V1.TableDeltaBatch.Position"/>; <see cref="V1.TableDeltaBatch.Seq"/> stays the
    /// per-subscription batch counter it always was.</summary>
    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeTable(
        V1.SubscribeTableRequest request,
        IServerStreamWriter<V1.TableDeltaBatch> responseStream,
        ServerCallContext context)
    {
        // Delta streams are keyed by table NAME (see TableGrain). Plan 016 wave 1: the request field
        // now takes an id OR a name through the one resolver, and the delta-stream key is the RESOLVED
        // name — so a caller holding only the id (what the console and the config export carry) can
        // subscribe without a round trip to translate it. The entitlement is checked against that same
        // resolved name, because that is the string an operator would have written into a scope, and
        // against the raw request when nothing resolved. Like SubscribePipeline above, an unknown key
        // is NOT an error here (it yields a silent stream); only an ambiguous name is, after the guard.
        var hit = await GrpcEntityRef.TableAsync(Registry, request.Name);
        var table = hit.Value;
        var tableName = table?.Name ?? request.Name;
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, tableName, table?.Tags);
        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, hit.Message));
        }

        var environment = table?.Environment ?? EnvironmentAmbient.Current;
        ReplayFrom? from = request.FromSeq > 0
            ? new ReplayFrom { Seq = request.FromSeq }
            : request.FromTimestampMs > 0
                ? new ReplayFrom { TimestampMs = request.FromTimestampMs }
                : null;

        long seq = 0;
        IEntityReplaySubscription handle;
        try
        {
            handle = await SubscribeWithReplayBufferAsync<IReadOnlyList<TableDeltaDto>>(
                onBatch => streams.SubscribeTableAsync(environment, tableName, from, onBatch),
                async (deltas, position, truncated) =>
                {
                    seq++;
                    var batch = new V1.TableDeltaBatch
                    {
                        TableName = tableName,
                        Seq = seq,
                        Position = position,
                        ReplayTruncated = truncated,
                    };
                    batch.Deltas.AddRange(deltas.Select(d => new V1.TableDelta
                    {
                        Row = GrpcValueConverter.ToStruct(d.Row),
                        Weight = d.Weight,
                    }));
                    await responseStream.WriteAsync(batch);
                });
        }
        catch (NotSupportedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    /// <summary>Plan 026 wave 2 — the buffer-then-flush dance <see cref="SubscribeSource"/> wrote inline
    /// (see its own doc comment for WHY: the facade delivers the entire retained snapshot before
    /// <c>Truncated</c> is knowable, so every item has to be held until the awaited subscribe call returns
    /// and the flag is known, then flushed in arrival order — a live item racing the replay/live handoff is
    /// buffered too, landing in its correct position rather than out of order), generalized so
    /// <see cref="SubscribePipeline"/> and <see cref="SubscribeTable"/> share it instead of repeating the
    /// semaphore/flag bookkeeping a second and third time. <paramref name="beginSubscribe"/> is the facade
    /// call itself, handed the one per-item callback (item, position) to pass through;
    /// <paramref name="writeAsync"/> is invoked once per item, strictly in arrival order, with
    /// <c>truncated</c> true only for the very first item written (replay or, if nothing was retained,
    /// the first live one) — exactly <see cref="IEntityReplaySubscription.Truncated"/>'s contract. A
    /// <see cref="NotSupportedException"/> from <paramref name="beginSubscribe"/> propagates to the
    /// caller, which maps it to <see cref="StatusCode.FailedPrecondition"/> like <see cref="SubscribeSource"/>
    /// does.</summary>
    private static async Task<IEntityReplaySubscription> SubscribeWithReplayBufferAsync<T>(
        Func<Func<T, long, Task>, Task<IEntityReplaySubscription>> beginSubscribe,
        Func<T, long, bool, Task> writeAsync)
    {
        var writeGate = new SemaphoreSlim(1, 1);
        var buffered = new List<(T Item, long Position)>();
        var flushed = false;
        var truncateNextLive = false;

        var handle = await beginSubscribe(async (item, position) =>
        {
            await writeGate.WaitAsync();
            try
            {
                if (!flushed)
                {
                    buffered.Add((item, position));
                    return;
                }

                var truncated = truncateNextLive;
                truncateNextLive = false;
                await writeAsync(item, position, truncated);
            }
            finally
            {
                writeGate.Release();
            }
        });

        await writeGate.WaitAsync();
        try
        {
            if (buffered.Count > 0)
            {
                var first = true;
                foreach (var (item, position) in buffered)
                {
                    await writeAsync(item, position, first && handle.Truncated);
                    first = false;
                }
            }
            else if (handle.Truncated)
            {
                // Nothing retained matched the request — surface the truncation on the first LIVE item
                // rather than lose it silently.
                truncateNextLive = true;
            }
            flushed = true;
        }
        finally
        {
            writeGate.Release();
        }

        return handle;
    }

    /// <summary>Keeps the RPC alive until the client disconnects/cancels (context.CancellationToken),
    /// then disposes the subscription handle — mirrors StreamBridgeService's subscribe-once-per-name
    /// lifecycle, but scoped to a single gRPC call instead of the whole process. Plan 025 G1: the handle
    /// is <see cref="IAsyncDisposable"/> rather than an Orleans <c>StreamSubscriptionHandle</c>, which is
    /// the only thing that changed here — the swallow-everything-on-unsubscribe rule is unchanged, and is
    /// what keeps a torn-down subscription from turning a normal client disconnect into a logged
    /// error.</summary>
    internal static async Task WaitForCancellationThenUnsubscribeAsync(
        IAsyncDisposable handle, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // expected on client disconnect / call cancellation
        }
        finally
        {
            try
            {
                await handle.DisposeAsync();
            }
            catch
            {
                // best-effort, mirrors StreamBridgeService's unsubscribe try/catch
            }
        }
    }
}
