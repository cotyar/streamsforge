using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.Streams;

namespace StreamsForge.Host.Streaming;

/// <summary>
/// An <see cref="IStreamProvider"/> backed by <see cref="PushStreamBus"/> instead of Orleans' pulling
/// agents. Registered under the SAME provider name as the memory-stream provider it replaces
/// (<c>StreamConstants.ProviderName</c>) when <c>Streams:Transport=push</c>, which is what lets every
/// existing producer and consumer — <c>this.GetStreamProvider(...)</c> in grains,
/// <c>client.GetStreamProvider(...)</c> in StreamBridgeService and the gRPC streaming services — keep
/// working unchanged: identical stream namespaces/keys, identical payload types, identical call sites.
/// Only the transport underneath differs. See <see cref="PushStreamBus"/> for the ordering, deadlock and
/// backpressure reasoning.
/// </summary>
public sealed class PushStreamProvider(string name, PushStreamBus bus, IGrainContextAccessor? contextAccessor) : IStreamProvider
{
    private readonly ConcurrentDictionary<(StreamId Id, Type Payload), object> _streams = new();

    public string Name { get; } = name;

    /// <summary>No queue, no cursors — a push stream cannot be replayed from a sequence token. (Orleans'
    /// memory adapter IS rewindable — <c>MemoryAdapterFactory.IsRewindable</c> returns true — but only
    /// within one <c>SimpleQueueCache</c> of 4096 events per hash-ring queue shared by every stream in the
    /// silo, and nothing in this codebase subscribes with a token: plan 026's replay lives ABOVE the
    /// transport, in each producer's own log, so it works identically here.)</summary>
    public bool IsRewindable => false;

    public PushStreamBus Bus => bus;

    public IAsyncStream<T> GetStream<T>(StreamId streamId) =>
        (IAsyncStream<T>)_streams.GetOrAdd((streamId, typeof(T)), _ => new PushAsyncStream<T>(Name, streamId, bus, contextAccessor));
}

/// <summary>Per (stream id, payload type) handle onto the bus. See <see cref="PushStreamProvider"/>.</summary>
public sealed class PushAsyncStream<T>(string providerName, StreamId streamId, PushStreamBus bus, IGrainContextAccessor? contextAccessor)
    : IAsyncStream<T>
{
    public bool IsRewindable => false;

    public string ProviderName => providerName;

    public StreamId StreamId => streamId;

    // ---- producer side -------------------------------------------------------------------------

    /// <summary>Synchronous fan-out onto every subscriber's channel — never blocks, never awaits a
    /// consumer. This is the property that keeps producer grain turns free of downstream call cycles.</summary>
    public Task OnNextAsync(T item, StreamSequenceToken? token = null)
    {
        bus.Publish(streamId, item);
        return Task.CompletedTask;
    }

    public Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken? token = null)
    {
        foreach (var item in batch) bus.Publish(streamId, item);
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync() => Task.CompletedTask;

    public Task OnErrorAsync(Exception ex) => Task.CompletedTask;

    // ---- consumer side -------------------------------------------------------------------------

    public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncObserver<T> observer) =>
        SubscribeCoreAsync(item => observer.OnNextAsync((T)item!, null));

    public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncObserver<T> observer, StreamSequenceToken? token, string? filterData = null) =>
        SubscribeCoreAsync(item => observer.OnNextAsync((T)item!, null));

    public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncBatchObserver<T> observer) =>
        SubscribeCoreAsync(item => observer.OnNextAsync([new SequentialItem<T>((T)item!, null!)]));

    public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncBatchObserver<T> observer, StreamSequenceToken? token) =>
        SubscribeCoreAsync(item => observer.OnNextAsync([new SequentialItem<T>((T)item!, null!)]));

    private Task<StreamSubscriptionHandle<T>> SubscribeCoreAsync(Func<object?, Task> handler)
    {
        // Captured HERE, on the subscriber's own thread: inside a grain turn this is the subscribing
        // activation; on a hosted service / gRPC call scope it is usually null.
        //
        // ONLY a real grain activation gets the turn hop. In this co-hosted process the ambient context on
        // a thread that has just awaited a grain call can also be the CLIENT's own grain context (Orleans'
        // hosted client is an IGrainContext + IGrainExtensionBinder too) — StreamBridgeService hits exactly
        // that. A client context is not an activation: it has no turn to enter, its Deactivated task is
        // already completed (which would tear the subscription down the instant it was created), and
        // memory streams deliver to a client subscription by invoking the callback directly anyway. The
        // discriminator is the hosted grain instance: every real grain implements IGrainBase, no client or
        // system-target context does.
        var ambient = contextAccessor?.GrainContext;
        var context = ambient?.GrainInstance is IGrainBase ? ambient : null;
        var entry = bus.Subscribe(streamId, handler, context);

        if (context is not null)
        {
            if (context is not IGrainExtensionBinder binder)
            {
                _ = bus.UnsubscribeAsync(entry.Id);
                throw new NotSupportedException(
                    $"Push streams cannot deliver to grain context {context.GrainId}: it does not support grain extensions.");
            }

            // One extension per activation, shared by all of its subscriptions (see PushDeliveryExtension).
            var (_, reference) = binder.GetOrSetExtension<PushDeliveryExtension, IPushDeliveryExtension>(
                () => new PushDeliveryExtension(bus, context));
            entry.Target = reference;

            // An activation that goes away without unsubscribing must not keep receiving into its dead
            // closures — mirrors an explicit memory-stream subscription's in-grain callback dying with it.
            _ = context.Deactivated.ContinueWith(
                _ => bus.RemoveAllFor(context), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

        bus.StartPump(entry);
        return Task.FromResult<StreamSubscriptionHandle<T>>(new PushStreamSubscriptionHandle<T>(providerName, streamId, entry, bus));
    }

    public Task<IList<StreamSubscriptionHandle<T>>> GetAllSubscriptionHandles()
    {
        var ambient = contextAccessor?.GrainContext;
        var context = ambient?.GrainInstance is IGrainBase ? ambient : null;
        IList<StreamSubscriptionHandle<T>> handles = bus.SubscriptionsFor(streamId, context)
            .Select(e => (StreamSubscriptionHandle<T>)new PushStreamSubscriptionHandle<T>(providerName, streamId, e, bus))
            .ToList();
        return Task.FromResult(handles);
    }

    // ---- identity ------------------------------------------------------------------------------

    public bool Equals(IAsyncStream<T>? other) => other is not null && other.StreamId.Equals(streamId) && other.ProviderName == providerName;

    public override bool Equals(object? obj) => obj is IAsyncStream<T> other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(providerName, streamId);

    public int CompareTo(IAsyncStream<T>? other) => other is null ? 1 : streamId.CompareTo(other.StreamId);
}

/// <summary>Unsubscribe handle for a push subscription. <c>ResumeAsync</c> is unsupported (no replayable
/// log exists — see <see cref="PushStreamProvider.IsRewindable"/>); nothing in this codebase calls it.</summary>
public sealed class PushStreamSubscriptionHandle<T>(string providerName, StreamId streamId, PushSubscriptionEntry entry, PushStreamBus bus)
    : StreamSubscriptionHandle<T>
{
    public override StreamId StreamId => streamId;

    public override string ProviderName => providerName;

    public override Guid HandleId => entry.Id;

    public override Task UnsubscribeAsync() => bus.UnsubscribeAsync(entry.Id);

    public override Task<StreamSubscriptionHandle<T>> ResumeAsync(IAsyncObserver<T> observer, StreamSequenceToken? token = null) =>
        throw new NotSupportedException("Push streams are not rewindable; re-subscribe instead of resuming.");

    public override Task<StreamSubscriptionHandle<T>> ResumeAsync(IAsyncBatchObserver<T> observer, StreamSequenceToken? token = null) =>
        throw new NotSupportedException("Push streams are not rewindable; re-subscribe instead of resuming.");

    public override bool Equals(StreamSubscriptionHandle<T>? other) => other is not null && other.HandleId == entry.Id;

    public override bool Equals(object? obj) => obj is StreamSubscriptionHandle<T> other && Equals(other);

    public override int GetHashCode() => entry.Id.GetHashCode();
}
