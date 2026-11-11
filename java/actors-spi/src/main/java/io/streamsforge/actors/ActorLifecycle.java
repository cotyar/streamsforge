package io.streamsforge.actors;

import java.util.concurrent.CompletionStage;

/** Optional hooks a state object may implement. Both run as turns (exclusive, serialized). */
public interface ActorLifecycle<S> {
    /** First turn of an activation, before any message. A failed stage fails the triggering call. */
    default CompletionStage<Void> onActivate(ActorContext<S> ctx) {
        return Turns.done();
    }

    /** Last turn of an activation (idle collection, explicit deactivate, runtime shutdown). */
    default CompletionStage<Void> onDeactivate(ActorContext<S> ctx) {
        return Turns.done();
    }
}
