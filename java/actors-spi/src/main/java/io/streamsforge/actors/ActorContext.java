package io.streamsforge.actors;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * What a turn sees. Valid only for the duration of the turn that received it (do not capture it
 * into another thread; capture {@link #self()} instead).
 */
public interface ActorContext<S> {
    ActorKey key();

    /** The activation's state object. Only ever touched by one turn at a time. */
    S state();

    /** A reference to this actor, safe to hand out and to use from anywhere. */
    ActorRef<S> self();

    /** The runtime, for calling other actors. */
    ActorRuntime runtime();

    /**
     * Starts (or replaces) a named periodic timer. Each tick is delivered as an ordinary turn,
     * serialized with every other turn — never concurrently with one. A tick that would fire while
     * the previous tick's turn is still queued is coalesced (skipped), so a slow actor is not
     * flooded. Timers die with the activation and do NOT keep it alive against idle collection
     * (Orleans timer semantics).
     */
    void startTimer(String name, Duration initialDelay, Duration period, Consumer<ActorContext<S>> tick);

    void cancelTimer(String name);

    /** Ask the runtime to deactivate this activation once the current turn completes. */
    void deactivateOnIdle();
}
