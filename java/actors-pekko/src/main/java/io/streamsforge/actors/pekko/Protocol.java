package io.streamsforge.actors.pekko;

import io.streamsforge.actors.ActorKey;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.NotInfluenceReceiveTimeout;
import org.apache.pekko.actor.typed.ActorRef;

/** Messages of the two actor tiers: {@link Shard}s (placement, passivation protocol) and {@link Entity}s (activations). */
final class Protocol {
    private Protocol() {}

    /** One unit of work for an activation (an ask, a tell or a timer tick). */
    interface Work {
        CompletionStage<?> run(io.streamsforge.actors.ActorContext<Object> ctx) throws Exception;

        void complete(Object value);

        void fail(Throwable t);

        /** Timer ticks do not count as activity for idle collection. */
        default boolean isTimer() {
            return false;
        }
    }

    // ---------------------------------------------------------------- shard protocol

    sealed interface ShardMsg permits Deliver, Passivate, DeactivateKey, DeactivateAll, ChildTerminated {}

    /** From senders: route {@code work} to the activation of {@code key}, spawning it if needed. */
    record Deliver(ActorKey key, Work work) implements ShardMsg {}

    /** From an entity: "I am idle / asked to deactivate; stop me". The shard answers with {@link Stop}. */
    record Passivate(ActorKey key, ActorRef<EntityMsg> entity) implements ShardMsg {}

    record DeactivateKey(ActorKey key, CompletableFuture<Void> done) implements ShardMsg {}

    /** Deactivate everything currently present; {@code done} completes when all of those are gone. */
    record DeactivateAll(CompletableFuture<Void> done) implements ShardMsg {}

    record ChildTerminated(ActorKey key, ActorRef<EntityMsg> entity) implements ShardMsg {}

    // ---------------------------------------------------------------- entity protocol

    sealed interface EntityMsg permits Run, Stop, TimerFire, Idle, TurnDone, ActivateDone, DeactDone {}

    record Run(Work work) implements EntityMsg {}

    /** From the shard, strictly after the last {@link Run} it will ever send to this incarnation. */
    record Stop() implements EntityMsg {}

    /** Sent directly (not through TimerScheduler, whose envelope loses the marker) so it never resets the receive timeout. */
    record TimerFire(String name, long gen) implements EntityMsg, NotInfluenceReceiveTimeout {}

    /** Receive-timeout message. */
    record Idle() implements EntityMsg, NotInfluenceReceiveTimeout {}

    record TurnDone(Work work, Object value, Throwable error) implements EntityMsg {}

    record ActivateDone(Work work, Throwable error) implements EntityMsg {}

    record DeactDone(Throwable error) implements EntityMsg {}
}
