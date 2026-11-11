package io.streamsforge.actors.pekko;

import io.streamsforge.actors.ActorKey;
import io.streamsforge.actors.pekko.Protocol.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/**
 * Owner of a slice of the key space and the single place where activations come and go. It runs the
 * same protocol as Pekko Cluster Sharding's <em>passivation</em>, which is what makes the
 * deactivation race tractable:
 *
 * <ol>
 *   <li>An entity that wants to go away (idle, {@code deactivateOnIdle}) sends {@link Passivate};
 *       an explicit {@code deactivate(key)} arrives as {@link DeactivateKey}.
 *   <li>The shard marks the slot <em>stopping</em> and sends {@link Stop} to the entity. Everything the
 *       shard delivered earlier is ahead of {@code Stop} in the entity's mailbox (same sender, FIFO),
 *       so it is processed before {@code onDeactivate} runs.
 *   <li>From then on the shard <em>buffers</em> new work for that key instead of forwarding it. The
 *       entity runs {@code onDeactivate} (possibly async) and stops itself.
 *   <li>On {@link ChildTerminated} (a {@code watchWith} signal) the shard removes the slot, and if
 *       anything was buffered, spawns a fresh incarnation and replays the buffer into it in order.
 * </ol>
 *
 * Because a shard is a single-threaded actor, "slot stopping" and "buffer" need no locks, and the old
 * incarnation is provably dead (Terminated) before the new one sees a message.
 */
final class Shard {
    private Shard() {}

    private static final class Slot {
        final ActorRef<EntityMsg> ref;
        boolean stopping;
        final ArrayDeque<Work> buffer = new ArrayDeque<>();
        final List<CompletableFuture<Void>> waiters = new ArrayList<>(0);

        Slot(ActorRef<EntityMsg> ref) {
            this.ref = ref;
        }
    }

    static Behavior<ShardMsg> create(PekkoActorRuntime rt) {
        return Behaviors.setup(ctx -> new Object() {
            final Map<ActorKey, Slot> slots = new HashMap<>();
            long incarnation;

            Behavior<ShardMsg> behavior() {
                return Behaviors.<ShardMsg>receive((c, msg) -> {
                    switch (msg) {
                        case Deliver d -> deliver(d.key(), d.work());
                        case Passivate p -> {
                            Slot s = slots.get(p.key());
                            if (s != null && s.ref.equals(p.entity())) stop(s);
                        }
                        case DeactivateKey d -> {
                            Slot s = slots.get(d.key());
                            if (s == null) d.done().complete(null);
                            else {
                                s.waiters.add(d.done());
                                stop(s);
                            }
                        }
                        case DeactivateAll d -> {
                            List<CompletableFuture<Void>> all = new ArrayList<>();
                            for (Slot s : slots.values()) {
                                CompletableFuture<Void> f = new CompletableFuture<>();
                                s.waiters.add(f);
                                all.add(f);
                                stop(s);
                            }
                            CompletableFuture.allOf(all.toArray(new CompletableFuture<?>[0]))
                                    .whenComplete((v, e) -> d.done().complete(null));
                        }
                        case ChildTerminated t -> terminated(t.key(), t.entity());
                    }
                    return Behaviors.same();
                });
            }

            Slot spawn(ActorKey key) {
                String name = URLEncoder.encode(key.toString(), StandardCharsets.UTF_8) + "-" + (incarnation++);
                ActorRef<EntityMsg> ref = ctx.spawn(Entity.create(rt, key, ctx.getSelf()), name);
                ctx.watchWith(ref, new ChildTerminated(key, ref));
                Slot s = new Slot(ref);
                slots.put(key, s);
                rt.activeSet().add(key);
                return s;
            }

            void deliver(ActorKey key, Work w) {
                Slot s = slots.get(key);
                if (s == null) s = spawn(key);
                if (s.stopping) s.buffer.add(w);
                else s.ref.tell(new Run(w));
            }

            void stop(Slot s) {
                if (s.stopping) return;
                s.stopping = true;
                s.ref.tell(new Stop());
            }

            void terminated(ActorKey key, ActorRef<EntityMsg> entity) {
                Slot s = slots.get(key);
                if (s == null || !s.ref.equals(entity)) return;
                slots.remove(key);
                rt.activeSet().remove(key);
                if (!s.buffer.isEmpty()) {
                    Slot fresh = spawn(key);
                    for (Work w : s.buffer) fresh.ref.tell(new Run(w));
                }
                for (CompletableFuture<Void> f : s.waiters) f.complete(null);
            }
        }.behavior());
    }
}
