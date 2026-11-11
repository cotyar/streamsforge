package io.streamsforge.host;

import io.streamsforge.actors.ActorRef;
import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.Turn;
import io.streamsforge.actors.Turns;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Shared, immutable-after-construction environment handed to every actor state. */
final class HostEnv {
    final ActorRuntime rt;
    final StreamsHost.Options opts;
    /** Asynchronous messages issued by {@link #send} that have not completed yet (see StreamsHost.quiesce). */
    final AtomicLong inflight = new AtomicLong();
    final Queue<Throwable> errors = new ConcurrentLinkedQueue<>();

    HostEnv(ActorRuntime rt, StreamsHost.Options opts) {
        this.rt = rt;
        this.opts = opts;
    }

    <S> ActorRef<S> ref(String type, String id) { return rt.ref(type, id); }

    ActorRef<TableView> viewRef(String table, int parallelism) {
        return rt.ref(parallelism == 1 ? Actors.TABLE : Actors.OUTPUT, table);
    }

    /**
     * Fire-and-forget (but accounted) async turn: used for every hop where the sender must NOT wait for the
     * receiver (publisher -> subscriber, timer tick -> own flush). Failures land in {@link #errors}.
     */
    <S, R> void send(ActorRef<S> ref, Turn<S, R> turn) {
        inflight.incrementAndGet();
        try {
            ref.ask(turn).whenComplete((v, e) -> {
                if (e != null) errors.add(Async.unwrap(e));
                inflight.decrementAndGet();
            });
        } catch (Throwable t) {
            errors.add(t);
            inflight.decrementAndGet();
        }
    }

    /** Awaited synchronous turn with no result. */
    <S> CompletionStage<Void> exec(ActorRef<S> ref, Consumer<S> fn) {
        return ref.ask(ctx -> {
            fn.accept(ctx.state());
            return Turns.done();
        });
    }
}
