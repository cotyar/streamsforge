package io.streamsforge.actors.pekko;

import io.streamsforge.actors.ActorKey;
import io.streamsforge.actors.ActorLifecycle;
import io.streamsforge.actors.ActorType;
import io.streamsforge.actors.Turns;
import io.streamsforge.actors.pekko.Protocol.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.pekko.actor.Cancellable;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.StashBuffer;

/**
 * One activation. The behavior has three states:
 *
 * <ul>
 *   <li><b>ready</b> — runs the next message as a turn. A turn that returns an already-completed stage
 *       finishes inline and the actor stays ready.
 *   <li><b>busy</b> — an async turn (or async {@code onActivate}) is in flight. The turn's stage is
 *       {@code pipeToSelf}'d back as a {@code TurnDone}; until it arrives every other message is
 *       <em>stashed</em>. On {@code TurnDone} the stash is replayed with {@code unstashAll(ready)}. This
 *       is "non-reentrant mailbox" expressed with Pekko's own stash.
 *   <li><b>deactivating</b> — the async {@code onDeactivate} turn is in flight; stops on completion.
 * </ul>
 *
 * Idle collection is Pekko's receive timeout: when it fires the entity asks its shard to
 * {@link Protocol.Passivate} it (the shard then sends {@link Protocol.Stop}; see {@link Shard}). The
 * timer ticks are scheduled with {@code ctx.scheduleOnce} (re-armed as each tick starts) and are
 * {@code NotInfluenceReceiveTimeout}, so timers do not keep an activation alive. (Pekko's
 * {@code TimerScheduler} was tried first and resets the receive timeout, so it cannot be used here.)
 */
final class Entity {

    private static final class TimerDef {
        final long gen;
        final Duration period;
        final Consumer<io.streamsforge.actors.ActorContext<Object>> tick;
        Cancellable handle = Cancellable.alreadyCancelled();

        TimerDef(long gen, Duration period, Consumer<io.streamsforge.actors.ActorContext<Object>> tick) {
            this.gen = gen;
            this.period = period;
            this.tick = tick;
        }
    }

    private final PekkoActorRuntime rt;
    private final ActorKey key;
    private final ActorType<Object> type;
    private final ActorRef<ShardMsg> shard;
    private final ActorContext<EntityMsg> pctx;
    private final StashBuffer<EntityMsg> stash;
    private final long idleNanos;

    private Object state;
    private boolean activated;
    private Ctx ctx;
    private boolean deactReq;
    private boolean passivating;
    private long lastActivity = System.nanoTime();
    private long gen;
    private final Map<String, TimerDef> timerDefs = new HashMap<>();

    private final Behavior<EntityMsg> ready = Behaviors.<EntityMsg>receive((c, m) -> onReady(m));
    private final Behavior<EntityMsg> busy = Behaviors.<EntityMsg>receive((c, m) -> onBusy(m));
    private final Behavior<EntityMsg> deactivating = Behaviors.<EntityMsg>receive((c, m) -> onDeactivating(m));

    private Entity(
            PekkoActorRuntime rt,
            ActorKey key,
            ActorRef<ShardMsg> shard,
            ActorContext<EntityMsg> pctx,
            StashBuffer<EntityMsg> stash) {
        this.rt = rt;
        this.key = key;
        this.type = rt.typeOf(key);
        this.shard = shard;
        this.pctx = pctx;
        this.stash = stash;
        this.idleNanos = type.idleTimeout() == null ? 0 : Math.max(1, type.idleTimeout().toNanos());
    }

    static Behavior<EntityMsg> create(PekkoActorRuntime rt, ActorKey key, ActorRef<ShardMsg> shard) {
        return Behaviors.<EntityMsg>setup(pctx -> Behaviors.<EntityMsg>withStash(Integer.MAX_VALUE, stash -> {
            Entity e = new Entity(rt, key, shard, pctx, stash);
            if (e.idleNanos > 0) pctx.setReceiveTimeout(Duration.ofNanos(e.idleNanos), new Idle());
            return e.ready;
        }));
    }

    // ------------------------------------------------------------------ ready

    private Behavior<EntityMsg> onReady(EntityMsg m) {
        switch (m) {
            case Run r -> {
                lastActivity = System.nanoTime();
                return execute(r.work());
            }
            case TimerFire t -> {
                return onTimer(t);
            }
            case Stop s -> {
                return beginDeactivation();
            }
            case Idle i -> {
                onIdle();
                return Behaviors.same();
            }
            default -> {
                return Behaviors.same(); // TurnDone/ActivateDone/DeactDone only exist in other states
            }
        }
    }

    private void onIdle() {
        long rem = lastActivity + idleNanos - System.nanoTime();
        if (rem > 0) {
            pctx.setReceiveTimeout(Duration.ofNanos(rem), new Idle()); // not idle after all (timer ticks, stashed idle)
        } else {
            passivate();
        }
    }

    private void passivate() {
        if (!passivating) {
            passivating = true;
            shard.tell(new Passivate(key, pctx.getSelf()));
        }
    }

    private Behavior<EntityMsg> onTimer(TimerFire t) {
        TimerDef d = timerDefs.get(t.name());
        if (d == null || d.gen != t.gen()) return Behaviors.same(); // cancelled or replaced
        // Re-arm BEFORE running the tick: there is then at most one tick outstanding (queued or stashed)
        // however long the actor stays busy, which is the "coalesced" behavior the SPI asks for.
        d.handle = arm(t.name(), t.gen(), d.period);
        return execute(new Work() {
            @Override
            public CompletionStage<?> run(io.streamsforge.actors.ActorContext<Object> c) {
                d.tick.accept(c);
                return Turns.done();
            }

            @Override
            public void complete(Object value) {}

            @Override
            public void fail(Throwable e) {
                rt.report(e);
            }

            @Override
            public boolean isTimer() {
                return true;
            }
        });
    }

    /** Starts a turn; returns the behavior to continue with. */
    private Behavior<EntityMsg> execute(Work w) {
        if (!activated) {
            CompletionStage<Void> act = null;
            try {
                state = type.stateFactory().apply(key);
                ctx = new Ctx();
                if (state instanceof ActorLifecycle<?> lc) {
                    @SuppressWarnings("unchecked")
                    ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                    act = l.onActivate(ctx);
                }
            } catch (Throwable e) {
                resetActivation();
                w.fail(unwrap(e));
                return Behaviors.same();
            }
            if (act == null) {
                activated = true;
            } else if (act instanceof CompletableFuture<Void> cf && cf.isDone()) {
                if (cf.isCompletedExceptionally()) {
                    resetActivation();
                    w.fail(failureOf(cf));
                    return Behaviors.same();
                }
                activated = true;
            } else {
                pctx.pipeToSelf(act, (v, e) -> new ActivateDone(w, e));
                return busy;
            }
        }
        return runBody(w);
    }

    private Behavior<EntityMsg> runBody(Work w) {
        CompletionStage<?> st;
        try {
            st = w.run(ctx);
            if (st == null) st = Turns.done();
        } catch (Throwable e) {
            w.fail(unwrap(e));
            return finished(w);
        }
        if (st instanceof CompletableFuture<?> cf && cf.isDone()) {
            if (cf.isCompletedExceptionally()) w.fail(failureOf(cf));
            else w.complete(cf.join());
            return finished(w);
        }
        pipe(st, w);
        return busy;
    }

    private <U> void pipe(CompletionStage<U> st, Work w) {
        pctx.pipeToSelf(st, (v, e) -> new TurnDone(w, v, e));
    }

    /** Bookkeeping after a turn completes. Returns ready (we are not in a stash replay here). */
    private Behavior<EntityMsg> finished(Work w) {
        if (!w.isTimer()) lastActivity = System.nanoTime();
        if (deactReq) {
            deactReq = false;
            passivate();
        }
        return Behaviors.same();
    }

    // ------------------------------------------------------------------ busy

    private Behavior<EntityMsg> onBusy(EntityMsg m) {
        switch (m) {
            case TurnDone d -> {
                if (d.error() != null) d.work().fail(unwrap(d.error()));
                else d.work().complete(d.value());
                finished(d.work());
                return stash.unstashAll(ready);
            }
            case ActivateDone d -> {
                if (d.error() != null) {
                    resetActivation();
                    d.work().fail(unwrap(d.error()));
                    return stash.unstashAll(ready);
                }
                activated = true;
                Behavior<EntityMsg> next = runBody(d.work());
                // runBody returns `busy` if the body is async too; otherwise we are ready again
                return next == busy ? busy : stash.unstashAll(ready);
            }
            default -> {
                stash.stash(m);
                return Behaviors.same();
            }
        }
    }

    // ------------------------------------------------------------------ deactivation

    private Behavior<EntityMsg> beginDeactivation() {
        cancelTimers();
        pctx.cancelReceiveTimeout();
        CompletionStage<Void> st = null;
        if (activated && state instanceof ActorLifecycle<?> lc) {
            try {
                @SuppressWarnings("unchecked")
                ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                st = l.onDeactivate(ctx);
            } catch (Throwable e) {
                rt.report(e);
            }
        }
        if (st == null) return Behaviors.stopped();
        if (st instanceof CompletableFuture<Void> cf && cf.isDone()) {
            if (cf.isCompletedExceptionally()) rt.report(failureOf(cf));
            return Behaviors.stopped();
        }
        pctx.pipeToSelf(st, (v, e) -> new DeactDone(e));
        return deactivating;
    }

    private Behavior<EntityMsg> onDeactivating(EntityMsg m) {
        switch (m) {
            case DeactDone d -> {
                if (d.error() != null) rt.report(unwrap(d.error()));
                return Behaviors.stopped();
            }
            case Run r -> {
                // Cannot happen (the shard buffers once it has sent Stop); never drop, hand it back.
                shard.tell(new Deliver(key, r.work()));
                return Behaviors.same();
            }
            default -> {
                return Behaviors.same();
            }
        }
    }

    private void resetActivation() {
        state = null;
        ctx = null;
        activated = false;
        cancelTimers();
    }

    private void cancelTimers() {
        for (TimerDef d : timerDefs.values()) d.handle.cancel();
        timerDefs.clear();
    }

    private Cancellable arm(String name, long g, Duration delay) {
        return pctx.scheduleOnce(delay, pctx.getSelf(), new TimerFire(name, g));
    }

    // ------------------------------------------------------------------ context

    private final class Ctx implements io.streamsforge.actors.ActorContext<Object> {
        @Override
        public ActorKey key() {
            return key;
        }

        @Override
        public Object state() {
            return state;
        }

        @Override
        public io.streamsforge.actors.ActorRef<Object> self() {
            return rt.ref(key);
        }

        @Override
        public io.streamsforge.actors.ActorRuntime runtime() {
            return rt;
        }

        @Override
        public void startTimer(
                String name,
                Duration initialDelay,
                Duration period,
                Consumer<io.streamsforge.actors.ActorContext<Object>> tick) {
            if (period.isZero() || period.isNegative()) throw new IllegalArgumentException("period must be positive");
            long g = ++gen;
            TimerDef old = timerDefs.remove(name);
            if (old != null) old.handle.cancel();
            TimerDef def = new TimerDef(g, period, tick);
            timerDefs.put(name, def);
            def.handle = arm(name, g, initialDelay.isNegative() ? Duration.ZERO : initialDelay);
        }

        @Override
        public void cancelTimer(String name) {
            TimerDef old = timerDefs.remove(name);
            if (old != null) old.handle.cancel();
        }

        @Override
        public void deactivateOnIdle() {
            deactReq = true;
        }
    }

    private static Throwable unwrap(Throwable e) {
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    private static Throwable failureOf(CompletableFuture<?> failed) {
        try {
            failed.join();
            return new IllegalStateException("not failed");
        } catch (CompletionException e) {
            return unwrap(e);
        }
    }
}
