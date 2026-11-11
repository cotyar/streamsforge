package io.streamsforge.actors.loom;

import io.streamsforge.actors.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Virtual actors on JDK 21 virtual threads: <b>one virtual thread per activation</b>, draining a
 * blocking queue. A turn's returned stage is simply <em>joined</em> — the virtual thread parks (cheap,
 * the carrier is released) and the mailbox is closed because nobody is reading it. That is the whole
 * non-reentrancy story: it falls out of straight-line code, which is the point of this runtime.
 *
 * <ul>
 *   <li>Idle collection is {@code queue.poll(remainingIdle)}: the thread wakes with nothing to do and
 *       retires the activation. Timer ticks wake the poll too but do not move {@code lastActivity},
 *       so they never extend the lease.
 *   <li>Deactivation race: sends and the retirement of an activation are serialized by the activation's
 *       {@link ReentrantLock}. {@code offer} refuses once the activation is {@code dead}; the dying
 *       thread, inside {@code ConcurrentHashMap.compute} (the map slot's bin lock), marks it dead,
 *       drains whatever is left in its queue and installs those messages — in order — as the initial
 *       queue of a successor activation in the same slot. A refused sender waits out the handover with
 *       a no-op {@code compute} and retries. Messages that arrive while {@code onDeactivate} is still
 *       running are simply queued behind it (the thread is busy and not reading), then forwarded.
 *   <li>No {@code synchronized}: every critical section uses {@code ReentrantLock} (and none contains
 *       blocking calls), so virtual threads are never pinned by the runtime itself. A user turn that
 *       blocks inside {@code synchronized} will pin its carrier (not our code to fix).
 * </ul>
 */
public final class LoomActorRuntime implements ActorRuntime {

    private final ScheduledThreadPoolExecutor scheduler;
    private final ConcurrentHashMap<String, ActorType<Object>> types = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ActorKey, Activation> activations = new ConcurrentHashMap<>();
    private volatile Consumer<Throwable> errorHandler = t -> t.printStackTrace();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeDone = new CompletableFuture<>();
    private volatile boolean closed;

    public LoomActorRuntime() {
        AtomicInteger sn = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "loom-actors-timer-" + sn.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
    }

    @Override
    public String name() {
        return "loom";
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> void register(ActorType<S> type) {
        if (types.putIfAbsent(type.name(), (ActorType<Object>) type) != null) {
            throw new IllegalArgumentException("actor type already registered: " + type.name());
        }
    }

    @Override
    public <S> ActorRef<S> ref(ActorKey key) {
        if (!types.containsKey(key.type())) throw new IllegalArgumentException("unknown actor type: " + key.type());
        return new Ref<>(key);
    }

    @Override
    public Set<ActorKey> activeKeys() {
        return Set.copyOf(activations.keySet());
    }

    @Override
    public void onUnhandledError(Consumer<Throwable> handler) {
        this.errorHandler = handler;
    }

    private void report(Throwable t) {
        try {
            errorHandler.accept(t);
        } catch (Throwable ignored) {
            // the error sink must not take the runtime down
        }
    }

    @Override
    public CompletionStage<Void> deactivate(ActorKey key) {
        DeactTask d = new DeactTask();
        for (;;) {
            Activation a = activations.get(key);
            if (a == null) return Turns.done();
            if (a.offer(d, false)) {
                a.ensureStarted();
                return d.done;
            }
            activations.compute(key, (k, v) -> v);
        }
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            closeDone.join();
            return;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!activations.isEmpty() && System.nanoTime() < deadline) {
                List<CompletableFuture<Void>> fs = new ArrayList<>();
                for (ActorKey k : activations.keySet()) fs.add(deactivate(k).toCompletableFuture());
                for (CompletableFuture<Void> f : fs) {
                    try {
                        f.get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        report(e);
                    }
                }
            }
        } finally {
            closed = true;
            for (Activation a : activations.values()) a.failEverything(new IllegalStateException("runtime closed"));
            scheduler.shutdownNow();
            closeDone.complete(null);
        }
    }

    private void send(ActorKey key, Task t) {
        if (closed) {
            t.fail(new IllegalStateException("runtime closed"));
            return;
        }
        for (;;) {
            Activation a = activations.get(key);
            if (a == null) a = activations.computeIfAbsent(key, k -> new Activation(k, types.get(k.type()), null));
            if (a.offer(t, true)) {
                a.ensureStarted();
                return;
            }
            activations.compute(key, (k, v) -> v); // dead: wait out the handover, then retry
        }
    }

    private final class Ref<S> implements ActorRef<S> {
        private final ActorKey key;

        Ref(ActorKey key) {
            this.key = key;
        }

        @Override
        public ActorKey key() {
            return key;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> CompletionStage<R> ask(Turn<S, R> turn) {
            CompletableFuture<R> f = new CompletableFuture<>();
            send(key, new AskTask((Turn<Object, Object>) turn, (CompletableFuture<Object>) f));
            return f;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void tell(Consumer<S> fn) {
            send(key, new TellTask((Consumer<Object>) fn));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Ref<?> r && r.key.equals(key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return "ref(" + key + ")";
        }
    }

    // ------------------------------------------------------------------------------------ tasks

    private abstract static class Task {
        abstract CompletionStage<?> run(ActorContext<Object> ctx) throws Exception;

        abstract void complete(Object value);

        abstract void fail(Throwable t);
    }

    private static final class AskTask extends Task {
        final Turn<Object, Object> turn;
        final CompletableFuture<Object> result;

        AskTask(Turn<Object, Object> turn, CompletableFuture<Object> result) {
            this.turn = turn;
            this.result = result;
        }

        @Override
        CompletionStage<?> run(ActorContext<Object> ctx) throws Exception {
            return turn.run(ctx);
        }

        @Override
        void complete(Object value) {
            result.complete(value);
        }

        @Override
        void fail(Throwable t) {
            result.completeExceptionally(t);
        }
    }

    private final class TellTask extends Task {
        final Consumer<Object> fn;

        TellTask(Consumer<Object> fn) {
            this.fn = fn;
        }

        @Override
        CompletionStage<?> run(ActorContext<Object> ctx) {
            fn.accept(ctx.state());
            return Turns.done();
        }

        @Override
        void complete(Object value) {}

        @Override
        void fail(Throwable t) {
            report(t);
        }
    }

    private final class TimerTask extends Task {
        final Timer timer;

        TimerTask(Timer timer) {
            this.timer = timer;
        }

        @Override
        CompletionStage<?> run(ActorContext<Object> ctx) {
            timer.queued.set(false);
            if (!timer.cancelled) timer.tick.accept(ctx);
            return Turns.done();
        }

        @Override
        void complete(Object value) {}

        @Override
        void fail(Throwable t) {
            report(t);
        }
    }

    private static final class DeactTask extends Task {
        final CompletableFuture<Void> done = new CompletableFuture<>();

        @Override
        CompletionStage<?> run(ActorContext<Object> ctx) {
            throw new IllegalStateException();
        }

        @Override
        void complete(Object value) {}

        @Override
        void fail(Throwable t) {
            done.completeExceptionally(t);
        }
    }

    private static final class Timer {
        final Consumer<ActorContext<Object>> tick;
        final AtomicBoolean queued = new AtomicBoolean();
        volatile boolean cancelled;
        volatile ScheduledFuture<?> handle;

        Timer(Consumer<ActorContext<Object>> tick) {
            this.tick = tick;
        }

        void cancel() {
            cancelled = true;
            ScheduledFuture<?> h = handle;
            if (h != null) h.cancel(false);
        }
    }

    // ------------------------------------------------------------------------------------ activation

    private final class Activation {
        final ActorKey key;
        final ActorType<Object> type;
        final long idleNanos;
        final ReentrantLock lock = new ReentrantLock();
        final LinkedBlockingQueue<Task> queue = new LinkedBlockingQueue<>();

        boolean dead; // guarded by lock
        volatile boolean started;
        volatile long lastActivity = System.nanoTime();

        // touched only by the activation's own thread
        Object state;
        boolean activated;
        boolean deactReq;
        Ctx ctx;
        final Map<String, Timer> timers = new HashMap<>();

        Activation(ActorKey key, ActorType<Object> type, List<Task> handedOver) {
            this.key = key;
            this.type = type;
            this.idleNanos = type.idleTimeout() == null ? 0 : Math.max(1, type.idleTimeout().toNanos());
            if (handedOver != null) queue.addAll(handedOver);
        }

        boolean offer(Task t, boolean activity) {
            lock.lock();
            try {
                if (dead) return false;
                queue.add(t);
                if (activity) lastActivity = System.nanoTime();
                return true;
            } finally {
                lock.unlock();
            }
        }

        void ensureStarted() {
            if (started) return;
            lock.lock();
            try {
                if (started) return;
                started = true;
            } finally {
                lock.unlock();
            }
            Thread.ofVirtual().name("actor-" + key).start(this::loop);
        }

        private void loop() {
            try {
                for (;;) {
                    if (deactReq) {
                        deactReq = false;
                        deactivateNow(null);
                        return;
                    }
                    Task t;
                    if (idleNanos == 0) {
                        t = queue.take();
                    } else {
                        long wait = lastActivity + idleNanos - System.nanoTime();
                        if (wait <= 0) {
                            // Lease expired. Anything still queued is a timer tick or a control task (a real
                            // message would have renewed the lease at offer time); take it without renewing.
                            t = queue.poll();
                            if (t == null) {
                                deactivateNow(null);
                                return;
                            }
                            if (t instanceof DeactTask d) {
                                deactivateNow(d);
                                return;
                            }
                            runTask(t);
                            continue;
                        }
                        t = queue.poll(wait, TimeUnit.NANOSECONDS);
                        if (t == null) continue; // timed out or woken by a tick: re-evaluate
                    }
                    if (t instanceof DeactTask d) {
                        deactivateNow(d);
                        return;
                    }
                    runTask(t);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                deactivateNow(null);
            } catch (Throwable e) {
                report(e);
                deactivateNow(null);
            }
        }

        // ---- turns

        private void runTask(Task t) {
            if (!activated) {
                try {
                    state = type.stateFactory().apply(key);
                    ctx = new Ctx(this);
                    if (state instanceof ActorLifecycle<?> lc) {
                        @SuppressWarnings("unchecked")
                        ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                        await(l.onActivate(ctx));
                    }
                    activated = true;
                } catch (Throwable e) {
                    resetActivation();
                    t.fail(unwrap(e));
                    return;
                }
            }
            try {
                CompletionStage<?> st = t.run(ctx);
                Object v = st == null ? null : await(st);
                t.complete(v);
            } catch (Throwable e) {
                t.fail(unwrap(e));
            }
            if (!(t instanceof TimerTask)) lastActivity = System.nanoTime();
        }

        private void resetActivation() {
            state = null;
            ctx = null;
            activated = false;
            cancelTimers();
        }

        // ---- timers

        void startTimer(String name, Duration initialDelay, Duration period, Consumer<ActorContext<Object>> tick) {
            if (period.isZero() || period.isNegative()) throw new IllegalArgumentException("period must be positive");
            Timer old = timers.remove(name);
            if (old != null) old.cancel();
            Timer timer = new Timer(tick);
            timers.put(name, timer);
            timer.handle = scheduler.scheduleAtFixedRate(
                    () -> fire(timer),
                    Math.max(0, initialDelay.toNanos()),
                    period.toNanos(),
                    TimeUnit.NANOSECONDS);
        }

        void cancelTimer(String name) {
            Timer old = timers.remove(name);
            if (old != null) old.cancel();
        }

        void cancelTimers() {
            for (Timer t : timers.values()) t.cancel();
            timers.clear();
        }

        private void fire(Timer timer) {
            if (timer.cancelled || !timer.queued.compareAndSet(false, true)) return;
            if (!offer(new TimerTask(timer), false)) timer.cancel();
        }

        // ---- deactivation

        private void deactivateNow(DeactTask d) {
            cancelTimers();
            if (activated && state instanceof ActorLifecycle<?> lc) {
                try {
                    @SuppressWarnings("unchecked")
                    ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                    CompletionStage<Void> st = l.onDeactivate(ctx);
                    if (st != null) await(st);
                } catch (Throwable e) {
                    report(unwrap(e));
                }
            }
            retire(d);
        }

        private void retire(DeactTask d) {
            List<Task> leftover = new ArrayList<>();
            Activation[] successor = new Activation[1];
            activations.compute(key, (k, cur) -> {
                lock.lock();
                try {
                    dead = true;
                    queue.drainTo(leftover);
                } finally {
                    lock.unlock();
                }
                if (cur != this) return cur;
                List<Task> forward = new ArrayList<>();
                for (Task t : leftover) {
                    if (t instanceof TimerTask || t instanceof DeactTask) continue;
                    forward.add(t);
                }
                if (forward.isEmpty() || closed) return null;
                Activation n = new Activation(key, type, forward);
                successor[0] = n;
                return n;
            });
            state = null;
            ctx = null;
            for (Task t : leftover) {
                if (t instanceof DeactTask other) other.done.complete(null);
                else if (successor[0] == null && !(t instanceof TimerTask)) {
                    t.fail(new IllegalStateException("runtime closed"));
                }
            }
            if (d != null) d.done.complete(null);
            if (successor[0] != null) successor[0].ensureStarted();
        }

        void failEverything(Throwable why) {
            List<Task> rest = new ArrayList<>();
            lock.lock();
            try {
                dead = true;
                queue.drainTo(rest);
            } finally {
                lock.unlock();
            }
            for (Task t : rest) {
                if (t instanceof DeactTask d) d.done.complete(null);
                else if (!(t instanceof TimerTask)) t.fail(why);
            }
        }
    }

    // ------------------------------------------------------------------------------------ context

    private final class Ctx implements ActorContext<Object> {
        private final Activation a;
        private final ActorRef<Object> self;

        Ctx(Activation a) {
            this.a = a;
            this.self = new Ref<>(a.key);
        }

        @Override
        public ActorKey key() {
            return a.key;
        }

        @Override
        public Object state() {
            return a.state;
        }

        @Override
        public ActorRef<Object> self() {
            return self;
        }

        @Override
        public ActorRuntime runtime() {
            return LoomActorRuntime.this;
        }

        @Override
        public void startTimer(String name, Duration initialDelay, Duration period, Consumer<ActorContext<Object>> tick) {
            a.startTimer(name, initialDelay, period, tick);
        }

        @Override
        public void cancelTimer(String name) {
            a.cancelTimer(name);
        }

        @Override
        public void deactivateOnIdle() {
            a.deactReq = true; // only ever called from the activation's own thread, inside a turn
        }
    }

    // ------------------------------------------------------------------------------------ util

    /** Blocks the (virtual) thread until the stage completes; rethrows its failure. */
    private static Object await(CompletionStage<?> st) throws Exception {
        CompletableFuture<?> f;
        if (st instanceof CompletableFuture<?> cf) {
            f = cf;
        } else {
            CompletableFuture<Object> g = new CompletableFuture<>();
            st.whenComplete((v, e) -> {
                if (e != null) g.completeExceptionally(e);
                else g.complete(v);
            });
            f = g;
        }
        try {
            return f.get();
        } catch (ExecutionException e) {
            Throwable c = unwrap(e);
            if (c instanceof Exception ex) throw ex;
            if (c instanceof Error er) throw er;
            throw e;
        }
    }

    private static Throwable unwrap(Throwable e) {
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }
}
