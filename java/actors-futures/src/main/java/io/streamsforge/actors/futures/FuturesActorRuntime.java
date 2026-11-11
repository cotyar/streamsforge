package io.streamsforge.actors.futures;

import io.streamsforge.actors.*;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Virtual actors over {@code CompletableFuture} mailboxes. No thread is ever dedicated to an actor
 * and no thread ever blocks waiting for a turn:
 *
 * <ul>
 *   <li>Each activation owns a queue plus a {@code scheduled} flag (both guarded by one monitor that
 *       is only ever held for a few field updates, never across user code). The first message into an
 *       idle mailbox flips {@code scheduled} and submits {@code drain()} to the executor; {@code drain}
 *       runs turns until the queue is empty.
 *   <li>A turn that returns an incomplete stage parks the mailbox: {@code drain} returns without
 *       clearing {@code scheduled}, and the stage's completion callback re-submits {@code drain}. That
 *       is the whole of "turn-based, non-reentrant" — the next turn is chained on the previous
 *       turn's stage.
 *   <li>Deactivation is a control task in the same queue (so it is ordered after everything already
 *       queued ahead of it and after the in-flight turn). When it completes, the activation is
 *       retired inside {@code ConcurrentHashMap.compute}: under the bin lock the old activation is
 *       marked dead, whatever is left in its queue is moved — in order — into a brand-new activation
 *       installed in the same map slot. A sender that raced and offered to the dead activation sees
 *       {@code offer == false}, waits on the bin via a no-op {@code compute}, and retries against the
 *       map. Hence: nothing is dropped, nothing runs on the dead activation, per-sender FIFO holds
 *       across the handover, and nothing queued behind {@code onDeactivate} can run until it is done.
 * </ul>
 *
 * <p>The default executor is an <em>async-mode ForkJoinPool</em> sized to the CPU count: turns are
 * short, mostly run-to-completion continuations, and a work-stealing FIFO pool gives cheap
 * submit-from-a-worker (actor→actor calls stay on the local deque) without the single shared queue
 * lock of a {@code ThreadPoolExecutor}. The price: a turn that blocks a pool thread (a user bug in
 * this model) starves the pool; there is no compensation.
 */
public final class FuturesActorRuntime implements ActorRuntime {

    private static final int TURN_BUDGET = 64; // turns per drain before yielding the pool thread

    private final Executor executor;
    private final ExecutorService ownedExecutor;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ConcurrentHashMap<String, ActorType<Object>> types = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ActorKey, Activation> activations = new ConcurrentHashMap<>();
    private volatile Consumer<Throwable> errorHandler = t -> t.printStackTrace();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeDone = new CompletableFuture<>();
    private volatile boolean closed;

    public FuturesActorRuntime() {
        this(null);
    }

    /** @param executor where turns run; {@code null} for the default ForkJoinPool (owned, shut down on close). */
    public FuturesActorRuntime(Executor executor) {
        if (executor == null) {
            AtomicInteger n = new AtomicInteger();
            ForkJoinPool pool = new ForkJoinPool(
                    Math.max(2, Runtime.getRuntime().availableProcessors()),
                    p -> {
                        ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                        t.setDaemon(true);
                        t.setName("futures-actors-" + n.incrementAndGet());
                        return t;
                    },
                    null,
                    true);
            this.ownedExecutor = pool;
            this.executor = pool;
        } else {
            this.ownedExecutor = null;
            this.executor = executor;
        }
        AtomicInteger sn = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "futures-actors-timer-" + sn.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
    }

    @Override
    public String name() {
        return "futures";
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
        DeactTask d = new DeactTask(false, true);
        for (;;) {
            Activation a = activations.get(key);
            if (a == null) return Turns.done();
            int r = a.offer(d, false);
            if (r != 0) {
                if (r == 2) a.schedule();
                return d.done;
            }
            activations.compute(key, (k, v) -> v); // wait for the retiring activation's handover to finish
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
            if (ownedExecutor != null) ownedExecutor.shutdown();
            closeDone.complete(null);
        }
    }

    // ------------------------------------------------------------------------------------ sending

    private void send(ActorKey key, Task t) {
        if (closed) {
            t.fail(new IllegalStateException("runtime closed"));
            return;
        }
        for (;;) {
            Activation a = activations.get(key);
            if (a == null) a = activations.computeIfAbsent(key, k -> new Activation(k, types.get(k.type()), null));
            int r = a.offer(t, true);
            if (r != 0) {
                if (r == 2) a.schedule();
                return;
            }
            activations.compute(key, (k, v) -> v); // dead: wait out the handover, then retry on the map's current entry
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
        /** Runs the turn body. Must not be called on a dead activation. */
        abstract CompletionStage<?> run(ActorContext<Object> ctx) throws Exception;

        abstract void complete(Object value);

        abstract void fail(Throwable t);
    }

    private final class AskTask extends Task {
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
            timer.queued.set(false); // from here a new tick may queue behind this one
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

    /** Control task: deactivate. {@code idle} ones are abandoned if the activation turned out not to be idle. */
    private static final class DeactTask extends Task {
        final boolean idle;
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final boolean explicit;

        DeactTask(boolean idle, boolean explicit) {
            this.idle = idle;
            this.explicit = explicit;
        }

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
        final Object lock = new Object();

        // guarded by lock
        final ArrayDeque<Task> queue = new ArrayDeque<>();
        boolean scheduled;
        boolean dead;
        boolean deactReq;
        boolean idleCheckPending;
        volatile long lastActivity = System.nanoTime();

        // touched only by turns (exclusive)
        Object state;
        boolean activated;
        Ctx ctx;
        final Map<String, Timer> timers = new HashMap<>();

        Activation(ActorKey key, ActorType<Object> type, List<Task> handedOver) {
            this.key = key;
            this.type = type;
            this.idleNanos = type.idleTimeout() == null ? 0 : Math.max(1, type.idleTimeout().toNanos());
            if (handedOver != null) queue.addAll(handedOver);
            // The first drain of a handed-over queue is started by the retiring activation (schedule()).
            this.scheduled = false;
        }

        /** @return 0 dead (retry), 1 queued (someone is draining), 2 queued and the caller must {@link #schedule()}. */
        int offer(Task t, boolean activity) {
            synchronized (lock) {
                if (dead) return 0;
                queue.add(t);
                if (activity) lastActivity = System.nanoTime();
                if (scheduled) return 1;
                scheduled = true;
                return 2;
            }
        }

        void schedule() {
            executor.execute(this::drain);
        }

        /** Used for the first drain of a handed-over queue and by the idle check. */
        void kick() {
            synchronized (lock) {
                if (dead || scheduled || queue.isEmpty()) return;
                scheduled = true;
            }
            schedule();
        }

        private Task next() {
            synchronized (lock) {
                for (;;) {
                    if (dead) {
                        scheduled = false;
                        return null;
                    }
                    Task t;
                    if (deactReq) {
                        deactReq = false;
                        return new DeactTask(false, false);
                    }
                    t = queue.poll();
                    if (t == null) {
                        scheduled = false;
                        return null;
                    }
                    if (t instanceof DeactTask d && d.idle
                            && (!queue.isEmpty() || System.nanoTime() - lastActivity < idleNanos)) {
                        continue; // not idle after all: a message beat the collector to it
                    }
                    return t;
                }
            }
        }

        void drain() {
            for (int budget = TURN_BUDGET; ; ) {
                Task t = next();
                if (t == null) {
                    maybeScheduleIdleCheck();
                    return;
                }
                if (t instanceof DeactTask d) {
                    runDeactivation(d);
                    return; // the activation is gone; the retire step restarts any successor
                }
                if (!runTask(t)) return; // async: the completion callback calls resume()
                if (--budget == 0) {
                    schedule(); // stay `scheduled`, let other actors have the thread
                    return;
                }
            }
        }

        void resume() {
            schedule();
        }

        /** @return true if the turn finished synchronously, false if resume() will be called later. */
        private boolean runTask(Task t) {
            if (!activated) {
                CompletionStage<Void> act = null;
                try {
                    state = type.stateFactory().apply(key);
                    ctx = new Ctx(this);
                    if (state instanceof ActorLifecycle<?> lc) {
                        @SuppressWarnings("unchecked")
                        ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                        act = l.onActivate(ctx);
                    }
                } catch (Throwable e) {
                    resetActivation();
                    t.fail(e);
                    return true;
                }
                if (act == null) {
                    activated = true;
                } else if (act instanceof CompletableFuture<Void> cf && cf.isDone()) {
                    if (cf.isCompletedExceptionally()) {
                        resetActivation();
                        t.fail(failureOf(cf));
                        return true;
                    }
                    activated = true;
                } else {
                    act.whenComplete((v, e) -> {
                        if (e != null) {
                            resetActivation();
                            t.fail(unwrap(e));
                            resume();
                        } else {
                            activated = true;
                            if (runBody(t)) resume();
                        }
                    });
                    return false;
                }
            }
            return runBody(t);
        }

        private void resetActivation() {
            state = null;
            ctx = null;
            activated = false;
            cancelTimers();
        }

        private boolean runBody(Task t) {
            CompletionStage<?> st;
            try {
                st = t.run(ctx);
                if (st == null) st = Turns.done();
            } catch (Throwable e) {
                t.fail(unwrap(e));
                finished(t);
                return true;
            }
            if (st instanceof CompletableFuture<?> cf && cf.isDone()) {
                finishTurn(t, cf);
                return true;
            }
            st.whenComplete((v, e) -> {
                if (e != null) t.fail(unwrap(e));
                else t.complete(v);
                finished(t);
                resume();
            });
            return false;
        }

        private void finishTurn(Task t, CompletableFuture<?> done) {
            if (done.isCompletedExceptionally()) t.fail(failureOf(done));
            else t.complete(done.join());
            finished(t);
        }

        private void finished(Task t) {
            if (!(t instanceof TimerTask)) lastActivity = System.nanoTime(); // timers do not keep an activation alive
        }

        // ---- idle collection

        void maybeScheduleIdleCheck() {
            if (idleNanos == 0) return;
            long delay;
            synchronized (lock) {
                if (dead || idleCheckPending) return;
                idleCheckPending = true;
                delay = Math.max(0, lastActivity + idleNanos - System.nanoTime());
            }
            scheduleIdleCheck(delay);
        }

        private void scheduleIdleCheck(long delayNanos) {
            try {
                scheduler.schedule(this::idleCheck, delayNanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException closedScheduler) {
                // runtime is shutting down
            }
        }

        private void idleCheck() {
            long again = -1;
            synchronized (lock) {
                idleCheckPending = false;
                if (dead || scheduled || !queue.isEmpty()) return; // busy: drain() re-arms the check when it goes idle
                long rem = lastActivity + idleNanos - System.nanoTime();
                if (rem > 0) {
                    idleCheckPending = true;
                    again = rem;
                } else {
                    queue.add(new DeactTask(true, false));
                }
            }
            if (again >= 0) scheduleIdleCheck(again);
            else kick();
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
            int r = offer(new TimerTask(timer), false);
            if (r == 0) {
                timer.cancel();
            } else if (r == 2) {
                schedule();
            }
        }

        // ---- deactivation

        private void runDeactivation(DeactTask d) {
            cancelTimers();
            CompletionStage<Void> st = null;
            if (activated && state instanceof ActorLifecycle<?> lc) {
                try {
                    @SuppressWarnings("unchecked")
                    ActorLifecycle<Object> l = (ActorLifecycle<Object>) lc;
                    st = l.onDeactivate(ctx);
                } catch (Throwable e) {
                    report(e);
                }
            }
            if (st == null) {
                retire(d);
            } else {
                st.whenComplete((v, e) -> {
                    if (e != null) report(unwrap(e));
                    retire(d);
                });
            }
        }

        /** Mark dead and hand the leftover queue to a successor, atomically w.r.t. senders. */
        private void retire(DeactTask d) {
            List<Task> leftover = new ArrayList<>();
            Activation[] successor = new Activation[1];
            activations.compute(key, (k, cur) -> {
                synchronized (lock) {
                    dead = true;
                    leftover.addAll(queue);
                    queue.clear();
                }
                if (cur != this) return cur; // cannot happen; be safe
                List<Task> forward = new ArrayList<>();
                for (Task t : leftover) {
                    if (t instanceof TimerTask) continue; // timers died with the activation
                    if (t instanceof DeactTask) continue; // their purpose is already served
                    forward.add(t);
                }
                if (forward.isEmpty() || closed) return null;
                Activation n = new Activation(key, type, forward);
                successor[0] = n;
                return n;
            });
            state = null;
            ctx = null;
            // everything below runs outside the map's bin lock
            for (Task t : leftover) {
                if (t instanceof DeactTask other) other.done.complete(null);
                else if (successor[0] == null && !(t instanceof TimerTask)) {
                    t.fail(new IllegalStateException("runtime closed"));
                }
            }
            d.done.complete(null);
            if (successor[0] != null) successor[0].kick();
        }

        void failEverything(Throwable why) {
            List<Task> rest;
            synchronized (lock) {
                dead = true;
                rest = new ArrayList<>(queue);
                queue.clear();
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
            return FuturesActorRuntime.this;
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
            synchronized (a.lock) {
                a.deactReq = true;
            }
        }
    }

    // ------------------------------------------------------------------------------------ util

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
        } catch (CompletionException | CancellationException e) {
            return unwrap(e);
        }
    }
}
