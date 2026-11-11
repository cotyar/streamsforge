package io.streamsforge.actors.testkit;

import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.actors.*;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The acceptance bar for every {@link ActorRuntime}. Subclass it in an implementation module and
 * implement {@link #newRuntime()}; every runtime must pass this suite unmodified.
 *
 * <p>Timing discipline: no test asserts that something happens "within" a short deadline. Positive
 * conditions are polled with generous (tens of seconds) deadlines; the few "nothing happens"
 * assertions can only pass spuriously, never fail spuriously.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public abstract class ActorRuntimeConformance {

    protected abstract ActorRuntime newRuntime();

    protected ActorRuntime rt;
    protected final List<Throwable> errors = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        rt = newRuntime();
        rt.onUnhandledError(errors::add);
    }

    @AfterEach
    void tearDown() {
        rt.close();
    }

    // ------------------------------------------------------------------ helpers

    static <T> T get(CompletionStage<T> s) throws Exception {
        return s.toCompletableFuture().get(60, TimeUnit.SECONDS);
    }

    static Throwable rootCause(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    static Throwable failureOf(CompletionStage<?> s) throws Exception {
        try {
            s.toCompletableFuture().get(60, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return rootCause(e);
        }
        return fail("stage was expected to fail");
    }

    static void awaitTrue(String what, BooleanSupplier cond) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("timed out waiting for: " + what);
            Thread.sleep(5);
        }
    }

    /** A stage that completes with {@code value} shortly, from another thread. */
    static <T> CompletionStage<T> later(T value, long micros) {
        CompletableFuture<T> f = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(micros, TimeUnit.MICROSECONDS).execute(() -> f.complete(value));
        return f;
    }

    // ------------------------------------------------------------------ registration

    @Test
    void unknownTypeAndDuplicateRegistrationThrow() {
        assertThrows(RuntimeException.class, () -> rt.ref("nope", "1"));
        rt.register(ActorType.of("dup", k -> new Object()));
        assertThrows(RuntimeException.class, () -> rt.register(ActorType.of("dup", k -> new Object())));
    }

    @Test
    void refNeverActivatesAndFirstMessageDoes() throws Exception {
        AtomicInteger created = new AtomicInteger();
        rt.register(ActorType.of("lazy", k -> {
            created.incrementAndGet();
            return new int[] {7};
        }));
        ActorRef<int[]> ref = rt.ref("lazy", "a");
        assertEquals(0, created.get());
        assertTrue(rt.activeKeys().isEmpty());
        assertEquals(Integer.valueOf(7), get(ref.call(s -> s[0])));
        assertEquals(1, created.get());
        assertEquals(java.util.Set.of(new ActorKey("lazy", "a")), rt.activeKeys());
        // a second ref to the same key reaches the same activation
        assertEquals(Integer.valueOf(7), get(rt.<int[]>ref("lazy", "a").call(s -> s[0])));
        assertEquals(1, created.get());
        // the factory sees the key
        AtomicReference<ActorKey> seen = new AtomicReference<>();
        rt.register(ActorType.of("keyed", k -> {
            seen.set(k);
            return new Object();
        }));
        get(rt.<Object>ref("keyed", "zz").call(s -> 1));
        assertEquals(new ActorKey("keyed", "zz"), seen.get());
    }

    // ------------------------------------------------------------------ turn exclusivity

    static final class Counter {
        int count; // deliberately plain: exclusivity + visibility are what is being tested
        final AtomicInteger inTurn = new AtomicInteger();
    }

    @Test
    void noTwoTurnsOfOneActorEverOverlap() throws Exception {
        rt.register(ActorType.of("excl", k -> new Counter()));
        AtomicInteger violations = new AtomicInteger();
        int threads = 16, perThread = 1500, keys = 3;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> done = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int tt = t;
            done.add(pool.submit(() -> {
                List<CompletionStage<?>> pending = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    ActorRef<Counter> ref = rt.ref("excl", "k" + ((tt + i) % keys));
                    boolean async = ThreadLocalRandom.current().nextInt(3) == 0;
                    pending.add(ref.ask(ctx -> {
                        Counter c = ctx.state();
                        if (c.inTurn.incrementAndGet() != 1) violations.incrementAndGet();
                        c.count++;
                        if (!async) {
                            c.inTurn.decrementAndGet();
                            return Turns.completed(c.count);
                        }
                        CompletableFuture<Integer> f = new CompletableFuture<>();
                        CompletableFuture.delayedExecutor(50, TimeUnit.MICROSECONDS).execute(() -> {
                            c.inTurn.decrementAndGet();
                            f.complete(c.count);
                        });
                        return f;
                    }));
                }
                for (CompletionStage<?> s : pending) {
                    try {
                        s.toCompletableFuture().get(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }
        for (Future<?> f : done) f.get(100, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(0, violations.get(), "overlapping turns observed");
        int total = 0;
        for (int k = 0; k < keys; k++) total += get(rt.<Counter>ref("excl", "k" + k).call(c -> c.count));
        assertEquals(threads * perThread, total, "lost or duplicated turns");
    }

    // ------------------------------------------------------------------ ordering

    @Test
    void tellIsFifoPerSender() throws Exception {
        rt.register(ActorType.of("fifo", k -> new HashMap<Integer, List<Integer>>()));
        int senders = 8, n = 5000;
        ActorRef<Map<Integer, List<Integer>>> ref = rt.ref("fifo", "one");
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int s = 0; s < senders; s++) {
            int ss = s;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < n; i++) {
                    int ii = i;
                    // mix tell and call from the same sender: both must stay in order
                    if (i % 10 == 9) ref.call(m -> m.computeIfAbsent(ss, x -> new ArrayList<>()).add(ii));
                    else ref.tell(m -> m.computeIfAbsent(ss, x -> new ArrayList<>()).add(ii));
                }
            });
            t.start();
            ts.add(t);
        }
        start.countDown();
        for (Thread t : ts) t.join();
        Map<Integer, List<Integer>> m = get(ref.call(x -> new HashMap<>(x)));
        for (int s = 0; s < senders; s++) {
            List<Integer> got = m.get(s);
            assertEquals(n, got.size(), "sender " + s);
            for (int i = 0; i < n; i++) assertEquals(i, got.get(i), "sender " + s + " position " + i);
        }
    }

    @Test
    void fifoPerSenderSurvivesDeactivationInTheMiddle() throws Exception {
        rt.register(ActorType.of("fifo2", k -> new Object()));
        ActorKey key = new ActorKey("fifo2", "x");
        ActorRef<Object> ref = rt.ref(key);
        AtomicBoolean stop = new AtomicBoolean();
        Thread chaos = new Thread(() -> {
            while (!stop.get()) {
                rt.deactivate(key);
                LockSupport.parkNanos(50_000);
            }
        });
        chaos.start();
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        int n = 20000;
        for (int i = 0; i < n; i++) {
            int ii = i;
            ref.tell(l -> order.add(ii));
        }
        stop.set(true);
        chaos.join();
        get(ref.call(l -> 0)); // barrier
        assertEquals(n, order.size());
        for (int i = 0; i < n; i++) assertEquals(i, order.get(i), "position " + i);
    }

    // ------------------------------------------------------------------ non-reentrancy & call chains

    @Test
    void asyncTurnAwaitingAnotherActorKeepsItsMailboxClosed() throws Exception {
        rt.register(ActorType.of("nr-a", k -> new Object()));
        rt.register(ActorType.of("nr-b", k -> new Object()));
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<String> gate = new CompletableFuture<>();
        ActorRef<Object> a = rt.ref("nr-a", "a");
        ActorRef<Object> b = rt.ref("nr-b", "b");

        CompletionStage<String> t1 = a.ask(ctx -> {
            log.add("T1-start");
            return b.<String>ask(bc -> gate).thenApply(v -> {
                log.add("T1-end");
                return v;
            });
        });
        awaitTrue("T1 started", () -> log.contains("T1-start"));
        CompletionStage<Integer> t2 = a.ask(ctx -> {
            log.add("T2");
            return Turns.completed(2);
        });
        a.tell(o -> log.add("T3"));
        Thread.sleep(300); // absence check: can only pass spuriously
        assertEquals(List.of("T1-start"), new ArrayList<>(log), "mailbox must stay closed while T1's stage is pending");
        assertFalse(t2.toCompletableFuture().isDone());
        gate.complete("done");
        assertEquals("done", get(t1));
        assertEquals(Integer.valueOf(2), get(t2));
        awaitTrue("T3", () -> log.contains("T3"));
        assertEquals(List.of("T1-start", "T1-end", "T2", "T3"), new ArrayList<>(log));
    }

    @Test
    void actorToActorCallChains() throws Exception {
        rt.register(ActorType.of("chain-a", k -> new int[1]));
        rt.register(ActorType.of("chain-b", k -> new int[1]));
        rt.register(ActorType.of("chain-c", k -> new int[1]));
        int chains = 3000;
        List<CompletionStage<Integer>> results = new ArrayList<>();
        for (int i = 0; i < chains; i++) {
            int ii = i;
            results.add(rt.<int[]>ref("chain-a", "a" + (i % 40)).<Integer>ask(ac -> {
                ac.state()[0]++;
                return ac.runtime().<int[]>ref("chain-b", "b" + (ii % 17)).<Integer>ask(bc -> {
                    bc.state()[0]++;
                    return bc.runtime().<int[]>ref("chain-c", "c" + (ii % 5)).<Integer>ask(cc -> {
                        cc.state()[0]++;
                        return later(ii, 20);
                    }).thenApply(v -> v + 1);
                }).thenApply(v -> v + 1);
            }));
        }
        for (int i = 0; i < chains; i++) assertEquals(Integer.valueOf(i + 2), get(results.get(i)));
        int sa = 0, sb = 0, sc = 0;
        for (int i = 0; i < 40; i++) sa += get(rt.<int[]>ref("chain-a", "a" + i).call(s -> s[0]));
        for (int i = 0; i < 17; i++) sb += get(rt.<int[]>ref("chain-b", "b" + i).call(s -> s[0]));
        for (int i = 0; i < 5; i++) sc += get(rt.<int[]>ref("chain-c", "c" + i).call(s -> s[0]));
        assertEquals(chains, sa);
        assertEquals(chains, sb);
        assertEquals(chains, sc);
    }

    // ------------------------------------------------------------------ failure isolation

    @Test
    void failuresFailTheAskButDoNotKillTheActivation() throws Exception {
        AtomicInteger created = new AtomicInteger();
        rt.register(ActorType.of("fail", k -> {
            created.incrementAndGet();
            return new int[1];
        }));
        ActorRef<int[]> ref = rt.ref("fail", "f");
        get(ref.call(s -> s[0] = 5));

        IllegalStateException boom = new IllegalStateException("boom");
        CompletionStage<Object> thrown = ref.ask(ctx -> {
            throw boom;
        });
        assertSame(boom, failureOf(thrown));

        IOException io = new IOException("checked");
        CompletionStage<Object> thrownChecked = ref.ask(ctx -> {
            throw io;
        });
        assertSame(io, failureOf(thrownChecked));

        RuntimeException failedStage = new RuntimeException("failed-stage");
        CompletionStage<Object> failed = ref.ask(ctx -> Turns.failed(failedStage));
        assertSame(failedStage, failureOf(failed));

        // async failure, completed from another thread
        CompletableFuture<Object> pending = new CompletableFuture<>();
        CompletionStage<Object> asyncFail = ref.ask(ctx -> pending);
        pending.completeExceptionally(failedStage);
        assertSame(failedStage, failureOf(asyncFail));

        assertEquals(Integer.valueOf(5), get(ref.call(s -> s[0])), "state must survive failures");
        assertEquals(1, created.get(), "activation must survive failures");
        assertTrue(errors.isEmpty(), "ask failures must not reach the unhandled-error handler: " + errors);
    }

    @Test
    void tellFailuresReachTheUnhandledErrorHandler() throws Exception {
        rt.register(ActorType.of("tellfail", k -> new int[1]));
        ActorRef<int[]> ref = rt.ref("tellfail", "t");
        IllegalArgumentException boom = new IllegalArgumentException("tell-boom");
        ref.tell(s -> {
            throw boom;
        });
        ref.tell(s -> s[0] = 9);
        awaitTrue("handler called", () -> !errors.isEmpty());
        assertSame(boom, rootCause(errors.get(0)));
        assertEquals(Integer.valueOf(9), get(ref.call(s -> s[0])), "the activation keeps going after a failed tell");
    }

    // ------------------------------------------------------------------ lifecycle

    static final class Life implements ActorLifecycle<Life> {
        final List<String> events;
        final String id;
        int n;

        Life(List<String> events, String id) {
            this.events = events;
            this.id = id;
        }

        @Override
        public CompletionStage<Void> onActivate(ActorContext<Life> ctx) {
            events.add("activate:" + id);
            return Turns.done();
        }

        @Override
        public CompletionStage<Void> onDeactivate(ActorContext<Life> ctx) {
            events.add("deactivate:" + id);
            return Turns.done();
        }
    }

    @Test
    void lifecycleOrderingAcrossExplicitDeactivation() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        rt.register(ActorType.of("life", k -> new Life(ev, k.id())));
        ActorRef<Life> ref = rt.ref("life", "a");
        get(ref.call(s -> ev.add("msg1")));
        get(ref.call(s -> ev.add("msg2")));
        get(rt.deactivate(ref.key()));
        assertFalse(rt.activeKeys().contains(ref.key()));
        assertEquals(List.of("activate:a", "msg1", "msg2", "deactivate:a"), new ArrayList<>(ev));
        get(ref.call(s -> ev.add("msg3")));
        assertEquals(List.of("activate:a", "msg1", "msg2", "deactivate:a", "activate:a", "msg3"), new ArrayList<>(ev));
        // deactivating something not active is a harmless no-op
        get(rt.deactivate(new ActorKey("life", "never-used")));
        get(rt.deactivate(ref.key()));
        get(rt.deactivate(ref.key()));
        assertEquals(7, ev.size(), "exactly one more deactivate; the repeats are no-ops");
        assertEquals("deactivate:a", ev.get(6));
    }

    @Test
    void asyncLifecycleHooksAreTurns() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        class Slow implements ActorLifecycle<Slow> {
            @Override
            public CompletionStage<Void> onActivate(ActorContext<Slow> ctx) {
                ev.add("activate-start");
                return later((Void) null, 100_000).thenApply(v -> {
                    ev.add("activate-end");
                    return null;
                });
            }

            @Override
            public CompletionStage<Void> onDeactivate(ActorContext<Slow> ctx) {
                ev.add("deactivate-start");
                return later((Void) null, 100_000).thenApply(v -> {
                    ev.add("deactivate-end");
                    return null;
                });
            }
        }
        rt.register(ActorType.of("slow", k -> new Slow()));
        ActorRef<Slow> ref = rt.ref("slow", "s");
        CompletionStage<Integer> m1 = ref.ask(c -> {
            ev.add("m1");
            return Turns.completed(1);
        });
        CompletionStage<Integer> m2 = ref.ask(c -> {
            ev.add("m2");
            return Turns.completed(2);
        });
        get(m1);
        get(m2);
        get(rt.deactivate(ref.key()));
        assertEquals(
                List.of("activate-start", "activate-end", "m1", "m2", "deactivate-start", "deactivate-end"),
                new ArrayList<>(ev));
    }

    @Test
    void failedActivationFailsTheTriggeringCallAndIsRetriedByTheNext() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger attempts = new AtomicInteger();
        class Flaky implements ActorLifecycle<Flaky> {
            @Override
            public CompletionStage<Void> onActivate(ActorContext<Flaky> ctx) {
                if (attempts.incrementAndGet() == 1) return Turns.failed(new IllegalStateException("no-activate"));
                ev.add("activated");
                return Turns.done();
            }

            @Override
            public CompletionStage<Void> onDeactivate(ActorContext<Flaky> ctx) {
                ev.add("deactivated");
                return Turns.done();
            }
        }
        rt.register(ActorType.of("flaky", k -> new Flaky()));
        ActorRef<Flaky> ref = rt.ref("flaky", "f");
        AtomicInteger ran = new AtomicInteger();
        Throwable t = failureOf(ref.ask(c -> {
            ran.incrementAndGet();
            return Turns.completed(1);
        }));
        assertEquals("no-activate", t.getMessage());
        assertEquals(0, ran.get(), "a turn must never run on an activation whose onActivate failed");
        assertEquals(Integer.valueOf(1), get(ref.ask(c -> {
            ran.incrementAndGet();
            return Turns.completed(1);
        })));
        assertEquals(1, ran.get());
        assertEquals(List.of("activated"), new ArrayList<>(ev), "no onDeactivate for an activation that never activated");
    }

    @Test
    void closeRunsOnDeactivateEverywhereAndIsIdempotent() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        rt.register(ActorType.of("closing", k -> new Life(ev, k.id())));
        for (int i = 0; i < 50; i++) get(rt.<Life>ref("closing", "k" + i).call(s -> 1));
        assertEquals(50, rt.activeKeys().size());
        rt.close();
        rt.close();
        assertEquals(50, ev.stream().filter(s -> s.startsWith("deactivate:")).count());
        assertEquals(50, ev.stream().filter(s -> s.startsWith("activate:")).count());
        assertTrue(rt.activeKeys().isEmpty());
        // use after close must fail cleanly (exception or failed stage), never hang or run
        AtomicBoolean ran = new AtomicBoolean();
        try {
            CompletionStage<Integer> s = rt.<Life>ref("closing", "late").ask(c -> {
                ran.set(true);
                return Turns.completed(1);
            });
            failureOf(s);
        } catch (RuntimeException expected) {
            // thrown synchronously is acceptable too
        }
        assertFalse(ran.get());
    }

    @Test
    void closeWaitsForAsyncOnDeactivate() throws Exception {
        AtomicInteger done = new AtomicInteger();
        class Slow implements ActorLifecycle<Slow> {
            @Override
            public CompletionStage<Void> onDeactivate(ActorContext<Slow> ctx) {
                return later((Void) null, 200_000).thenApply(v -> {
                    done.incrementAndGet();
                    return null;
                });
            }
        }
        rt.register(ActorType.of("slowclose", k -> new Slow()));
        for (int i = 0; i < 20; i++) get(rt.<Slow>ref("slowclose", "k" + i).call(s -> 1));
        rt.close();
        assertEquals(20, done.get());
    }

    // ------------------------------------------------------------------ idle collection

    @Test
    void idleActivationsAreCollectedAndReactivatedWithFreshState() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        rt.register(ActorType.of("idle", k -> new Life(ev, k.id())).withIdleTimeout(Duration.ofMillis(100)));
        ActorRef<Life> ref = rt.ref("idle", "i");
        assertEquals(Integer.valueOf(1), get(ref.call(s -> ++s.n)));
        assertEquals(Integer.valueOf(2), get(ref.call(s -> ++s.n)));
        awaitTrue("collected", () -> !rt.activeKeys().contains(ref.key()));
        assertEquals(List.of("activate:i", "deactivate:i"), new ArrayList<>(ev));
        assertEquals(Integer.valueOf(1), get(ref.call(s -> ++s.n)), "fresh state after re-activation");
        assertEquals("activate:i", ev.get(2));
        awaitTrue("collected again", () -> !rt.activeKeys().contains(ref.key()));
        assertEquals(4, ev.size());
    }

    @Test
    void activityKeepsAnActivationAlive() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        rt.register(ActorType.of("alive", k -> new Life(ev, k.id())).withIdleTimeout(Duration.ofMillis(1500)));
        ActorRef<Life> ref = rt.ref("alive", "a");
        for (int i = 0; i < 25; i++) {
            get(ref.call(s -> ++s.n));
            Thread.sleep(100);
        }
        assertEquals(Integer.valueOf(26), get(ref.call(s -> ++s.n)), "state must survive 2.5s of activity under a 1.5s idle timeout");
        assertEquals(List.of("activate:a"), new ArrayList<>(ev));
    }

    @Test
    void deactivateOnIdleFromInsideATurnForwardsQueuedMessages() throws Exception {
        List<String> ev = Collections.synchronizedList(new ArrayList<>());
        rt.register(ActorType.of("selfdeact", k -> new Life(ev, k.id())));
        ActorRef<Life> ref = rt.ref("selfdeact", "s");
        AtomicLong executed = new AtomicLong();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletionStage<Integer> first = ref.ask(ctx -> {
            ctx.deactivateOnIdle();
            return release.thenApply(v -> 1);
        });
        for (int i = 0; i < 100; i++) ref.tell(s -> executed.incrementAndGet());
        release.complete(null);
        assertEquals(Integer.valueOf(1), get(first));
        awaitTrue("all 100 tells ran", () -> executed.get() == 100);
        // Where the 100 tells ran is runtime-specific (before onDeactivate on a runtime that drains already-delivered
        // messages first, or on a fresh activation on one that forwards them); what is common is that none is lost
        // and that deactivateOnIdle really deactivates.
        awaitTrue("deactivated", () -> ev.stream().anyMatch(e -> e.startsWith("deactivate:")));
        awaitTrue("collected", () -> !rt.activeKeys().contains(ref.key()) || ev.stream().filter(e -> e.startsWith("activate:")).count() == 2);
    }

    // ------------------------------------------------------------------ timers

    static final class Ticker {
        final AtomicInteger inTurn = new AtomicInteger();
    }

    @Test
    void timersAreTurnsAndNeverOverlapOtherTurns() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        AtomicInteger violations = new AtomicInteger();
        rt.register(ActorType.of("tick", k -> new Ticker()));
        ActorRef<Ticker> ref = rt.ref("tick", "t");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(5), c -> {
                if (c.state().inTurn.incrementAndGet() != 1) violations.incrementAndGet();
                ticks.incrementAndGet();
                Thread.onSpinWait();
                c.state().inTurn.decrementAndGet();
            });
            return Turns.done();
        }));
        for (int i = 0; i < 300; i++) {
            get(ref.ask(ctx -> {
                if (ctx.state().inTurn.incrementAndGet() != 1) violations.incrementAndGet();
                Thread.onSpinWait();
                ctx.state().inTurn.decrementAndGet();
                return Turns.done();
            }));
        }
        awaitTrue("ticks", () -> ticks.get() >= 10);
        assertEquals(0, violations.get());
    }

    @Test
    void timerTicksAreCoalescedWhileTheActorIsBusy() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        rt.register(ActorType.of("coalesce", k -> new Ticker()));
        ActorRef<Ticker> ref = rt.ref("coalesce", "c");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ofMillis(1), Duration.ofMillis(10), c -> ticks.incrementAndGet());
            return Turns.done();
        }));
        awaitTrue("some ticks", () -> ticks.get() >= 3);
        CompletableFuture<Void> hold = new CompletableFuture<>();
        CompletableFuture<Integer> before = new CompletableFuture<>();
        CompletionStage<Void> blocked = ref.ask(ctx -> {
            before.complete(ticks.get());
            return hold;
        });
        int c0 = before.get(60, TimeUnit.SECONDS);
        Thread.sleep(500); // ~50 periods pass with the actor busy
        hold.complete(null);
        get(blocked);
        int c1 = get(ref.call(s -> ticks.get())); // runs after whatever tick(s) were queued before it
        assertTrue(c1 - c0 <= 4, "ticks must be coalesced while busy, but " + (c1 - c0) + " ran");
    }

    @Test
    void cancelledOrReplacedTimersStop() throws Exception {
        AtomicInteger a = new AtomicInteger(), b = new AtomicInteger();
        rt.register(ActorType.of("cancel", k -> new Ticker()));
        ActorRef<Ticker> ref = rt.ref("cancel", "c");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(5), c -> a.incrementAndGet());
            return Turns.done();
        }));
        awaitTrue("a ticking", () -> a.get() >= 3);
        // replace under the same name: the old tick body must stop being invoked
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(5), c -> b.incrementAndGet());
            return Turns.done();
        }));
        int aAtReplace = a.get();
        awaitTrue("b ticking", () -> b.get() >= 3);
        assertEquals(aAtReplace, a.get(), "the replaced timer must not tick after the replacing turn");
        // cancel
        get(ref.ask(ctx -> {
            ctx.cancelTimer("t");
            return Turns.done();
        }));
        int bAtCancel = b.get();
        Thread.sleep(250);
        get(ref.call(s -> 0));
        assertEquals(bAtCancel, b.get(), "no tick may run after cancelTimer's turn");
    }

    @Test
    void timersDoNotKeepAnActivationAliveAndDieWithIt() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        rt.register(ActorType.of("tickidle", k -> new Ticker()).withIdleTimeout(Duration.ofMillis(300)));
        ActorRef<Ticker> ref = rt.ref("tickidle", "t");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(10), c -> ticks.incrementAndGet());
            return Turns.done();
        }));
        awaitTrue("ticking", () -> ticks.get() >= 3);
        awaitTrue("collected despite the timer", () -> !rt.activeKeys().contains(ref.key()));
        int atCollection = ticks.get();
        Thread.sleep(300);
        assertEquals(atCollection, ticks.get(), "the timer must die with its activation");
        assertFalse(rt.activeKeys().contains(ref.key()), "a dead timer must not resurrect the activation");
    }

    @Test
    void explicitDeactivationKillsTimers() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        rt.register(ActorType.of("tickdeact", k -> new Ticker()));
        ActorRef<Ticker> ref = rt.ref("tickdeact", "t");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(5), c -> ticks.incrementAndGet());
            return Turns.done();
        }));
        awaitTrue("ticking", () -> ticks.get() >= 3);
        get(rt.deactivate(ref.key()));
        int at = ticks.get();
        Thread.sleep(250);
        assertEquals(at, ticks.get());
        assertFalse(rt.activeKeys().contains(ref.key()));
    }

    @Test
    void timerTickFailuresReachTheUnhandledErrorHandler() throws Exception {
        rt.register(ActorType.of("tickfail", k -> new Ticker()));
        ActorRef<Ticker> ref = rt.ref("tickfail", "t");
        RuntimeException boom = new RuntimeException("tick-boom");
        get(ref.ask(ctx -> {
            ctx.startTimer("t", Duration.ZERO, Duration.ofMillis(20), c -> {
                throw boom;
            });
            return Turns.done();
        }));
        awaitTrue("handler", () -> !errors.isEmpty());
        assertSame(boom, rootCause(errors.get(0)));
        assertEquals(Integer.valueOf(1), get(ref.call(s -> 1)));
    }

    // ------------------------------------------------------------------ the hard part: deactivation races

    static final class KeyStats {
        final AtomicInteger liveTurns = new AtomicInteger(); // turns (incl. lifecycle hooks) running right now
        final AtomicInteger liveActivations = new AtomicInteger(); // between onActivate and onDeactivate
    }

    /** Shared bookkeeping for the race tests. All counters are EXTERNAL to the actors' own state, so
     *  they see across activations of the same key. */
    final class RaceWorld {
        final Map<String, KeyStats> stats = new ConcurrentHashMap<>();
        final AtomicLong executed = new AtomicLong();
        final AtomicInteger violations = new AtomicInteger();
        final AtomicLong activations = new AtomicLong();
        final AtomicLong deactivations = new AtomicLong();

        KeyStats of(String id) {
            return stats.computeIfAbsent(id, x -> new KeyStats());
        }

        void enter(KeyStats s) {
            if (s.liveTurns.incrementAndGet() != 1) violations.incrementAndGet();
        }

        void exit(KeyStats s) {
            s.liveTurns.decrementAndGet();
        }

        /** Runs {@code onDone} either inline or after a short hop, at random. */
        CompletionStage<Void> maybeAsync(Runnable onDone) {
            ThreadLocalRandom r = ThreadLocalRandom.current();
            if (r.nextInt(6) != 0) {
                onDone.run();
                return Turns.done();
            }
            CompletableFuture<Void> f = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(r.nextInt(20, 400), TimeUnit.MICROSECONDS).execute(() -> {
                onDone.run();
                f.complete(null);
            });
            return f;
        }

        final class Racer implements ActorLifecycle<Racer> {
            final KeyStats st;

            Racer(ActorKey k) {
                st = of(k.id());
            }

            @Override
            public CompletionStage<Void> onActivate(ActorContext<Racer> ctx) {
                enter(st);
                if (st.liveActivations.incrementAndGet() != 1) violations.incrementAndGet();
                activations.incrementAndGet();
                return maybeAsync(() -> exit(st));
            }

            @Override
            public CompletionStage<Void> onDeactivate(ActorContext<Racer> ctx) {
                enter(st);
                if (st.liveActivations.decrementAndGet() != 0) violations.incrementAndGet();
                deactivations.incrementAndGet();
                return maybeAsync(() -> exit(st));
            }
        }

        void register(String type, Duration idle) {
            ActorType<Racer> t = ActorType.of(type, k -> new Racer(k));
            rt.register(idle == null ? t : t.withIdleTimeout(idle));
        }
    }

    /** Hammers {@code keys} actors from many threads with tell/ask/async-ask while chaos threads
     *  deactivate them continuously; every message must execute exactly once, and no turn may
     *  ever overlap another turn or lifecycle hook of the same key (across activations). */
    private void raceTest(String type, Duration idle, int threads, int perThread, int keys, boolean pace, int maxPending)
            throws Exception {
        RaceWorld w = new RaceWorld();
        w.register(type, idle);
        AtomicLong sent = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> chaos = new ArrayList<>();
        for (int c = 0; c < 3; c++) {
            Thread t = new Thread(() -> {
                while (!stop.get()) {
                    ThreadLocalRandom r = ThreadLocalRandom.current();
                    ActorKey k = new ActorKey(type, "k" + r.nextInt(keys));
                    CompletionStage<Void> d = rt.deactivate(k);
                    if (r.nextBoolean()) d.toCompletableFuture().join();
                    LockSupport.parkNanos(r.nextInt(200_000));
                }
            });
            t.start();
            chaos.add(t);
        }
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> fs = new ArrayList<>();
        for (int th = 0; th < threads; th++) {
            fs.add(pool.submit(() -> {
                List<CompletableFuture<?>> pending = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    ThreadLocalRandom r = ThreadLocalRandom.current();
                    ActorRef<RaceWorld.Racer> ref = rt.ref(type, "k" + r.nextInt(keys));
                    sent.incrementAndGet();
                    switch (r.nextInt(3)) {
                        case 0 -> ref.tell(s -> {
                            w.enter(s.st);
                            w.executed.incrementAndGet();
                            w.exit(s.st);
                        });
                        case 1 -> pending.add(ref.<Integer>ask(ctx -> {
                                    RaceWorld.Racer s = ctx.state();
                                    w.enter(s.st);
                                    w.executed.incrementAndGet();
                                    w.exit(s.st);
                                    return Turns.completed(1);
                                })
                                .toCompletableFuture());
                        default -> pending.add(ref.<Integer>ask(ctx -> {
                                    RaceWorld.Racer s = ctx.state();
                                    w.enter(s.st);
                                    w.executed.incrementAndGet();
                                    CompletableFuture<Integer> f = new CompletableFuture<>();
                                    CompletableFuture.delayedExecutor(
                                                    ThreadLocalRandom.current().nextInt(10, 200),
                                                    TimeUnit.MICROSECONDS)
                                            .execute(() -> {
                                                w.exit(s.st);
                                                f.complete(1);
                                            });
                                    return f;
                                })
                                .toCompletableFuture());
                    }
                    if (pace) LockSupport.parkNanos(r.nextInt(3_000_000));
                    if (pending.size() > maxPending) {
                        for (CompletableFuture<?> p : pending) p.join();
                        pending.clear();
                    }
                }
                for (CompletableFuture<?> p : pending) p.join();
            }));
        }
        for (Future<?> f : fs) f.get(100, TimeUnit.SECONDS);
        pool.shutdown();
        stop.set(true);
        for (Thread t : chaos) t.join();
        // barrier per key: everything told before this point is ahead of this ask in the key's logical FIFO
        for (int k = 0; k < keys; k++) get(rt.<RaceWorld.Racer>ref(type, "k" + k).call(s -> 0));
        assertEquals(sent.get(), w.executed.get(), "every message must execute exactly once");
        assertEquals(0, w.violations.get(), "overlapping turns / activations observed");
        assertTrue(errors.isEmpty(), "unexpected errors: " + errors);
        rt.close();
        assertEquals(w.activations.get(), w.deactivations.get(), "every activation must be deactivated by close()");
        System.out.println("[conformance] " + type + ": " + w.activations.get() + " activations for " + keys + " keys, " + sent.get() + " messages");
        if (pace || maxPending <= 2) assertTrue(w.activations.get() > keys * 2L, "the chaos threads must actually have forced re-activations");
        for (KeyStats s : w.stats.values()) {
            assertEquals(0, s.liveActivations.get());
            assertEquals(0, s.liveTurns.get());
        }
    }

    @Test
    void deactivationRaceStress() throws Exception {
        raceTest("race", null, 16, 6000, 4, false, 2000);
    }

    /** Short queues: almost every deactivation lands while senders are mid-flight. */
    @Test
    void deactivationRaceStressLockstep() throws Exception {
        raceTest("race-lock", null, 8, 4000, 4, false, 1);
    }

    @Test
    void deactivationRaceStressManyKeys() throws Exception {
        raceTest("race-many", null, 16, 4000, 64, false, 2000);
    }

    @Test
    void idleCollectionRaceStress() throws Exception {
        raceTest("race-idle", Duration.ofMillis(2), 8, 600, 3, true, 2000);
    }

    // ------------------------------------------------------------------ scale

    @Test
    void hundredThousandActivationsFanOut() throws Exception {
        rt.register(ActorType.of("fan", k -> new long[] {Long.parseLong(k.id())}));
        int n = 100_000;
        List<CompletableFuture<Long>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            futures.add(rt.<long[]>ref("fan", Integer.toString(i)).call(s -> s[0] * 2).toCompletableFuture());
        }
        for (int i = 0; i < n; i++) assertEquals(i * 2L, futures.get(i).get(100, TimeUnit.SECONDS));
        assertEquals(n, rt.activeKeys().size());
        // and they all still answer
        List<CompletableFuture<Long>> second = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            second.add(rt.<long[]>ref("fan", Integer.toString(i)).call(s -> ++s[0]).toCompletableFuture());
        }
        for (int i = 0; i < n; i++) assertEquals(i + 1L, second.get(i).get(100, TimeUnit.SECONDS));
        rt.close();
        assertTrue(rt.activeKeys().isEmpty());
    }
}
