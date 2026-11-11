package io.streamsforge.actors.testkit;

import io.streamsforge.actors.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Assumptions;

/**
 * Off by default. Run with {@code ./gradlew :actors-<x>:test -Dbench=true --tests '*Benchmark*' -i}
 * and read the table from the test's stdout. Crude wall-clock numbers (one warm-up pass, one
 * measured pass) — good for comparing runtimes on one machine, not for publishing.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
public abstract class ActorRuntimeBenchmark {

    protected abstract ActorRuntime newRuntime();

    @Test
    void benchmark() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("bench"), "benchmark is off by default; run with -Dbench=true");
        for (int pass = 0; pass < 2; pass++) {
            boolean measured = pass == 1;
            double pingPong = pingPong();
            double pipelined = pipelined();
            double[] fan = fanOut();
            double chain = chainLatencyMicros();
            if (measured) {
                String name;
                try (ActorRuntime r = newRuntime()) {
                    name = r.name();
                }
                System.out.printf(
                        "BENCH | %s | %,.0f | %,.0f | %,.0f | %,.0f | %.1f |%n",
                        name, pingPong, pipelined, fan[0], fan[1], chain);
            }
        }
    }

    /** 8 client threads, each doing strictly sequential asks against its own actor. */
    private double pingPong() throws Exception {
        try (ActorRuntime rt = newRuntime()) {
            rt.register(ActorType.of("pp", k -> new long[1]));
            int clients = 8, per = 50_000;
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            List<Future<?>> fs = new ArrayList<>();
            long t0 = System.nanoTime();
            for (int c = 0; c < clients; c++) {
                int cc = c;
                fs.add(pool.submit(() -> {
                    ActorRef<long[]> ref = rt.ref("pp", "c" + cc);
                    for (int i = 0; i < per; i++) ref.call(s -> ++s[0]).toCompletableFuture().join();
                }));
            }
            for (Future<?> f : fs) f.get();
            long ns = System.nanoTime() - t0;
            pool.shutdown();
            return clients * (double) per / (ns / 1e9);
        }
    }

    /** One producer firing 1M asks at 1k actors without waiting, then waiting for all. */
    private double pipelined() throws Exception {
        try (ActorRuntime rt = newRuntime()) {
            rt.register(ActorType.of("pl", k -> new long[1]));
            int n = 1_000_000, keys = 1000;
            List<ActorRef<long[]>> refs = new ArrayList<>();
            for (int i = 0; i < keys; i++) refs.add(rt.ref("pl", "k" + i));
            CompletableFuture<?>[] last = new CompletableFuture<?>[keys];
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) last[i % keys] = refs.get(i % keys).call(s -> ++s[0]).toCompletableFuture();
            for (CompletableFuture<?> f : last) f.get();
            long ns = System.nanoTime() - t0;
            return n / (ns / 1e9);
        }
    }

    private double[] fanOut() throws Exception {
        try (ActorRuntime rt = newRuntime()) {
            rt.register(ActorType.of("fan", k -> new long[1]));
            int n = 100_000;
            double[] ms = new double[2];
            for (int pass = 0; pass < 2; pass++) {
                CompletableFuture<?>[] fs = new CompletableFuture<?>[n];
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++) {
                    fs[i] = rt.<long[]>ref("fan", Integer.toString(i)).call(s -> ++s[0]).toCompletableFuture();
                }
                CompletableFuture.allOf(fs).get();
                ms[pass] = (System.nanoTime() - t0) / 1e6;
            }
            return ms;
        }
    }

    /** Sequential A -> B -> C async chains (each hop awaits the next): latency of one full chain. */
    private double chainLatencyMicros() throws Exception {
        try (ActorRuntime rt = newRuntime()) {
            rt.register(ActorType.of("ca", k -> new long[1]));
            rt.register(ActorType.of("cb", k -> new long[1]));
            rt.register(ActorType.of("cc", k -> new long[1]));
            int n = 20_000;
            ActorRef<long[]> a = rt.ref("ca", "a");
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                a.<Long>ask(ac -> ac.runtime()
                                .<long[]>ref("cb", "b")
                                .<Long>ask(bc -> bc.runtime()
                                        .<long[]>ref("cc", "c")
                                        .<Long>ask(cc -> Turns.completed(1L))
                                        .thenApply(v -> v + 1))
                                .thenApply(v -> v + 1))
                        .toCompletableFuture()
                        .join();
            }
            return (System.nanoTime() - t0) / 1e3 / n;
        }
    }
}
