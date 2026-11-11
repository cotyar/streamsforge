package io.streamsforge.host;

import static io.streamsforge.host.Fx.TRADES;
import static io.streamsforge.host.Fx.ev;
import static io.streamsforge.host.Fx.get;

import io.streamsforge.engine.EventRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Off by default: {@code ./gradlew :host:test -Dbench=true --tests '*HostBenchmarkTest*' -i}. Crude single-JVM numbers:
 * one warm-up pass (fresh fixture), one measured pass per cell. Throughput = events published / wall time from the
 * first publish until {@link StreamsHost#quiesce} returns (i.e. until the last event is reflected in the table / the
 * pipeline's windows have been closed and delivered).
 */
class HostBenchmarkTest {
    private static final int EVENTS = Integer.getInteger("bench.events", 400_000);
    private static final int BATCH = 1000;
    private static final int KEYS = 100;

    private static List<List<EventRecord>> batches(int n) {
        var out = new ArrayList<List<EventRecord>>();
        var cur = new ArrayList<EventRecord>(BATCH);
        for (int i = 0; i < n; i++) {
            cur.add(ev(i, "trades", "symbol", "S" + (i % KEYS), "price", 1.0, "qty", (long) (i % 10 + 1)));
            if (cur.size() == BATCH) {
                out.add(cur);
                cur = new ArrayList<>(BATCH);
            }
        }
        if (!cur.isEmpty()) out.add(cur);
        return out;
    }

    private static double run(Fx.Rt rt, String kind, int parallelism, List<List<EventRecord>> data, int events) {
        var opts = StreamsHost.Options.defaults().withFlushPeriod(Duration.ofMillis(20)).withWatermarkPeriod(null);
        try (var fx = new Fx(rt, opts)) {
            fx.source(TRADES);
            if (kind.equals("table")) {
                fx.table("agg", "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total FROM trades GROUP BY symbol", parallelism);
            } else {
                fx.pipeline("win", "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL");
                get(fx.host.subscribePipeline("win", rows -> { }));
            }
            long t0 = System.nanoTime();
            var window = new ArrayList<CompletionStage<Void>>();
            for (var b : data) {
                window.add(fx.host.publish("trades", b));
                if (window.size() >= 64) {
                    for (var s : window) get(s);
                    window.clear();
                }
            }
            for (var s : window) get(s);
            fx.quiesce();
            if (kind.equals("pipeline")) {
                get(fx.host.advanceWatermark("win", events + 60_000));
                fx.quiesce();
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            if (!fx.host.errors().isEmpty()) throw new AssertionError(fx.host.errors().toString());
            return events / secs;
        }
    }

    @Test
    void throughput() {
        Assumptions.assumeTrue("true".equals(System.getProperty("bench")), "benchmark is off by default (-Dbench=true)");
        var warm = batches(100_000);
        var data = batches(EVENTS);
        int total = data.stream().mapToInt(List::size).sum();

        var rows = new ArrayList<String>();
        var cells = List.of(new Object[] {"source -> GROUP BY table, P=1", "table", 1},
            new Object[] {"source -> GROUP BY table, P=4", "table", 4},
            new Object[] {"source -> tumbling-window pipeline", "pipeline", 1});
        var runtimes = Fx.runtimes().map(a -> (Fx.Rt) a.get()[0]).toList();
        for (var c : cells) {
            var line = new StringBuilder("| " + c[0] + " |");
            for (var rt : runtimes) {
                run(rt, (String) c[1], (Integer) c[2], warm, 100_000);
                double eps = run(rt, (String) c[1], (Integer) c[2], data, total);
                line.append(String.format(" %,d |", Math.round(eps)));
            }
            rows.add(line.toString());
        }
        var out = new StringBuilder("\nHOST BENCHMARK (" + total + " events, batches of " + BATCH + ", " + KEYS + " keys), events/s\n");
        out.append("| scenario |");
        runtimes.forEach(r -> out.append(' ').append(r.name()).append(" |"));
        out.append("\n|---|").append("---:|".repeat(runtimes.size())).append('\n');
        rows.forEach(r -> out.append(r).append('\n'));
        System.out.println(out);
    }
}
