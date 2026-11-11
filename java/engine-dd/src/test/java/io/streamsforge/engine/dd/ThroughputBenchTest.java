package io.streamsforge.engine.dd;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableDelta;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Rough throughput of the engine's TableExecutor vs the dataflow-compiled query on the same event streams. NOT a
 * benchmark (one short unwarmed-ish run in the test JVM, noisy): it prints events/second so the README can quote an
 * order of magnitude, and checks that both sides end in the same state. Size with -Ddd.bench.events=N.
 */
class ThroughputBenchTest {
    static final int EVENTS = Integer.getInteger("dd.bench.events", 40_000);
    static final int EPOCH = Integer.getInteger("dd.bench.epoch", 500);
    static final int SYMBOLS = 1000;

    record Ev(String src, EventRecord row, long w) {}

    static List<Ev> stream(long seed, int n) {
        var rnd = new Random(seed);
        var live = new ArrayList<Ev>();
        var out = new ArrayList<Ev>(n);
        for (int i = 0; i < n; i++) {
            if (!live.isEmpty() && rnd.nextInt(100) < 20) {
                var v = live.remove(rnd.nextInt(live.size()));
                out.add(new Ev(v.src, v.row, -1));
            } else {
                boolean trade = rnd.nextInt(100) < 80;
                var e = new EventRecord();
                e.put("_ts", (long) i);
                e.put("sym", "S" + rnd.nextInt(SYMBOLS));
                if (trade) {
                    e.put("px", rnd.nextInt(400) * 0.25);
                    e.put("qty", (long) rnd.nextInt(50));
                    e.put("ok", rnd.nextBoolean());
                    e.put("fee", rnd.nextInt(9) * 0.25);
                } else {
                    e.put("bid", rnd.nextInt(400) * 0.25);
                    e.put("n", (long) rnd.nextInt(4));
                }
                var ev = new Ev(trade ? "trades" : "quotes", e, 1);
                live.add(ev);
                out.add(ev);
            }
        }
        return out;
    }

    /** Returns {engine events/s, dataflow events/s}. */
    static double[] race(String sql, List<Ev> events) {
        var enginePlan = SqlCompiler.compileTable(sql, Diff.ALL, Map.of()).plan();
        var ddPlan = Diff.plan(sql, Diff.ALL, null);

        long t0 = System.nanoTime();
        var engine = enginePlan.createExecutor();
        for (var e : events) engine.onTableDelta(e.src, new TableDelta(e.row, e.w));
        long engineNs = System.nanoTime() - t0;

        t0 = System.nanoTime();
        var dd = ddPlan.newQuery();
        int inEpoch = 0;
        for (var e : events) {
            dd.update(e.src, e.row, e.w);
            if (++inEpoch == EPOCH) { dd.commit(); inEpoch = 0; }
        }
        if (inEpoch > 0) dd.commit();
        long ddNs = System.nanoTime() - t0;

        assertEquals(Diff.weights(engine), weightsOf(dd), "both sides must end in the same state: " + sql);
        return new double[] {events.size() / (engineNs / 1e9), events.size() / (ddNs / 1e9)};
    }

    static Map<String, Long> weightsOf(DataflowTableQuery dd) {
        var m = new java.util.LinkedHashMap<String, Long>();
        for (var e : dd.snapshot().entrySet()) m.put(e.getKey(), e.getValue().weight());
        return m;
    }

    @Test
    void roughThroughput() {
        var events = stream(42, EVENTS);
        var queries = new String[][] {
            {"filter+project", "SELECT sym, px * 2 AS p2 FROM trades WHERE px > 50.0 AND ok = true"},
            {"group-by (1000 groups, 5 aggs)", "SELECT sym, COUNT(*) AS c, SUM(qty) AS sq, MIN(px) AS mn, MAX(px) AS mx, AVG(px) AS av FROM trades GROUP BY sym"},
            {"inner join (1000 keys)", "SELECT t.sym AS sym, t.px AS px, q.bid AS bid FROM trades t JOIN quotes q ON t.sym = q.sym WHERE q.n = 1"},
            {"join + group-by", "SELECT t.sym AS sym, COUNT(*) AS c, SUM(t.qty) AS sq FROM trades t JOIN quotes q ON t.sym = q.sym GROUP BY t.sym"},
        };
        race(queries[0][1], events.subList(0, Math.min(events.size(), 10_000))); // warm-up
        System.out.printf("BENCH %d events (80%% trades / 20%% quotes, 20%% of events retract), epoch=%d, %d symbols%n", EVENTS, EPOCH, SYMBOLS);
        for (var q : queries) {
            var r = race(q[1], events);
            System.out.printf("BENCH %-34s engine %,10.0f ev/s   dataflow %,10.0f ev/s   (dataflow/engine = %.2fx)%n", q[0], r[0], r[1], r[1] / r[0]);
        }
    }
}
