package io.streamsforge.host;

import static io.streamsforge.host.Fx.QUOTES;
import static io.streamsforge.host.Fx.REF;
import static io.streamsforge.host.Fx.TRADES;
import static io.streamsforge.host.Fx.canonOf;
import static io.streamsforge.host.Fx.ev;
import static io.streamsforge.host.Fx.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.SourceSchema;
import java.util.ArrayList;
import java.util.List;

/** Scenarios 1 and 2: source -> pipeline and source -> table (and table/pipeline -> table), on every runtime. */
class HostScenarioTest {

    // ---------------------------------------------------------------- 1. source -> pipeline

    @OnAllRuntimes
    void tumblingWindowGroupByEmitsAfterWatermarkAdvance(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.pipeline("vol", "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total FROM trades "
                + "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL");
            var results = Fx.<EventRecord>sync(new ArrayList<>());
            get(fx.host.subscribePipeline("vol", results::addAll));

            fx.publish("trades", List.of(
                ev(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L),
                ev(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 20L),
                ev(3000, "trades", "symbol", "MSFT", "price", 1.0, "qty", 5L)));
            fx.quiesce();
            assertTrue(results.isEmpty(), "EMIT FINAL: nothing before the watermark passes the window end");

            get(fx.host.advanceWatermark("vol", 10500)); // watermark 9500 < 10000
            fx.quiesce();
            assertTrue(results.isEmpty());

            get(fx.host.advanceWatermark("vol", 12000)); // watermark 11000 >= 10000
            fx.quiesce();
            assertEquals(2, results.size(), results.toString());
            var bySymbol = new java.util.TreeMap<String, EventRecord>();
            for (var r : results) bySymbol.put((String) r.get("symbol"), r);
            assertEquals(2L, bySymbol.get("AAPL").get("cnt"));
            assertEquals(30L, bySymbol.get("AAPL").get("total"));
            assertEquals(1L, bySymbol.get("MSFT").get("cnt"));
            assertEquals(5L, bySymbol.get("MSFT").get("total"));
            assertEquals(0L, bySymbol.get("AAPL").get("window_start"));
            assertEquals(10000L, bySymbol.get("AAPL").get("window_end"));
            assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
        }
    }

    @OnAllRuntimes
    void streamStreamJoinMatchesWithinTheBound(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.source(QUOTES);
            fx.pipeline("tq", "SELECT t.symbol AS symbol, t.price AS price, q.bid AS bid FROM trades t "
                + "INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol");
            var results = Fx.<EventRecord>sync(new ArrayList<>());
            get(fx.host.subscribePipeline("tq", results::addAll));

            fx.publish("quotes", List.of(ev(1000, "quotes", "symbol", "AAPL", "bid", 99.5)));
            fx.quiesce();
            fx.publish("trades", List.of(
                ev(2000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L),     // within 5s of the quote
                ev(9000, "trades", "symbol", "AAPL", "price", 101.0, "qty", 1L),     // 8s after it: no match
                ev(2500, "trades", "symbol", "MSFT", "price", 300.0, "qty", 1L)));   // no quote
            fx.quiesce();

            assertEquals(1, results.size(), results.toString());
            assertEquals("AAPL", results.get(0).get("symbol"));
            assertEquals(100.0, results.get(0).get("price"));
            assertEquals(99.5, results.get(0).get("bid"));
            assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
        }
    }

    @OnAllRuntimes
    void watermarkTimerAdvancesWithoutTheManualHook(Fx.Rt rt) {
        var clock = new java.util.concurrent.atomic.AtomicLong(0);
        var opts = StreamsHost.Options.defaults().withFlushPeriod(java.time.Duration.ofMillis(10))
            .withWatermarkPeriod(java.time.Duration.ofMillis(10)).withClock(clock::get);
        try (var fx = new Fx(rt, opts)) {
            fx.source(TRADES);
            fx.pipeline("vol", "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL");
            var results = Fx.<EventRecord>sync(new ArrayList<>());
            get(fx.host.subscribePipeline("vol", results::addAll));
            fx.publish("trades", List.of(ev(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L)));
            fx.quiesce();
            clock.set(20_000); // the fake clock jumps; the actor timer (not the test) advances the watermark
            Fx.await("the watermark timer to close the window", () -> !results.isEmpty());
            assertEquals(1L, results.get(0).get("cnt"));
        }
    }

    // ---------------------------------------------------------------- 2. source -> table

    private static final String GROUP_SQL = "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total FROM trades GROUP BY symbol";

    @OnAllRuntimes
    void groupByEmitsRetractionAssertionPairs(Fx.Rt rt) {
        for (int p : new int[] {1, 2}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("totals", GROUP_SQL, p);
                var batches = Fx.<TableDeltaBatch>sync(new ArrayList<>());
                get(fx.host.subscribeTable("totals", batches::add));

                var all = new ArrayList<EventRecord>();
                var first = List.of(ev(1, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L));
                var second = List.of(ev(2, "trades", "symbol", "AAPL", "price", 1.0, "qty", 5L));
                fx.publish("trades", first);
                fx.quiesce();
                fx.publish("trades", second);
                fx.quiesce();
                all.addAll(first);
                all.addAll(second);

                // The second event moves AAPL from (n=1,total=10) to (n=2,total=15): a retraction and an assertion.
                var weights = new ArrayList<Long>();
                for (var b : batches) for (var d : b.deltas()) weights.add(d.weight());
                assertTrue(weights.contains(-1L), "p=" + p + " expected a retraction in " + weights);
                assertTrue(weights.contains(1L));

                var exec = Fx.oracle(GROUP_SQL, TRADES);
                for (var e : all) exec.onStreamEvent("trades", e);
                assertEquals(canonOf(exec), fx.snap("totals"), "p=" + p);
                assertEquals(canonOf(exec), Fx.fold(batches), "folding the published deltas reproduces the snapshot (p=" + p + ")");
                assertEquals(1, fx.snap("totals").size());
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }

    @OnAllRuntimes
    void joinOfTwoSources(Fx.Rt rt) {
        String sql = "SELECT t.symbol AS symbol, t.qty AS qty, r.tag AS tag FROM trades t JOIN ref r ON t.symbol = r.symbol";
        for (int p : new int[] {1, 2, 4}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.source(REF);
                fx.table("tagged", sql, p);
                var trades = List.of(
                    ev(1, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L),
                    ev(2, "trades", "symbol", "MSFT", "price", 1.0, "qty", 20L),
                    ev(3, "trades", "symbol", "GOOG", "price", 1.0, "qty", 30L));
                var refs = List.of(
                    ev(1, "ref", "symbol", "AAPL", "tag", "tech"),
                    ev(2, "ref", "symbol", "MSFT", "tag", "tech"),
                    ev(3, "ref", "symbol", "IBM", "tag", "old"));
                fx.publish("trades", trades);
                fx.publish("ref", refs);
                fx.quiesce();

                var exec = Fx.oracle(sql, TRADES, REF);
                for (var e : trades) exec.onStreamEvent("trades", e);
                for (var e : refs) exec.onStreamEvent("ref", e);
                assertEquals(2, canonOf(exec).size());
                assertEquals(canonOf(exec), fx.snap("tagged"), "p=" + p);
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }

    @OnAllRuntimes
    void latestBy(Fx.Rt rt) {
        var orders = Fx.schema("orders", "order_id", FieldKind.STRING, "stage", FieldKind.STRING, "filled", FieldKind.LONG);
        String sql = "SELECT order_id, stage, filled FROM orders LATEST BY (order_id)";
        for (int p : new int[] {1, 2}) {
            try (var fx = new Fx(rt)) {
                fx.source(orders);
                fx.table("current_orders", sql, p);
                var evts = List.of(
                    ev(1, "orders", "order_id", "o1", "stage", "NEW", "filled", 0L),
                    ev(2, "orders", "order_id", "o2", "stage", "NEW", "filled", 0L),
                    ev(3, "orders", "order_id", "o1", "stage", "PARTIAL", "filled", 5L),
                    ev(4, "orders", "order_id", "o1", "stage", "FILLED", "filled", 10L));
                // One batch per event, quiesced in between: arrival order is the order LATEST BY sees.
                for (var e : evts) {
                    fx.publish("orders", List.of(e));
                    fx.quiesce();
                }
                var exec = Fx.oracle(sql, orders);
                for (var e : evts) exec.onStreamEvent("orders", e);
                assertEquals(2, canonOf(exec).size());
                assertEquals(canonOf(exec), fx.snap("current_orders"), "p=" + p);
                assertTrue(canonOf(exec).keySet().stream().anyMatch(k -> k.contains("FILLED")));
            }
        }
    }

    @OnAllRuntimes
    void tableOverTable(Fx.Rt rt) {
        String upSql = GROUP_SQL;
        String downSql = "SELECT symbol, total FROM totals WHERE total > 25";
        for (int[] ps : new int[][] {{1, 1}, {1, 2}, {2, 1}, {2, 2}}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("totals", upSql, ps[0]);
                fx.table("big", downSql, ps[1]);
                var evts = new ArrayList<EventRecord>();
                for (int i = 0; i < 40; i++) {
                    evts.add(ev(i, "trades", "symbol", "S" + (i % 5), "price", 1.0, "qty", (long) (i % 7 + 1)));
                }
                for (int i = 0; i < evts.size(); i += 8) {
                    fx.publish("trades", evts.subList(i, i + 8));
                    fx.quiesce();
                }
                var up = Fx.oracle(upSql, TRADES);
                for (var e : evts) up.onStreamEvent("trades", e);
                var upOut = Fx.compileOut(upSql, TRADES);
                var down = Fx.oracle(downSql, new SourceSchema[0], new SourceSchema[] {new SourceSchema("totals", upOut.fields())});
                // The oracle's downstream is fed the oracle upstream's snapshot (final state is order-independent).
                var seed = new ArrayList<io.streamsforge.engine.TableDelta>();
                for (var rw : up.snapshot().values()) seed.add(new io.streamsforge.engine.TableDelta(rw.row(), rw.weight()));
                down.onTableDeltaBatch("totals", seed);
                assertEquals(canonOf(up), fx.snap("totals"), java.util.Arrays.toString(ps));
                assertEquals(canonOf(down), fx.snap("big"), java.util.Arrays.toString(ps));
                assertFalse(canonOf(down).isEmpty());
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }

    @OnAllRuntimes
    void tableOverPipeline(Fx.Rt rt) {
        String pipeSql = "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total FROM trades "
            + "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        String tableSql = "SELECT symbol, SUM(total) AS grand, SUM(cnt) AS trades FROM windows GROUP BY symbol";
        for (int p : new int[] {1, 2}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.pipeline("windows", pipeSql);
                fx.table("grand", tableSql, p);
                fx.publish("trades", List.of(
                    ev(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L),
                    ev(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 20L),
                    ev(3000, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L),
                    ev(12000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 7L)));
                fx.quiesce();
                get(fx.host.advanceWatermark("windows", 30_000)); // closes [0,10000) and [10000,20000)
                fx.quiesce();

                var snap = fx.snap("grand");
                assertEquals(2, snap.size(), "p=" + p + " " + snap);
                assertTrue(snap.keySet().stream().anyMatch(k -> k.contains("AAPL") && k.contains("37") && k.contains("3")), snap.toString());
                assertTrue(snap.keySet().stream().anyMatch(k -> k.contains("MSFT")), snap.toString());
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }
}
