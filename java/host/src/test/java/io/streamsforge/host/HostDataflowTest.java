package io.streamsforge.host;

import static io.streamsforge.host.Fx.REF;
import static io.streamsforge.host.Fx.TRADES;
import static io.streamsforge.host.Fx.canonOf;
import static io.streamsforge.host.Fx.ev;
import static io.streamsforge.host.Fx.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Scenarios 3 (partitioned equivalence), 4 (late attach) and 5 (concurrent publishers). */
class HostDataflowTest {
    private static final String[] SYMBOLS = {"AAPL", "MSFT", "GOOG", "AMZN", "IBM", "ORCL", "SAP", "NFLX", "TSLA", "META", "NVDA", "AMD"};

    private static List<EventRecord> randomTrades(Random rnd, int n, long ts0) {
        var out = new ArrayList<EventRecord>(n);
        for (int i = 0; i < n; i++) {
            out.add(ev(ts0 + i, "trades", "symbol", SYMBOLS[rnd.nextInt(SYMBOLS.length)], "price", 1.0, "qty", (long) (rnd.nextInt(100) + 1)));
        }
        return out;
    }

    private static List<EventRecord> randomRefs(Random rnd, int n) {
        var out = new ArrayList<EventRecord>(n);
        for (int i = 0; i < n; i++) {
            out.add(ev(i, "ref", "symbol", SYMBOLS[rnd.nextInt(SYMBOLS.length - 2)], "tag", "t" + rnd.nextInt(4)));
        }
        return out;
    }

    // ---------------------------------------------------------------- 3. partitioned equivalence

    @OnAllRuntimes
    void groupByConvergesToTheSameSnapshotAtParallelism1_2_4(Fx.Rt rt) {
        String sql = "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total, MIN(qty) AS lo, MAX(qty) AS hi FROM trades GROUP BY symbol";
        for (long seed : new long[] {1, 42, 20261111}) {
            var rnd = new Random(seed);
            var events = randomTrades(rnd, 600, 0);
            var expected = Fx.oracle(sql, TRADES);
            for (var e : events) expected.onStreamEvent("trades", e);
            Map<String, Long> want = canonOf(expected);
            assertEquals(SYMBOLS.length, want.size());

            for (int p : new int[] {1, 2, 4}) {
                try (var fx = new Fx(rt)) {
                    fx.source(TRADES);
                    fx.table("agg", sql, p);
                    // Random batch sizes, with a quiesce now and then so epochs of different sizes interleave.
                    int i = 0;
                    while (i < events.size()) {
                        int len = Math.min(events.size() - i, 1 + rnd.nextInt(40));
                        fx.publish("trades", events.subList(i, i + len));
                        i += len;
                        if (rnd.nextInt(5) == 0) fx.quiesce();
                    }
                    fx.quiesce();
                    assertEquals(want, fx.snap("agg"), "seed=" + seed + " p=" + p);
                    assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
                }
            }
        }
    }

    @OnAllRuntimes
    void joinConvergesToTheSameSnapshotAtParallelism1_2_4(Fx.Rt rt) {
        String sql = "SELECT t.symbol AS symbol, t.qty AS qty, r.tag AS tag FROM trades t JOIN ref r ON t.symbol = r.symbol";
        for (long seed : new long[] {7, 99}) {
            var rnd = new Random(seed);
            var trades = randomTrades(rnd, 300, 0);
            var refs = randomRefs(rnd, 30);
            var expected = Fx.oracle(sql, TRADES, REF);
            for (var e : trades) expected.onStreamEvent("trades", e);
            for (var e : refs) expected.onStreamEvent("ref", e);
            Map<String, Long> want = canonOf(expected);
            assertFalse(want.isEmpty());

            for (int p : new int[] {1, 2, 4}) {
                try (var fx = new Fx(rt)) {
                    fx.source(TRADES);
                    fx.source(REF);
                    fx.table("joined", sql, p);
                    // Interleave the two inputs in random slices.
                    int ti = 0, ri = 0;
                    while (ti < trades.size() || ri < refs.size()) {
                        if (ri < refs.size() && (ti >= trades.size() || rnd.nextBoolean())) {
                            int len = Math.min(refs.size() - ri, 1 + rnd.nextInt(6));
                            fx.publish("ref", refs.subList(ri, ri + len));
                            ri += len;
                        } else {
                            int len = Math.min(trades.size() - ti, 1 + rnd.nextInt(30));
                            fx.publish("trades", trades.subList(ti, ti + len));
                            ti += len;
                        }
                        if (rnd.nextInt(6) == 0) fx.quiesce();
                    }
                    fx.quiesce();
                    assertEquals(want, fx.snap("joined"), "seed=" + seed + " p=" + p);
                    assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
                }
            }
        }
    }

    // ---------------------------------------------------------------- 4. late attach

    private static final String UP_SQL = "SELECT symbol, SUM(qty) AS total, COUNT(*) AS n FROM trades GROUP BY symbol";
    private static final String DOWN_SQL = "SELECT SUM(total) AS grand, SUM(n) AS cnt FROM totals";

    @OnAllRuntimes
    void downstreamTableCreatedWhileUpstreamReceivesAContinuousStream(Fx.Rt rt) throws Exception {
        for (int[] ps : new int[][] {{1, 1}, {1, 2}, {2, 1}, {2, 2}}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("totals", UP_SQL, ps[0]);

                var rnd = new Random(31 + ps[0] * 10 + ps[1]);
                int batches = 300, perBatch = 20;
                var all = new ArrayList<List<EventRecord>>();
                for (int b = 0; b < batches; b++) all.add(randomTrades(rnd, perBatch, b * perBatch));

                var halfway = new CountDownLatch(1);
                var publisher = new Thread(() -> {
                    for (int b = 0; b < batches; b++) {
                        get(fx.host.publish("trades", all.get(b)));
                        if (b == batches / 3) halfway.countDown();
                    }
                }, "publisher");
                publisher.start();
                halfway.await();
                // The stream keeps flowing while the downstream table subscribes, takes its snapshot pair and starts.
                get(fx.host.createTable("grand", DOWN_SQL, ps[1]));
                get(fx.host.start("grand"));
                publisher.join();
                fx.quiesce();

                long qty = 0, n = 0;
                for (var b : all) for (var e : b) { qty += (Long) e.get("qty"); n++; }
                var snap = fx.snap("grand");
                assertEquals(1, snap.size(), "ps=" + java.util.Arrays.toString(ps) + " " + snap);
                String row = snap.keySet().iterator().next();
                assertTrue(row.contains("\"grand\":" + qty) && row.contains("\"cnt\":" + n),
                    "ps=" + java.util.Arrays.toString(ps) + " expected grand=" + qty + " cnt=" + n + " but " + row);

                // And the upstream itself is exact.
                var up = Fx.oracle(UP_SQL, TRADES);
                for (var b : all) for (var e : b) up.onStreamEvent("trades", e);
                assertEquals(canonOf(up), fx.snap("totals"));
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }

    @OnAllRuntimes
    void attachedConsumerReplicaEqualsTheTable(Fx.Rt rt) throws Exception {
        for (int p : new int[] {1, 2}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("totals", UP_SQL, p);
                var rnd = new Random(5);
                var stop = new AtomicBoolean();
                var count = new AtomicInteger();
                var publisher = new Thread(() -> {
                    while (!stop.get() && count.get() < 400) {
                        get(fx.host.publish("trades", randomTrades(rnd, 10, count.getAndIncrement() * 10L)));
                    }
                }, "publisher");
                publisher.start();
                Fx.await("some data", () -> count.get() > 20);

                var seen = Fx.<TableDeltaBatch>sync(new ArrayList<>());
                var attach = get(fx.host.attachTable("totals", seen::add));
                publisher.join();
                fx.quiesce();

                // replica = snapshot + every batch with epoch > lastEpoch.
                var replica = new TreeMap<String, Long>(Fx.canon(attach.snapshot()));
                long last = attach.lastEpoch();
                for (var b : seen) {
                    if (b.epoch() <= last) continue;
                    for (TableDelta d : b.deltas()) {
                        replica.merge(io.streamsforge.engine.TableExecutor.canonicalRowKeyOf(d.row()), d.weight(), Long::sum);
                    }
                }
                replica.values().removeIf(w -> w == 0);
                assertEquals(fx.snap("totals"), replica, "p=" + p);
                get(attach.subscription().cancel());
            }
        }
    }

    // ---------------------------------------------------------------- 5. concurrent publishers

    @OnAllRuntimes
    void manyConcurrentPublishersMatchTheSingleThreadedExecutor(Fx.Rt rt) throws Exception {
        String sql = "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total FROM trades GROUP BY symbol";
        int threads = 8, batchesPerThread = 60, perBatch = 12;
        var perThread = new ArrayList<List<List<EventRecord>>>();
        var expected = Fx.oracle(sql, TRADES);
        for (int t = 0; t < threads; t++) {
            var rnd = new Random(1000 + t);
            var mine = new ArrayList<List<EventRecord>>();
            for (int b = 0; b < batchesPerThread; b++) {
                var batch = randomTrades(rnd, perBatch, t * 1_000_000L + b * perBatch);
                mine.add(batch);
                for (var e : batch) expected.onStreamEvent("trades", e); // the multiset, in some single order
            }
            perThread.add(mine);
        }
        Map<String, Long> want = canonOf(expected);

        for (int p : new int[] {1, 4}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("agg", sql, p);
                var start = new CountDownLatch(1);
                var pool = new ArrayList<Thread>();
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                for (int t = 0; t < threads; t++) {
                    var mine = perThread.get(t);
                    var th = new Thread(() -> {
                        try {
                            start.await();
                            for (var batch : mine) get(fx.host.publish("trades", batch));
                        } catch (Throwable e) {
                            failure.set(e);
                        }
                    }, "pub-" + t);
                    th.start();
                    pool.add(th);
                }
                start.countDown();
                for (var th : pool) th.join();
                assertEquals(null, failure.get());
                fx.quiesce();
                assertEquals(want, fx.snap("agg"), "p=" + p);
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }
}
