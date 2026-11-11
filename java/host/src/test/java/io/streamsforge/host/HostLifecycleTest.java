package io.streamsforge.host;

import static io.streamsforge.host.Fx.TRADES;
import static io.streamsforge.host.Fx.canonOf;
import static io.streamsforge.host.Fx.ev;
import static io.streamsforge.host.Fx.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.actors.ActorKey;
import io.streamsforge.engine.EventRecord;
import java.util.ArrayList;
import java.util.List;

/** Scenario 6: stop/start/delete, refusals; plus what idle collection may and may not do. */
class HostLifecycleTest {
    private static final String SQL = "SELECT symbol, COUNT(*) AS n, SUM(qty) AS total FROM trades GROUP BY symbol";

    private static List<EventRecord> trades(long ts0, String... symbols) {
        var l = new ArrayList<EventRecord>();
        long ts = ts0;
        for (var s : symbols) l.add(ev(ts++, "trades", "symbol", s, "price", 1.0, "qty", 2L));
        return l;
    }

    @OnAllRuntimes
    void stopDropsStateAndStartRebuildsFromNewEvents(Fx.Rt rt) {
        for (int p : new int[] {1, 3}) {
            try (var fx = new Fx(rt)) {
                fx.source(TRADES);
                fx.table("agg", SQL, p);
                fx.publish("trades", trades(0, "A", "A", "B"));
                fx.quiesce();
                assertEquals(2, fx.snap("agg").size(), "p=" + p);

                get(fx.host.stop("agg"));
                assertTrue(fx.snap("agg").isEmpty(), "stop discards the (unpersisted) state, p=" + p);
                // Publishing while the table is stopped is fine: nobody is subscribed.
                fx.publish("trades", trades(10, "C"));
                fx.quiesce();

                get(fx.host.start("agg"));
                assertTrue(fx.snap("agg").isEmpty(), "no replay of events published while stopped");
                fx.publish("trades", trades(20, "D", "D"));
                fx.quiesce();
                var exec = Fx.oracle(SQL, TRADES);
                for (var e : trades(20, "D", "D")) exec.onStreamEvent("trades", e);
                assertEquals(canonOf(exec), fx.snap("agg"), "p=" + p);

                // Idempotent.
                get(fx.host.start("agg"));
                get(fx.host.stop("agg"));
                get(fx.host.stop("agg"));
                assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
            }
        }
    }

    @OnAllRuntimes
    void pipelineStopStart(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.pipeline("flt", "SELECT symbol, qty FROM trades WHERE qty > 1");
            var got = Fx.<EventRecord>sync(new ArrayList<>());
            get(fx.host.subscribePipeline("flt", got::addAll));
            fx.publish("trades", trades(0, "A"));
            fx.quiesce();
            assertEquals(1, got.size());
            get(fx.host.stop("flt"));
            fx.publish("trades", trades(1, "B"));
            fx.quiesce();
            assertEquals(1, got.size(), "a stopped pipeline produces nothing");
            get(fx.host.start("flt"));
            fx.publish("trades", trades(2, "C"));
            fx.quiesce();
            assertEquals(2, got.size());
        }
    }

    @OnAllRuntimes
    void unsubscribeStopsDelivery(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.table("agg", SQL, 1);
            var seen = Fx.<TableDeltaBatch>sync(new ArrayList<>());
            var sub = get(fx.host.subscribeTable("agg", seen::add));
            fx.publish("trades", trades(0, "A"));
            fx.quiesce();
            int before = seen.size();
            assertTrue(before > 0);
            get(sub.cancel());
            fx.publish("trades", trades(1, "B"));
            fx.quiesce();
            assertEquals(before, seen.size());
        }
    }

    @OnAllRuntimes
    void deleteRemovesTheEntityAndFreesTheName(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.table("agg", SQL, 2);
            fx.publish("trades", trades(0, "A"));
            fx.quiesce();

            // A source that something reads cannot be deleted.
            var e = assertThrows(CatalogException.class, () -> get(fx.host.delete("trades")));
            assertTrue(e.getMessage().contains("agg"), e.getMessage());

            get(fx.host.delete("agg")); // deletes a RUNNING table (stops it first)
            assertThrows(CatalogException.class, () -> fx.snap("agg"));
            assertThrows(CatalogException.class, () -> get(fx.host.start("agg")));
            assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());

            // The name is free again, and the new table starts empty.
            fx.table("agg", "SELECT symbol, qty FROM trades", 1);
            assertTrue(fx.snap("agg").isEmpty());
            fx.publish("trades", trades(5, "Z"));
            fx.quiesce();
            assertEquals(1, fx.snap("agg").size());

            get(fx.host.delete("agg"));
            get(fx.host.delete("trades"));
            assertTrue(get(fx.host.list()).isEmpty());
        }
    }

    @OnAllRuntimes
    void invalidSqlIsRefusedWithDiagnostics(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            var p = assertThrows(CatalogException.class, () -> get(fx.host.createPipeline("bad", "SELEKT nothing FROM trades")));
            assertFalse(p.diagnostics().isEmpty());
            assertTrue(p.diagnostics().get(0).line() >= 1);

            var unknown = assertThrows(CatalogException.class, () -> get(fx.host.createPipeline("bad", "SELECT x FROM nosuchsource")));
            assertFalse(unknown.diagnostics().isEmpty());

            var t = assertThrows(CatalogException.class, () -> get(fx.host.createTable("bad", "SELECT nosuchcolumn FROM trades GROUP BY symbol", 1)));
            assertFalse(t.diagnostics().isEmpty());

            // A pipeline reads sources only (hard rule 6): another pipeline's name is not a source.
            fx.pipeline("ok", "SELECT symbol FROM trades");
            assertThrows(CatalogException.class, () -> get(fx.host.createPipeline("chained", "SELECT symbol FROM ok")));

            // Refused shapes leave nothing behind: the names are still free.
            assertEquals(2, get(fx.host.list()).size()); // trades, ok
            fx.table("bad", "SELECT symbol FROM trades", 1);

            // A shape the partitioned dataflow cannot run is refused at parallelism >= 2 and fine at 1.
            String derived = "SELECT x.symbol FROM (SELECT symbol FROM trades) x";
            assertThrows(CatalogException.class, () -> get(fx.host.createTable("derived2", derived, 2)));
            get(fx.host.createTable("derived1", derived, 1));
            assertThrows(CatalogException.class, () -> get(fx.host.createTable("zero", "SELECT symbol FROM trades", 0)));
        }
    }

    @OnAllRuntimes
    void nameCollisionsAreRefusedAcrossKinds(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            assertThrows(CatalogException.class, () -> fx.source(TRADES));
            assertThrows(CatalogException.class, () -> get(fx.host.createPipeline("trades", "SELECT symbol FROM trades")));
            assertThrows(CatalogException.class, () -> get(fx.host.createTable("trades", "SELECT symbol FROM trades", 1)));
            fx.pipeline("pp", "SELECT symbol FROM trades");
            assertThrows(CatalogException.class, () -> get(fx.host.createTable("pp", "SELECT symbol FROM trades", 1)));
            fx.table("tt", "SELECT symbol FROM trades", 1);
            assertThrows(CatalogException.class, () -> get(fx.host.createPipeline("tt", "SELECT symbol FROM trades")));
            assertThrows(CatalogException.class, () -> get(fx.host.createSource("tt", Fx.schema("tt"))));
            assertThrows(CatalogException.class, () -> get(fx.host.createSource("not an identifier", Fx.schema("x"))));
            // The refused creates did not disturb the originals.
            assertEquals(3, get(fx.host.list()).size());
        }
    }

    @OnAllRuntimes
    void dependentTablesAreProtected(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            get(fx.host.createTable("up", SQL, 1));
            // A table cannot start before the table it reads.
            get(fx.host.createTable("down", "SELECT symbol, total FROM up", 1));
            var notRunning = assertThrows(CatalogException.class, () -> get(fx.host.start("down")));
            assertTrue(notRunning.getMessage().contains("up"), notRunning.getMessage());
            get(fx.host.start("up"));
            get(fx.host.start("down"));
            // ...nor stop while a running table reads it.
            assertThrows(CatalogException.class, () -> get(fx.host.stop("up")));
            assertThrows(CatalogException.class, () -> get(fx.host.delete("up")));
            get(fx.host.stop("down"));
            get(fx.host.stop("up"));
            get(fx.host.delete("down"));
            get(fx.host.delete("up"));
        }
    }

    @OnAllRuntimes
    void publishingToAnUnknownOrStoppedSourceFails(Fx.Rt rt) {
        try (var fx = new Fx(rt)) {
            assertThrows(CatalogException.class, () -> fx.publish("ghost", trades(0, "A")));
            fx.source(TRADES);
            get(fx.host.stop("trades"));
            assertThrows(IllegalStateException.class, () -> fx.publish("trades", trades(0, "A")));
            get(fx.host.start("trades"));
            fx.publish("trades", trades(0, "A"));
        }
    }

    // ---------------------------------------------------------------- idle collection

    @OnAllRuntimes
    void runningEntitiesAreNeverIdleCollectedAndStopReleasesTheirActivations(Fx.Rt rt) throws Exception {
        try (var fx = new Fx(rt)) {
            fx.source(TRADES);
            fx.table("agg", SQL, 3);
            fx.publish("trades", trades(0, "A", "B", "C"));
            fx.quiesce();
            var before = snapshotOfKeys(fx);
            assertTrue(before.stream().anyMatch(k -> k.type().equals(Actors.STAGE)), before.toString());
            assertTrue(before.stream().anyMatch(k -> k.type().equals(Actors.INGEST)));
            assertTrue(before.contains(new ActorKey(Actors.OUTPUT, "agg")));

            // Every type is registered with idleTimeout == null: a long quiet spell collects nothing and loses nothing.
            Thread.sleep(300);
            assertEquals(before, snapshotOfKeys(fx));
            assertEquals(2 + 1, fx.snap("agg").size()); // A, B, C -> three groups
            fx.publish("trades", trades(100, "A"));
            fx.quiesce();
            assertTrue(fx.snap("agg").keySet().stream().anyMatch(k -> k.contains("\"n\":2")));

            // stop() is the one thing that releases the per-(stage, partition) and ingest activations.
            get(fx.host.stop("agg"));
            var after = snapshotOfKeys(fx);
            assertFalse(after.stream().anyMatch(k -> k.type().equals(Actors.STAGE) || k.type().equals(Actors.INGEST)), after.toString());
            assertTrue(fx.host.errors().isEmpty(), fx.host.errors().toString());
        }
    }

    private static java.util.Set<ActorKey> snapshotOfKeys(Fx fx) {
        return new java.util.HashSet<>(fx.rt.activeKeys());
    }

}
