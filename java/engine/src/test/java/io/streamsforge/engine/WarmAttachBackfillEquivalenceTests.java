package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Wishlist #14 option (a) — the real fix (backfill on attach), exercised at the Engine level exactly the
 * way {@code TableGrain.AttachToTableInputAsync} / {@code TableActor}'s Dapr mirror now use it: read an
 * upstream's current ({@link TableExecutor#snapshot()}, {@link TableExecutor#lastEpoch()}) pair,
 * admit {@code snapshot()}'s rows into the downstream as ONE {@link TableExecutor#onTableDeltaBatch} call
 * (exactly like any other table-delta batch — same plan, same GROUP BY/JOIN/LATEST BY machinery, not a
 * bypass), then keep applying every later live delta the same way.
 *
 * <p>THE REGRESSION CHECK the wishlist calls for: a table attached warm and correctly backfilled must be
 * INDISTINGUISHABLE, row for row, from the identical table built cold over the identical input from the
 * start — and its {@link TableExecutor#unmatchedRetractions()} must read 0.
 */
class WarmAttachBackfillEquivalenceTests {
    private static final SourceSchema TICKS = schema("ticks", "g", FieldKind.STRING, "x", FieldKind.DOUBLE);

    private long ts;

    private static TableExecutor newLatestBy() {
        return compileTableAndCreate("SELECT g, x FROM ticks LATEST BY (g)", TICKS);
    }

    private static TableExecutor newRolled(String sql) {
        var latest = compileTable("SELECT g, x FROM ticks LATEST BY (g)", TICKS);
        assertTrue(latest.ok(), String.valueOf(latest.diagnostics()));
        var input = new SourceSchema("latest_x", latest.outputSchema().fields());
        var rolled = compileTable(sql, new SourceSchema[0], new SourceSchema[] {input});
        assertTrue(rolled.ok(), String.valueOf(rolled.diagnostics()));
        return rolled.plan().createExecutor();
    }

    private static Map<String, Long> toMap(TableExecutor exec) {
        var m = new HashMap<String, Long>();
        for (var v : exec.snapshot().values()) m.put(exec.canonicalRowKey(v.row()), v.weight());
        return m;
    }

    private static void assertSameRows(TableExecutor expected, TableExecutor actual) {
        var e = toMap(expected);
        var a = toMap(actual);
        assertEquals(e.size(), a.size());
        for (var kv : e.entrySet()) {
            assertTrue(a.containsKey(kv.getKey()), "warm-attached table is missing row '" + kv.getKey() + "'");
            assertEquals(kv.getValue(), a.get(kv.getKey()));
        }
    }

    @Test
    void warm_attach_backfill_matches_a_cold_built_table_exactly_and_clears_UnmatchedRetractions() {
        // COLD reference: built from t=0, fed every event live — never attaches warm.
        var coldLatest = newLatestBy();
        var coldRolled = newRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        // WARM subject: its own upstream is warmed first; warmRolled does not exist as a consumer yet.
        var warmLatest = newLatestBy();
        var warmRolled = newRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        ts = 1000;

        // Phase 1 — warm the upstream with several keys AND an update to an existing key (retract+assert:
        // exactly the shape that used to destroy the group), all while warmRolled does not exist yet.
        feedUpstreamsOnly(coldLatest, coldRolled, warmLatest, "a", 1.0);
        feedUpstreamsOnly(coldLatest, coldRolled, warmLatest, "b", 2.0);
        feedUpstreamsOnly(coldLatest, coldRolled, warmLatest, "a", 3.0);
        feedUpstreamsOnly(coldLatest, coldRolled, warmLatest, "c", 4.0);

        // Phase 2 — warmRolled "attaches": read (snapshot, lastEpoch) atomically (no admission happens
        // between these two reads on this single-threaded executor) and admit the snapshot as ONE batch —
        // precisely AttachToTableInputAsync's own sequence.
        var snapshot = warmLatest.snapshot();
        long attachEpoch = warmLatest.lastEpoch();
        assertTrue(attachEpoch >= 0, "an upstream that has admitted rows must report a real LastEpoch");
        var seedDeltas = new ArrayList<TableDelta>();
        for (var v : snapshot.values()) seedDeltas.add(new TableDelta(v.row(), v.weight()));
        assertEquals(3, seedDeltas.size()); // a, b, c — the update to "a" superseded, not duplicated
        warmRolled.onTableDeltaBatch("latest_x", seedDeltas);

        // Both tables must already agree after the backfill alone, before any live traffic reaches warmRolled.
        assertSameRows(coldRolled, warmRolled);
        assertEquals(0L, warmRolled.unmatchedRetractions());

        // Phase 3 — keep driving live traffic through both in lockstep, including retractions of keys that
        // predate the attach, and keep asserting equivalence at every step: this must stay true, not just
        // happen to hold once.
        feedBothLive(coldLatest, coldRolled, warmLatest, warmRolled, "a", 6.0);  // update of a pre-attach key
        feedBothLive(coldLatest, coldRolled, warmLatest, warmRolled, "d", 7.0);  // a brand-new key, never seen by either table before
        feedBothLive(coldLatest, coldRolled, warmLatest, warmRolled, "b", 8.0);  // update of another pre-attach key
        feedBothLive(coldLatest, coldRolled, warmLatest, warmRolled, "c", 9.0);  // update of the last pre-attach key
        feedBothLive(coldLatest, coldRolled, warmLatest, warmRolled, "a", 10.0); // a pre-attach key updated a second time
    }

    private void feedUpstreamsOnly(TableExecutor coldLatest, TableExecutor coldRolled, TableExecutor warmLatest, String g, double x) {
        for (var d : coldLatest.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x))) {
            coldRolled.onTableDelta("latest_x", d);
        }
        warmLatest.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x));
        ts++;
    }

    private void feedBothLive(TableExecutor coldLatest, TableExecutor coldRolled, TableExecutor warmLatest, TableExecutor warmRolled, String g, double x) {
        for (var d : coldLatest.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x))) {
            coldRolled.onTableDelta("latest_x", d);
        }

        var upstreamOut = warmLatest.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x));
        if (upstreamOut.size() > 0) {
            warmRolled.onTableDeltaBatch("latest_x", upstreamOut);
        }
        ts++;

        assertSameRows(coldRolled, warmRolled);
        assertEquals(0L, warmRolled.unmatchedRetractions());
    }

    /** Cascading backfill: TableGrain.AttachToTableInputAsync republishes what it backfills through
     *  the SAME ApplyAndPublishAsync path live traffic uses, which is what lets a THIRD table (chained off the
     *  just-backfilled second one) also backfill correctly. This test pins the underlying Engine mechanism that
     *  makes that true: feeding a downstream's OWN emitted output — including what it emits from ITS OWN
     *  backfill admission — into a third executor's backfill produces the same result as if the whole
     *  three-hop chain had been built cold and live from the start. */
    @Test
    void backfill_cascades_correctly_through_a_three_hop_chain() {
        // COLD reference: A -> B (GROUP BY) -> C (GROUP BY over B), all live from t=0.
        var coldA = newLatestBy();
        var coldB = newRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        Map<String, FieldKind> bFields = new LinkedHashMap<>();
        bFields.put("g", FieldKind.STRING);
        bFields.put("n", FieldKind.LONG);
        var bOutputSchema = new SourceSchema("rolled_b", bFields);
        var coldCCompiled = compileTable("SELECT COUNT(*) AS groups FROM rolled_b", new SourceSchema[0], new SourceSchema[] {bOutputSchema});
        assertTrue(coldCCompiled.ok(), String.valueOf(coldCCompiled.diagnostics()));
        var coldC = coldCCompiled.plan().createExecutor();

        // WARM subject: A is warmed alone first; B then C attach afterwards, in order, each backfilling
        // from the one before it — exactly a real multi-hop "created mid-session" scenario.
        var warmA = newLatestBy();
        var warmB = newRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var warmCCompiled = compileTable("SELECT COUNT(*) AS groups FROM rolled_b", new SourceSchema[0], new SourceSchema[] {bOutputSchema});
        assertTrue(warmCCompiled.ok(), String.valueOf(warmCCompiled.diagnostics()));
        var warmC = warmCCompiled.plan().createExecutor();

        ts = 1000;
        feedA(coldA, coldB, coldC, warmA, "a", 1.0);
        feedA(coldA, coldB, coldC, warmA, "b", 2.0);
        feedA(coldA, coldB, coldC, warmA, "c", 3.0);

        // B attaches to A.
        var aSnapshot = warmA.snapshot();
        var bSeed = new ArrayList<TableDelta>();
        for (var v : aSnapshot.values()) bSeed.add(new TableDelta(v.row(), v.weight()));
        warmB.onTableDeltaBatch("latest_x", bSeed);

        // C attaches to B — B's OWN current snapshot (which now reflects its own just-applied backfill,
        // mirroring TableGrain publishing what AttachToTableInputAsync just admitted) is what C reads, not
        // the transient backfill return value — exactly what a real attach call would see.
        var bSnapshot = warmB.snapshot();
        var cSeed = new ArrayList<TableDelta>();
        for (var v : bSnapshot.values()) cSeed.add(new TableDelta(v.row(), v.weight()));
        warmC.onTableDeltaBatch("rolled_b", cSeed);

        assertSameRows(coldB, warmB);
        assertSameRows(coldC, warmC);
        assertEquals(0L, warmB.unmatchedRetractions());
        assertEquals(0L, warmC.unmatchedRetractions());

        // One more live tick through the whole chain, both sides, to prove it stays in lockstep afterwards.
        for (var d : coldA.onStreamEvent("ticks", evt(ts, "ticks", "g", "d", "x", 4.0))) {
            var bOut = coldB.onTableDelta("latest_x", d);
            for (var bd : bOut) coldC.onTableDelta("rolled_b", bd);
        }
        var warmAOut = warmA.onStreamEvent("ticks", evt(ts, "ticks", "g", "d", "x", 4.0));
        if (warmAOut.size() > 0) {
            var warmBOut = warmB.onTableDeltaBatch("latest_x", warmAOut);
            if (warmBOut.size() > 0) warmC.onTableDeltaBatch("rolled_b", warmBOut);
        }
        ts++;

        assertSameRows(coldB, warmB);
        assertSameRows(coldC, warmC);
    }

    private void feedA(TableExecutor coldA, TableExecutor coldB, TableExecutor coldC, TableExecutor warmA, String g, double x) {
        for (var d : coldA.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x))) {
            var bOut = coldB.onTableDelta("latest_x", d);
            for (var bd : bOut) coldC.onTableDelta("rolled_b", bd);
        }
        warmA.onStreamEvent("ticks", evt(ts, "ticks", "g", g, "x", x));
        ts++;
    }
}
