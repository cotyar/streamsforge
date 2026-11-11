package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.junit.jupiter.api.Test;

/**
 * Wishlist #15 — "retract/assert of one upstream change should be applied atomically downstream". Pins
 * the reported symptom end to end through three chained table-mode executors, exactly the demo's shape:
 *
 * <p>{@code ticks (stream) --LATEST BY--> latest_x --GROUP BY--> agg --LEFT JOIN--> joined}
 *
 * <p>A single change to one key in {@code ticks} makes {@code latest_x} emit retract(old)+assert(new) — TWO
 * deltas for ONE upstream change. Fed through a per-element loop each element gets its own epoch, so the LEFT
 * JOIN downstream pads the joined row's right-hand columns with NULL in between. {@code onTableDeltaBatch} keeps a
 * whole upstream batch under ONE epoch and the engine nets that epoch's raw output by canonical row key before
 * it is ever returned, so the pad never leaves the table.
 */
class EpochAtomicConsolidationTests {
    private static final SourceSchema TICKS = schema("ticks", "g", FieldKind.STRING, "x", FieldKind.DOUBLE);
    private static final SourceSchema ORDERS = schema("orders", "tag", FieldKind.STRING);

    private record Chain(TableExecutor latest, TableExecutor agg, TableExecutor joined) {}

    /** Builds the three-table chain described in the class doc. {@code agg} is a GROUP BY with exactly ONE
     *  contributing row per group (SUM over a LATEST BY table, one row per key) — chosen deliberately: with only
     *  one contributor, agg's OWN TableReduceOp output for a value change is a clean two-element
     *  [retract(old), assert(new)]. */
    private static Chain buildChain() {
        var latest = compileTable("SELECT g, x FROM ticks LATEST BY (g)", TICKS);
        assertTrue(latest.ok(), String.valueOf(latest.diagnostics()));
        var latestSchema = new SourceSchema("latest_x", latest.outputSchema().fields());

        var agg = compileTable("SELECT g, SUM(x) AS total FROM latest_x GROUP BY g", new SourceSchema[0], new SourceSchema[] {latestSchema});
        assertTrue(agg.ok(), String.valueOf(agg.diagnostics()));
        var aggSchema = new SourceSchema("agg", agg.outputSchema().fields());

        var joined = compileTable("SELECT o.tag, b.total FROM orders o LEFT JOIN agg b ON o.tag = b.g", new SourceSchema[] {ORDERS}, new SourceSchema[] {aggSchema});
        assertTrue(joined.ok(), String.valueOf(joined.diagnostics()));

        return new Chain(latest.plan().createExecutor(), agg.plan().createExecutor(), joined.plan().createExecutor());
    }

    @Test
    void leftJoinOntoAggregateTable_OneUpstreamChange_IsAppliedAtomically_NeverObservesAnIntermediateNull() {
        var chain = buildChain();
        var latest = chain.latest();
        var agg = chain.agg();
        var joined = chain.joined();
        long ts = 1000;

        // Seed: key "a" gets its first value (5.0), fed through the whole chain via the batch entry point —
        // exactly how TableGrain.OnTableDeltaBatchAsync/TableActor.ProcessTableDeltasAsync feed a real
        // upstream table's published batch post-fix.
        var seedLatest = latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 5.0));
        var seedAgg = agg.onTableDeltaBatch("latest_x", seedLatest);
        joined.onTableDeltaBatch("agg", seedAgg);

        // The LEFT side row that will match it.
        joined.onStreamEvent("orders", evt(ts++, "orders", "tag", "a"));

        var before = single(joined.snapshot());
        assertEquals(5.0, before.getValue().row().get("total"));

        // ONE upstream change: key "a" moves from 5.0 to 9.0. LATEST BY always emits retract(old)+assert(new)
        // for an existing key — one upstream change, two deltas.
        var latestDeltas = latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 9.0));
        assertEquals(2, latestDeltas.size());

        // Both hops go through the BATCH entry point, one call each — this is the fix under test.
        var aggDeltas = agg.onTableDeltaBatch("latest_x", latestDeltas);
        var joinedDeltas = joined.onTableDeltaBatch("agg", aggDeltas);

        // THE ASSERTION: nothing `joined` ever returns for this one upstream change carries a NULL total
        // for "a" — the reported flap never reaches a caller/consumer of this table.
        for (var delta : joinedDeltas) {
            if (Objects.equals(delta.row().get("tag"), "a")) {
                assertNotNull(delta.row().get("total"));
            }
        }

        // And the net effect is exactly the expected retract(old)+assert(new) — not zero, not a longer
        // flapping sequence collapsed to nothing it shouldn't have been.
        assertEquals(2, joinedDeltas.size());
        var retracted = single(joinedDeltas, d -> d.weight() < 0);
        assertEquals(5.0, retracted.row().get("total"));
        var asserted = single(joinedDeltas, d -> d.weight() > 0);
        assertEquals(9.0, asserted.row().get("total"));

        var after = single(joined.snapshot());
        assertEquals(9.0, after.getValue().row().get("total"));
    }

    /** The contrast that proves the fix is load-bearing and the test above isn't vacuous: feeding the EXACT
     *  same two-delta change through the OLD per-element shape (one onTableDelta call per element) DOES
     *  produce an observable intermediate NULL, because each element gets its own epoch and the epoch
     *  consolidation never sees more than one element at a time to net against. */
    @Test
    void withoutBatchAdmission_TheSamePerElementLoop_DoesObserveAnIntermediateNull() {
        var chain = buildChain();
        var latest = chain.latest();
        var agg = chain.agg();
        var joined = chain.joined();
        long ts = 1000;

        var seedLatest = latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 5.0));
        for (var d : seedLatest) {
            for (var d2 : agg.onTableDelta("latest_x", d)) {
                joined.onTableDelta("agg", d2);
            }
        }
        joined.onStreamEvent("orders", evt(ts++, "orders", "tag", "a"));

        var latestDeltas = latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 9.0));
        assertEquals(2, latestDeltas.size());

        var observedNullTotalForA = false;
        for (var d : latestDeltas) { // pre-fix TableGrain.OnTableDeltaBatchAsync: one onTableDelta call per element
            for (var d2 : agg.onTableDelta("latest_x", d)) { // pre-fix downstream loop, same shape
                for (var d3 : joined.onTableDelta("agg", d2)) {
                    if (Objects.equals(d3.row().get("tag"), "a") && d3.row().get("total") == null) {
                        observedNullTotalForA = true;
                    }
                }
            }
        }

        assertTrue(observedNullTotalForA, "expected the per-element admission shape to reproduce the reported NULL flap");

        // Same eventual consistency either way — only the atomicity in between differs.
        var after = single(joined.snapshot());
        assertEquals(9.0, after.getValue().row().get("total"));
    }
}
