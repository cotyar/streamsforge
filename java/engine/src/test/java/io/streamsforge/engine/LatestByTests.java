package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Plan 002 L3 (deferred sugar, landed alongside L2) — table-mode {@code LATEST BY (col[, col...])}: keeps the
 * most recent row per key, ordered by {@code _ts}, emitting retract(old)/assert(new) pairs on replacement. See
 * runtime/ops/TableLatestByOp for the pinned semantics this file's executor tests prove.
 */
class LatestByTests {
    private static EventRecord orderEvt(long ts, String orderId, String stage) { return orderEvt(ts, orderId, stage, 0L); }

    private static EventRecord orderEvt(long ts, String orderId, String stage, long filledQty) {
        return evt(ts, "order_events", "order_id", orderId, "stage", stage, "filled_qty", filledQty);
    }

    // ------------------------------------------------------------------
    // Parser / grammar + mutual-exclusion diagnostics
    // ------------------------------------------------------------------

    @Test
    void latestByParsesAndCompilesInTableMode() {
        var r = compileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", ORDER_EVENTS);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
    }

    /** The CDC pattern docs/cdc.md publishes, pinned here because the doc used to print it with
     *  the clauses the other way round ({@code LATEST BY id WHERE ...}, and without the parentheses) — which does
     *  not parse, and nothing caught it. WHERE comes before LATEST BY, and the key list is parenthesised.
     *  If this test ever has to change, docs/cdc.md changes with it. */
    @Test
    void whereComesBeforeLatestBy_theCdcMirrorPatternAsDocumented() {
        var r = compileTable("SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)", ORDER_EVENTS);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));

        var wrongOrder = compileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id) WHERE stage <> 'CANCELLED'", ORDER_EVENTS);
        assertFalse(wrongOrder.ok());
    }

    @Test
    void latestByWithMultipleKeysParses() {
        var r = compileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id, stage)", ORDER_EVENTS);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
    }

    @Test
    void latestByInPipelineModeIsRejected() {
        var r = compile("SELECT order_id FROM order_events LATEST BY (order_id)", ORDER_EVENTS);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("LATEST BY is table-mode only")));
    }

    @Test
    void latestByCombinedWithGroupByIsRejected() {
        var r = compileTable("SELECT order_id, COUNT(*) AS cnt FROM order_events LATEST BY (order_id) GROUP BY order_id", ORDER_EVENTS);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("LATEST BY may not be combined with GROUP BY")));
    }

    @Test
    void latestByCombinedWithAggregateIsRejected() {
        var r = compileTable("SELECT order_id, COUNT(*) AS cnt FROM order_events LATEST BY (order_id)", ORDER_EVENTS);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("LATEST BY may not be combined with aggregate functions")));
    }

    @Test
    void latestByCombinedWithWindowIsRejected() {
        var r = compileTable("SELECT order_id FROM order_events LATEST BY (order_id) WINDOW TUMBLING(SIZE 5 SECONDS)", ORDER_EVENTS);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("WINDOW clause not allowed in table mode")));
    }

    // ------------------------------------------------------------------
    // Executor
    // ------------------------------------------------------------------

    private static TableExecutor createLatestByOrderId() {
        return compileTableAndCreate("SELECT order_id, stage FROM order_events LATEST BY (order_id)", ORDER_EVENTS);
    }

    @Test
    void firstAssertionForAKeyEmitsOnlyAnAssertion() {
        var exec = createLatestByOrderId();
        var deltas = exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));

        var assertion = single(deltas);
        assertEquals(1L, assertion.weight());
        assertEquals("NEW", assertion.row().get("stage"));
    }

    @Test
    void newerArrivalReplacesEmittingRetractThenAssert() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));

        var deltas = exec.onStreamEvent("order_events", orderEvt(2000, "O1", "ACK"));

        assertEquals(2, deltas.size());
        assertEquals(-1L, deltas.get(0).weight());
        assertEquals("NEW", deltas.get(0).row().get("stage"));
        assertEquals(1L, deltas.get(1).weight());
        assertEquals("ACK", deltas.get(1).row().get("stage"));
    }

    @Test
    void strictlyOlderArrivalIsIgnored() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(2000, "O1", "ACK"));

        var deltas = exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW")); // older _ts

        assertTrue(deltas.isEmpty());
        var snapshot = exec.snapshot();
        var current = single(snapshot);
        assertEquals("ACK", current.getValue().row().get("stage"));
    }

    @Test
    void tiedTimestampArrivalReplaces() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));

        // Same _ts as the currently-retained row (">=" replaces on ties — see class doc).
        var deltas = exec.onStreamEvent("order_events", orderEvt(1000, "O1", "ACK"));

        assertEquals(2, deltas.size());
        assertEquals(-1L, deltas.get(0).weight());
        assertEquals(1L, deltas.get(1).weight());
        assertEquals("ACK", deltas.get(1).row().get("stage"));
    }

    @Test
    void upstreamRetractionOfTheCurrentlyRetainedRowDropsTheKey() {
        var exec = createLatestByOrderId();
        var row = orderEvt(1000, "O1", "NEW");
        exec.onStreamEvent("order_events", row);

        // Simulates an upstream retraction (e.g. a WHERE flip further up a chain) of the SAME row this op
        // currently holds — see TableZSetTests' class doc on driving retractions via onTableDelta.
        var deltas = exec.onTableDelta("order_events", new TableDelta(row, -1));

        var retraction = single(deltas);
        assertEquals(-1L, retraction.weight());
        assertEquals("NEW", retraction.row().get("stage"));
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void retractionOfANonCurrentRowIsANoOp() {
        var exec = createLatestByOrderId();
        var rowA = orderEvt(1000, "O1", "NEW");
        var rowB = orderEvt(2000, "O1", "ACK");
        exec.onStreamEvent("order_events", rowA);
        exec.onStreamEvent("order_events", rowB); // ACK is now current; NEW already retracted

        var deltas = exec.onTableDelta("order_events", new TableDelta(rowA, -1)); // retract the STALE row

        assertTrue(deltas.isEmpty());
        var current = single(exec.snapshot());
        assertEquals("ACK", current.getValue().row().get("stage"));
    }

    @Test
    void distinctKeysAreTrackedIndependently() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1000, "O2", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(2000, "O1", "ACK"));

        var snapshot = exec.snapshot();
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.values().stream().anyMatch(v -> "O1".equals(v.row().get("order_id")) && "ACK".equals(v.row().get("stage"))));
        assertTrue(snapshot.values().stream().anyMatch(v -> "O2".equals(v.row().get("order_id")) && "NEW".equals(v.row().get("stage"))));
    }

    @Test
    void chainedLatestByTableFeedsAnotherTable() {
        var t1 = compileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", new SourceSchema[] {ORDER_EVENTS}, null);
        assertTrue(t1.ok(), String.valueOf(t1.diagnostics()));
        var exec1 = t1.plan().createExecutor();

        var orderStatesSchema = new SourceSchema("order_states", t1.outputSchema().fields());
        var t2 = compileTable("SELECT order_id, stage FROM order_states WHERE stage = 'ACK'", new SourceSchema[0], new SourceSchema[] {orderStatesSchema});
        assertTrue(t2.ok(), String.valueOf(t2.diagnostics()));
        var exec2 = t2.plan().createExecutor();

        for (var d : exec1.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"))) {
            exec2.onTableDelta("order_states", d);
        }
        assertTrue(exec2.snapshot().isEmpty()); // NEW doesn't pass downstream WHERE stage = 'ACK'

        for (var d : exec1.onStreamEvent("order_events", orderEvt(2000, "O1", "ACK"))) {
            exec2.onTableDelta("order_states", d);
        }
        var downstream = single(exec2.snapshot());
        assertEquals("ACK", downstream.getValue().row().get("stage"));
    }
}
