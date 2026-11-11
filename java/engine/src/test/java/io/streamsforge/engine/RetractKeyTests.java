package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Wishlist "explicit key retraction through ingest": a client-pushed row carrying
 * {@code "_retract": true} emits weight -1 for the LAST ASSERTED ROW of that key in a LATEST BY table
 * — freeing the key from TableExecutor.snapshot() entirely, unlike the pre-existing
 * {@code WHERE _op <> 'd'} tombstone pattern (docs/cdc.md), which only hides it. Exercised through
 * TableExecutor.onStreamEvent — the same public entry point LatestByTests uses — so these tests prove the
 * real path: TableIngestOp reads "_retract" off the raw event and flips the weight; TableLatestByOp then
 * retracts by KEY rather than by matching row content, because a retraction event only ever carries the key
 * columns, never the row it means to remove.
 */
class RetractKeyTests {
    private static EventRecord orderEvt(long ts, String orderId, String stage) {
        return evt(ts, "order_events", "order_id", orderId, "stage", stage);
    }

    /** A retract-flagged event still needs SOME value for the LATEST BY key column(s) — that is
     *  how TableLatestByOp.encodeKey resolves which key to drop — but nothing else; "stage" here is
     *  intentionally absent/default, mirroring a real client that sends only the key plus "_retract". */
    private static EventRecord retractEvt(String orderId) {
        return evt(0, "order_events", "order_id", orderId, "_retract", true);
    }

    private static TableExecutor createLatestByOrderId() {
        return compileTableAndCreate("SELECT order_id, stage FROM order_events LATEST BY (order_id)", ORDER_EVENTS);
    }

    @Test
    void retractFreesTheKeyFromTheSnapshot_notJustHidesIt() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        single(exec.snapshot());

        var deltas = exec.onStreamEvent("order_events", retractEvt("O1"));

        var retraction = single(deltas);
        assertEquals(-1L, retraction.weight());
        assertEquals("NEW", retraction.row().get("stage")); // the row it actually held, not the sparse retract row
        assertTrue(exec.snapshot().isEmpty()); // freed, not merely filtered out of query results
    }

    @Test
    void retractOfAKeyThatWasNeverAssertedIsANoOp() {
        var exec = createLatestByOrderId();

        var deltas = exec.onStreamEvent("order_events", retractEvt("never-seen"));

        assertTrue(deltas.isEmpty());
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void doubleRetractDoesNotDoubleRetract() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));

        var first = exec.onStreamEvent("order_events", retractEvt("O1"));
        var second = exec.onStreamEvent("order_events", retractEvt("O1"));

        single(first);
        assertEquals(-1L, first.get(0).weight());
        assertTrue(second.isEmpty()); // the key is already gone — nothing left to retract, no phantom second -1
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void retractOnlyAffectsItsOwnKey() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1000, "O2", "NEW"));

        exec.onStreamEvent("order_events", retractEvt("O1"));

        var remaining = single(exec.snapshot());
        assertEquals("O2", remaining.getValue().row().get("order_id"));
    }

    @Test
    void assertAfterRetractStartsCleanNotAsAReplace() {
        var exec = createLatestByOrderId();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        exec.onStreamEvent("order_events", retractEvt("O1"));

        // A fresh assertion for the freed key behaves exactly like the FIRST assertion for a key that
        // was never seen (LatestByTests.firstAssertionForAKeyEmitsOnlyAnAssertion) — one assert, no
        // paired retraction of stale state, because the key genuinely has nothing retained anymore.
        var deltas = exec.onStreamEvent("order_events", orderEvt(2000, "O1", "REOPENED"));

        var assertion = single(deltas);
        assertEquals(1L, assertion.weight());
        assertEquals("REOPENED", assertion.row().get("stage"));
    }

    /** The spec's fourth required case: a GROUP BY/aggregate table reading FROM a LATEST BY
     *  table (table-over-table — the same wiring chainedLatestByTableFeedsAnotherTable in
     *  LatestByTests already proves for ordinary replacement) must reflect a key retraction
     *  correctly — the count must go DOWN, never corrupt into a negative or a phantom re-add. */
    @Test
    void downstreamAggregateOverALatestByTableStaysCorrectAcrossARetract() {
        var t1 = compileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", new SourceSchema[] {ORDER_EVENTS}, null);
        assertTrue(t1.ok(), String.valueOf(t1.diagnostics()));
        var exec1 = t1.plan().createExecutor();

        var orderStatesSchema = new SourceSchema("order_states", t1.outputSchema().fields());
        var t2 = compileTable("SELECT COUNT(*) AS cnt FROM order_states", new SourceSchema[0], new SourceSchema[] {orderStatesSchema});
        assertTrue(t2.ok(), String.valueOf(t2.diagnostics()));
        var exec2 = t2.plan().createExecutor();

        for (var d : exec1.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"))) {
            exec2.onTableDelta("order_states", d);
        }
        for (var d : exec1.onStreamEvent("order_events", orderEvt(1000, "O2", "NEW"))) {
            exec2.onTableDelta("order_states", d);
        }

        var beforeRetract = single(exec2.snapshot());
        assertEquals(2L, beforeRetract.getValue().row().get("cnt"));

        for (var d : exec1.onStreamEvent("order_events", retractEvt("O1"))) {
            exec2.onTableDelta("order_states", d);
        }

        var afterRetract = single(exec2.snapshot());
        assertEquals(1L, afterRetract.getValue().row().get("cnt")); // down by exactly one, not corrupted/negative
        assertFalse(exec1.snapshot().values().stream().anyMatch(v -> "O1".equals(v.row().get("order_id"))));
    }
}
