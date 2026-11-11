package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Wishlist item 13 gap 2 ("A WHERE clause can silently drop a retraction"), proven end to end through
 * the public TableExecutor.onStreamEvent entry point — the same public API RetractKeyTests already uses for
 * the no-WHERE case, extended here to the documented CDC-mirror shape (docs/cdc.md, pinned by
 * LatestByTests.whereComesBeforeLatestBy): {@code WHERE stage <> 'CANCELLED' LATEST BY (order_id)}. A client
 * retraction carries only the LATEST BY key ("order_id"), never "stage" — so before the fix, evaluating the
 * WHERE clause against a row with no "stage" field at all evaluated false/null, and TableFilterProjectOp
 * dropped the retraction before TableLatestByOp ever saw it.
 *
 * <p>CHOICE MADE: option (a) — a key retraction bypasses WHERE outright, since it targets a KEY, not a row
 * obligated to qualify on content it does not carry.
 */
class RetractKeyWithWhereClauseTests {
    private static EventRecord orderEvt(long ts, String orderId, String stage) {
        return evt(ts, "order_events", "order_id", orderId, "stage", stage);
    }

    /** Deliberately carries ONLY the LATEST BY key, exactly like RetractKeyTests.retractEvt's own
     *  doc explains a real client would — "stage" (the WHERE column) is genuinely absent, not defaulted
     *  or blanked. */
    private static EventRecord retractEvt(String orderId) {
        return evt(0, "order_events", "order_id", orderId, "_retract", true);
    }

    private static TableExecutor createLatestByOrderIdFilteredByStage() {
        return compileTableAndCreate("SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)", ORDER_EVENTS);
    }

    @Test
    void retractStillFreesTheKey_evenThoughTheWhereColumnIsMissingFromTheRetractRow() {
        var exec = createLatestByOrderIdFilteredByStage();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        single(exec.snapshot());

        var deltas = exec.onStreamEvent("order_events", retractEvt("O1"));

        // Before the fix this was an empty list — the retraction died in TableFilterProjectOp's
        // WHERE check and never reached TableLatestByOp at all; the key would have stayed retained
        // forever with nothing anywhere reporting that anything went wrong.
        var retraction = single(deltas);
        assertEquals(-1L, retraction.weight());
        assertEquals("NEW", retraction.row().get("stage")); // the row it actually held, not the sparse retract row
        assertTrue(exec.snapshot().isEmpty()); // genuinely freed
    }

    @Test
    void retractOfAnAlreadyFilteredOutKeyIsStillANoOp() {
        // A row that never passed WHERE in the first place (so TableLatestByOp never retained it) —
        // the retraction must still be a harmless no-op, not an error, matching TableLatestByOp's own
        // documented "unknown key" behavior.
        var exec = createLatestByOrderIdFilteredByStage();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "CANCELLED")); // fails WHERE, never retained
        assertTrue(exec.snapshot().isEmpty());

        var deltas = exec.onStreamEvent("order_events", retractEvt("O1"));

        assertTrue(deltas.isEmpty());
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void ordinaryWhereFilteringOfNonRetractRowsIsUnaffected() {
        // Regression check at the public-API level, mirroring LatestByTests' own CDC-mirror coverage:
        // the fix must not turn WHERE into a no-op for anything other than a genuine key retraction.
        var exec = createLatestByOrderIdFilteredByStage();

        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "CANCELLED"));
        assertTrue(exec.snapshot().isEmpty()); // still correctly filtered out

        exec.onStreamEvent("order_events", orderEvt(1000, "O2", "NEW"));
        single(exec.snapshot()); // still correctly admitted
    }

    @Test
    void retractThenReassertBehavesLikeAFreshKey() {
        var exec = createLatestByOrderIdFilteredByStage();
        exec.onStreamEvent("order_events", orderEvt(1000, "O1", "NEW"));
        exec.onStreamEvent("order_events", retractEvt("O1"));
        assertTrue(exec.snapshot().isEmpty());

        var deltas = exec.onStreamEvent("order_events", orderEvt(2000, "O1", "REOPENED"));

        var assertion = single(deltas);
        assertEquals(1L, assertion.weight());
        assertEquals("REOPENED", assertion.row().get("stage"));
    }
}
