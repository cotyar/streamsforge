package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.ops.TableFilterProjectOp;
import io.streamsforge.engine.runtime.ops.TableIngestOp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Wishlist item 13 gap 2, at the op level (see RetractKeyWithWhereClauseTests for the same fix
 * proven end to end through TableExecutor.onStreamEvent on the documented CDC-mirror shape): a client
 * key retraction carries ONLY the key columns it means to retract, never the rest of the row — so a WHERE
 * clause over any OTHER column would, before this fix, see a missing field, evaluate false/null, and drop
 * the retraction in TableFilterProjectOp before it ever reached TableLatestByOp. This file pins the fix
 * directly on TableFilterProjectOp.
 */
class TableFilterProjectOpKeyRetractionTests {
    private static final Epoch E0 = new Epoch(0);

    @Test
    void onBatch_AKeyRetractionBypassesWhereEvenWhenTheWhereColumnIsMissing() {
        // The exact documented CDC-mirror shape (docs/cdc.md, pinned by LatestByTests.whereComesBeforeLatestBy):
        // WHERE filters on "stage", the LATEST BY key is "order_id" — a retraction only ever carries
        // order_id, never stage.
        var compiled = compileTable(
            "SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)",
            ORDER_EVENTS).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var retractEvt = evt(0, "order_events", "order_id", "O1", "_retract", true); // no "stage" at all
        var admitted = ingest.onBatch(E0, List.of(new TableDelta(retractEvt, 1)));
        assertEquals(-1L, admitted.get(0).weight()); // TableIngestOp already flipped it

        var filtered = op.onBatch(E0, admitted);

        var d = single(filtered); // NOT dropped, despite "stage" being absent from the row
        assertEquals(-1L, d.weight());
        assertFalse(d.row().fields().containsKey(alias + "_stage")); // still genuinely missing — bypass, not a default
    }

    @Test
    void onBatch_AnOrdinaryRowStillObeysWhere_RegressionCheck() {
        // The fix must not turn WHERE into a no-op for everything — only for a genuine key retraction.
        var compiled = compileTable(
            "SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)",
            ORDER_EVENTS).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var cancelled = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "order_events", "order_id", "O1", "stage", "CANCELLED"), 1)));
        assertTrue(op.onBatch(E0, cancelled).isEmpty()); // fails WHERE, correctly dropped — same as before the fix

        var open = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "order_events", "order_id", "O1", "stage", "NEW"), 1)));
        single(op.onBatch(E0, open)); // passes WHERE, admitted — same as before the fix
    }

    @Test
    void onBatchTerminal_AKeyRetractionBypassesWhereAndIsProjected() {
        // A plain (non-LATEST-BY) terminal shape — RetractConsumerValidation would reject this table as a
        // live direct consumer of a retract-carrying source at the ingest boundary, but this op has no
        // visibility into that, so it must still behave safely and consistently rather than depend on never
        // seeing one — see the class doc's "at worst incomplete" note.
        var compiled = compileTable("SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED'", ORDER_EVENTS).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var retractEvt = evt(0, "order_events", "order_id", "O1", "_retract", true);
        var admitted = ingest.onBatch(E0, List.of(new TableDelta(retractEvt, 1)));

        var outp = op.onBatchTerminal(E0, admitted);

        var d = single(outp);
        assertEquals(-1L, d.weight());
        assertEquals("O1", d.row().get("order_id"));
        assertNull(d.row().get("stage")); // projected straight through; genuinely absent, not defaulted
    }
}
