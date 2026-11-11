package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.evt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.ops.TableDistinctOp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — TableDistinctOp exercised directly (NOT through TableExecutor's façade), same
 * per-op unit-test style as TableOpsUnitTests. Pins the zero-crossing presence rule (weight &gt; 0, NOT
 * != 0) and order independence (a negative delta arriving before its matching positive one is recorded as
 * debt, not dropped).
 */
class TableDistinctOpUnitTests {
    private static final Epoch E0 = new Epoch(0);

    private static EventRecord row(String symbol) { return evt(0, "u", "symbol", symbol); }

    @Test
    void firstAssertionOfARowEmitsPlusOne() {
        var op = new TableDistinctOp();

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        var d = single(outp);
        assertEquals(1L, d.weight());
        assertEquals("AAPL", d.row().get("symbol"));
    }

    @Test
    void secondAssertionOfTheIdenticalRowEmitsNothing() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        assertTrue(outp.isEmpty());
    }

    @Test
    void thirdAssertionAlsoEmitsNothing() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        assertTrue(outp.isEmpty());
    }

    @Test
    void retractingOneOfTwoContributionsEmitsNothing() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1)));

        assertTrue(outp.isEmpty());
    }

    @Test
    void retractingTheLastContributionEmitsMinusOne() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1)));

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1)));

        var d = single(outp);
        assertEquals(-1L, d.weight());
    }

    @Test
    void outOfOrderRetractionBeforeAnyAssertionEmitsNothingAndRecordsDebt() {
        var op = new TableDistinctOp();

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1)));

        assertTrue(outp.isEmpty()); // weight went 0 -> -1: neither endpoint is positive, so no emission
    }

    @Test
    void assertionThatOnlyCancelsAnEarlierOutOfOrderRetractionEmitsNothing() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1))); // debt: weight = -1

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1))); // weight -1 -> 0

        assertTrue(outp.isEmpty()); // never crossed into positive — the row was never truly "distinct-present"
    }

    @Test
    void aTrueAssertionAfterTheDebtCancelsOutDoesEmit() {
        var op = new TableDistinctOp();
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), -1))); // weight -1
        op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));  // weight 0 (cancelled, silent)

        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1))); // weight 0 -> 1: genuine crossing

        var d = single(outp);
        assertEquals(1L, d.weight());
    }

    @Test
    void distinctRowsAreTrackedIndependently() {
        var op = new TableDistinctOp();

        var outp1 = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1)));
        var outp2 = op.onBatch(E0, List.of(new TableDelta(row("MSFT"), 1)));

        single(outp1);
        single(outp2);
    }

    @Test
    void onFrontierIsADocumentedPassThrough() {
        var op = new TableDistinctOp();
        assertTrue(op.onFrontier(E0).isEmpty());
        assertTrue(op.onFrontier(new Epoch(999)).isEmpty());
    }

    @Test
    void multipleDeltasInOneBatchAreEachEvaluatedAgainstTheRunningTotal_NotNettedAcrossTheBatch() {
        var op = new TableDistinctOp();

        // Within a single batch: assert then retract the SAME row. Each delta is folded into the running
        // weight and evaluated for a crossing IN ORDER (same per-delta model as TableReduceOp's own
        // onDelta) — so this is +1 (0 -> 1, crossing) immediately followed by -1 (1 -> 0, crossing back),
        // NOT netted into "no change".
        var outp = op.onBatch(E0, List.of(new TableDelta(row("AAPL"), 1), new TableDelta(row("AAPL"), -1)));

        assertEquals(2, outp.size());
        assertEquals(1L, outp.get(0).weight());
        assertEquals(-1L, outp.get(1).weight());
    }
}
