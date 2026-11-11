package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — {@code UNION} (distinct) end to end in TABLE mode: the only mode that supports it (pipeline mode
 * rejects it). Wires a TableDistinctOp downstream of the union's own branch concatenation. Highest-value cases
 * per plan 008: a row asserted from both branches emits exactly one +1; retracting one branch emits nothing;
 * retracting both emits -1; an out-of-order retraction (arriving before its assertion) self-heals.
 */
class UnionDistinctTableTests {
    private static final SourceSchema LEFT = schema("left_src", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);
    private static final SourceSchema RIGHT = schema("right_src", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);
    private static final String SQL = "SELECT symbol, tag FROM left_src UNION SELECT symbol, tag FROM right_src";

    private static final Supplier<EventRecord> LEFT_ROW = () -> evt(0, "left_src", "symbol", "AAPL", "tag", "watch");
    private static final Supplier<EventRecord> RIGHT_ROW = () -> evt(0, "right_src", "symbol", "AAPL", "tag", "watch");

    private static TableExecutor createExec() {
        return compileTableAndCreate(SQL, new SourceSchema[0], new SourceSchema[] {LEFT, RIGHT});
    }

    @Test
    void compilesAndPlanSummaryNamesPlainUnion() {
        var r = compileTable(SQL, new SourceSchema[0], new SourceSchema[] {LEFT, RIGHT});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertTrue(r.planSummary().contains("UNION"));
        assertFalse(r.planSummary().contains("UNION ALL"));
    }

    @Test
    void firstBranchsAssertionEmitsExactlyOnePlusOne() {
        var exec = createExec();

        var delta = single(exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1)));
        assertEquals(1L, delta.weight());
        assertEquals(1L, single(exec.snapshot().values()).weight());
    }

    @Test
    void secondBranchsAssertionOfTheIdenticalRowEmitsNothing() {
        var exec = createExec();

        exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));
        var secondEmission = exec.onTableDelta("right_src", new TableDelta(RIGHT_ROW.get(), 1));

        assertTrue(secondEmission.isEmpty()); // no +1 fired — the row was already present from branch 0
        assertEquals(1L, single(exec.snapshot().values()).weight()); // distinct: weight stays 1, not 2
    }

    @Test
    void retractingOneOfTwoAssertingBranchesEmitsNothing() {
        var exec = createExec();
        exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));
        exec.onTableDelta("right_src", new TableDelta(RIGHT_ROW.get(), 1));

        var retraction = exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), -1));

        assertTrue(retraction.isEmpty()); // the OTHER branch still asserts this row — nothing to retract yet
        assertEquals(1, exec.snapshot().size());
        assertEquals(1L, single(exec.snapshot().values()).weight());
    }

    @Test
    void retractingBothAssertingBranchesEmitsMinusOne() {
        var exec = createExec();
        exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));
        exec.onTableDelta("right_src", new TableDelta(RIGHT_ROW.get(), 1));
        exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), -1));

        var finalRetraction = exec.onTableDelta("right_src", new TableDelta(RIGHT_ROW.get(), -1));

        var d = single(finalRetraction);
        assertEquals(-1L, d.weight());
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void aRetractionArrivingBeforeItsAssertionSelfHeals() {
        var exec = createExec();

        // Out-of-order: a retraction for a row NEITHER branch has ever asserted yet.
        var earlyRetraction = exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), -1));
        assertTrue(earlyRetraction.isEmpty()); // never claimed presence — nothing to retract

        // The matching assertion arrives later — nets against the debt, still never surfaces.
        var lateAssertion = exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));
        assertTrue(lateAssertion.isEmpty());
        assertTrue(exec.snapshot().isEmpty());

        // A THIRD, genuinely-new assertion (from the other branch) is the true first presence — and DOES surface.
        var trueAssertion = exec.onTableDelta("right_src", new TableDelta(RIGHT_ROW.get(), 1));
        var d = single(trueAssertion);
        assertEquals(1L, d.weight());
        assertEquals(1L, single(exec.snapshot().values()).weight());
    }

    @Test
    void distinctRowsFromEachBranchBothAppearIndependently() {
        var exec = createExec();

        exec.onTableDelta("left_src", new TableDelta(evt(0, "left_src", "symbol", "AAPL", "tag", "watch"), 1));
        exec.onTableDelta("right_src", new TableDelta(evt(0, "right_src", "symbol", "MSFT", "tag", "core"), 1));

        var snapshot = exec.snapshot();
        assertEquals(2, snapshot.size());
        snapshot.values().forEach(v -> assertEquals(1L, v.weight()));
    }

    @Test
    void duplicateAssertionsFromTheSameBranchAlsoDedup() {
        // Not just cross-branch — two identical asserting deltas from the SAME branch (e.g. a source that
        // legitimately re-sends) must dedup too, since DISTINCT counts contributing weight, not distinct
        // branches specifically.
        var exec = createExec();

        exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));
        var second = exec.onTableDelta("left_src", new TableDelta(LEFT_ROW.get(), 1));

        assertTrue(second.isEmpty());
        assertEquals(1L, single(exec.snapshot().values()).weight());
    }
}
