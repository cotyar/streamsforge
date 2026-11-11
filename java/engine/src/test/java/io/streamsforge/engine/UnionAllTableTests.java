package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — {@code UNION ALL} end to end in TABLE mode. Unlike pipeline mode, table mode's union root needs
 * no dedup op at all: the consolidation ledger already sums weights per canonical row key regardless of which
 * branch a delta came from. These tests prove that weight arithmetic directly: two branches asserting the SAME
 * row nets to weight 2 (not 1 — contrast with UnionDistinctTableTests).
 */
class UnionAllTableTests {
    // Two distinctly-named table inputs with the identical shape, so a canonically-identical row can be fed
    // through either branch and land on the exact same consolidated key.
    private static final SourceSchema LEFT = schema("left_src", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);
    private static final SourceSchema RIGHT = schema("right_src", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);
    private static final String SQL = "SELECT symbol, tag FROM left_src UNION ALL SELECT symbol, tag FROM right_src";

    private static TableExecutor createExec() {
        return compileTableAndCreate(SQL, new SourceSchema[0], new SourceSchema[] {LEFT, RIGHT});
    }

    @Test
    void compilesWithAUnifiedOutputSchema() {
        var r = compileTable(SQL, new SourceSchema[0], new SourceSchema[] {LEFT, RIGHT});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(2, r.outputSchema().fields().size());
        assertEquals(List.of("left_src", "right_src"),
            Stream.concat(r.streamInputs().stream(), r.tableInputs().stream()).sorted().toList());
    }

    @Test
    void singleBranchAssertionProducesWeightOne() {
        var exec = createExec();

        var delta = single(exec.onTableDelta("left_src", new TableDelta(evt(0, "left_src", "symbol", "AAPL", "tag", "watch"), 1)));
        assertEquals(1L, delta.weight());

        var snapshot = exec.snapshot();
        var only = single(snapshot.values());
        assertEquals(1L, only.weight());
    }

    @Test
    void identicalRowAssertedFromBothBranchesSumsWeightsToTwo_NoDedup() {
        var exec = createExec();

        exec.onTableDelta("left_src", new TableDelta(evt(0, "left_src", "symbol", "AAPL", "tag", "watch"), 1));
        exec.onTableDelta("right_src", new TableDelta(evt(0, "right_src", "symbol", "AAPL", "tag", "watch"), 1));

        var snapshot = exec.snapshot();
        var only = single(snapshot.values());
        assertEquals(2L, only.weight()); // UNION ALL: no dedup — contrast with UnionDistinctTableTests (weight 1)
    }

    @Test
    void retractingOneBranchLeavesTheOtherBranchsAssertionStanding() {
        var exec = createExec();
        Supplier<EventRecord> row = () -> evt(0, "left_src", "symbol", "AAPL", "tag", "watch");
        Supplier<EventRecord> rowR = () -> evt(0, "right_src", "symbol", "AAPL", "tag", "watch");

        exec.onTableDelta("left_src", new TableDelta(row.get(), 1));
        exec.onTableDelta("right_src", new TableDelta(rowR.get(), 1));
        assertEquals(2L, single(exec.snapshot().values()).weight());

        var retracted = exec.onTableDelta("left_src", new TableDelta(row.get(), -1));
        var d = single(retracted);
        assertEquals(-1L, d.weight());

        assertEquals(1L, single(exec.snapshot().values()).weight());
    }

    @Test
    void retractingBothBranchesEmptiesTheSnapshot() {
        var exec = createExec();
        Supplier<EventRecord> row = () -> evt(0, "left_src", "symbol", "AAPL", "tag", "watch");
        Supplier<EventRecord> rowR = () -> evt(0, "right_src", "symbol", "AAPL", "tag", "watch");

        exec.onTableDelta("left_src", new TableDelta(row.get(), 1));
        exec.onTableDelta("right_src", new TableDelta(rowR.get(), 1));
        exec.onTableDelta("left_src", new TableDelta(row.get(), -1));
        exec.onTableDelta("right_src", new TableDelta(rowR.get(), -1));

        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void distinctRowsFromEachBranchBothAppearIndependently() {
        var exec = createExec();

        exec.onTableDelta("left_src", new TableDelta(evt(0, "left_src", "symbol", "AAPL", "tag", "watch"), 1));
        exec.onTableDelta("right_src", new TableDelta(evt(0, "right_src", "symbol", "MSFT", "tag", "core"), 1));

        var snapshot = exec.snapshot();
        assertEquals(2, snapshot.size());
        var symbols = snapshot.values().stream().map(v -> (String) v.row().get("symbol")).sorted().toList();
        assertEquals(List.of("AAPL", "MSFT"), symbols);
    }

    @Test
    void planSummaryNamesUnionAll() {
        var r = compileTable(SQL, new SourceSchema[0], new SourceSchema[] {LEFT, RIGHT});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertTrue(r.planSummary().contains("UNION ALL"));
        assertTrue(r.planSummary().contains("SELECT 2"));
    }
}
