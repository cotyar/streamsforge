package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 wave 2c-A table-mode outer-join deterministic-replay test, modeled on
 * TableDeterministicReplayTests: three fresh op chains fed an IDENTICAL batch sequence must produce
 * byte-identical output, call for call, delta for delta -- proof the pad/flip machinery doesn't hide any
 * shared/static state or ordering dependency between runs. Each sequence below runs a pad -> product ->
 * pad ROUND TRIP (assert a lone unmatched row, match it, then retract the match) so the flip re-emissions
 * -- the part of TableOuterJoinOp's state machine most likely to leak hidden per-run state -- sit inside
 * the compared window, not just in the final snapshot.
 */
class TableOuterJoinReplayTests {
    private static final String LEFT_SQL = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private static final String FULL_SQL = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";

    private static List<List<TableDelta>> runLeftBatchSequence(TableExecutor exec) {
        var perCallOutputs = new ArrayList<List<TableDelta>>();

        perCallOutputs.add(exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true))); // pad
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1))); // product + pad retract
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // round trip: retract product + repad
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1500, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true))); // NULL-key pad, never indexed
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1))); // no counterpart -> no output

        return perCallOutputs;
    }

    private static List<List<TableDelta>> runFullBatchSequence(TableExecutor exec) {
        var perCallOutputs = new ArrayList<List<TableDelta>>();

        perCallOutputs.add(exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true))); // left own-pad
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1))); // right own-pad, no counterpart
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1))); // flip: product + left-pad retract
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // round trip: retract product + repad left
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1500, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true))); // left NULL-key pad

        return perCallOutputs;
    }

    private static String canon(List<TableDelta> deltas) {
        return deltas.stream().map(d -> d.weight() + ":" + JsonText.serializeCanonicalRow(d.row())).collect(Collectors.joining("|"));
    }

    private static void assertReplayIdentical(String sql, Function<TableExecutor, List<List<TableDelta>>> runBatchSequence) {
        var compileResult = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileResult.ok(), String.valueOf(compileResult.diagnostics()));
        var plan = compileResult.plan();

        var runs = new ArrayList<List<List<TableDelta>>>();
        for (int i = 0; i < 3; i++) runs.add(runBatchSequence.apply(plan.createExecutor()));

        for (int r = 1; r < runs.size(); r++) {
            assertEquals(runs.get(0).size(), runs.get(r).size());
            for (int i = 0; i < runs.get(0).size(); i++) {
                assertEquals(runs.get(0).get(i).size(), runs.get(r).get(i).size());
                assertEquals(canon(runs.get(0).get(i)), canon(runs.get(r).get(i)));
            }
        }
    }

    @Test
    void leftJoin_SameBatchSequenceOnThreeFreshExecutorsProducesByteIdenticalOutputs_PadProductPadRoundTrip() {
        assertReplayIdentical(LEFT_SQL, TableOuterJoinReplayTests::runLeftBatchSequence);
    }

    @Test
    void fullJoin_SameBatchSequenceOnThreeFreshExecutorsProducesByteIdenticalOutputs_PadProductPadRoundTrip() {
        assertReplayIdentical(FULL_SQL, TableOuterJoinReplayTests::runFullBatchSequence);
    }
}
