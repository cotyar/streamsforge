package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Table-mode deterministic-replay test (plan 003 M1 acceptance: "a deterministic-replay test (same
 * batches => same outputs)"). Feeds an identical sequence of stream events / table deltas — spanning a
 * join, a WHERE filter, and a retraction cascading through the join — into two FRESH op chains built from the
 * same compiled plan, and asserts the two runs produce byte-identical output sequences, call for call, delta
 * for delta.
 */
class TableDeterministicReplayTests {
    private static final String SQL = "SELECT t.symbol, r.tag, t.price FROM trades t JOIN ref r ON t.symbol = r.symbol WHERE t.price > 10";

    private static List<List<TableDelta>> runBatchSequence(TableExecutor exec) {
        var perCallOutputs = new ArrayList<List<TableDelta>>();

        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1500, "trades", "symbol", "AAPL", "price", 5.0, "qty", 1L, "active", true))); // fails WHERE
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 2)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 3L, "active", true)));
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // retraction cascades through the join

        return perCallOutputs;
    }

    private static String canon(List<TableDelta> deltas) {
        return deltas.stream().map(d -> d.weight() + ":" + JsonText.serializeCanonicalRow(d.row())).collect(Collectors.joining("|"));
    }

    @Test
    void sameBatchSequenceTwiceOnFreshOpChainsProducesIdenticalOutputs() {
        var compileResult = compileTable(SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileResult.ok());
        var plan = compileResult.plan();

        // Two independent, freshly-constructed executors (= fresh op chains) sharing only the immutable
        // compiled plan — exactly plan 003 M1's replay guarantee target.
        var run1 = runBatchSequence(plan.createExecutor());
        var run2 = runBatchSequence(plan.createExecutor());

        assertEquals(run1.size(), run2.size());
        for (int i = 0; i < run1.size(); i++) {
            assertEquals(run1.get(i).size(), run2.get(i).size());
            assertEquals(canon(run1.get(i)), canon(run2.get(i)));
        }
    }

    @Test
    void thirdIndependentRunAlsoMatches_NotJustPairwise() {
        var compileResult = compileTable(SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        var plan = compileResult.plan();

        var runs = new ArrayList<List<List<TableDelta>>>();
        for (int i = 0; i < 3; i++) runs.add(runBatchSequence(plan.createExecutor()));

        var canonical = runs.stream()
            .map(r -> r.stream().map(TableDeterministicReplayTests::canon).collect(Collectors.joining(";")))
            .distinct().collect(Collectors.toList());
        assertEquals(1, canonical.size()); // all three runs collapse to the same canonical sequence
    }
}
