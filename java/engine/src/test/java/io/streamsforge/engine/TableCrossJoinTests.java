package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W2-A: CROSS JOIN in table mode. Unlike TableJoinTests' equi-join, there is no join key at all —
 * every left row must match every right row (the cartesian product), which TablePlanner realizes by
 * synthesizing a constant key (NumberLiteral(0)) on both sides of the same TableJoinOp equi-join used for
 * INNER. This file exercises that the trick actually produces cartesian-product-with-weight-multiplication
 * semantics end to end.
 *
 * <p>NOT PORTED (the partitioned dataflow, {@code TableDataflowPlan}/{@code PartitionedTableHarness}, is not in
 * the Java engine): {@code CrossJoinEdgesAreNeverArrangeable}, {@code CreateDataflowThrowsAboveParallelism1},
 * {@code AtParallelism1ResultsMatchTheClassicExecutor}.
 */
class TableCrossJoinTests {
    private static final String SQL = "SELECT t.symbol, r.tag FROM trades t CROSS JOIN ref r";

    private static TableCompileResult compileIt() {
        return compileTable(SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
    }

    private static TableExecutor create() {
        return compileTableAndCreate(SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
    }

    @Test
    void crossJoinCompilesInTableMode() {
        var r = compileIt();
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
    }

    @Test
    void cartesianProductMultipliesWeights() {
        var exec = create();

        // ref (a table input) arrives first with nothing on the trades side yet -> no match, no output —
        // same "one side arrives first" shape as TableJoinTests.leftArrivalMatchesAlreadyIndexedRightSide,
        // just with no ON clause to satisfy (there is none; every row matches unconditionally).
        var noMatch = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2));
        assertTrue(noMatch.isEmpty());

        var matched = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var delta = single(matched);
        assertEquals(2L, delta.weight()); // weight(trades)=1 * weight(ref)=2 — the whole point of a cross join
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals("watchlist", delta.row().get("tag"));
    }

    @Test
    void retractionCascadesANegativeWeightDeltaAndPrunesTheSnapshot() {
        var exec = create();

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var matched = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2));
        single(matched);
        assertEquals(1, exec.snapshot().size());

        var retracted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -2));
        var delta = single(retracted);
        assertEquals(-2L, delta.weight());
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals("watchlist", delta.row().get("tag"));

        // Consolidated: the earlier +2 and this -2 net to zero -> the row is pruned from the snapshot.
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void snapshotMatchesTheFullCartesianSet() {
        var exec = create();

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        exec.onStreamEvent("trades", evt(1001, "trades", "symbol", "MSFT", "price", 50.0, "qty", 5L, "active", true));
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "core-tag", "tag", "core"), 1));
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "watch-tag", "tag", "watch"), 1));

        // No ON clause -> every trades row combines with every ref row: 2 x 2 = 4 rows, each weight 1,
        // regardless of the (deliberately) mismatched "symbol" values -- a cross join ignores key equality
        // entirely, which is exactly what distinguishes it from the equi-join TableJoinTests covers.
        var snapshot = exec.snapshot();
        assertEquals(4, snapshot.size());
        for (var v : snapshot.values()) assertEquals(1L, v.weight());

        var pairs = new ArrayList<String>();
        for (var v : snapshot.values()) pairs.add((String) v.row().get("symbol") + "\u0000" + (String) v.row().get("tag"));
        pairs.sort(null);
        var expected = new ArrayList<>(List.of("AAPL\u0000core", "AAPL\u0000watch", "MSFT\u0000core", "MSFT\u0000watch"));
        expected.sort(null);
        assertEquals(expected, pairs);
    }

    @Test
    void planSummaryRendersTheCrossSymbol() {
        var r = compileIt();
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertTrue(r.planSummary().contains("×[CROSS] ref AS r"), r.planSummary());
    }

    private record Ev(String origin, EventRecord row, long weight, boolean isTable) {}

    private static String canon(java.util.Map<String, RowWeight> snap) {
        var sb = new StringBuilder();
        new java.util.TreeMap<>(snap).forEach((k, v) -> { if (sb.length() > 0) sb.append("\n"); sb.append(k).append('=').append(v.weight()); });
        return sb.toString();
    }

    @Test
    void crossJoinEdgesAreNeverArrangeable() {
        var plan = compileIt().plan();
        var dataflow = plan.createDataflow(1);
        assertTrue(dataflow.arrangeableExternalEdges().isEmpty());
    }

    @Test
    void createDataflowThrowsAboveParallelism1() {
        var result = compileIt();
        assertTrue(result.ok());
        var ex = org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class, () -> result.plan().createDataflow(4));
        assertTrue(ex.getMessage().contains("Use Parallelism = 1"));
        assertTrue(ex.getMessage().contains("CROSS JOIN"));
    }

    @Test
    void atParallelism1ResultsMatchTheClassicExecutor() {
        var result = compileIt();
        assertTrue(result.ok());
        var plan = result.plan();

        var classic = plan.createExecutor();
        var harness = new PartitionedTableHarness(plan.createDataflow(1));

        var events = List.of(
            new Ev("ref", evt(0, "ref", "symbol", "core-tag", "tag", "core"), 1, true),
            new Ev("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true), 1, false),
            new Ev("trades", evt(1001, "trades", "symbol", "MSFT", "price", 50.0, "qty", 5L, "active", true), 1, false),
            new Ev("ref", evt(0, "ref", "symbol", "watch-tag", "tag", "watch"), 1, true),
            new Ev("ref", evt(0, "ref", "symbol", "core-tag", "tag", "core"), -1, true)); // retraction cascades too

        for (var e : events) {
            if (e.isTable()) classic.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classic.onStreamEvent(e.origin(), e.row());
            harness.admit(e.origin(), e.row(), e.weight());
        }

        assertEquals(canon(classic.snapshot()), canon(harness.snapshot()));
        assertFalse(classic.snapshot().isEmpty()); // sanity: the comparison isn't vacuously true over an empty set
    }
}
