package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Port of orleans/tests/StreamsForge.Engine.Tests/PostgresNullSemanticsTests.cs — SUM and NOT IN follow PostgreSQL's
 * NULL rules (aligned 2026-11-11): SUM over no non-NULL value is NULL; {@code x NOT IN (S)} is TRUE for an empty S,
 * FALSE on a match, and otherwise NULL (filtered) when x is NULL or S contains a NULL.
 */
class PostgresNullSemanticsTests {
    private static EventRecord trade(String symbol, Long qty, long ts) {
        return evt(ts, "trades", "symbol", symbol, "price", 1.0, "qty", qty, "active", true);
    }
    private static EventRecord trade(String symbol, Long qty) { return trade(symbol, qty, 1000); }
    private static EventRecord refRow(String symbol, String tag) { return evt(0, "ref", "symbol", symbol, "tag", tag); }
    private static EventRecord refRow(String symbol) { return refRow(symbol, "x"); }

    private static Set<String> symbols(TableExecutor exec) {
        var out = new HashSet<String>();
        for (var r : exec.snapshot().values()) if (r.weight() > 0) out.add((String) r.row().get("symbol"));
        return out;
    }
    private static Set<String> set(String... s) { var out = new HashSet<String>(); Collections.addAll(out, s); return out; }

    private static Map<String, Object> onlyRow(TableExecutor exec) {
        var rows = exec.snapshot().values();
        assertEquals(1, rows.size());
        return rows.iterator().next().row();
    }

    private static TableExecutor table(String sql) {
        return compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
    }

    // ---------------------------------------------------------------- SUM

    @Test
    void tableSumOverAnAllNullGroupIsNullWhileCountIsZero() {
        var exec = compileTableAndCreate("SELECT symbol, SUM(qty) AS s, COUNT(qty) AS c, COUNT(*) AS n FROM trades GROUP BY symbol", TRADES);
        exec.onStreamEvent("trades", trade("A", null));
        exec.onStreamEvent("trades", trade("A", null));
        var row = onlyRow(exec);
        assertNull(row.get("s"));
        assertEquals(0L, row.get("c"));
        assertEquals(2L, row.get("n"));
    }

    @Test
    void tableSumBecomesNullAgainWhenItsLastNonNullValueIsRetracted() {
        var exec = compileTableAndCreate("SELECT symbol, SUM(qty) AS s FROM trades GROUP BY symbol", TRADES);
        exec.onTableDelta("trades", new TableDelta(trade("A", null), 1));
        exec.onTableDelta("trades", new TableDelta(trade("A", 5L), 1));
        assertEquals(5L, onlyRow(exec).get("s"));
        exec.onTableDelta("trades", new TableDelta(trade("A", 5L), -1));
        assertNull(onlyRow(exec).get("s"));
    }

    @Test
    void tableSumOfValuesThatCancelToZeroIsZeroNotNull() {
        var exec = compileTableAndCreate("SELECT symbol, SUM(qty) AS s FROM trades GROUP BY symbol", TRADES);
        exec.onStreamEvent("trades", trade("A", 5L));
        exec.onStreamEvent("trades", trade("A", -5L));
        assertEquals(0L, onlyRow(exec).get("s"));
    }

    @Test
    void pipelineSumOverAnAllNullWindowGroupIsNull() {
        var exec = compileAndCreate("SELECT symbol, SUM(qty) AS s, COUNT(qty) AS c FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)", TRADES);
        exec.onEvent("trades", trade("A", null, 1000));
        exec.onEvent("trades", trade("B", 3L, 1000));
        var rows = exec.advanceWatermark(10_000);
        var a = rows.stream().filter(r -> "A".equals(r.get("symbol"))).toList();
        assertEquals(1, a.size());
        assertNull(a.get(0).get("s"));
        assertEquals(0L, a.get(0).get("c"));
        assertEquals(3L, rows.stream().filter(r -> "B".equals(r.get("symbol"))).findFirst().orElseThrow().get("s"));
    }

    // ---------------------------------------------------------------- NOT IN, table mode

    private static final String NOT_IN_SQL = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
    private static final String IN_SQL = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";

    @Test
    void notInANullInTheSubqueryFiltersEveryNonMatchingRowAndItsRetractionRestoresThem() {
        var exec = table(NOT_IN_SQL);
        exec.onTableDelta("ref", new TableDelta(refRow("AAPL"), 1));
        exec.onStreamEvent("trades", trade("MSFT", 1L));
        exec.onStreamEvent("trades", trade("AAPL", 1L));
        assertEquals(set("MSFT"), symbols(exec));

        var poisoned = exec.onTableDelta("ref", new TableDelta(refRow(null), 1));
        assertEquals(1, poisoned.size());
        assertEquals(-1L, poisoned.get(0).weight());
        assertTrue(symbols(exec).isEmpty());

        exec.onStreamEvent("trades", trade("IBM", 1L));
        assertTrue(symbols(exec).isEmpty());

        exec.onTableDelta("ref", new TableDelta(refRow(null), -1));
        assertEquals(set("MSFT", "IBM"), symbols(exec));
    }

    @Test
    void notInANullOuterValuePassesOnlyWhileTheSubqueryIsEmpty() {
        var exec = table(NOT_IN_SQL);
        exec.onStreamEvent("trades", trade(null, 1L));
        assertEquals(set((String) null), symbols(exec));

        exec.onTableDelta("ref", new TableDelta(refRow("AAPL"), 1));
        assertTrue(symbols(exec).isEmpty());

        exec.onTableDelta("ref", new TableDelta(refRow("AAPL"), -1));
        assertEquals(set((String) null), symbols(exec));

        exec.onTableDelta("ref", new TableDelta(refRow(null), 1));
        assertTrue(symbols(exec).isEmpty());
    }

    @Test
    void notInANullOnlySubqueryFiltersEverythingButAnEmptyOnePassesEverything() {
        var exec = table(NOT_IN_SQL);
        exec.onStreamEvent("trades", trade("MSFT", 1L));
        exec.onStreamEvent("trades", trade(null, 1L));
        assertEquals(set("MSFT", null), symbols(exec));
        exec.onTableDelta("ref", new TableDelta(refRow(null), 1));
        assertTrue(symbols(exec).isEmpty());
    }

    @Test
    void notInConvergesWhateverTheOrderOfSubqueryAndOuterDeltas() {
        record In(String input, TableDelta delta) {}
        var deltas = List.of(
            new In("ref", new TableDelta(refRow("AAPL"), 1)),
            new In("ref", new TableDelta(refRow(null, "n1"), 1)),
            new In("ref", new TableDelta(refRow(null, "n1"), -1)),
            new In("trades", new TableDelta(trade("AAPL", 1L), 1)),
            new In("trades", new TableDelta(trade("MSFT", 1L), 1)),
            new In("trades", new TableDelta(trade(null, 1L), 1)));
        var rng = new Random(7);
        for (int round = 0; round < 50; round++) {
            var exec = table(NOT_IN_SQL);
            var shuffled = new ArrayList<>(deltas);
            Collections.shuffle(shuffled, rng);
            for (var d : shuffled) exec.onTableDelta(d.input(), d.delta());
            assertEquals(set("MSFT"), symbols(exec), "round " + round);
        }
    }

    @Test
    void inIgnoresNullsOnBothSides() {
        var exec = table(IN_SQL);
        exec.onTableDelta("ref", new TableDelta(refRow("AAPL"), 1));
        exec.onTableDelta("ref", new TableDelta(refRow(null), 1));
        exec.onStreamEvent("trades", trade("AAPL", 1L));
        exec.onStreamEvent("trades", trade("MSFT", 1L));
        exec.onStreamEvent("trades", trade(null, 1L));
        assertEquals(set("AAPL"), symbols(exec));
    }

    @Test
    void notExistsIsUnaffectedByNullsInTheSubquery() {
        var exec = table("SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)");
        exec.onStreamEvent("trades", trade("MSFT", 1L));
        assertEquals(set("MSFT"), symbols(exec));
        exec.onTableDelta("ref", new TableDelta(refRow(null), 1));
        assertTrue(symbols(exec).isEmpty());
    }

    // ---------------------------------------------------------------- NOT IN, pipeline mode

    private static final String PIPELINE_NOT_IN =
        "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) "
        + "SELECT t.symbol, COUNT(*) AS n FROM trades t WHERE t.symbol NOT IN (SELECT symbol FROM hot) "
        + "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";

    @Test
    void pipelineNotInANullInTheSubquerySnapshotFiltersNonMatchingRows() {
        var exec = compileAndCreate(PIPELINE_NOT_IN, TRADES);
        exec.onEvent("trades", trade("AAPL", 1L, 1000));
        exec.onEvent("trades", trade(null, 1L, 1500));
        exec.advanceWatermark(11000);

        exec.onEvent("trades", trade("MSFT", 1L, 12500));
        assertTrue(exec.advanceWatermark(20000).stream().noneMatch(r -> "MSFT".equals(r.get("symbol"))));
    }

    @Test
    void pipelineNotInANullOuterValuePassesOnlyBeforeTheSubqueryHasEmittedAnything() {
        var exec = compileAndCreate(PIPELINE_NOT_IN, TRADES);
        exec.onEvent("trades", trade(null, 1L, 1000));
        assertTrue(exec.advanceWatermark(11000).stream().anyMatch(r -> r.get("symbol") == null));

        exec.onEvent("trades", trade(null, 1L, 12000));
        assertTrue(exec.advanceWatermark(30000).stream().noneMatch(r -> r.get("symbol") == null));
    }
}
