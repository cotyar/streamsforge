package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Port of orleans/tests/StreamsForge.Engine.Tests/TableReduceOrderInsensitivityTests.cs — table-mode GROUP BY must
 * not depend on whether a row's retraction arrives before or after its assertion. Found by engine-dd's shuffled
 * differential test; see the C# class doc for the two causes that were fixed.
 */
class TableReduceOrderInsensitivityTests {
    private static TableExecutor create(String sql) {
        return compileTableAndCreate(sql, new SourceSchema[0], new SourceSchema[] {TRADES});
    }

    private static EventRecord trade(String symbol, double price, Long qty) {
        return evt(0, "trades", "symbol", symbol, "price", price, "qty", qty, "active", true);
    }

    private static Object only(TableExecutor exec, String column) {
        var rows = exec.snapshot().values();
        assertEquals(1, rows.size());
        return rows.iterator().next().row().get(column);
    }

    @Test
    void retractionBeforeAssertion_withTheGroupAtZeroWeight_keepsTheAggregateState() {
        var exec = create("SELECT symbol, SUM(qty) AS s, MAX(qty) AS mx, MIN(qty) AS mn, AVG(qty) AS a FROM trades GROUP BY symbol");
        exec.onTableDelta("trades", new TableDelta(trade("A", 1.0, 5L), -1));
        exec.onTableDelta("trades", new TableDelta(trade("A", 2.0, 7L), 1));
        assertTrue(exec.snapshot().isEmpty());
        exec.onTableDelta("trades", new TableDelta(trade("A", 1.0, 5L), 1));

        assertEquals(7L, only(exec, "s"));
        assertEquals(7L, only(exec, "mx"));
        assertEquals(7L, only(exec, "mn"));
        assertEquals(7.0, only(exec, "a"));
    }

    @Test
    void maxIgnoresNothing_aRetractionOfAnUnseenValueIsNettedByItsLaterAssertion() {
        var exec = create("SELECT symbol, MAX(qty) AS mx, MIN(qty) AS mn, COUNT(*) AS c FROM trades GROUP BY symbol");
        exec.onTableDelta("trades", new TableDelta(trade("B", 1.0, 3L), 1));
        exec.onTableDelta("trades", new TableDelta(trade("B", 2.0, 4L), 1));
        exec.onTableDelta("trades", new TableDelta(trade("B", 3.0, 5L), -1));
        exec.onTableDelta("trades", new TableDelta(trade("B", 3.0, 5L), 1));

        assertEquals(4L, only(exec, "mx"));
        assertEquals(3L, only(exec, "mn"));
        assertEquals(2L, only(exec, "c"));
    }

    @Test
    void countDistinctAndMedian_areNotResurrectedByAnEarlyRetraction() {
        var exec = create("SELECT symbol, COUNT(DISTINCT qty) AS d, MEDIAN(qty) AS md FROM trades GROUP BY symbol");
        exec.onTableDelta("trades", new TableDelta(trade("C", 1.0, 10L), 1));
        exec.onTableDelta("trades", new TableDelta(trade("C", 2.0, 20L), 1));
        exec.onTableDelta("trades", new TableDelta(trade("C", 3.0, 90L), -1));
        exec.onTableDelta("trades", new TableDelta(trade("C", 3.0, 90L), 1));

        assertEquals(2L, only(exec, "d"));
        assertEquals(15.0, only(exec, "md"));
    }

    @Test
    void aGroupWhoseRowsAllNetOut_isStillDropped() {
        var exec = create("SELECT symbol, SUM(qty) AS s, MAX(qty) AS mx FROM trades GROUP BY symbol");
        exec.onTableDelta("trades", new TableDelta(trade("D", 1.0, 5L), -1));
        exec.onTableDelta("trades", new TableDelta(trade("D", 1.0, 5L), 1));
        assertTrue(exec.snapshot().isEmpty());

        exec.onTableDelta("trades", new TableDelta(trade("D", 2.0, 8L), 1));
        assertEquals(8L, only(exec, "s"));
        assertEquals(8L, only(exec, "mx"));
    }

    private static List<TableDelta> randomDeltas(Random rng, boolean nullableQty) {
        var deltas = new ArrayList<TableDelta>();
        for (int i = 0; i < 60; i++) {
            Long qty = nullableQty && rng.nextInt(4) == 0 ? null : (long) (1 + rng.nextInt(5));
            var row = trade(String.valueOf((char) ('A' + rng.nextInt(3))), i, qty);
            deltas.add(new TableDelta(row, 1));
            if (rng.nextInt(3) == 0) deltas.add(new TableDelta(row, -1));
        }
        return deltas;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void anyDeliveryOrderOfTheSameDeltas_convergesToTheSameSnapshot(int seed) {
        String sql = "SELECT symbol, COUNT(*) AS c, COUNT(qty) AS cq, SUM(qty) AS s, AVG(qty) AS a, MIN(qty) AS mn, "
            + "MAX(qty) AS mx, COUNT(DISTINCT qty) AS d, MEDIAN(qty) AS md FROM trades GROUP BY symbol";
        var rng = new Random(seed);
        var deltas = randomDeltas(rng, true);

        var inOrder = create(sql);
        for (var d : deltas) inOrder.onTableDelta("trades", d);
        var expected = inOrder.snapshot();

        for (int round = 0; round < 20; round++) {
            var shuffled = new ArrayList<>(deltas);
            Collections.shuffle(shuffled, rng);
            var exec = create(sql);
            for (var d : shuffled) exec.onTableDelta("trades", d);
            var actual = exec.snapshot();

            assertEquals(new TreeSet<>(expected.keySet()), new TreeSet<>(actual.keySet()), "seed " + seed + " round " + round);
            for (var e : expected.entrySet()) assertEquals(e.getValue().weight(), actual.get(e.getKey()).weight());
        }
    }

    /** VAR_POP / STDDEV_SAMP re-centre on the first value seen, so order changes the last bits: compare within tolerance. */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void varianceIsOrderInsensitiveWithinFloatingTolerance(int seed) {
        String sql = "SELECT symbol, VAR_POP(qty) AS v, STDDEV_SAMP(qty) AS sd FROM trades GROUP BY symbol";
        var rng = new Random(seed);
        var deltas = randomDeltas(rng, false);

        var inOrder = create(sql);
        for (var d : deltas) inOrder.onTableDelta("trades", d);
        var expected = bySymbol(inOrder);

        for (int round = 0; round < 20; round++) {
            var shuffled = new ArrayList<>(deltas);
            Collections.shuffle(shuffled, rng);
            var exec = create(sql);
            for (var d : shuffled) exec.onTableDelta("trades", d);
            var actual = bySymbol(exec);

            assertEquals(expected.keySet(), actual.keySet());
            for (var e : expected.entrySet()) {
                assertEquals(e.getValue()[0], actual.get(e.getKey())[0], 1e-9);
                assertEquals(e.getValue()[1], actual.get(e.getKey())[1], 1e-9);
            }
        }
    }

    private static Map<String, double[]> bySymbol(TableExecutor exec) {
        var out = new TreeMap<String, double[]>();
        for (var rw : exec.snapshot().values()) {
            out.put((String) rw.row().get("symbol"), new double[] {(Double) rw.row().get("v"), (Double) rw.row().get("sd")});
        }
        return out;
    }
}
