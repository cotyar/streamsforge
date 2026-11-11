package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.last;
import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.compileTableAndCreate;
import static io.streamsforge.engine.TestHelpers.evt;
import static io.streamsforge.engine.TestHelpers.schema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Z-set aggregation semantics: running (unwindowed) GROUP BY aggregates that emit
 *  retraction/assertion pairs as groups change, and vanish (retraction only) when their contributing
 *  weight reaches zero. Retraction requires a weighted delta — raw stream events always arrive at +1, so
 *  these tests drive the executor via onTableDelta (simulating an upstream table's output), exactly the
 *  path a real table-over-table chain uses. */
class TableZSetTests {
    private static final SourceSchema PRICES = schema("prices", "symbol", FieldKind.STRING, "price", FieldKind.DOUBLE, "id", FieldKind.STRING);

    private static TableExecutor createCountBySymbol() {
        return compileTableAndCreate("SELECT symbol, COUNT(*) AS cnt FROM prices GROUP BY symbol", new SourceSchema[0], new SourceSchema[] {PRICES});
    }

    private static TableExecutor create(String sql) {
        return compileTableAndCreate(sql, new SourceSchema[0], new SourceSchema[] {PRICES});
    }

    private static EventRecord row(String symbol, double price, String id) {
        return evt(0, "prices", "symbol", symbol, "price", price, "id", id);
    }

    @Test
    void firstContributionToAGroupEmitsOnlyAnAssertion() {
        var exec = createCountBySymbol();
        var deltas = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));

        var assertion = single(deltas);
        assertEquals(1L, assertion.weight());
        assertEquals(1L, assertion.row().get("cnt"));
        assertEquals("AAPL", assertion.row().get("symbol"));
    }

    @Test
    void subsequentContributionEmitsRetractThenAssert() {
        var exec = createCountBySymbol();
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));

        var deltas = exec.onTableDelta("prices", new TableDelta(row("AAPL", 101.0, "r2"), 1));

        assertEquals(2, deltas.size());
        assertEquals(-1L, deltas.get(0).weight());
        assertEquals(1L, deltas.get(0).row().get("cnt"));
        assertEquals(1L, deltas.get(1).weight());
        assertEquals(2L, deltas.get(1).row().get("cnt"));
    }

    @Test
    void groupVanishesWhenContributingWeightReachesZero() {
        var exec = createCountBySymbol();
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));

        var deltas = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), -1));

        // Group's total weight went from 1 to 0 — only the retraction is emitted, group is dropped.
        var retraction = single(deltas);
        assertEquals(-1L, retraction.weight());
        assertEquals(1L, retraction.row().get("cnt"));

        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void minRetractionWithDuplicates_RemovingOneLeavesMinUnchanged_RemovingBothExposesNextValue() {
        var exec = create("SELECT symbol, MIN(price) AS low FROM prices GROUP BY symbol");

        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1)); // low=100
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r2"), 1)); // still low=100 (dup min)
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 105.0, "r3"), 1)); // still low=100

        var afterRemoveOne = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), -1));
        var lastAssertAfterRemoveOne = last(afterRemoveOne);
        assertEquals(100.0, lastAssertAfterRemoveOne.row().get("low")); // r2 still contributes 100 — min unchanged

        var afterRemoveBoth = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r2"), -1));
        var lastAssertAfterRemoveBoth = last(afterRemoveBoth);
        assertEquals(105.0, lastAssertAfterRemoveBoth.row().get("low")); // both 100s gone — min moves to next value
    }

    @Test
    void maxRetractionWithDuplicates_RemovingOneLeavesMaxUnchanged_RemovingBothExposesNextValue() {
        var exec = create("SELECT symbol, MAX(price) AS high FROM prices GROUP BY symbol");

        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r2"), 1));
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 95.0, "r3"), 1));

        var afterRemoveOne = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), -1));
        assertEquals(100.0, last(afterRemoveOne).row().get("high"));

        var afterRemoveBoth = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r2"), -1));
        assertEquals(95.0, last(afterRemoveBoth).row().get("high"));
    }

    @Test
    void avgIsCorrectUnderRetraction() {
        var exec = create("SELECT symbol, AVG(price) AS avgp FROM prices GROUP BY symbol");

        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.onTableDelta("prices", new TableDelta(row("AAPL", 200.0, "r2"), 1));
        assertEquals(150.0, last(afterSecond).row().get("avgp"));

        var afterRetract = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), -1));
        assertEquals(200.0, last(afterRetract).row().get("avgp"));

        var afterReAdd = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        assertEquals(150.0, last(afterReAdd).row().get("avgp"));
    }

    @Test
    void sumIsSubtractableUnderRetraction() {
        var exec = create("SELECT symbol, SUM(price) AS total FROM prices GROUP BY symbol");

        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.onTableDelta("prices", new TableDelta(row("AAPL", 50.0, "r2"), 1));
        assertEquals(150.0, last(afterSecond).row().get("total"));

        var afterRetract = exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), -1));
        assertEquals(50.0, last(afterRetract).row().get("total"));
    }

    @Test
    void globalAggregateWithoutGroupByIsASingleGroup() {
        var exec = create("SELECT COUNT(*) AS cnt FROM prices");

        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.onTableDelta("prices", new TableDelta(row("MSFT", 200.0, "r2"), 1));

        assertEquals(2L, last(afterSecond).row().get("cnt"));
    }

    @Test
    void endToEndSnapshotReflectsConsolidatedState() {
        var exec = createCountBySymbol();
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 100.0, "r1"), 1));
        exec.onTableDelta("prices", new TableDelta(row("AAPL", 101.0, "r2"), 1));
        exec.onTableDelta("prices", new TableDelta(row("MSFT", 300.0, "r3"), 1));

        var snapshot = exec.snapshot();
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.values().stream().anyMatch(v -> java.util.Objects.equals(v.row().get("symbol"), "AAPL") && java.util.Objects.equals(v.row().get("cnt"), 2L)));
        assertTrue(snapshot.values().stream().anyMatch(v -> java.util.Objects.equals(v.row().get("symbol"), "MSFT") && java.util.Objects.equals(v.row().get("cnt"), 1L)));
    }
}
