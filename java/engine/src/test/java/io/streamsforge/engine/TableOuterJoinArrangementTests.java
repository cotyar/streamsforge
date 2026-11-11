package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.REF;
import static io.streamsforge.engine.TestHelpers.TRADES;
import static io.streamsforge.engine.TestHelpers.compileTable;
import static io.streamsforge.engine.TestHelpers.evt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 wave 2c-A: arrangement-sharing coverage for table-mode LEFT/RIGHT/FULL joins, modeled on ArrangementTests,
 * generalized to a table PAIR where one does plain INNER and the other does LEFT over the identical raw input and key.
 */
class TableOuterJoinArrangementTests {
    private record Ev(String origin, EventRecord row, long weight, boolean isTable) {}

    private static Ev s(String source, EventRecord row) { return new Ev(source, row, 1, false); }

    private static Ev t(String table, EventRecord row, long weight) { return new Ev(table, row, weight, true); }

    private static String canon(TableExecutor exec) {
        return exec.snapshot().entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(kv -> kv.getKey() + "=" + kv.getValue().weight()).collect(Collectors.joining("\n"));
    }

    private static String canon(PartitionedTableHarness harness) {
        return harness.snapshot().entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(kv -> kv.getKey() + "=" + kv.getValue().weight()).collect(Collectors.joining("\n"));
    }

    private static TableEdgeDescriptor arrangeableEdgeFor(TableDataflowPlan dataflow, String name) {
        var matches = dataflow.arrangeableExternalEdges().stream()
            .filter(e -> dataflow.externalInputNameOf(e).equals(name)).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    private static final String LEFT_SQL = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private static final String FULL_SQL = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";

    // ------------------------------------------------------------------------------------------------
    // The important one: two DIFFERENT tables over the SAME raw input+key, one INNER and one LEFT, at the SAME
    // partition count -- both must converge to their own classic single-partition baseline.
    // ------------------------------------------------------------------------------------------------

    @Test
    void sharedArrangement_InnerAndLeftOverSameRawInputAndKey_BothConvergeToOwnClassicBaseline() {
        final String sqlInner = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
        final String sqlLeft = LEFT_SQL;

        var compileInner = compileTable(sqlInner, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        var compileLeft = compileTable(sqlLeft, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileInner.ok());
        assertTrue(compileLeft.ok());

        var dataflowInner = compileInner.plan().createDataflow(4);
        var dataflowLeft = compileLeft.plan().createDataflow(4);

        // Precondition for sharing: both tables' "trades" edge resolves to the identical keySpec.
        var tradesEdgeInner = arrangeableEdgeFor(dataflowInner, "trades");
        var tradesEdgeLeft = arrangeableEdgeFor(dataflowLeft, "trades");
        assertEquals(dataflowInner.keySpecOf(tradesEdgeInner), dataflowLeft.keySpecOf(tradesEdgeLeft));

        // The SAME raw event stream feeds BOTH tables' classic executors and BOTH tables' harnesses.
        var events = List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)), // no ref row: INNER drops it, LEFT null-pads it
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1)); // retraction cascades through both

        var classicInner = compileInner.plan().createExecutor();
        var classicLeft = compileLeft.plan().createExecutor();
        var harnessInner = new PartitionedTableHarness(dataflowInner);
        var harnessLeft = new PartitionedTableHarness(dataflowLeft);

        for (var e : events) {
            if (e.isTable()) {
                classicInner.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
                classicLeft.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            } else {
                classicInner.onStreamEvent(e.origin(), e.row());
                classicLeft.onStreamEvent(e.origin(), e.row());
            }
            harnessInner.admit(e.origin(), e.row(), e.weight());
            harnessLeft.admit(e.origin(), e.row(), e.weight());
        }

        assertEquals(canon(classicInner), canon(harnessInner));
        assertEquals(canon(classicLeft), canon(harnessLeft));

        // Sanity: genuinely different outputs, not vacuously equal -- the retraction leaves INNER's only surviving
        // group empty (AAPL's own match unwound too), while LEFT still carries both rows, NULL-padded (MSFT never
        // matched; AAPL re-padded once its match retracted).
        assertNotEquals(canon(harnessInner), canon(harnessLeft));
        assertTrue(harnessInner.snapshot().isEmpty());
        assertEquals(2, harnessLeft.snapshot().size());
        assertTrue(harnessLeft.snapshot().values().stream().anyMatch(v -> "MSFT".equals(v.row().get("symbol")) && v.row().get("tag") == null));
        assertTrue(harnessLeft.snapshot().values().stream().anyMatch(v -> "AAPL".equals(v.row().get("symbol")) && v.row().get("tag") == null));
    }

    // ------------------------------------------------------------------------------------------------
    // A LEFT join's right edge is still arrangeable for a bare-column key -- same rule non-outer joins get, unaffected
    // by JoinKind.
    // ------------------------------------------------------------------------------------------------

    @Test
    void leftJoin_BothEdgesArrangeable_BareColumnKeys() {
        var plan = compileTable(LEFT_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(2, dataflow.arrangeableExternalEdges().size());

        var tradesEdge = arrangeableEdgeFor(dataflow, "trades");
        assertEquals(List.of("symbol"), tradesEdge.arrangeKeyFields());
        assertEquals("Left", tradesEdge.role());

        var refEdge = arrangeableEdgeFor(dataflow, "ref");
        assertEquals(List.of("symbol"), refEdge.arrangeKeyFields());
        assertEquals("Right", refEdge.role());
    }

    // ------------------------------------------------------------------------------------------------
    // FULL join: both sides still arrangeable too -- FULL pads both sides, but arrangeability only cares about "is this
    // side's key a bare own-field reference", unaffected by which sides pad.
    // ------------------------------------------------------------------------------------------------

    @Test
    void fullJoin_BothEdgesArrangeable_BareColumnKeys() {
        var plan = compileTable(FULL_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(2, dataflow.arrangeableExternalEdges().size());

        var tradesEdge = arrangeableEdgeFor(dataflow, "trades");
        assertEquals(List.of("symbol"), tradesEdge.arrangeKeyFields());
        assertEquals("Left", tradesEdge.role());

        var refEdge = arrangeableEdgeFor(dataflow, "ref");
        assertEquals(List.of("symbol"), refEdge.arrangeKeyFields());
        assertEquals("Right", refEdge.role());
    }
}
