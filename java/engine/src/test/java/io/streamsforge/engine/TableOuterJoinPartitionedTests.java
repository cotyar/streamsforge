package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.QUOTES;
import static io.streamsforge.engine.TestHelpers.REF;
import static io.streamsforge.engine.TestHelpers.TRADES;
import static io.streamsforge.engine.TestHelpers.compileTable;
import static io.streamsforge.engine.TestHelpers.evt;
import static io.streamsforge.engine.TestHelpers.schema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.TableDataflowBuilder;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableEdgeMode;
import io.streamsforge.engine.dataflow.TableStageDescriptor;
import io.streamsforge.engine.dataflow.TableStageKind;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 008 wave 2c-A: the PartitionedDataflowTests _StageGraph/_Equivalence/_Deterministic triad, specialized to
 * table-mode LEFT/RIGHT/FULL OUTER joins (see TableOuterJoinOp's class doc for the T1/T2/T3 invariant and the NULL-key
 * asymmetry every event set below deliberately exercises).
 *
 * <p>RIGHT's event set deliberately compiles "ref r RIGHT JOIN trades t" (table FIRST, stream as the JOIN alias) rather
 * than the more idiomatic "trades t RIGHT JOIN ref r" -- a genuine pad -> flip -> UN-flip cycle needs the side that
 * TRIGGERS the flip to be retractable, and TableExecutor.onStreamEvent is assert-only (weight always +1).
 */
class TableOuterJoinPartitionedTests {
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

    private static String messages(TableCompileResult r) {
        return r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining("; "));
    }

    /** Classic single-partition TableExecutor vs the partitioned harness at each of {@code partitionCounts}. */
    private static void assertEquivalence(TableCompileResult compileResult, List<Ev> events, int[] partitionCounts) {
        assertTrue(compileResult.ok(), messages(compileResult));
        var plan = compileResult.plan();

        var classic = plan.createExecutor();
        for (var e : events) {
            if (e.isTable()) classic.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classic.onStreamEvent(e.origin(), e.row());
        }
        var classicCanon = canon(classic);

        for (int p : partitionCounts) {
            var dataflow = plan.createDataflow(p);
            var harness = new PartitionedTableHarness(dataflow);
            for (var e : events) harness.admit(e.origin(), e.row(), e.weight());
            assertEquals(classicCanon, canon(harness));
        }
    }

    /** FIFO/LIFO/reversed admission orders must converge to the identical consolidated snapshot. */
    private static void assertDeterministic(TableCompileResult compileResult, List<Ev> events) {
        assertTrue(compileResult.ok(), messages(compileResult));
        var plan = compileResult.plan();
        int partitionCount = 4;

        var fifoHarness = new PartitionedTableHarness(plan.createDataflow(partitionCount));
        for (var e : events) fifoHarness.admit(e.origin(), e.row(), e.weight(), false);

        var lifoHarness = new PartitionedTableHarness(plan.createDataflow(partitionCount));
        for (var e : events) lifoHarness.admit(e.origin(), e.row(), e.weight(), true);

        var reversedHarness = new PartitionedTableHarness(plan.createDataflow(partitionCount));
        var reversed = new ArrayList<>(events);
        Collections.reverse(reversed);
        for (var e : reversed) reversedHarness.admit(e.origin(), e.row(), e.weight(), false);

        var fifo = canon(fifoHarness);
        assertEquals(fifo, canon(lifoHarness));
        assertEquals(fifo, canon(reversedHarness));
    }

    private static final String LEFT_SQL = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private static final String RIGHT_SQL = "SELECT r.tag, t.symbol FROM ref r RIGHT JOIN trades t ON r.symbol = t.symbol";
    private static final String FULL_SQL = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";
    private static final String CHAINED_LEFT_THEN_INNER_SQL =
        "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON r.tag = q.symbol";

    private static TableCompileResult compileTradesRef(String sql) {
        return compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
    }

    // ------------------------------------------------------------------------------------------------
    // Event sets. Each satisfies, at minimum: a key that pads then flips (match arrives) then un-flips (match
    // retracted); a NULL-key row on the padded side; a row on the other side with no counterpart; and a retraction.
    // ------------------------------------------------------------------------------------------------

    private static List<Ev> leftEvents() {
        return List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)), // pads: right bucket empty
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1), // other side, no counterpart ever -> LEFT doesn't pad right -> no output
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1), // flips AAPL: product + left-pad retract
            s("trades", evt(2, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true)), // NULL-key row on padded (left) side -> immediate pad, never indexed
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1)); // retraction -> un-flips AAPL: retract product + repad
    }

    private static List<Ev> rightEvents() {
        return List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)), // own pad: Left(ref) has no AAPL yet
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1), // other side (left/ref), no counterpart ever -> RIGHT doesn't pad left -> no output
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1), // flips AAPL: product + right(trades)-pad retract
            s("trades", evt(2, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true)), // NULL-key row on padded (right/trades) side -> immediate pad, never indexed
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1)); // retraction -> un-flips AAPL: retract product + repad trades row
    }

    private static List<Ev> fullEvents() {
        return List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)), // left own-pad
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1), // right own-pad; also the "no counterpart" row -- stays padded forever
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1), // flips AAPL: product + left-pad retract
            s("trades", evt(2, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true)), // NULL-key row on the left-padded side
            t("ref", evt(0, "ref", "symbol", null, "tag", "orphan"), 1), // NULL-key row on the right-padded side too (FULL pads both)
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1)); // retraction -> un-flips AAPL: retract product + repad trades row
    }

    private static List<Ev> chainedLeftThenInnerEvents() {
        return List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)), // LEFT pads (r.tag=NULL) -> INNER drops it (NULL key never indexed)
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 3L, "active", true)), // never matches ref at all -> stays LEFT-padded -> still dropped by INNER
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "TAG1"), 1), // LEFT flips (pad retracts, dropped, no-op) + product flows into INNER, unmatched there yet
            s("quotes", evt(3, "quotes", "symbol", "TAG1", "bid", 99.5, "ask", 100.5)), // INNER's own match fires -> final row appears
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "TAG1"), -1)); // retraction cascades: LEFT un-flips (repads NULL, still dropped) -> final row disappears
    }

    // ------------------------------------------------------------------------------------------------
    // _StageGraph
    // ------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
    void outerJoin_StageGraph_BothEdgesHashPartitioned_StageKindStillJoin_RoutingKeyCarriesFullList(String kind) {
        var sql = "SELECT t.symbol, r.tag FROM trades t " + kind + " JOIN ref r ON t.symbol = r.symbol";
        var compiled = compileTradesRef(sql).plan().compiled();
        var built = TableDataflowBuilder.build(compiled, 4);

        var twoInEdge = built.stages().stream().filter(st -> st.inEdges().size() == 2).collect(Collectors.toList());
        assertEquals(1, twoInEdge.size());
        TableStageDescriptor joinStage = twoInEdge.get(0);
        // Plan 008 deliberately did NOT add a dedicated TableStageKind for outer joins -- the coarse stage kind is
        // unchanged from plain INNER/CROSS; only the op JoinChainStageExecutor instantiates differs.
        assertEquals(TableStageKind.JOIN, joinStage.kind());
        assertEquals(2, joinStage.inEdges().size());
        for (var e : joinStage.inEdges()) assertEquals(TableEdgeMode.HASH_PARTITION, e.mode());
        assertEquals(List.of("Left", "Right"), joinStage.inEdges().stream().map(TableEdgeDescriptor::role).collect(Collectors.toList()));

        var leftEdge = singleWithRole(joinStage, "Left");
        var rightEdge = singleWithRole(joinStage, "Right");
        assertEquals(1, built.routingSpecs().get(leftEdge.edgeId().value()).keyExprs.size());
        assertEquals(1, built.routingSpecs().get(rightEdge.edgeId().value()).keyExprs.size());
    }

    private static TableEdgeDescriptor singleWithRole(TableStageDescriptor stage, String role) {
        var matches = stage.inEdges().stream().filter(e -> e.role().equals(role)).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    private static final SourceSchema ORDERS = schema("orders", "sym", FieldKind.STRING, "venue", FieldKind.STRING);
    private static final SourceSchema FILLS = schema("fills", "sym", FieldKind.STRING, "venue", FieldKind.STRING, "qty", FieldKind.LONG);

    @Test
    void outerJoin_StageGraph_CompositeKey_RoutingSpecCarriesFullKeyList_NotFoldedToFirstComponent() {
        final String sql = "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";
        var compiled = compileTable(sql, new SourceSchema[] {ORDERS}, new SourceSchema[] {FILLS}).plan().compiled();
        var built = TableDataflowBuilder.build(compiled, 4);

        var twoInEdge = built.stages().stream().filter(st -> st.inEdges().size() == 2).collect(Collectors.toList());
        assertEquals(1, twoInEdge.size());
        var joinStage = twoInEdge.get(0);
        assertEquals(TableStageKind.JOIN, joinStage.kind());
        var leftEdge = singleWithRole(joinStage, "Left");
        var rightEdge = singleWithRole(joinStage, "Right");
        assertEquals(TableEdgeMode.HASH_PARTITION, leftEdge.mode());
        assertEquals(TableEdgeMode.HASH_PARTITION, rightEdge.mode());

        // The routing key spec must carry BOTH equi-conjuncts -- not the pre-008 "first component only, rest folded into
        // a residual" shape non-outer joins still get.
        assertEquals(2, built.routingSpecs().get(leftEdge.edgeId().value()).keyExprs.size());
        assertEquals(2, built.routingSpecs().get(rightEdge.edgeId().value()).keyExprs.size());
    }

    // ------------------------------------------------------------------------------------------------
    // _Equivalence
    // ------------------------------------------------------------------------------------------------

    @Test
    void leftJoin_Equivalence() { assertEquivalence(compileTradesRef(LEFT_SQL), leftEvents(), new int[] {1, 4}); }

    @Test
    void rightJoin_Equivalence() { assertEquivalence(compileTradesRef(RIGHT_SQL), rightEvents(), new int[] {1, 4}); }

    @Test
    void fullJoin_Equivalence() { assertEquivalence(compileTradesRef(FULL_SQL), fullEvents(), new int[] {1, 4}); }

    @Test
    void chainedLeftThenInner_Equivalence() {
        assertEquivalence(compileTable(CHAINED_LEFT_THEN_INNER_SQL, new SourceSchema[] {TRADES, QUOTES}, new SourceSchema[] {REF}),
            chainedLeftThenInnerEvents(), new int[] {1, 4});
    }

    // ------------------------------------------------------------------------------------------------
    // _Deterministic
    // ------------------------------------------------------------------------------------------------

    @Test
    void leftJoin_Deterministic() { assertDeterministic(compileTradesRef(LEFT_SQL), leftEvents()); }

    @Test
    void rightJoin_Deterministic() { assertDeterministic(compileTradesRef(RIGHT_SQL), rightEvents()); }

    /** FULL: the full FIFO/LIFO/reversed determinism triad, same as leftJoin_Deterministic/rightJoin_Deterministic. */
    @Test
    void fullJoin_Deterministic() { assertDeterministic(compileTradesRef(FULL_SQL), fullEvents()); }

    /**
     * FIXED (plan 008 wave 2c-A follow-up): the ledger (mirrored exactly by PartitionedTableHarness.applyConsolidation)
     * tracks outstanding NEGATIVE running weight for a canonical row key in a side debt table instead of silently
     * discarding a negative delta that arrives before the dictionary has ever seen that key. With the admission sequence
     * reversed, ref's retraction arrives first, TableOuterJoinOp correctly emits its own T2 pad with weight -1 for a
     * canonical row never seen; the debt nets against the later +1 to exactly 0 and both structures prune the key.
     */
    @Test
    void fullJoin_FullyReversedAdmissionOrder_ConvergesWithForward() {
        var compileResult = compileTradesRef(FULL_SQL);
        var plan = compileResult.plan();
        var events = fullEvents();
        var reversedEvents = new ArrayList<>(events);
        Collections.reverse(reversedEvents);

        var classicForward = plan.createExecutor();
        for (var e : events) {
            if (e.isTable()) classicForward.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classicForward.onStreamEvent(e.origin(), e.row());
        }

        var classicReversed = plan.createExecutor();
        for (var e : reversedEvents) {
            if (e.isTable()) classicReversed.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classicReversed.onStreamEvent(e.origin(), e.row());
        }

        var forwardCanon = canon(classicForward);

        // Forward and fully-reversed top-level admission order now converge on the IDENTICAL consolidated snapshot: 4
        // rows, no spurious (NULL,"core") pad in either order.
        assertEquals(forwardCanon, canon(classicReversed));
        assertEquals(4, classicForward.snapshot().size());
        assertFalse(classicForward.snapshot().values().stream().anyMatch(v -> v.row().get("symbol") == null && "core".equals(v.row().get("tag"))));
        assertEquals(4, classicReversed.snapshot().size());
        assertFalse(classicReversed.snapshot().values().stream().anyMatch(v -> v.row().get("symbol") == null && "core".equals(v.row().get("tag"))));

        // Reproduces through the partitioned harness too, both FIFO and LIFO intra-admission work-queue order, on top of
        // the already-reversed top-level order.
        var harnessReversedFifo = new PartitionedTableHarness(plan.createDataflow(4));
        for (var e : reversedEvents) harnessReversedFifo.admit(e.origin(), e.row(), e.weight(), false);
        assertEquals(forwardCanon, canon(harnessReversedFifo));

        var harnessReversedLifo = new PartitionedTableHarness(plan.createDataflow(4));
        for (var e : reversedEvents) harnessReversedLifo.admit(e.origin(), e.row(), e.weight(), true);
        assertEquals(forwardCanon, canon(harnessReversedLifo));
    }

    @Test
    void chainedLeftThenInner_Deterministic() {
        assertDeterministic(compileTable(CHAINED_LEFT_THEN_INNER_SQL, new SourceSchema[] {TRADES, QUOTES}, new SourceSchema[] {REF}),
            chainedLeftThenInnerEvents());
    }

    // ------------------------------------------------------------------------------------------------
    // Known skew hazard, pinned as known-and-correct: every NULL-keyed padded row hashes to the SAME partition, because
    // TableOuterJoinOp never indexes or looks up a NULL-keyed row -- so the skew never threatens correctness, only balance.
    // ------------------------------------------------------------------------------------------------

    @Test
    void nullKeyedPaddedRows_AllRouteToOnePartition_ButSnapshotMatchesClassic() {
        var compileResult = compileTradesRef(LEFT_SQL);
        assertTrue(compileResult.ok());
        var plan = compileResult.plan();
        var dataflow = plan.createDataflow(4);

        var joinStages = dataflow.stages().stream().filter(st -> st.kind() == TableStageKind.JOIN).collect(Collectors.toList());
        assertEquals(1, joinStages.size());
        var leftEdge = singleWithRole(joinStages.get(0), "Left");

        var nullRows = List.of(
            evt(1, "trades", "symbol", null, "price", 1.0, "qty", 1L, "active", true),
            evt(2, "trades", "symbol", null, "price", 2.0, "qty", 2L, "active", true),
            evt(3, "trades", "symbol", null, "price", 3.0, "qty", 3L, "active", true),
            evt(4, "trades", "symbol", null, "price", 4.0, "qty", 4L, "active", true));

        var partitions = nullRows.stream().map(r -> dataflow.partitionOf(leftEdge.edgeId(), r)).distinct().collect(Collectors.toList());
        assertEquals(1, partitions.size()); // the skew: every NULL-keyed row lands on the identical partition

        var events = new ArrayList<Ev>(List.of(
            s("trades", evt(10, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1)));
        for (var r : nullRows) events.add(new Ev("trades", r, 1, false));

        var classic = plan.createExecutor();
        for (var e : events) {
            if (e.isTable()) classic.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classic.onStreamEvent(e.origin(), e.row());
        }

        var harness = new PartitionedTableHarness(dataflow);
        for (var e : events) harness.admit(e.origin(), e.row(), e.weight());

        assertEquals(canon(classic), canon(harness)); // correctness holds despite every NULL row sharing one partition
        assertEquals(5, classic.snapshot().size()); // 4 distinct NULL pads + 1 AAPL product
    }

    // ------------------------------------------------------------------------------------------------
    // CROSS JOIN: the >1-partition guard must still fire when CROSS is chained AFTER an outer join (hop i>0) --
    // TableDataflowBuilder.build's check is per-join, unconditional on chain position or the preceding join's kind.
    // ------------------------------------------------------------------------------------------------

    @Test
    void chainedLeftThenCross_CreateDataflowThrows_UseParallelism1() {
        var sql = "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol CROSS JOIN quotes q";
        var result = compileTable(sql, new SourceSchema[] {TRADES, QUOTES}, new SourceSchema[] {REF});
        assertTrue(result.ok(), result.diagnostics().stream().map(d -> d.line() + ":" + d.column() + " " + d.message()).collect(Collectors.joining(";")));
        var ex = assertThrows(UnsupportedOperationException.class, () -> result.plan().createDataflow(4));
        assertTrue(ex.getMessage().contains("Use Parallelism = 1"));
        assertTrue(ex.getMessage().contains("CROSS JOIN"));
    }
}
