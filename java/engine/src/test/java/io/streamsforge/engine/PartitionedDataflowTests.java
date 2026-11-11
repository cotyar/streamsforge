package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.ORDER_EVENTS;
import static io.streamsforge.engine.TestHelpers.REF;
import static io.streamsforge.engine.TestHelpers.STRUCTURES;
import static io.streamsforge.engine.TestHelpers.TRADES;
import static io.streamsforge.engine.TestHelpers.compileTable;
import static io.streamsforge.engine.TestHelpers.evt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableEdgeMode;
import io.streamsforge.engine.dataflow.TableStageDescriptor;
import io.streamsforge.engine.dataflow.TableStageKind;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 003 M2 acceptance §5: stage-graph construction (5a), determinism (5b), and the equivalence oracle (5c) —
 * classic single-partition {@link TableExecutor} vs the partitioned {@link PartitionedTableHarness} at P=1 and P=4,
 * for six representative plan shapes: aggregate-only, join+aggregate, unnest+aggregate, latest-by, semi-join, and
 * scalar broadcast.
 */
class PartitionedDataflowTests {
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

    private static String diagnostics(TableCompileResult r) {
        return r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining("; "));
    }

    private static void assertEquivalence(TableCompileResult compileResult, List<Ev> events, int[] partitionCounts) {
        assertTrue(compileResult.ok(), diagnostics(compileResult));
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

    /** Same event sequence fed to fresh P=4 harnesses twice — once draining FIFO, once LIFO — asserts identical
     *  consolidated output (plan 003 M2 acceptance 5b: "fed identical batch sets in different arrival orders ⇒
     *  identical consolidated output"). */
    private static void assertDeterministic(TableCompileResult compileResult, List<Ev> events) { assertDeterministic(compileResult, events, 4); }

    private static void assertDeterministic(TableCompileResult compileResult, List<Ev> events, int partitionCount) {
        assertTrue(compileResult.ok());
        var plan = compileResult.plan();

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
        // A fully reversed ADMISSION order isn't guaranteed to match for plans with order-sensitive semantics (LATEST
        // BY: "last write wins" depends on timestamp, not arrival order, so it DOES still hold). For plans where it
        // holds (every representative shape here), assert it too — the stronger claim.
        assertEquals(fifo, canon(reversedHarness));
    }

    private static List<TableStageKind> kindsOf(List<TableStageDescriptor> stages) {
        return stages.stream().map(TableStageDescriptor::kind).collect(Collectors.toList());
    }

    private static TableEdgeDescriptor singleEdgeFrom(List<TableEdgeDescriptor> edges, int fromStageId) {
        var matches = edges.stream().filter(e -> e.fromStageId() == fromStageId).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    private static TableStageDescriptor singleStageOfKind(List<TableStageDescriptor> stages, TableStageKind kind) {
        var matches = stages.stream().filter(s -> s.kind() == kind).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    private static TableEdgeDescriptor singleEdgeWithRole(TableStageDescriptor stage, String role) {
        var matches = stage.inEdges().stream().filter(e -> e.role().equals(role)).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    // ------------------------------------------------------------------------------------------------
    // 1) Aggregate-only: no JOIN, straight into Reduce.
    // ------------------------------------------------------------------------------------------------

    private static final String AGG_SQL = "SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol";

    @Test
    void aggregateOnly_StageGraph() {
        var plan = compileTable(AGG_SQL, TRADES).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.FILTER_PROJECT, TableStageKind.REDUCE), kindsOf(dataflow.stages()));
        var ingestToFp = singleEdgeFrom(dataflow.edges(), dataflow.stages().get(0).stageId());
        assertEquals(TableEdgeMode.HASH_PARTITION, ingestToFp.mode()); // no join -> content-hash fan-out out of the 1-partition ingest
        var fpToReduce = singleEdgeFrom(dataflow.edges(), dataflow.stages().get(1).stageId());
        assertEquals(TableEdgeMode.HASH_PARTITION, fpToReduce.mode()); // partitioned on GROUP BY key
        assertEquals(TableEdgeMode.GATHER, dataflow.terminalEdge().mode());
        assertEquals(dataflow.stages().get(2).stageId(), dataflow.terminalEdge().fromStageId());
    }

    @Test
    void aggregateOnly_Equivalence() {
        var events = new ArrayList<Ev>(List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "AAPL", "price", 11.0, "qty", 3L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(4, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true))));
        assertEquivalence(compileTable(AGG_SQL, TRADES), events, new int[] {1, 4});
    }

    @Test
    void aggregateOnly_Deterministic() {
        assertDeterministic(compileTable(AGG_SQL, TRADES), List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "AAPL", "price", 11.0, "qty", 1L, "active", true)),
            s("trades", evt(4, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true))));
    }

    // ------------------------------------------------------------------------------------------------
    // 2) Join + aggregate: trades (stream) JOIN ref (table) ON symbol, GROUP BY.
    // ------------------------------------------------------------------------------------------------

    private static final String JOIN_AGG_SQL = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";

    @Test
    void joinAggregate_StageGraph() {
        var plan = compileTable(JOIN_AGG_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.INGEST, TableStageKind.JOIN, TableStageKind.FILTER_PROJECT, TableStageKind.REDUCE),
            kindsOf(dataflow.stages()));

        var joinStage = singleStageOfKind(dataflow.stages(), TableStageKind.JOIN);
        assertEquals(2, joinStage.inEdges().size());
        for (var e : joinStage.inEdges()) assertEquals(TableEdgeMode.HASH_PARTITION, e.mode()); // co-partitioned on the join key
        assertEquals(List.of("Left", "Right"), joinStage.inEdges().stream().map(TableEdgeDescriptor::role).collect(Collectors.toList()));
    }

    @Test
    void joinAggregate_Equivalence() {
        var events = new ArrayList<Ev>(List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "AAPL", "price", 11.0, "qty", 3L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(4, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true)), // no matching ref row
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // retraction cascades through the join
        assertEquivalence(compileTable(JOIN_AGG_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}), events, new int[] {1, 4});
    }

    @Test
    void joinAggregate_Deterministic() {
        assertDeterministic(compileTable(JOIN_AGG_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}), List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "AAPL", "price", 11.0, "qty", 3L, "active", true))));
    }

    // ------------------------------------------------------------------------------------------------
    // 3) Unnest + aggregate: structures CROSS JOIN UNNEST(legs), GROUP BY trade_id.
    // ------------------------------------------------------------------------------------------------

    private static final String UNNEST_AGG_SQL = "SELECT s.trade_id, COUNT(*) AS legcount FROM structures s CROSS JOIN UNNEST(s.legs) AS l GROUP BY s.trade_id";

    @Test
    void unnestAggregate_StageGraph() {
        var plan = compileTable(UNNEST_AGG_SQL, STRUCTURES).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.UNNEST, TableStageKind.FILTER_PROJECT, TableStageKind.REDUCE),
            kindsOf(dataflow.stages()));

        var unnestStage = singleStageOfKind(dataflow.stages(), TableStageKind.UNNEST);
        assertEquals(1, unnestStage.inEdges().size());
        // Unnest at chain position 0, fed by the 1-partition ingest -> content-hash fan-out (stateless, no key).
        assertEquals(TableEdgeMode.HASH_PARTITION, unnestStage.inEdges().get(0).mode());
    }

    @Test
    void unnestAggregate_Equivalence() {
        java.util.function.Function<String[], Object> legs = ccys -> {
            var list = new ArrayList<Object>();
            for (String c : ccys) {
                var d = new LinkedHashMap<String, Object>();
                d.put("ccy", c);
                list.add(d);
            }
            return list;
        };
        var events = new ArrayList<Ev>(List.of(
            s("structures", evt(1, "structures", "trade_id", "T1", "legs", legs.apply(new String[] {"USD", "EUR"}), "payload", null)),
            s("structures", evt(2, "structures", "trade_id", "T2", "legs", legs.apply(new String[] {"GBP"}), "payload", null)),
            s("structures", evt(3, "structures", "trade_id", "T1", "legs", legs.apply(new String[] {"USD", "EUR", "JPY"}), "payload", null))));
        assertEquivalence(compileTable(UNNEST_AGG_SQL, STRUCTURES), events, new int[] {1, 4});
    }

    // ------------------------------------------------------------------------------------------------
    // 4) LATEST BY: argmax-by-timestamp, no join, no aggregation.
    // ------------------------------------------------------------------------------------------------

    private static final String LATEST_BY_SQL = "SELECT order_id, stage, filled_qty FROM order_events LATEST BY (order_id)";

    @Test
    void latestBy_StageGraph() {
        var plan = compileTable(LATEST_BY_SQL, ORDER_EVENTS).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.FILTER_PROJECT, TableStageKind.LATEST_BY), kindsOf(dataflow.stages()));
        var latestByStage = singleStageOfKind(dataflow.stages(), TableStageKind.LATEST_BY);
        assertEquals(TableEdgeMode.HASH_PARTITION, latestByStage.inEdges().get(0).mode()); // partitioned on the LATEST BY key
        assertEquals(latestByStage.stageId(), dataflow.terminalEdge().fromStageId());
    }

    @Test
    void latestBy_Equivalence() {
        var events = new ArrayList<Ev>(List.of(
            s("order_events", evt(100, "order_events", "order_id", "O1", "stage", "new", "filled_qty", 0L)),
            s("order_events", evt(200, "order_events", "order_id", "O2", "stage", "new", "filled_qty", 0L)),
            s("order_events", evt(300, "order_events", "order_id", "O1", "stage", "partial", "filled_qty", 5L)),
            s("order_events", evt(250, "order_events", "order_id", "O1", "stage", "stale", "filled_qty", 1L)), // late arrival, older ts
            s("order_events", evt(400, "order_events", "order_id", "O2", "stage", "filled", "filled_qty", 10L))));
        assertEquivalence(compileTable(LATEST_BY_SQL, ORDER_EVENTS), events, new int[] {1, 4});
    }

    // ------------------------------------------------------------------------------------------------
    // 5) Semi-join: trades WHERE symbol IN (SELECT symbol FROM ref) — always a derived/broadcast join.
    // ------------------------------------------------------------------------------------------------

    private static final String SEMI_SQL = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";

    @Test
    void semiJoin_StageGraph() {
        var plan = compileTable(SEMI_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.SEMI_ANTI, TableStageKind.FILTER_PROJECT), kindsOf(dataflow.stages()));
        var semiStage = singleStageOfKind(dataflow.stages(), TableStageKind.SEMI_ANTI);
        var rightEdge = singleEdgeWithRole(semiStage, "Right");
        assertEquals(TableEdgeMode.BROADCAST, rightEdge.mode());
        assertEquals(-1, rightEdge.fromStageId()); // external: fed by ref directly, not another stage
        assertTrue(rightEdge.externalInputNames().contains("ref"));
        var leftEdge = singleEdgeWithRole(semiStage, "Left");
        assertEquals(TableEdgeMode.HASH_PARTITION, leftEdge.mode()); // chain position 0, fed by 1-partition ingest -> content-hash fan-out
    }

    @Test
    void semiJoin_Equivalence() {
        var events = new ArrayList<Ev>(List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)), // no ref row: filtered
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1),
            s("trades", evt(3, "trades", "symbol", "MSFT", "price", 21.0, "qty", 1L, "active", true)), // now passes
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // presence flips off -> AAPL rows retract
        assertEquivalence(compileTable(SEMI_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}), events, new int[] {1, 4});
    }

    @Test
    void semiJoin_Deterministic() {
        assertDeterministic(compileTable(SEMI_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}), List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1)));
    }

    // ------------------------------------------------------------------------------------------------
    // 6) Scalar broadcast: uncorrelated scalar subquery over the SAME stream ("trades" feeds two edges).
    // ------------------------------------------------------------------------------------------------

    private static final String SCALAR_SQL = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";

    @Test
    void scalarBroadcast_StageGraph() {
        var plan = compileTable(SCALAR_SQL, TRADES).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(List.of(TableStageKind.INGEST, TableStageKind.JOIN, TableStageKind.FILTER_PROJECT), kindsOf(dataflow.stages()));
        var joinStage = singleStageOfKind(dataflow.stages(), TableStageKind.JOIN);
        var rightEdge = singleEdgeWithRole(joinStage, "Right");
        assertEquals(TableEdgeMode.BROADCAST, rightEdge.mode());
        assertEquals(-1, rightEdge.fromStageId());
        assertTrue(rightEdge.externalInputNames().contains("trades"));

        // "trades" feeds TWO edges: the main FROM ingest AND the scalar subquery's broadcast edge.
        var trainEdges = dataflow.edgesForExternalInput("trades");
        assertEquals(2, trainEdges.size());
        assertTrue(trainEdges.stream().anyMatch(e -> e.mode() == TableEdgeMode.BROADCAST));
        assertTrue(trainEdges.stream().anyMatch(e -> e.mode() != TableEdgeMode.BROADCAST));
    }

    @Test
    void scalarBroadcast_Equivalence() {
        var events = new ArrayList<Ev>(List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true))));
        assertEquivalence(compileTable(SCALAR_SQL, TRADES), events, new int[] {1, 4});
    }

    @Test
    void scalarBroadcast_Deterministic() {
        assertDeterministic(compileTable(SCALAR_SQL, TRADES), List.of(
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true))));
    }

    // ------------------------------------------------------------------------------------------------
    // Unsupported shape: derived table/CTE in FROM position -> M2 explicitly out of scope (Parallelism pinned to 1
    // for such tables; see TableDataflowPlan's class doc).
    // ------------------------------------------------------------------------------------------------

    @Test
    void derivedTableInFromPosition_CreateDataflowThrows() {
        var result = compileTable("SELECT x.symbol FROM (SELECT symbol FROM trades) x", TRADES);
        assertTrue(result.ok());
        assertThrows(UnsupportedOperationException.class, () -> result.plan().createDataflow(4));
    }
}
