package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.ops.TableFilterProjectOp;
import io.streamsforge.engine.runtime.ops.TableIngestOp;
import io.streamsforge.engine.runtime.ops.TableJoinOp;
import io.streamsforge.engine.runtime.ops.TableReduceOp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Per-op unit tests for the table-mode operator chain (plan 003 M1 acceptance: "new per-op unit tests").
 * Each op is instantiated directly — NOT through TableExecutor's façade — and driven with explicit
 * state-in/deltas-in/deltas-out assertions, covering "join retraction cascades and reduce retract+assert
 * pairs" at the op level specifically (these tests assert on each op's own exposed state — ZSetIndex
 * contents, TableReduceOp.groups — which the façade-level tests can't reach).
 */
class TableOpsUnitTests {
    private static final Epoch E0 = new Epoch(0);
    private static final Epoch E1 = new Epoch(1);

    // ------------------------------------------------------------------
    // TableIngestOp — stateless: alias-tag + weight passthrough.
    // ------------------------------------------------------------------

    @Test
    void ingestOp_TagsRowWithAliasAndPassesWeightThrough() {
        var op = new TableIngestOp("t");
        var evt = evt(1000, "trades", "symbol", "AAPL");

        var outp = op.onBatch(E0, List.of(new TableDelta(evt, 3)));

        var d = single(outp);
        assertEquals(3L, d.weight());
        assertEquals("AAPL", d.row().fields().get("t_symbol"));
        assertEquals(1000L, d.row().ts());
    }

    @Test
    void ingestOp_BatchOfMultipleDeltasPreservesOrderAndEachWeight() {
        var op = new TableIngestOp("r");
        var batch = List.of(
            new TableDelta(evt(0, "ref", "symbol", "AAPL"), 2),
            new TableDelta(evt(0, "ref", "symbol", "MSFT"), -1));

        var outp = op.onBatch(E0, batch);

        assertEquals(2, outp.size());
        assertEquals("AAPL", outp.get(0).row().fields().get("r_symbol"));
        assertEquals(2L, outp.get(0).weight());
        assertEquals("MSFT", outp.get(1).row().fields().get("r_symbol"));
        assertEquals(-1L, outp.get(1).weight());
    }

    @Test
    void ingestOp_OnFrontierIsADocumentedPassThrough() {
        var op = new TableIngestOp("t");
        assertTrue(op.onFrontier(E0).isEmpty());
        assertTrue(op.onFrontier(new Epoch(999)).isEmpty());
    }

    // ------------------------------------------------------------------
    // TableJoinOp — bilinear equi-join; state = both sides' ZSetIndex.
    // ------------------------------------------------------------------

    private static TableJoinOp createJoinOp() {
        var compiled = compileTable("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan().compiled();
        var j = compiled.joins.get(0);
        return new TableJoinOp(j.leftKey, j.rightKey, j.residual, compiled.bindings);
    }

    @Test
    void joinOp_RightArrivalMatchesAlreadyIndexedLeftSide_StateHoldsBothRows() {
        var join = createJoinOp();
        var left = new TableIngestOp("t");
        var right = new TableIngestOp("r");

        var leftDelta = left.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1)));
        assertTrue(join.onLeftBatch(E0, leftDelta).isEmpty()); // nothing on the right side to match yet

        var rightDelta = right.onBatch(E0, List.of(new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2)));
        var matched = join.onRightBatch(E0, rightDelta);

        var d = single(matched);
        assertEquals(2L, d.weight()); // weight(left)=1 * weight(right)=2
        assertEquals("AAPL", d.row().fields().get("t_symbol"));
        assertEquals("watchlist", d.row().fields().get("r_tag"));

        // State: both sides now hold exactly the one row that was indexed on each.
        var key = TableKeyEncoding.encodeScalar("AAPL");
        single(join.left.lookup(key));
        single(join.right.lookup(key));
    }

    @Test
    void joinOp_RetractionCascadesANegativeWeightDelta_AndPrunesTheIndexEntry() {
        var join = createJoinOp();
        var left = new TableIngestOp("t");
        var right = new TableIngestOp("r");

        join.onLeftBatch(E0, left.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1))));

        var rightAssert = right.onBatch(E0, List.of(new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2)));
        single(join.onRightBatch(E0, rightAssert));

        var rightRetract = right.onBatch(E1, List.of(new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -2)));
        var cascaded = join.onRightBatch(E1, rightRetract);

        var d = single(cascaded);
        assertEquals(-2L, d.weight());
        assertEquals("AAPL", d.row().fields().get("t_symbol"));
        assertEquals("watchlist", d.row().fields().get("r_tag"));

        // State: the retraction netted the right index's entry to zero weight — ZSetIndex prunes it.
        var key = TableKeyEncoding.encodeScalar("AAPL");
        assertTrue(join.right.lookup(key).isEmpty());
        // Left side is untouched by a right-side retraction.
        single(join.left.lookup(key));
    }

    @Test
    void joinOp_NullKeyContributesNothingAndIsNotIndexed() {
        var join = createJoinOp();
        var left = new TableIngestOp("t");

        var leftDelta = left.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", null), 1)));
        assertTrue(join.onLeftBatch(E0, leftDelta).isEmpty());
        assertTrue(join.left.lookup(TableKeyEncoding.encodeScalar(null)).isEmpty());
    }

    @Test
    void joinOp_OnFrontierIsADocumentedPassThrough() {
        assertTrue(createJoinOp().onFrontier(E0).isEmpty());
    }

    // ------------------------------------------------------------------
    // TableFilterProjectOp — stateless WHERE (+terminal projection).
    // ------------------------------------------------------------------

    @Test
    void filterProjectOp_OnBatchTerminal_ProjectsPassingRowsAndDropsFailing() {
        var compiled = compileTable("SELECT symbol, price FROM trades WHERE price > 50", TRADES).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var passing = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL", "price", 100.0), 1)));
        var outPassing = op.onBatchTerminal(E0, passing);
        var d = single(outPassing);
        assertEquals(1L, d.weight());
        assertEquals("AAPL", d.row().get("symbol"));
        assertEquals(100.0, d.row().get("price"));

        var failing = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "MSFT", "price", 10.0), 1)));
        assertTrue(op.onBatchTerminal(E0, failing).isEmpty());
    }

    @Test
    void filterProjectOp_OnBatch_FiltersButLeavesWorkingRowShapeForDownstreamReduce() {
        var compiled = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades WHERE price > 50 GROUP BY symbol", TRADES).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var passing = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL", "price", 100.0), 1)));
        var filtered = op.onBatch(E0, passing);

        var rd = single(filtered);
        assertEquals(1L, rd.weight());
        assertEquals("AAPL", rd.row().fields().get(alias + "_symbol"));

        var failing = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "MSFT", "price", 1.0), 1)));
        assertTrue(op.onBatch(E0, failing).isEmpty());
    }

    @Test
    void filterProjectOp_OnFrontierIsADocumentedPassThrough() {
        var compiled = compileTable("SELECT symbol FROM trades", TRADES).plan().compiled();
        assertTrue(new TableFilterProjectOp(compiled).onFrontier(E0).isEmpty());
    }

    // ------------------------------------------------------------------
    // TableReduceOp — running GROUP BY aggregates; state = per-group aggregator dictionary.
    // ------------------------------------------------------------------

    @Test
    void reduceOp_FirstContributionEmitsOnlyAnAssertion_AndCreatesGroupState() {
        var compiled = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES).plan().compiled();
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.sources.get(0).alias);

        var admitted = ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1)));
        var outp = op.onBatch(E0, admitted);

        var d = single(outp);
        assertEquals(1L, d.weight());
        assertEquals(1L, d.row().get("cnt"));

        assertEquals(1, op.groups.size());
        var key = TableKeyEncoding.encodeGroupKey(new Object[] {"AAPL"});
        assertEquals(1L, op.groups.get(key).totalWeight);
    }

    @Test
    void reduceOp_SecondContributionEmitsRetractThenAssertPair_AndUpdatesGroupState() {
        var compiled = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES).plan().compiled();
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.sources.get(0).alias);

        op.onBatch(E0, ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1))));
        var second = op.onBatch(E1, ingest.onBatch(E1, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1))));

        assertEquals(2, second.size());
        assertEquals(-1L, second.get(0).weight());
        assertEquals(1L, second.get(0).row().get("cnt"));
        assertEquals(1L, second.get(1).weight());
        assertEquals(2L, second.get(1).row().get("cnt"));

        var key = TableKeyEncoding.encodeGroupKey(new Object[] {"AAPL"});
        assertEquals(2L, op.groups.get(key).totalWeight);
    }

    @Test
    void reduceOp_GroupVanishesWhenWeightReachesZero_AndIsRemovedFromState() {
        var compiled = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES).plan().compiled();
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.sources.get(0).alias);

        op.onBatch(E0, ingest.onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), 1))));
        var retracted = op.onBatch(E1, ingest.onBatch(E1, List.of(new TableDelta(evt(1000, "trades", "symbol", "AAPL"), -1))));

        var d = single(retracted);
        assertEquals(-1L, d.weight());
        assertTrue(op.groups.isEmpty()); // group dropped from state entirely
    }

    @Test
    void reduceOp_OnFrontierIsADocumentedPassThrough() {
        var compiled = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES).plan().compiled();
        assertTrue(new TableReduceOp(compiled).onFrontier(E0).isEmpty());
    }
}
