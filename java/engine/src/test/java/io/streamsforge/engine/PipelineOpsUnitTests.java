package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ops.PipelineFilterProjectOp;
import io.streamsforge.engine.runtime.ops.PipelineJoinOp;
import io.streamsforge.engine.runtime.ops.PipelineWindowOp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Per-op unit tests for the pipeline-mode (streaming) operator chain (plan 003 M1 Part B / acceptance:
 * "new per-op unit tests"). Each op is instantiated directly — not through PipelineExecutor's façade —
 * with explicit state-in/rows-in/rows-out assertions, mirroring TableOpsUnitTests' structure for table mode.
 */
class PipelineOpsUnitTests {
    // ------------------------------------------------------------------
    // PipelineJoinOp — buffered interval join; state = per-side buffered entries.
    // ------------------------------------------------------------------

    private static PipelineJoinOp createInnerJoinOp() {
        var compiled = compile("SELECT t.symbol, q.bid FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES).plan().compiled();
        var j = compiled.joins.get(0);
        return new PipelineJoinOp(j.kind, j.within, j.leftKey, j.rightKey, j.residual, compiled.bindings,
            List.of(new WorkingRow.Side(compiled.sources.get(0).alias, compiled.sources.get(0).schema)),
            new WorkingRow.Side(j.alias, j.schema));
    }

    @Test
    void joinOp_MatchWithinWindowEmitsCombinedRow_AndBuffersBothSides() {
        var join = createInnerJoinOp();

        var leftRow = WorkingRow.fromEvent("t", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        assertTrue(join.onLeft(leftRow).isEmpty());

        var rightRow = WorkingRow.fromEvent("q", evt(1200, "quotes", "symbol", "AAPL", "bid", 99.5, "ask", 100.5));
        var matched = join.onRight(rightRow);

        var combined = single(matched);
        assertEquals("AAPL", combined.fields().get("t_symbol"));
        assertEquals(99.5, combined.fields().get("q_bid"));

        assertEquals(1, join.left.size());
        assertEquals(1, join.right.size());
        assertTrue(join.left.get(0).matched);
        assertTrue(join.right.get(0).matched);
    }

    @Test
    void joinOp_EvictBeyondWithinRemovesUnmatchedEntriesFromState() {
        var join = createInnerJoinOp();
        join.onLeft(WorkingRow.fromEvent("t", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true)));

        var evicted = join.evict(7000); // 1000 + 5000(within) = 6000 < 7000
        assertTrue(evicted.isEmpty()); // INNER join: no null-padded row on an unmatched eviction
        assertTrue(join.left.isEmpty()); // but the buffered entry IS gone (state pruned)
    }

    // ------------------------------------------------------------------
    // PipelineFilterProjectOp — stateless WHERE (+terminal projection).
    // ------------------------------------------------------------------

    @Test
    void filterProjectOp_OnBatchTerminal_ProjectsPassingRowsAndDropsFailing() {
        var compiled = compile("SELECT symbol, price FROM trades WHERE price > 50", TRADES).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new PipelineFilterProjectOp(compiled);

        var passing = WorkingRow.fromEvent(alias, evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var outp = op.onBatchTerminal(List.of(passing));
        var row = single(outp);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(100.0, row.get("price"));

        var failing = WorkingRow.fromEvent(alias, evt(1000, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L, "active", true));
        assertTrue(op.onBatchTerminal(List.of(failing)).isEmpty());
    }

    @Test
    void filterProjectOp_OnBatch_FiltersButLeavesWorkingRowForDownstreamWindow() {
        var compiled = compile(
            "SELECT symbol, COUNT(*) AS cnt FROM trades WHERE price > 50 GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)",
            TRADES).plan().compiled();
        var alias = compiled.sources.get(0).alias;
        var op = new PipelineFilterProjectOp(compiled);

        var passing = WorkingRow.fromEvent(alias, evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var filtered = op.onBatch(List.of(passing));
        var row = single(filtered);
        assertEquals("AAPL", row.fields().get(alias + "_symbol"));
    }

    // ------------------------------------------------------------------
    // PipelineWindowOp — tumbling/hopping/session aggregate state; covered end-to-end by
    // ExecutorWindowAndAggregateTests via the façade. This adds a direct-instantiation check that the
    // op's own State dictionaries reflect what's documented (state in/rows in/rows out).
    // ------------------------------------------------------------------

    @Test
    void windowOp_TumblingWindow_TracksOneStateEntryPerWindowGroup() {
        var compiled = compile(
            "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            TRADES).plan().compiled();
        var window = new PipelineWindowOp(compiled);
        var alias = compiled.sources.get(0).alias;

        window.onRow(WorkingRow.fromEvent(alias, evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true)));
        window.onRow(WorkingRow.fromEvent(alias, evt(1500, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L, "active", true)));

        assertEquals(2, window.states.size()); // one window-state entry per distinct group in [0,10000)

        var closed = window.evict(12000);
        assertEquals(2, closed.size());
        assertTrue(window.states.isEmpty()); // closed windows are removed from state
    }
}
