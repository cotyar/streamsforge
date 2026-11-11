package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 wave 2b: table-mode LEFT/RIGHT/FULL JOIN, end to end through the wired TableExecutor
 * (compiled real SQL, public onStreamEvent/onTableDelta/snapshot surface) — TableJoinTests/TableCrossJoinTests
 * style, unlike TableOuterJoinOpUnitTests' direct op-level construction. This is the failure mode the whole
 * wave exists to prevent: a table-mode LEFT/RIGHT/FULL JOIN that compiles and runs but silently behaves like
 * INNER (no null-padding) — every trace below asserts the actual pad/product/retraction deltas, not just a
 * final row count.
 *
 * <p>NOT PORTED (partitioned dataflow is not in the Java engine):
 * {@code LeftJoin_Parallelism1And4_Equivalent_AndActuallyNullPads}.
 */
class TableOuterJoinTests {
    private static final String LEFT_SQL = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";

    private static final SourceSchema[] TRADES_ONLY = {TRADES};
    private static final SourceSchema[] REF_ONLY = {REF};

    // ------------------------------------------------------------------------------------------------
    // LEFT — full lifecycle: pad on first left row; product + pad retraction on first match; a second
    // match producing products with no pad traffic; retracting the last match re-asserting the pad.
    // ------------------------------------------------------------------------------------------------

    @Test
    void leftJoin_FullLifecycleTrace_PadThenProductsThenRepad() {
        var exec = compileTableAndCreate(LEFT_SQL, TRADES_ONLY, REF_ONLY);

        // 1. Left arrives alone: right bucket is empty -> pads, and the pad is what's in the snapshot.
        var step1 = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL"));
        var pad1 = single(step1);
        assertEquals(1L, pad1.weight());
        assertEquals("AAPL", pad1.row().get("symbol"));
        assertNull(pad1.row().get("tag"));
        single(exec.snapshot());

        // 2. First matching right row: the product asserts AND the earlier pad retracts (nets to zero,
        // pruned from the snapshot — only the product row survives).
        var step2 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 1));
        assertEquals(2, step2.size());
        var product1 = single(step2, d -> d.row().get("tag") != null);
        assertEquals(1L, product1.weight());
        assertEquals("watchlist", product1.row().get("tag"));
        var padRetract1 = single(step2, d -> d.row().get("tag") == null);
        assertEquals(-1L, padRetract1.weight());
        var snap2 = exec.snapshot();
        var only2 = single(snap2);
        assertEquals("watchlist", only2.getValue().row().get("tag"));

        // 3. A second right row under the SAME key: products only — the flip is per-key presence, not
        // per-delta, so a key that's already present doesn't re-trigger any pad traffic.
        var step3 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1));
        var product2 = single(step3);
        assertEquals(1L, product2.weight());
        assertEquals("core", product2.row().get("tag"));
        assertEquals(2, exec.snapshot().size());

        // 4. Retract one right row (NOT the last survivor): product retraction only, no pad — one right
        // row (core) still keeps the key present.
        var step4 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -1));
        var retract1 = single(step4);
        assertEquals(-1L, retract1.weight());
        assertEquals("watchlist", retract1.row().get("tag"));
        single(exec.snapshot());

        // 5. Retract the LAST right row: the product retracts AND the pad re-asserts — presence flips
        // back to absent, so every currently-indexed left row under this key is padded again.
        var step5 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1));
        assertEquals(2, step5.size());
        var retract2 = single(step5, d -> d.row().get("tag") != null);
        assertEquals(-1L, retract2.weight());
        var repad = single(step5, d -> d.row().get("tag") == null);
        assertEquals(1L, repad.weight());
        var fin = single(exec.snapshot());
        assertEquals("AAPL", fin.getValue().row().get("symbol"));
        assertNull(fin.getValue().row().get("tag"));
    }

    // ------------------------------------------------------------------------------------------------
    // RIGHT mirrors LEFT: the lone (unmatched) RIGHT row pads itself; a matching LEFT arrival retracts it.
    // ------------------------------------------------------------------------------------------------

    @Test
    void rightJoin_LoneRightRowPads_ThenMatchingLeftRetractsThePad() {
        var exec = compileTableAndCreate(
            "SELECT t.symbol, r.tag FROM trades t RIGHT JOIN ref r ON t.symbol = r.symbol", TRADES_ONLY, REF_ONLY);

        var step1 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 1));
        var pad = single(step1);
        assertEquals(1L, pad.weight());
        assertNull(pad.row().get("symbol"));
        assertEquals("watchlist", pad.row().get("tag"));
        single(exec.snapshot());

        var step2 = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL"));
        assertEquals(2, step2.size());
        var product = single(step2, d -> d.row().get("symbol") != null);
        assertEquals(1L, product.weight());
        var padRetract = single(step2, d -> d.row().get("symbol") == null);
        assertEquals(-1L, padRetract.weight());

        var fin = single(exec.snapshot());
        assertEquals("AAPL", fin.getValue().row().get("symbol"));
        assertEquals("watchlist", fin.getValue().row().get("tag"));
    }

    // ------------------------------------------------------------------------------------------------
    // FULL: both sides pad independently for unmatched keys; never emits an all-NULL (null, null) row.
    // ------------------------------------------------------------------------------------------------

    @Test
    void fullJoin_PadsBothSides_NeverEmitsNullNullRow() {
        var exec = compileTableAndCreate(
            "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol", TRADES_ONLY, REF_ONLY);

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL")); // unmatched left -> pads itself
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "other"), 1)); // unmatched right, different key -> pads itself

        var snapBefore = exec.snapshot();
        assertEquals(2, snapBefore.size());
        for (var v : snapBefore.values()) assertFalse(v.row().get("symbol") == null && v.row().get("tag") == null);

        var step3 = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 1)); // matches the AAPL left row
        assertEquals(2, step3.size()); // product + the AAPL left-pad's retraction

        var snapAfter = exec.snapshot();
        assertEquals(2, snapAfter.size()); // AAPL product (was left-only-pad) + MSFT pad (untouched)
        assertTrue(snapAfter.values().stream().anyMatch(v -> Objects.equals(v.row().get("symbol"), "AAPL") && Objects.equals(v.row().get("tag"), "watchlist")));
        assertTrue(snapAfter.values().stream().anyMatch(v -> v.row().get("symbol") == null && Objects.equals(v.row().get("tag"), "other")));
    }

    // ------------------------------------------------------------------------------------------------
    // Chained joins: LEFT then INNER eliminates the null-keyed pad (INNER's ordinary NULL-never-matches
    // rule applies to the pad's own NULL column); LEFT then LEFT propagates it instead, padding again.
    // ------------------------------------------------------------------------------------------------

    @Test
    void chainedLeftThenInner_NullKeyedPadIsEliminated() {
        var exec = compileTableAndCreate(
            "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON r.tag = q.symbol",
            new SourceSchema[] {TRADES, QUOTES}, REF_ONLY);

        // trades AAPL has no matching ref row -> the LEFT join pads (r.tag = NULL). That pad flows into
        // the SECOND join's Left input keyed on r.tag — an ordinary INNER equi-join, and r.tag is NULL,
        // so it is silently dropped (never indexed, never emitted) by TableJoinOp's usual NULL rule —
        // exactly like any other NULL-keyed row hitting an INNER join, pad or not.
        var step = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL"));
        assertTrue(step.isEmpty());
        assertTrue(exec.snapshot().isEmpty());

        // ref now matches AAPL -> the pad retracts (itself dropped, so nothing to retract) and the
        // product (r.tag = "TAG1") flows into the second join, indexed but still unmatched (no quotes yet).
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "TAG1"), 1));
        assertTrue(exec.snapshot().isEmpty());

        // quotes(TAG1) arrives -> the second join's INNER match finally fires.
        exec.onStreamEvent("quotes", evt(2000, "quotes", "symbol", "TAG1", "bid", 99.5, "ask", 100.5));
        var fin = single(exec.snapshot());
        assertEquals("AAPL", fin.getValue().row().get("symbol"));
        assertEquals("TAG1", fin.getValue().row().get("tag"));
        assertEquals(99.5, fin.getValue().row().get("bid"));
    }

    @Test
    void chainedLeftThenLeft_NullKeyedPadIsPaddedAgain_NotEliminated() {
        var exec = compileTableAndCreate(
            "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol LEFT JOIN quotes q ON r.tag = q.symbol",
            new SourceSchema[] {TRADES, QUOTES}, REF_ONLY);

        // Same unmatched trades row as above, but now the SECOND join is LEFT too: r.tag is NULL, so its
        // own key evaluates to NULL — T3's rule (a LEFT join pads its own NULL-keyed rows immediately,
        // never dropping them) fires again, producing a fully-padded (symbol, NULL, NULL) row instead of
        // silently vanishing.
        var step = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL"));
        var row = single(step);
        assertEquals(1L, row.weight());
        assertEquals("AAPL", row.row().get("symbol"));
        assertNull(row.row().get("tag"));
        assertNull(row.row().get("bid"));

        var only = single(exec.snapshot());
        assertEquals("AAPL", only.getValue().row().get("symbol"));
        assertNull(only.getValue().row().get("tag"));
        assertNull(only.getValue().row().get("bid"));
    }

    // ------------------------------------------------------------------------------------------------
    // Pads flowing into GROUP BY: unmatched rows (NULL on the padded column) group together under a
    // NULL key exactly like any other NULL GROUP BY value — no special-casing needed downstream.
    // ------------------------------------------------------------------------------------------------

    @Test
    void padsFlowIntoGroupBy_UnmatchedRowsGroupUnderNullTag() {
        var exec = compileTableAndCreate(
            "SELECT r.tag, COUNT(*) AS cnt FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol GROUP BY r.tag",
            TRADES_ONLY, REF_ONLY);

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL"));
        exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "MSFT"));

        var snap1 = exec.snapshot();
        var nullGroup1 = single(snap1);
        assertNull(nullGroup1.getValue().row().get("tag"));
        assertEquals(2L, nullGroup1.getValue().row().get("cnt"));

        // AAPL now matches ref -> its pad leaves the NULL group (cnt 2 -> 1) and a new "core" group appears.
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1));

        var snap2 = exec.snapshot();
        assertEquals(2, snap2.size());
        var nullGroup2 = single(snap2.values(), v -> v.row().get("tag") == null);
        assertEquals(1L, nullGroup2.row().get("cnt"));
        var coreGroup = single(snap2.values(), v -> Objects.equals(v.row().get("tag"), "core"));
        assertEquals(1L, coreGroup.row().get("cnt"));
    }

    private record Ev(String origin, EventRecord row, long weight, boolean isTable) {}

    private static String canon(java.util.Map<String, RowWeight> snap) {
        var sb = new StringBuilder();
        new java.util.TreeMap<>(snap).forEach((k, v) -> { if (sb.length() > 0) sb.append("\n"); sb.append(k).append('=').append(v.weight()); });
        return sb.toString();
    }

    @Test
    void leftJoin_Parallelism1And4_Equivalent_AndActuallyNullPads() {
        var result = compileTable(LEFT_SQL, TRADES_ONLY, REF_ONLY);
        assertTrue(result.ok(), String.valueOf(result.diagnostics()));
        var plan = result.plan();

        var events = java.util.List.of(
            new Ev("trades", evt(1, "trades", "symbol", "AAPL"), 1, false),
            new Ev("trades", evt(2, "trades", "symbol", "MSFT"), 1, false), // never matches - must stay padded, not vanish
            new Ev("ref", evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 1, true),
            new Ev("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1, true),
            new Ev("ref", evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -1, true));

        var classic = plan.createExecutor();
        for (var e : events) {
            if (e.isTable()) classic.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            else classic.onStreamEvent(e.origin(), e.row());
        }
        var classicCanon = canon(classic.snapshot());
        assertFalse(classic.snapshot().isEmpty());

        // The silent-INNER check: MSFT never matched anything; a genuine LEFT join keeps it, NULL-padded.
        assertTrue(classic.snapshot().values().stream().anyMatch(v -> "MSFT".equals(v.row().get("symbol")) && v.row().get("tag") == null));

        for (int p : new int[] {1, 4}) {
            var dataflow = plan.createDataflow(p);
            var harness = new PartitionedTableHarness(dataflow);
            for (var e : events) harness.admit(e.origin(), e.row(), e.weight());

            assertEquals(classicCanon, canon(harness.snapshot()));
            assertTrue(harness.snapshot().values().stream().anyMatch(v -> "MSFT".equals(v.row().get("symbol")) && v.row().get("tag") == null));
        }
    }
}
