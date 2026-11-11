package io.streamsforge.engine.dd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Minimal repros of the engine deviations the differential tests found (see README "Discrepancies"). Each repro runs
 * the SAME input against the engine and against the dataflow compiler; the engine is the side that is wrong.
 * Disabled repros assert the CORRECT answer, so they go green when the engine is fixed.
 */
class EngineIssuesTest {
    static EventRecord trade(long ts, String sym, Double px, Long qty) {
        var e = new EventRecord();
        e.put("_ts", ts);
        e.put("sym", sym);
        e.put("px", px);
        e.put("qty", qty);
        e.put("ok", true);
        e.put("fee", 0.5);
        return e;
    }

    static TableExecutor engine(String sql) {
        var r = SqlCompiler.compileTable(sql, Diff.ALL, Map.of());
        if (!r.ok()) throw new AssertionError(r.diagnostics().toString());
        return r.plan().createExecutor();
    }

    static DataflowTableQuery dd(String sql) { return dd(sql, DataflowTableCompiler.Options.ENGINE_COMPATIBLE); }

    static DataflowTableQuery dd(String sql, DataflowTableCompiler.Options o) {
        var out = DataflowTableCompiler.compile(sql, Diff.ALL, Map.of(), o);
        if (!out.ok()) throw new AssertionError(out.message() + out.diagnostics());
        return out.plan().newQuery();
    }

    static Object only(Map<String, io.streamsforge.engine.RowWeight> snap, String col) {
        assertEquals(1, snap.size(), "expected exactly one output row: " + snap.keySet());
        return snap.values().iterator().next().row().get(col);
    }

    // ------------------------------------------------------------------------------------------------------------
    // 1. GROUP BY is not order-insensitive: a retraction that overtakes its assertion corrupts the group.
    // ------------------------------------------------------------------------------------------------------------

    static final String GROUPED = "SELECT sym, SUM(qty) AS s, MAX(qty) AS mx, COUNT(*) AS c FROM trades GROUP BY sym";

    @Test
    void controlDataflowIsOrderInsensitiveForGroupBy() {
        var q = dd(GROUPED);
        q.update("trades", trade(1, "A", 1.0, 5L), -1);   // retraction of r1 arrives first ...
        q.update("trades", trade(2, "A", 1.0, 7L), 1);    // r2
        q.update("trades", trade(1, "A", 1.0, 5L), 1);    // ... r1's assertion arrives last: net multiset = {r2}
        q.commit();
        assertEquals(7L, only(q.snapshot(), "s"));
        assertEquals(7L, only(q.snapshot(), "mx"));
        assertEquals(1L, only(q.snapshot(), "c"));
    }

    @Test
    @Disabled("bug: TableReduceOp drops a group's state when its total weight crosses exactly 0 even though the aggregators "
        + "still hold a non-empty (+r2, -r1) multiset, so a retraction arriving before its assertion leaves the group with r1's "
        + "values instead of r2's (SUM=5, MAX=5; correct: 7). Z-set aggregation must be order-insensitive.")
    void engineGroupByIsOrderInsensitive() {
        var e = engine(GROUPED);
        e.onTableDelta("trades", new TableDelta(trade(1, "A", 1.0, 5L), -1));
        e.onTableDelta("trades", new TableDelta(trade(2, "A", 1.0, 7L), 1));
        e.onTableDelta("trades", new TableDelta(trade(1, "A", 1.0, 5L), 1));
        assertEquals(7L, only(e.snapshot(), "s"));
        assertEquals(7L, only(e.snapshot(), "mx"));
    }

    /** MIN/MAX in isolation: the group's total weight never reaches 0 here, so the state drop above is not involved. */
    @Test
    @Disabled("bug: MinMaxZAggregator ignores a retraction of a value it has not seen (only `weight > 0` creates an entry), so "
        + "retract(5) before assert(5) leaves 5 in the multiset with net weight 0: MAX stays 5 although the rows are {3, 4}.")
    void engineMaxIgnoresRetractionOfUnseenValue() {
        var e = engine("SELECT sym, MAX(qty) AS mx, COUNT(*) AS c FROM trades GROUP BY sym");
        e.onTableDelta("trades", new TableDelta(trade(0, "B", 1.0, 3L), 1));
        e.onTableDelta("trades", new TableDelta(trade(0, "B", 1.0, 4L), 1));  // two live rows: the group stays alive throughout
        e.onTableDelta("trades", new TableDelta(trade(1, "B", 1.0, 5L), -1)); // retraction of 5 first ...
        e.onTableDelta("trades", new TableDelta(trade(1, "B", 1.0, 5L), 1));  // ... then its assertion: the net rows are {3, 4}
        assertEquals(4L, only(e.snapshot(), "mx"));
    }

    // ------------------------------------------------------------------------------------------------------------
    // 2. SUM over a group whose argument is NULL in every row.
    // ------------------------------------------------------------------------------------------------------------

    static final String SUM_NULLS = "SELECT sym, SUM(qty) AS s, AVG(qty) AS a, MIN(qty) AS mn, COUNT(qty) AS c FROM trades GROUP BY sym";

    @Test
    void engineSumOfNothingIsZeroWhileAvgMinAreNull_pinsCurrentBehaviour() {
        var e = engine(SUM_NULLS);
        e.onTableDelta("trades", new TableDelta(trade(0, "A", 1.0, null), 1));
        assertEquals(0L, only(e.snapshot(), "s"));
        assertNull(only(e.snapshot(), "a"));
        assertNull(only(e.snapshot(), "mn"));
        assertEquals(0L, only(e.snapshot(), "c"));

        // The dataflow compiler matches by default and follows ANSI SQL under Options(sqlStandardEmptySum=true).
        var q = dd(SUM_NULLS);
        q.update("trades", trade(0, "A", 1.0, null), 1);
        q.commit();
        assertEquals(0L, only(q.snapshot(), "s"));
        var std = dd(SUM_NULLS, new DataflowTableCompiler.Options(true));
        std.update("trades", trade(0, "A", 1.0, null), 1);
        std.commit();
        assertNull(only(std.snapshot(), "s"));
    }

    @Test
    @Disabled("bug (ANSI deviation): SUM over a group whose argument is NULL in every row returns 0; ANSI SQL returns NULL, and the "
        + "engine's own AVG/MIN/MAX/VAR_SAMP return NULL for the same group (docs: 'undefined, not zero').")
    void engineSumOfOnlyNullsIsNull() {
        var e = engine(SUM_NULLS);
        e.onTableDelta("trades", new TableDelta(trade(0, "A", 1.0, null), 1));
        assertNull(only(e.snapshot(), "s"));
    }

    // ------------------------------------------------------------------------------------------------------------
    // 3. [NOT] IN (subquery) and NULL.
    // ------------------------------------------------------------------------------------------------------------

    static EventRecord ref(String sym) {
        var e = new EventRecord();
        e.put("_ts", 0L);
        e.put("sym", sym);
        e.put("tag", "x");
        return e;
    }

    static final String NOT_IN = "SELECT sym, qty FROM trades WHERE sym NOT IN (SELECT sym FROM ref)";

    @Test
    void notInWithNullInSubquery_pinsCurrentBehaviour() {
        // engine and dataflow agree: NULLs in the subquery are ignored by NOT IN.
        var e = engine(NOT_IN);
        var q = dd(NOT_IN);
        for (var d : new Object[][] {{"ref", ref(null)}, {"ref", ref("B")}, {"trades", trade(0, "A", 1.0, 1L)}, {"trades", trade(0, "B", 1.0, 1L)}}) {
            e.onTableDelta((String) d[0], new TableDelta((EventRecord) d[1], 1));
            q.update((String) d[0], (EventRecord) d[1], 1);
        }
        q.commit();
        assertEquals(1, e.snapshot().size());
        assertEquals(e.snapshot().keySet(), q.snapshot().keySet());
    }

    @Test
    @Disabled("bug (ANSI deviation, shared by engine and dataflow compiler): `x NOT IN (subquery)` where the subquery yields a NULL "
        + "must be UNKNOWN for every x not matched by a non-NULL element, i.e. return no rows (ANSI SQL). The engine's TableSemiAntiOp "
        + "ignores NULL right keys, so row A is returned.")
    void notInWithNullInSubqueryReturnsNothing() {
        var e = engine(NOT_IN);
        e.onTableDelta("ref", new TableDelta(ref(null), 1));
        e.onTableDelta("ref", new TableDelta(ref("B"), 1));
        e.onTableDelta("trades", new TableDelta(trade(0, "A", 1.0, 1L), 1));
        assertEquals(0, e.snapshot().size());
    }

    @Test
    @Disabled("bug (ANSI deviation, shared by engine and dataflow compiler): `NULL NOT IN (empty subquery)` is TRUE in ANSI SQL so a "
        + "trade with a NULL sym must be returned while ref is empty; TableSemiAntiOp drops every row with a NULL left key.")
    void nullNotInEmptySubqueryIsTrue() {
        var e = engine(NOT_IN);
        e.onTableDelta("trades", new TableDelta(trade(0, null, 1.0, 1L), 1));
        assertEquals(1, e.snapshot().size());
    }
}
