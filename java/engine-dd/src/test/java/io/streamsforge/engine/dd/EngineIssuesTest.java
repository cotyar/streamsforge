package io.streamsforge.engine.dd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Minimal repros of the engine deviations the differential tests found (see README "Discrepancies"). Each repro runs
 * the SAME input against the engine and against the dataflow compiler; the engine is the side that is wrong.
 * Repros assert the CORRECT answer (PostgreSQL's); all three deviations found so far are fixed (2026-11-11).
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
    // Fixed 2026-11-11 (was @Disabled) — see engine TableReduceOrderInsensitivityTests.
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
    // Fixed 2026-11-11 (was @Disabled) — see engine TableReduceOrderInsensitivityTests.
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
    // Fixed 2026-11-11: SUM follows PostgreSQL — NULL over a group with no non-NULL value. Was a pin of SUM = 0.
    void engineAndDataflowAgree_sumOfNothingIsNullLikeAvgMin() {
        var e = engine(SUM_NULLS);
        e.onTableDelta("trades", new TableDelta(trade(0, "A", 1.0, null), 1));
        assertNull(only(e.snapshot(), "s"));
        assertNull(only(e.snapshot(), "a"));
        assertNull(only(e.snapshot(), "mn"));
        assertEquals(0L, only(e.snapshot(), "c"));

        var q = dd(SUM_NULLS);
        q.update("trades", trade(0, "A", 1.0, null), 1);
        q.commit();
        assertNull(only(q.snapshot(), "s"));
        var legacy = dd(SUM_NULLS, DataflowTableCompiler.Options.LEGACY_ZERO_SUM);
        legacy.update("trades", trade(0, "A", 1.0, null), 1);
        legacy.commit();
        assertEquals(0L, only(legacy.snapshot(), "s"));
    }

    @Test
    // Fixed 2026-11-11 (was @Disabled).
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
    // Fixed 2026-11-11: PostgreSQL three-valued NOT IN in both. Was a pin of "NULLs in the subquery are ignored".
    void engineAndDataflowAgree_notInWithNullInSubquery() {
        var e = engine(NOT_IN);
        var q = dd(NOT_IN);
        for (var d : new Object[][] {{"ref", ref(null)}, {"ref", ref("B")}, {"trades", trade(0, "A", 1.0, 1L)}, {"trades", trade(0, "B", 1.0, 1L)},
                                     {"trades", trade(0, null, 1.0, 1L)}}) {
            e.onTableDelta((String) d[0], new TableDelta((EventRecord) d[1], 1));
            q.update((String) d[0], (EventRecord) d[1], 1);
        }
        q.commit();
        assertEquals(0, e.snapshot().size());
        assertEquals(e.snapshot().keySet(), q.snapshot().keySet());

        // Retracting the NULL brings A back (B still matches; the NULL-sym trade stays out: S is non-empty).
        e.onTableDelta("ref", new TableDelta(ref(null), -1));
        q.update("ref", ref(null), -1);
        q.commit();
        assertEquals(1, e.snapshot().size());
        assertEquals(e.snapshot().keySet(), q.snapshot().keySet());
    }

    @Test
    // Fixed 2026-11-11 (was @Disabled): `x NOT IN (S)` with a NULL in S is NULL for every unmatched x.
    void notInWithNullInSubqueryReturnsNothing() {
        var e = engine(NOT_IN);
        e.onTableDelta("ref", new TableDelta(ref(null), 1));
        e.onTableDelta("ref", new TableDelta(ref("B"), 1));
        e.onTableDelta("trades", new TableDelta(trade(0, "A", 1.0, 1L), 1));
        assertEquals(0, e.snapshot().size());
    }

    @Test
    // Fixed 2026-11-11 (was @Disabled): `NULL NOT IN (empty)` is TRUE.
    void nullNotInEmptySubqueryIsTrue() {
        var e = engine(NOT_IN);
        e.onTableDelta("trades", new TableDelta(trade(0, null, 1.0, 1L), 1));
        assertEquals(1, e.snapshot().size());

        var q = dd(NOT_IN);
        q.update("trades", trade(0, null, 1.0, 1L), 1);
        q.commit();
        assertEquals(e.snapshot().keySet(), q.snapshot().keySet());
    }
}
