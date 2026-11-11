package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Plan 008 wave 2b: composite (multi-conjunct) equi-keys for table-mode OUTER joins — {@code ON a.x=b.x AND
 * a.y=b.y} must become a genuine two-component key (TableOuterJoinOp's leftKeys/rightKeys, both
 * components indexed together), not the pre-008 "first conjunct is the key, every other conjunct folds
 * into a residual" shape. That folded shape is still exactly what non-outer joins get; this file
 * is specifically about the NEW composite-aware path Left/Right/Full take instead.
 */
class TableOuterJoinCompositeKeyTests {
    private static final SourceSchema ORDERS = schema("orders", "sym", FieldKind.STRING, "venue", FieldKind.STRING);
    private static final SourceSchema FILLS = schema("fills", "sym", FieldKind.STRING, "venue", FieldKind.STRING, "qty", FieldKind.LONG);

    private static final String COMPOSITE_SQL =
        "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";

    private static final SourceSchema[] S = {ORDERS};
    private static final SourceSchema[] T = {FILLS};

    @Test
    void bothComponentsBecomeTheKey_NotOneKeyPlusAResidual() {
        var compiled = compileTable(COMPOSITE_SQL, S, T).plan().compiled();
        var j = compiled.joins.get(0);

        assertEquals(2, j.leftKeys.size());
        assertEquals(2, j.rightKeys.size());
        // Left/Right/Full: residual is the PURE (non-equi-only) residual — null here, since BOTH ON
        // conjuncts are equi-comparisons and neither is left over. Pre-008 (and still today for a
        // non-outer join compiling this same shape), the second conjunct would instead have folded into
        // residual as "o.venue = f.venue", with leftKey/rightKey holding only the first component — the
        // exact shape this composite path deliberately avoids for Left/Right/Full.
        assertNull(j.residual);
    }

    @Test
    void rowDifferingInSecondComponent_DoesNotMatch_Pads() {
        var exec = compileTableAndCreate(COMPOSITE_SQL, S, T);

        // Same "sym", different "venue" — a single-key join on sym alone would wrongly match these.
        exec.onTableDelta("fills", new TableDelta(evt(0, "fills", "sym", "AAPL", "venue", "NYSE", "qty", 10L), 1));
        var step = exec.onStreamEvent("orders", evt(1000, "orders", "sym", "AAPL", "venue", "NASDAQ"));

        var pad = single(step);
        assertEquals(1L, pad.weight());
        assertEquals("AAPL", pad.row().get("sym"));
        assertEquals("NASDAQ", pad.row().get("venue"));
        assertNull(pad.row().get("qty"));

        var only = single(exec.snapshot());
        assertNull(only.getValue().row().get("qty"));
    }

    @Test
    void rowMatchingBothComponents_Products_NoPad() {
        var exec = compileTableAndCreate(COMPOSITE_SQL, S, T);

        exec.onTableDelta("fills", new TableDelta(evt(0, "fills", "sym", "AAPL", "venue", "NASDAQ", "qty", 10L), 1));
        var step = exec.onStreamEvent("orders", evt(1000, "orders", "sym", "AAPL", "venue", "NASDAQ"));

        var product = single(step);
        assertEquals(1L, product.weight());
        assertEquals(10L, product.row().get("qty"));

        var only = single(exec.snapshot());
        assertEquals(10L, only.getValue().row().get("qty"));
    }

    @Test
    void genuinelyNonEquiResidual_StillAppliesAlongsideTheCompositeKey() {
        final String sql =
            "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue AND f.qty > 0";

        var compiled = compileTable(sql, S, T).plan().compiled();
        var j = compiled.joins.get(0);
        assertEquals(2, j.leftKeys.size()); // still just the two equi-conjuncts
        assertNotNull(j.residual); // "f.qty > 0" is genuinely non-equi -> stays a residual, not a key

        var exec = compileTableAndCreate(sql, S, T);

        // Both key components match, but the residual ("qty > 0") fails -> still pads, not a product.
        exec.onTableDelta("fills", new TableDelta(evt(0, "fills", "sym", "AAPL", "venue", "NASDAQ", "qty", 0L), 1));
        var step1 = exec.onStreamEvent("orders", evt(1000, "orders", "sym", "AAPL", "venue", "NASDAQ"));
        var pad = single(step1);
        assertNull(pad.row().get("qty"));

        // A second fills row for the SAME composite key that DOES pass the residual -> product + unpad.
        var step2 = exec.onTableDelta("fills", new TableDelta(evt(0, "fills", "sym", "AAPL", "venue", "NASDAQ", "qty", 5L), 1));
        assertEquals(2, step2.size());
        var product = single(step2, d -> d.row().get("qty") != null);
        assertEquals(5L, product.row().get("qty"));
        var unpad = single(step2, d -> d.row().get("qty") == null);
        assertEquals(-1L, unpad.weight());
    }
}
