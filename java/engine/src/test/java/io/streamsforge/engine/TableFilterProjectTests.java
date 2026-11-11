package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Filter/project (no GROUP BY, no JOIN): weight passes through unchanged; WHERE simply drops
 *  rows that don't match (no retraction machinery needed — nothing was asserted for a filtered-out row). */
class TableFilterProjectTests {
    @Test
    void rowsPassingWhereAreEmittedWithWeightUnchanged() {
        var exec = compileTableAndCreate("SELECT symbol, price FROM trades WHERE price > 50", TRADES);

        var deltas = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));

        var delta = single(deltas);
        assertEquals(1L, delta.weight());
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals(100.0, delta.row().get("price"));
    }

    @Test
    void rowsFailingWhereAreDropped() {
        var exec = compileTableAndCreate("SELECT symbol, price FROM trades WHERE price > 50", TRADES);

        var deltas = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 10.0, "qty", 10L, "active", true));

        assertTrue(deltas.isEmpty());
    }

    @Test
    void projectionPassesUpstreamTableWeightThroughUnchanged() {
        var exec = compileTableAndCreate("SELECT symbol, tag FROM ref", new SourceSchema[0], new SourceSchema[] {REF});

        var deltas = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 3));

        var delta = single(deltas);
        assertEquals(3L, delta.weight()); // filter/project: weight passthrough, no multiplication
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals("core", delta.row().get("tag"));
    }

    @Test
    void retractionThroughProjectionAlsoPassesThroughUnchanged() {
        var exec = compileTableAndCreate("SELECT symbol, tag FROM ref", new SourceSchema[0], new SourceSchema[] {REF});

        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 3));
        var retracted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -3));

        var delta = single(retracted);
        assertEquals(-3L, delta.weight());
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void qualifiedStarExpandsAllColumnsOfThatAliasUnprefixedWithSingleSource() {
        // Single-source table (no joins): mirrors bare SELECT *'s "no prefix" rule.
        var exec = compileTableAndCreate("SELECT t.* FROM trades t", TRADES);

        var deltas = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));

        var delta = single(deltas);
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals(100.0, delta.row().get("price"));
        assertEquals(10L, delta.row().get("qty"));
        assertEquals(true, delta.row().get("active"));
    }

    @Test
    void qualifiedStarWithJoinPrefixesAndCombinesWithExtraColumn() {
        var sql = "SELECT t.*, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var deltas = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1));

        var delta = single(deltas);
        assertEquals("AAPL", delta.row().get("t_symbol"));
        assertEquals(100.0, delta.row().get("t_price"));
        // r.tag keeps its ordinary (unqualified) default column name, same as ValidatorTests'
        // QualifiedStarWithJoinExpandsAllTradeColumnsPrefixedPlusExtraColumn — only the star-expanded t.*
        // columns are alias-prefixed.
        assertEquals("core", delta.row().get("tag"));
    }
}
