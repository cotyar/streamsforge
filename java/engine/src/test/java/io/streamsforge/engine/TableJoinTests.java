package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Relational delta-join (table mode): both sides fully indexed, weight multiplication on match,
 *  and retraction propagation through the join when an upstream (table-sourced) side is retracted.
 *  Output columns are unqualified (SELECT t.symbol -> "symbol"), unlike the alias-prefixed WorkingRow
 *  field keys used internally during the join itself. */
class TableJoinTests {
    private static final String SQL = "SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol";

    private static TableExecutor create() {
        return compileTableAndCreate(SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
    }

    @Test
    void leftArrivalMatchesAlreadyIndexedRightSide() {
        // ref (a table input) arrives first with nothing on the trades side yet — no match, no output.
        // trades (the FROM/left side, a stream) then arrives and matches against ref's populated index.
        var exec = create();

        var noMatch = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2));
        assertTrue(noMatch.isEmpty());

        var matched = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var delta = single(matched);
        assertEquals(2L, delta.weight()); // weight(trades)=1 * weight(ref)=2
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals("watchlist", delta.row().get("tag"));
    }

    @Test
    void rightArrivalMatchesAlreadyIndexedLeftSide() {
        var exec = create();

        var noMatch = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 5L, "active", true));
        assertTrue(noMatch.isEmpty());

        var matched = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "core"), 5));
        var delta = single(matched);
        assertEquals(5L, delta.weight()); // weight(ref)=5 * weight(trades)=1
        assertEquals("MSFT", delta.row().get("symbol"));
        assertEquals("core", delta.row().get("tag"));
    }

    @Test
    void retractingAMatchedRowPropagatesANegativeWeightDelta() {
        var exec = create();

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var matched = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 2));
        single(matched);

        var retracted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -2));
        var delta = single(retracted);
        assertEquals(-2L, delta.weight());
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals("watchlist", delta.row().get("tag"));

        // Consolidated: the earlier +2 and this -2 net to zero — the row is gone from the snapshot.
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void fanOutMultipleMatchesEachEmitTheirOwnWeightedDelta() {
        var exec = create();

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));

        var first = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "a"), 1));
        var second = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "b"), 1));

        single(first);
        single(second);
        assertEquals(2, exec.snapshot().size()); // two distinct combined rows (different tag), both weight 1
    }

    @Test
    void nonMatchingKeyProducesNoOutput() {
        var exec = create();

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var noMatch = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "core"), 1));

        assertTrue(noMatch.isEmpty());
    }
}
