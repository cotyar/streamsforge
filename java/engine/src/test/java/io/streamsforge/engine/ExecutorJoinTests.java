package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class ExecutorJoinTests {
    private static final String JOIN_SELECT = "SELECT t.symbol AS symbol, t.price AS price, q.bid AS bid FROM trades t";

    @Test
    void innerJoinEmitsOnMatchImmediately() {
        var exec = compileAndCreate(JOIN_SELECT + " INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);

        var afterTrade = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        assertTrue(afterTrade.isEmpty());

        var afterQuote = exec.onEvent("quotes", evt(1200, "quotes", "symbol", "AAPL", "bid", 99.5, "ask", 100.5));
        assertEquals(1, afterQuote.size());
        assertEquals("AAPL", afterQuote.get(0).get("symbol"));
        assertEquals(100.0, afterQuote.get(0).get("price"));
        assertEquals(99.5, afterQuote.get(0).get("bid"));
        assertEquals(1200L, afterQuote.get(0).timestamp()); // combined ts = max(1000,1200)
    }

    @Test
    void innerJoinNoMatchNoNullPadOnEviction() {
        var exec = compileAndCreate(JOIN_SELECT + " INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));

        var evicted = exec.advanceWatermark(8000); // watermark = 7000 > 1000 + 5000
        assertTrue(evicted.isEmpty());
    }

    @Test
    void leftJoinNullPadsUnmatchedOnEviction() {
        var exec = compileAndCreate(JOIN_SELECT + " LEFT JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));

        var immediate = exec.advanceWatermark(6500); // watermark = 5500, not yet past 1000+5000=6000
        assertTrue(immediate.isEmpty());

        var evicted = exec.advanceWatermark(8000); // watermark = 7000 > 6000
        assertEquals(1, evicted.size());
        assertEquals("AAPL", evicted.get(0).get("symbol"));
        assertNull(evicted.get(0).get("bid"));
    }

    @Test
    void rightJoinNullPadsUnmatchedLeftColumnsOnEviction() {
        var exec = compileAndCreate(JOIN_SELECT + " RIGHT JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        exec.onEvent("quotes", evt(1000, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));

        var evicted = exec.advanceWatermark(8000);
        assertEquals(1, evicted.size());
        assertNull(evicted.get(0).get("symbol"));
        assertNull(evicted.get(0).get("price"));
        assertEquals(99.0, evicted.get(0).get("bid"));
    }

    @Test
    void fullJoinNullPadsBothSidesIndependently() {
        var exec = compileAndCreate(JOIN_SELECT + " FULL JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.onEvent("quotes", evt(1100, "quotes", "symbol", "MSFT", "bid", 50.0, "ask", 51.0));

        var evicted = exec.advanceWatermark(8000);
        assertEquals(2, evicted.size());
        PortAsserts.containsMatch(evicted, r -> Objects.equals(r.get("symbol"), "AAPL") && r.get("bid") == null);
        PortAsserts.containsMatch(evicted, r -> r.get("symbol") == null && Objects.equals(r.get("bid"), 50.0));
    }

    @Test
    void crossJoinMatchesAllBufferedRowsRegardlessOfKey() {
        var exec = compileAndCreate(JOIN_SELECT + " CROSS JOIN quotes q WITHIN 5 SECONDS", TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1100, "trades", "symbol", "MSFT", "price", 200.0, "qty", 1L, "active", true));

        var results = exec.onEvent("quotes", evt(1200, "quotes", "symbol", "GOOG", "bid", 1.0, "ask", 1.1));
        assertEquals(2, results.size()); // matches both buffered trades, ignoring symbol key
    }

    @Test
    void multiWayJoinFoldsLeftToRightAndCombinedTimestampIsMax() {
        var sql = "SELECT t.symbol AS symbol, q.bid AS bid, r.tag AS tag FROM trades t " +
                  "INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "INNER JOIN ref r WITHIN 5 SECONDS ON t.symbol = r.symbol";
        var exec = compileAndCreate(sql, TRADES, QUOTES, REF);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var afterQuote = exec.onEvent("quotes", evt(1100, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));
        assertTrue(afterQuote.isEmpty()); // waiting on ref

        var afterRef = exec.onEvent("ref", evt(1200, "ref", "symbol", "AAPL", "tag", "blue-chip"));
        assertEquals(1, afterRef.size());
        assertEquals("AAPL", afterRef.get(0).get("symbol"));
        assertEquals(99.0, afterRef.get(0).get("bid"));
        assertEquals("blue-chip", afterRef.get(0).get("tag"));
        assertEquals(1200L, afterRef.get(0).timestamp());
    }

    @Test
    void selfJoinDeliversEventToBothAliases() {
        var sql = "SELECT a.symbol AS asym, b.symbol AS bsym FROM trades a INNER JOIN trades b WITHIN 5 SECONDS ON a.symbol = b.symbol";
        var exec = compileAndCreate(sql, TRADES);

        // The very first trade event is delivered to both alias 'a' (FROM role) and alias 'b' (JOIN role),
        // so it can match against itself.
        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        assertEquals(1, results.size());
        assertEquals("AAPL", results.get(0).get("asym"));
        assertEquals("AAPL", results.get(0).get("bsym"));
    }

    @Test
    void joinOnJsonArrowArrowExtractedEquiKeyMatches() {
        // ON e.payload ->> 'order' ->> 'symbol'... — wait, the equi-key itself is 'payload -> 'order' ->> 'symbol''
        // (an object step then a text-extracting step). The equi-key extractor must walk through the
        // JsonAccessExpr nodes on the JSON side to recognize this as a valid hash equi-key against t.symbol.
        var sql = "SELECT e.eventType AS eventType, e.payload -> 'user' ->> 'tier' AS tier, t.price AS price FROM events e " +
                  "JOIN trades t WITHIN 5 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol";
        var exec = compileAndCreate(sql, EVENTS, TRADES);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("user", new LinkedHashMap<String, Object>(Map.of("tier", "gold")));
        payload.put("order", new LinkedHashMap<String, Object>(Map.of("symbol", "AAPL")));

        var afterEvent = exec.onEvent("events", evt(1000, "events", "eventType", "order.placed", "payload", payload));
        assertTrue(afterEvent.isEmpty());

        var afterTrade = exec.onEvent("trades", evt(1100, "trades", "symbol", "AAPL", "price", 150.0, "qty", 10L, "active", true));
        assertEquals(1, afterTrade.size());
        assertEquals("order.placed", afterTrade.get(0).get("eventType"));
        assertEquals("gold", afterTrade.get(0).get("tier"));
        assertEquals(150.0, afterTrade.get(0).get("price"));

        // A trade for a different symbol must not match.
        var noMatch = exec.onEvent("trades", evt(1200, "trades", "symbol", "MSFT", "price", 250.0, "qty", 5L, "active", true));
        assertTrue(noMatch.isEmpty());
    }
}
