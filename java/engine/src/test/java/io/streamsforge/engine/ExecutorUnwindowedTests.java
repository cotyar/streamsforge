package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExecutorUnwindowedTests {
    @Test
    void filterAndProjectEmitsImmediatelyPerEvent() {
        var exec = compileAndCreate("SELECT symbol AS sym, price * qty AS notional FROM trades WHERE price > 10", TRADES);

        var passing = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 3L, "active", true));
        assertEquals(1, passing.size());
        assertEquals("AAPL", passing.get(0).get("sym"));
        assertEquals(300.0, passing.get(0).get("notional"));

        var filtered = exec.onEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 5.0, "qty", 3L, "active", true));
        assertTrue(filtered.isEmpty());
    }

    @Test
    void outputTimestampIsRowTimestamp() {
        var exec = compileAndCreate("SELECT symbol FROM trades", TRADES);
        var results = exec.onEvent("trades", evt(4242, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        assertEquals(1, results.size());
        assertEquals(4242L, results.get(0).timestamp());
    }

    @Test
    void unknownEventSourceProducesNoOutput() {
        var exec = compileAndCreate("SELECT symbol FROM trades", TRADES, QUOTES);
        var results = exec.onEvent("quotes", evt(1000, "quotes", "symbol", "AAPL", "bid", 1.0, "ask", 1.1));
        assertTrue(results.isEmpty());
    }

    @Test
    void defaultColumnNamesUseIdentifierOrFunctionName() {
        var exec = compileAndCreate("SELECT symbol, ABS(price) FROM trades", TRADES);
        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", -5.0, "qty", 1L, "active", true));
        assertEquals(1, results.size());
        assertEquals("AAPL", results.get(0).get("symbol"));
        assertEquals(5.0, results.get(0).get("abs"));
    }

    @Test
    void whereFiltersOnJsonArrowArrowExtractedValue() {
        var exec = compileAndCreate(
            "SELECT eventType, payload -> 'order' ->> 'symbol' AS symbol FROM events WHERE payload -> 'user' ->> 'tier' = 'gold'",
            EVENTS);

        Map<String, Object> gold = new LinkedHashMap<>();
        gold.put("user", new LinkedHashMap<String, Object>(Map.of("tier", "gold")));
        gold.put("order", new LinkedHashMap<String, Object>(Map.of("symbol", "AAPL")));
        Map<String, Object> silver = new LinkedHashMap<>();
        silver.put("user", new LinkedHashMap<String, Object>(Map.of("tier", "silver")));
        silver.put("order", new LinkedHashMap<String, Object>(Map.of("symbol", "MSFT")));

        var passing = exec.onEvent("events", evt(1000, "events", "eventType", "order.placed", "payload", gold));
        assertEquals(1, passing.size());
        assertEquals("AAPL", passing.get(0).get("symbol"));

        var filtered = exec.onEvent("events", evt(2000, "events", "eventType", "order.placed", "payload", silver));
        assertTrue(filtered.isEmpty());
    }
}
