package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** JSON expressions ('->'/'->>') in table mode — the exact SQL seeded as RegistryGrain's
 *  "gold_tier_orders" demo table. */
class TableJsonTests {
    private static final String GOLD_TIER_SQL =
        "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
        "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'";

    private static final SourceSchema APP_EVENTS = schema("app_events", "eventType", FieldKind.STRING, "payload", FieldKind.JSON);

    private static EventRecord jsonEvent(String tier, String symbol) {
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("tier", tier);
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("symbol", symbol);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("user", user);
        payload.put("order", order);
        return evt(1000, "app_events", "eventType", "order.placed", "payload", payload);
    }

    @Test
    void goldTierSeedSqlCompiles() {
        var r = compileTable(GOLD_TIER_SQL, APP_EVENTS);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertTrue(r.streamInputs().contains("app_events"));
    }

    @Test
    void goldTierEventIncrementsOrderCountForItsSymbol() {
        var exec = compileTableAndCreate(GOLD_TIER_SQL, APP_EVENTS);

        var deltas = exec.onStreamEvent("app_events", jsonEvent("gold", "AAPL"));

        var delta = single(deltas);
        assertEquals(1L, delta.weight());
        assertEquals("AAPL", delta.row().get("symbol"));
        assertEquals(1L, delta.row().get("orders"));
    }

    @Test
    void nonGoldTierEventIsFilteredOutByWhere() {
        var exec = compileTableAndCreate(GOLD_TIER_SQL, APP_EVENTS);

        var deltas = exec.onStreamEvent("app_events", jsonEvent("silver", "AAPL"));

        assertTrue(deltas.isEmpty());
    }

    @Test
    void secondGoldTierEventForSameSymbolRetractsAndReasserts() {
        var exec = compileTableAndCreate(GOLD_TIER_SQL, APP_EVENTS);

        exec.onStreamEvent("app_events", jsonEvent("gold", "AAPL"));
        var deltas = exec.onStreamEvent("app_events", jsonEvent("gold", "AAPL"));

        assertEquals(2, deltas.size());
        assertEquals(-1L, deltas.get(0).weight());
        assertEquals(1L, deltas.get(0).row().get("orders"));
        assertEquals(1L, deltas.get(1).weight());
        assertEquals(2L, deltas.get(1).row().get("orders"));
    }
}
