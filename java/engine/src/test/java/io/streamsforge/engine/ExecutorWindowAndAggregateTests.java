package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class ExecutorWindowAndAggregateTests {
    @Test
    void tumblingWindowClosesOnlyWhenWatermarkPassesEnd() {
        var sql = "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total FROM trades " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        var r1 = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L, "active", true));
        assertTrue(r1.isEmpty()); // EMIT FINAL: no update rows

        var r2 = exec.onEvent("trades", evt(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 20L, "active", true));
        assertTrue(r2.isEmpty());

        var notYet = exec.advanceWatermark(10500); // watermark = 9500, window end = 10000, not yet passed
        assertTrue(notYet.isEmpty());

        var closed = exec.advanceWatermark(12000); // watermark = 11000 >= 10000
        var row = single(closed);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(2L, row.get("cnt"));
        assertEquals(30L, row.get("total"));
        assertEquals(0L, row.get("window_start"));
        assertEquals(10000L, row.get("window_end"));
        assertEquals(10000L, row.timestamp());
    }

    @Test
    void hoppingWindowAssignsRowToEveryOverlappingWindow() {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades " +
                  "GROUP BY symbol WINDOW HOPPING(SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(7000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));

        // ts=7000 belongs to windows [0,10000) and [5000,15000)
        var closedFirst = exec.advanceWatermark(11500); // watermark = 10500 >= 10000, closes [0,10000)
        var firstRow = single(closedFirst);
        assertEquals(0L, firstRow.get("window_start"));
        assertEquals(10000L, firstRow.get("window_end"));
        assertEquals(1L, firstRow.get("cnt"));

        var closedSecond = exec.advanceWatermark(16500); // watermark = 15500 >= 15000
        var secondRow = single(closedSecond);
        assertEquals(5000L, secondRow.get("window_start"));
        assertEquals(15000L, secondRow.get("window_end"));
        assertEquals(1L, secondRow.get("cnt"));
    }

    @Test
    void sessionWindowExtendsWithinGapAndSplitsAcrossGap() {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW SESSION(GAP 5 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(0, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(3000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true)); // within gap of 0+5000, extends
        // ts=20000 is beyond 3000+5000=8000, so it starts a new session; it also jumps the watermark to
        // 20000-1000=19000 via OnEvent's own max(seen event ts)-lateness advance.
        exec.onEvent("trades", evt(20000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));

        var closedFirst = exec.advanceWatermark(10000); // watermark stays max(19000, 9000) = 19000 > 3000 + 5000
        var firstRow = single(closedFirst);
        assertEquals(0L, firstRow.get("window_start"));
        assertEquals(3000L, firstRow.get("window_end"));
        assertEquals(2L, firstRow.get("cnt"));

        var closedSecond = exec.advanceWatermark(27000); // watermark = max(19000, 26000) = 26000 > 20000 + 5000 = 25000
        var secondRow = single(closedSecond);
        assertEquals(20000L, secondRow.get("window_start"));
        assertEquals(1L, secondRow.get("cnt"));
    }

    @Test
    void emitChangesProducesUpdateRowsThenFinalRow() {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT CHANGES";
        var exec = compileAndCreate(sql, TRADES);

        var r1 = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        var u1 = single(r1);
        assertEquals(false, u1.get("_final"));
        assertEquals(1L, u1.get("cnt"));

        var r2 = exec.onEvent("trades", evt(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        var u2 = single(r2);
        assertEquals(false, u2.get("_final"));
        assertEquals(2L, u2.get("cnt"));

        var closed = exec.advanceWatermark(12000);
        var fin = single(closed);
        assertEquals(true, fin.get("_final"));
        assertEquals(2L, fin.get("cnt"));
    }

    @Test
    void distinctGroupsProduceSeparateWindowRows() {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1500, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1600, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L, "active", true));

        var closed = exec.advanceWatermark(12000);
        assertEquals(2, closed.size());
        PortAsserts.containsMatch(closed, r -> Objects.equals(r.get("symbol"), "AAPL") && Objects.equals(r.get("cnt"), 1L));
        PortAsserts.containsMatch(closed, r -> Objects.equals(r.get("symbol"), "MSFT") && Objects.equals(r.get("cnt"), 2L));
    }

    @Test
    void countStarCountsRowsCountColumnSkipsNulls() {
        var sql = "SELECT symbol, COUNT(*) AS all_rows, COUNT(qty) AS non_null_qty FROM trades " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L, "active", true));
        exec.onEvent("trades", evt(1100, "trades", "symbol", "AAPL", "price", 1.0, "qty", null, "active", true));
        exec.onEvent("trades", evt(1200, "trades", "symbol", "AAPL", "price", 1.0, "qty", 5L, "active", true));

        var row = single(exec.advanceWatermark(12000));
        assertEquals(3L, row.get("all_rows"));
        assertEquals(2L, row.get("non_null_qty"));
    }

    @Test
    void avgMinMaxAggregates() {
        var sql = "SELECT symbol, AVG(price) AS avgp, MIN(price) AS minp, MAX(price) AS maxp, MIN(symbol) AS mins " +
                  "FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 10.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1100, "trades", "symbol", "AAPL", "price", 20.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1200, "trades", "symbol", "AAPL", "price", 30.0, "qty", 1L, "active", true));

        var row = single(exec.advanceWatermark(12000));
        assertEquals(20.0, row.get("avgp"));
        assertEquals(10.0, row.get("minp"));
        assertEquals(30.0, row.get("maxp"));
        assertEquals("AAPL", row.get("mins"));
    }

    @Test
    void lateEventIsDroppedAndCounted() {
        var exec = compileAndCreate("SELECT symbol FROM trades", TRADES);

        exec.onEvent("trades", evt(10000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        assertEquals(9000L, exec.getWatermark()); // 10000 - 1000 allowed lateness

        var results = exec.onEvent("trades", evt(5000, "trades", "symbol", "MSFT", "price", 1.0, "qty", 1L, "active", true));
        assertTrue(results.isEmpty());
        assertEquals(1L, exec.getLateEvents());
    }

    @Test
    void lateEventDropViaExplicitAdvanceWatermark() {
        var exec = compileAndCreate("SELECT symbol FROM trades", TRADES);

        exec.advanceWatermark(11000); // watermark = 10000
        var results = exec.onEvent("trades", evt(9000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
        assertTrue(results.isEmpty());
        assertEquals(1L, exec.getLateEvents());
    }

    private static Object payloadWithTier(String tier) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("user", new LinkedHashMap<String, Object>(Map.of("tier", tier)));
        return m;
    }

    @Test
    void groupByJsonArrowArrowExtractedValueAggregatesPerDistinctKey() {
        var sql = "SELECT payload -> 'user' ->> 'tier' AS tier, COUNT(*) AS cnt " +
                  "FROM events GROUP BY payload -> 'user' ->> 'tier' WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, EVENTS);

        exec.onEvent("events", evt(1000, "events", "eventType", "e", "payload", payloadWithTier("gold")));
        exec.onEvent("events", evt(1100, "events", "eventType", "e", "payload", payloadWithTier("silver")));
        exec.onEvent("events", evt(1200, "events", "eventType", "e", "payload", payloadWithTier("gold")));

        var closed = exec.advanceWatermark(12000);
        assertEquals(2, closed.size());
        PortAsserts.containsMatch(closed, r -> Objects.equals(r.get("tier"), "gold") && Objects.equals(r.get("cnt"), 2L));
        PortAsserts.containsMatch(closed, r -> Objects.equals(r.get("tier"), "silver") && Objects.equals(r.get("cnt"), 1L));
    }
}
