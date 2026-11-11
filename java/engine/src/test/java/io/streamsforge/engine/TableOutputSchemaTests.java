package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TableOutputSchemaTests {
    @Test
    void plainProjectionPreservesColumnKinds() {
        var r = compileTable("SELECT symbol, price, qty, active FROM trades", TRADES);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("price"));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("qty"));
        assertEquals(FieldKind.BOOL, r.outputSchema().fields().get("active"));
    }

    @Test
    void aggregatesProduceLongAndDoubleKinds() {
        var sql = "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total_qty, AVG(price) AS avgp, MIN(price) AS low, MAX(price) AS high FROM trades GROUP BY symbol";
        var r = compileTable(sql, TRADES);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("cnt"));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("total_qty")); // SUM over a Long column stays Long
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("avgp")); // AVG always Double
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("low")); // MIN over a Double column
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("high"));
    }

    @Test
    void arrowArrowJsonExtractionIsStringKind() {
        var sql = "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
                  "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'";
        var appEvents = schema("app_events", "eventType", FieldKind.STRING, "payload", FieldKind.JSON);
        var r = compileTable(sql, appEvents);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("orders"));
    }

    @Test
    void starProjectionUsesSourceSchemaKinds() {
        var r = compileTable("SELECT * FROM trades", TRADES);
        assertTrue(r.ok(), String.valueOf(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("price"));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("qty"));
    }
}
