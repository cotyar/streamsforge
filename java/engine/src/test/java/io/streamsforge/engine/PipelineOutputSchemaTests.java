package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** CompileResult.OutputSchema — projection-derived column kinds for stream pipelines
 *  (mirror of TableOutputSchemaTests for table mode). */
class PipelineOutputSchemaTests {
    @Test
    void windowedAggregate_DerivesKindsFromProjection() {
        var r = compile(
            "SELECT symbol, COUNT(*) AS trades, SUM(price * qty) AS notional " +
            "FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
            TRADES);

        assertTrue(r.ok());
        var fields = r.outputSchema().fields();
        assertEquals(FieldKind.STRING, fields.get("symbol"));
        assertEquals(FieldKind.LONG, fields.get("trades"));
        assertEquals(FieldKind.DOUBLE, fields.get("notional"));
    }

    @Test
    void qualifiedStar_ExpandsWithSourceKinds() {
        var r = compile("SELECT t.* FROM trades t", TRADES);

        assertTrue(r.ok());
        var fields = r.outputSchema().fields();
        assertEquals(FieldKind.DOUBLE, fields.get("price"));
        assertEquals(FieldKind.LONG, fields.get("qty"));
        assertEquals(FieldKind.BOOL, fields.get("active"));
    }

    @Test
    void jsonAccess_KeepsJsonAndTextKinds() {
        var r = compile(
            "SELECT eventType, payload -> 'user' AS user_obj, payload ->> 'id' AS id_text FROM events",
            EVENTS);

        assertTrue(r.ok());
        var fields = r.outputSchema().fields();
        assertEquals(FieldKind.JSON, fields.get("user_obj"));
        assertEquals(FieldKind.STRING, fields.get("id_text"));
    }

    @Test
    void failedCompile_HasNullOutputSchema() {
        var r = compile("SELEKT nope", TRADES);
        assertFalse(r.ok());
        assertNull(r.outputSchema());
    }
}
