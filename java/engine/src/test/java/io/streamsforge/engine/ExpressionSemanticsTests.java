package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExpressionSemanticsTests {
    private static Object evalSingle(String selectExpr, Object... eventFields) {
        var exec = compileAndCreate("SELECT " + selectExpr + " AS x FROM trades", TRADES);
        var results = exec.onEvent("trades", evt(1000, "trades", eventFields));
        assertEquals(1, results.size());
        return results.get(0).get("x");
    }

    private static List<EventRecord> evalWhere(String whereExpr, Object... eventFields) {
        var exec = compileAndCreate("SELECT symbol AS x FROM trades WHERE " + whereExpr, TRADES);
        return exec.onEvent("trades", evt(1000, "trades", eventFields));
    }

    /** Dictionary&lt;string, object?&gt; literal: alternating key/value. */
    private static Map<String, Object> dict(Object... kv) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static List<Object> list(Object... items) { return new ArrayList<>(Arrays.asList(items)); }

    @Test
    void nullPlusNumberIsNull() {
        assertNull(evalSingle("price + 1", "price", null, "symbol", "A", "qty", 1L, "active", true));
    }

    @Test
    void nullEqualsNullIsNullSoWhereFiltersRowOut() {
        var results = evalWhere("price = price", "price", null, "symbol", "A", "qty", 1L, "active", true);
        assertTrue(results.isEmpty());
    }

    @Test
    void mixedLongAndDoublePromotesToDouble() {
        var v = evalSingle("qty + price", "qty", 2L, "price", 1.5, "symbol", "A", "active", true);
        assertEquals(Double.class, v.getClass());
        assertEquals(3.5, v);
    }

    @Test
    void longPlusLongStaysLong() {
        var v = evalSingle("qty + 3", "qty", 2L, "price", 1.5, "symbol", "A", "active", true);
        assertEquals(Long.class, v.getClass());
        assertEquals(5L, v);
    }

    @Test
    void divisionAlwaysProducesDouble() {
        var v = evalSingle("qty / 2", "qty", 4L, "price", 1.0, "symbol", "A", "active", true);
        assertEquals(Double.class, v.getClass());
        assertEquals(2.0, v);
    }

    @Test
    void divisionByZeroIsNull() {
        assertNull(evalSingle("qty / 0", "qty", 4L, "price", 1.0, "symbol", "A", "active", true));
    }

    @Test
    void stringComparisonIsOrdinal() {
        var v = evalSingle("symbol > 'AAA'", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true);
        assertEquals(true, v);
    }

    @Test
    void coalesceReturnsFirstNonNull() {
        var v = evalSingle("COALESCE(price, qty, 99)", "price", null, "qty", null, "symbol", "A", "active", true);
        assertEquals(99L, v);
    }

    @Test
    void absRoundUpperLowerFunctions() {
        assertEquals(5.0, evalSingle("ABS(price)", "price", -5.0, "symbol", "a", "qty", 1L, "active", true));
        assertEquals(3.14, evalSingle("ROUND(price, 2)", "price", 3.14159, "symbol", "a", "qty", 1L, "active", true));
        assertEquals("AAPL", evalSingle("UPPER(symbol)", "symbol", "aapl", "price", 1.0, "qty", 1L, "active", true));
        assertEquals("aapl", evalSingle("LOWER(symbol)", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true));
    }

    @Test
    void threeValuedAndLogic() {
        // NULL AND FALSE = FALSE
        var r1 = evalWhere("active AND price > 0", "active", null, "price", -1.0, "symbol", "a", "qty", 1L);
        assertTrue(r1.isEmpty());

        // TRUE AND NULL = NULL -> filtered out
        var r2 = evalWhere("active AND price > 0", "active", true, "price", null, "symbol", "a", "qty", 1L);
        assertTrue(r2.isEmpty());
    }

    @Test
    void threeValuedOrLogic() {
        // NULL OR TRUE = TRUE -> row passes
        var r1 = evalWhere("active OR qty > 0", "active", null, "qty", 5L, "price", 1.0, "symbol", "a");
        assertEquals(1, r1.size());

        // FALSE OR NULL = NULL -> filtered
        var r2 = evalWhere("active OR price > 100", "active", false, "price", null, "qty", 1L, "symbol", "a");
        assertTrue(r2.isEmpty());
    }

    @Test
    void notNullIsNull() {
        var results = evalWhere("NOT active", "active", null, "price", 1.0, "qty", 1L, "symbol", "a");
        assertTrue(results.isEmpty());
    }

    @Test
    void unaryMinusNegatesNumbers() {
        assertEquals(-5L, evalSingle("-qty", "qty", 5L, "price", 1.0, "symbol", "a", "active", true));
        assertEquals(-1.5, evalSingle("-price", "price", 1.5, "qty", 1L, "symbol", "a", "active", true));
    }

    @Test
    void moduloOperator() {
        assertEquals(1L, evalSingle("qty % 2", "qty", 5L, "price", 1.0, "symbol", "a", "active", true));
    }

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') evaluator semantics
    // ------------------------------------------------------------------

    private static Object evalJson(String selectExpr, Object payload) {
        var exec = compileAndCreate("SELECT " + selectExpr + " AS x FROM events", EVENTS);
        var results = exec.onEvent("events", evt(1000, "events", "eventType", "e", "payload", payload));
        assertEquals(1, results.size());
        return results.get(0).get("x");
    }

    @Test
    void arrowReturnsObjectFieldValue() {
        var payload = dict("symbol", "AAPL", "qty", 10L);
        assertEquals("AAPL", evalJson("payload -> 'symbol'", payload));
        assertEquals(10L, evalJson("payload -> 'qty'", payload));
    }

    @Test
    void arrowReturnsArrayElementByZeroBasedIndex() {
        var payload = dict("tags", list("web", "mobile", "api"));
        assertEquals("web", evalJson("payload -> 'tags' -> 0", payload));
        assertEquals("mobile", evalJson("payload -> 'tags' -> 1", payload));
    }

    @Test
    void arrowArrayIndexOutOfRangeIsNull() {
        var payload = dict("tags", list("web"));
        assertNull(evalJson("payload -> 'tags' -> 5", payload));
    }

    @Test
    void arrowOnNonListWithIntegerKeyIsNull() {
        var payload = dict("symbol", "AAPL");
        assertNull(evalJson("payload -> 'symbol' -> 0", payload)); // 'symbol' is a string leaf, not a list
    }

    @Test
    void arrowMissingKeyIsNull() {
        var payload = dict("symbol", "AAPL");
        assertNull(evalJson("payload -> 'missing'", payload));
    }

    @Test
    void arrowOnNonObjectWithStringKeyIsNull() {
        var payload = dict("tags", list("web"));
        assertNull(evalJson("payload -> 'tags' -> 'notAKey'", payload)); // 'tags' is a list, not an object
    }

    @Test
    void nullLeftOperandPropagatesThroughArrowAndArrowArrow() {
        assertNull(evalJson("payload -> 'x'", null));
        assertNull(evalJson("payload ->> 'x'", null));
    }

    @Test
    void arrowArrowStringifiesEachPrimitiveTypeAndPropagatesNull() {
        var payload = dict(
            "s", "hello",
            "n", 3.5,
            "l", 42L,
            "b", true,
            "bf", false,
            "nul", null);
        assertEquals("hello", evalJson("payload ->> 's'", payload));
        assertEquals("3.5", evalJson("payload ->> 'n'", payload));
        assertEquals("42", evalJson("payload ->> 'l'", payload));
        assertEquals("true", evalJson("payload ->> 'b'", payload));
        assertEquals("false", evalJson("payload ->> 'bf'", payload));
        assertNull(evalJson("payload ->> 'nul'", payload)); // JSON null stored at the key
        assertNull(evalJson("payload ->> 'missing'", payload)); // key absent entirely
    }

    @Test
    void arrowArrowSerializesNestedDictAndListToCompactJson() {
        var payload = dict(
            "order", dict("symbol", "AAPL", "qty", 10L),
            "tags", list("web", "mobile"));
        assertEquals("{\"symbol\":\"AAPL\",\"qty\":10}", evalJson("payload ->> 'order'", payload));
        assertEquals("[\"web\",\"mobile\"]", evalJson("payload ->> 'tags'", payload));
    }

    @Test
    void chainedArrowThenArrowArrowExtractsDeeplyNestedField() {
        var payload = dict("user", dict("tier", "gold"));
        assertEquals("gold", evalJson("payload -> 'user' ->> 'tier'", payload));
    }

    @Test
    void bareJsonColumnProjectsAsIsInSelect() {
        var payload = dict("a", 1L);
        var result = evalJson("payload", payload);
        assertInstanceOf(Map.class, result);
        assertEquals(1L, ((Map<?, ?>) result).get("a"));
    }
}
