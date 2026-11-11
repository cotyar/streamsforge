package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Plan 009 Round C wave C1 — runtime (ExpressionEvaluator) behavior of TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/
 * TO_STRING and CAST(expr AS type) sugar, in both pipeline and table mode. Result-kind/arity/diagnostics live in
 * {@link TypeConversionValidatorTests}; canonical-conversion-rule unit coverage lives in
 * {@link FieldValueConversionTests}. Every function here is total: an unconvertible or NULL argument yields NULL,
 * never an exception (DESIGN.md §D11).
 */
class TypeConversionFunctionsTests {
    private static final SourceSchema MIXED = schema(
        "mixed",
        "s", FieldKind.STRING, "l", FieldKind.LONG, "d", FieldKind.DOUBLE,
        "b", FieldKind.BOOL, "j", FieldKind.JSON);

    private static final SourceSchema PRICE_FEED = schema(
        "pricefeed", "symbol", FieldKind.STRING, "price_str", FieldKind.STRING);

    private static Object evalSingle(String selectExpr, Object... eventFields) {
        var exec = compileAndCreate("SELECT " + selectExpr + " AS x FROM mixed", MIXED);
        var results = exec.onEvent("mixed", evt(1000, "mixed", eventFields));
        assertEquals(1, results.size());
        return results.get(0).get("x");
    }

    @SuppressWarnings("unused")
    private static Object evalSingleTable(String selectExpr, Object... eventFields) {
        var exec = compileTableAndCreate("SELECT " + selectExpr + " AS x FROM mixed", MIXED);
        var deltas = exec.onStreamEvent("mixed", evt(1000, "mixed", eventFields));
        assertEquals(1, deltas.size());
        return deltas.get(0).row().get("x");
    }

    private static Map<String, Object> dict(Object... kv) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // ------------------------------------------------------------------
    // TO_LONG — every source kind
    // ------------------------------------------------------------------

    @Test
    void toLong_FromLong_IsIdentity() {
        assertEquals(42L, evalSingle("TO_LONG(l)", "l", 42L, "s", "x", "d", 1.0, "b", true));
    }

    @Test
    void toLong_FromDouble_Truncates() {
        assertEquals(3L, evalSingle("TO_LONG(d)", "d", 3.9, "s", "x", "l", 1L, "b", true));
    }

    @Test
    void toLong_FromBool_IsOneOrZero() {
        assertEquals(1L, evalSingle("TO_LONG(b)", "b", true, "s", "x", "l", 1L, "d", 1.0));
        assertEquals(0L, evalSingle("TO_LONG(b)", "b", false, "s", "x", "l", 1L, "d", 1.0));
    }

    @Test
    void toLong_FromNumericString_Parses() {
        assertEquals(42L, evalSingle("TO_LONG(s)", "s", "42", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toLong_FromNonNumericString_IsNull() {
        assertNull(evalSingle("TO_LONG(s)", "s", "abc", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toLong_FromOverflowingNumericString_IsNull() {
        assertNull(evalSingle("TO_LONG(s)", "s", "99999999999999999999", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toLong_NullInputIsNull() {
        assertNull(evalSingle("TO_LONG(s)", "s", null, "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // TO_DOUBLE — every source kind
    // ------------------------------------------------------------------

    @Test
    void toDouble_FromLong_Promotes() {
        assertEquals(5.0, evalSingle("TO_DOUBLE(l)", "l", 5L, "s", "x", "d", 1.0, "b", true));
    }

    @Test
    void toDouble_FromDouble_IsIdentity() {
        assertEquals(3.5, evalSingle("TO_DOUBLE(d)", "d", 3.5, "s", "x", "l", 1L, "b", true));
    }

    @Test
    void toDouble_FromBool_IsOneOrZero() {
        assertEquals(1.0, evalSingle("TO_DOUBLE(b)", "b", true, "s", "x", "l", 1L, "d", 1.0));
    }

    @Test
    void toDouble_FromNumericString_Parses() {
        assertEquals(3.14, evalSingle("TO_DOUBLE(s)", "s", "3.14", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toDouble_FromEmptyString_IsNull() {
        assertNull(evalSingle("TO_DOUBLE(s)", "s", "", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toDouble_FromNonNumericString_IsNull() {
        assertNull(evalSingle("TO_DOUBLE(s)", "s", "not-a-number", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toDouble_NullInputIsNull() {
        assertNull(evalSingle("TO_DOUBLE(s)", "s", null, "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // TO_BOOL — every source kind, including the documented permissive-string finding
    // ------------------------------------------------------------------

    @Test
    void toBool_FromLong_IsNonzeroIsTrue() {
        assertEquals(true, evalSingle("TO_BOOL(l)", "l", 2L, "s", "x", "d", 1.0, "b", true));
        assertEquals(false, evalSingle("TO_BOOL(l)", "l", 0L, "s", "x", "d", 1.0, "b", true));
    }

    @Test
    void toBool_FromDouble_IsNonzeroIsTrue() {
        assertEquals(true, evalSingle("TO_BOOL(d)", "d", 2.5, "s", "x", "l", 1L, "b", true));
        assertEquals(false, evalSingle("TO_BOOL(d)", "d", 0.0, "s", "x", "l", 1L, "b", true));
    }

    @ParameterizedTest
    @CsvSource({"true,true", "TRUE,true", "false,false", "0,false", "'',false"})
    void toBool_FromRecognizedStringSpellings(String input, boolean expected) {
        assertEquals(expected, evalSingle("TO_BOOL(s)", "s", input, "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toBool_FromAnyOtherStringIsTrue_DocumentedFinding() {
        // See FieldValueConversion's class doc: this is a pinned, documented divergence from a strict
        // "true/false/0/1 else NULL" rule — the canonical function must match
        // FieldValueCoercion.TryToBool's existing (permissive) inbound-path behavior exactly.
        assertEquals(true, evalSingle("TO_BOOL(s)", "s", "banana", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toBool_NullInputIsNull() {
        assertNull(evalSingle("TO_BOOL(s)", "s", null, "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // TO_TIMESTAMP — epoch-ms (number or numeric string) or ISO-8601 text
    // ------------------------------------------------------------------

    @Test
    void toTimestamp_FromLong_IsIdentity() {
        assertEquals(1_700_000_000_000L, evalSingle("TO_TIMESTAMP(l)", "l", 1_700_000_000_000L, "s", "x", "d", 1.0, "b", true));
    }

    @Test
    void toTimestamp_FromDouble_Truncates() {
        assertEquals(1_700_000_000_000L, evalSingle("TO_TIMESTAMP(d)", "d", 1_700_000_000_000.0, "s", "x", "l", 1L, "b", true));
    }

    @Test
    void toTimestamp_FromNumericString_ParsesAsEpochMs() {
        assertEquals(1_700_000_000_000L, evalSingle("TO_TIMESTAMP(s)", "s", "1700000000000", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toTimestamp_FromIso8601Text_Parses() {
        assertEquals(1_700_000_000_000L, evalSingle("TO_TIMESTAMP(s)", "s", "2023-11-14T22:13:20Z", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toTimestamp_FromUnparseableStringIsNull() {
        assertNull(evalSingle("TO_TIMESTAMP(s)", "s", "not-a-timestamp", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toTimestamp_FromBoolIsNull() {
        assertNull(evalSingle("TO_TIMESTAMP(b)", "b", true, "s", "x", "l", 1L, "d", 1.0));
    }

    @Test
    void toTimestamp_NullInputIsNull() {
        assertNull(evalSingle("TO_TIMESTAMP(s)", "s", null, "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // TO_STRING — culture-invariant, every source kind, plus JSON/timestamp special cases
    // ------------------------------------------------------------------

    @Test
    void toString_FromString_IsIdentity() {
        assertEquals("hi", evalSingle("TO_STRING(s)", "s", "hi", "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toString_FromLong_IsInvariantIntegerText() {
        assertEquals("42", evalSingle("TO_STRING(l)", "l", 42L, "s", "x", "d", 1.0, "b", true));
    }

    @Test
    void toString_FromDouble_IsInvariantText() {
        assertEquals("3.5", evalSingle("TO_STRING(d)", "d", 3.5, "s", "x", "l", 1L, "b", true));
    }

    @Test
    void toString_FromBool_IsLowercaseLiteral() {
        assertEquals("true", evalSingle("TO_STRING(b)", "b", true, "s", "x", "l", 1L, "d", 1.0));
        assertEquals("false", evalSingle("TO_STRING(b)", "b", false, "s", "x", "l", 1L, "d", 1.0));
    }

    @Test
    void toString_NullInputIsNull() {
        assertNull(evalSingle("TO_STRING(s)", "s", null, "l", 1L, "d", 1.0, "b", true));
    }

    @Test
    void toString_OfToTimestamp_RendersIso8601() {
        // Syntactic detection: TO_STRING(TO_TIMESTAMP(...)) renders ISO-8601 text rather than a bare
        // integer — detected from the AST shape (the argument being a TO_TIMESTAMP call) rather than from a
        // runtime type tag, which doesn't exist for FieldKind.Timestamp vs FieldKind.Long.
        var v = evalSingle("TO_STRING(TO_TIMESTAMP(l))", "l", 1_700_000_000_000L, "s", "x", "d", 1.0, "b", true);
        assertEquals("2023-11-14T22:13:20.000Z", v);
    }

    @Test
    void toString_OfCastAsTimestamp_RendersIso8601() {
        // CAST(... AS TIMESTAMP) desugars to the same TO_TIMESTAMP node, so the same detection fires.
        var v = evalSingle("TO_STRING(CAST(l AS TIMESTAMP))", "l", 1_700_000_000_000L, "s", "x", "d", 1.0, "b", true);
        assertEquals("2023-11-14T22:13:20.000Z", v);
    }

    @Test
    void toString_OfABareLongDoesNotRenderIso8601() {
        // Contrast with the two tests above: without a syntactic TO_TIMESTAMP wrapper, a long renders
        // as a plain integer even if it happens to be epoch-ms-shaped.
        assertEquals("1700000000000", evalSingle("TO_STRING(l)", "l", 1_700_000_000_000L, "s", "x", "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // Round trip
    // ------------------------------------------------------------------

    @Test
    void roundTrip_ToStringOfToLong() {
        assertEquals("42", evalSingle("TO_STRING(TO_LONG(s))", "s", "42", "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // JSON leaves — the wave's motivating case: a terminal '->' access cast to a usable type
    // ------------------------------------------------------------------

    private static Object evalJson(String selectExpr, Object payload) {
        var exec = compileAndCreate("SELECT " + selectExpr + " AS x FROM events", EVENTS);
        var results = exec.onEvent("events", evt(1000, "events", "eventType", "e", "payload", payload));
        assertEquals(1, results.size());
        return results.get(0).get("x");
    }

    @Test
    void toDouble_OverArrowAccess_ParsesAQuotedNumber() {
        // The repo's own documented '->'/'->>' gotcha made survivable: a producer that quoted its
        // numbers (payload -> 'qty' is a JSON string leaf "10.5") can now be summed/compared.
        var payload = dict("qty", "10.5");
        assertEquals(10.5, evalJson("TO_DOUBLE(payload -> 'qty')", payload));
    }

    @Test
    void toLong_OverArrowAccess_OnANumericJsonLeaf() {
        var payload = dict("qty", 10L);
        assertEquals(10L, evalJson("TO_LONG(payload -> 'qty')", payload));
    }

    @Test
    void toString_OverArrowAccess_OnACompositeJsonNode_RendersCompactJsonText() {
        var payload = dict("order", dict("symbol", "AAPL", "qty", 10L));
        assertEquals("{\"symbol\":\"AAPL\",\"qty\":10}", evalJson("TO_STRING(payload -> 'order')", payload));
    }

    // ------------------------------------------------------------------
    // CAST(expr AS type) sugar — identical results to the TO_* function form
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "TO_LONG(s),CAST(s AS LONG)",
        "TO_DOUBLE(s),CAST(s AS DOUBLE)",
        "TO_BOOL(s),CAST(s AS BOOL)",
        "TO_TIMESTAMP(s),CAST(s AS TIMESTAMP)"})
    void castSugarProducesIdenticalResultsToFunctionForm(String fnForm, String castForm) {
        var fnResult = evalSingle(fnForm, "s", "42", "l", 1L, "d", 1.0, "b", true);
        var castResult = evalSingle(castForm, "s", "42", "l", 1L, "d", 1.0, "b", true);
        assertEquals(fnResult, castResult);
    }

    @Test
    void castSugarOnUnconvertibleInputAlsoYieldsNull() {
        assertNull(evalSingle("CAST(s AS LONG)", "s", "not-a-number", "l", 1L, "d", 1.0, "b", true));
    }

    // ------------------------------------------------------------------
    // Casts inside WHERE / GROUP BY
    // ------------------------------------------------------------------

    @Test
    void castInWhereClauseFiltersOnTheConvertedValue() {
        var exec = compileAndCreate("SELECT s AS x FROM mixed WHERE TO_DOUBLE(s) > 10", MIXED);

        var passing = exec.onEvent("mixed", evt(1000, "mixed", "s", "15.5", "l", 1L, "d", 1.0, "b", true));
        assertEquals(1, passing.size());

        var failing = exec.onEvent("mixed", evt(1000, "mixed", "s", "abc", "l", 1L, "d", 1.0, "b", true));
        assertTrue(failing.isEmpty()); // TO_DOUBLE('abc') > 10 is NULL > 10 = NULL -> WHERE filters it out
    }

    @Test
    void castInGroupByGroupsRowsByTheConvertedValue_TableMode() {
        var exec = compileTableAndCreate(
            "SELECT TO_LONG(s) AS grp, COUNT(*) AS cnt FROM mixed GROUP BY TO_LONG(s)", MIXED);

        exec.onStreamEvent("mixed", evt(1000, "mixed", "s", "1", "l", 1L, "d", 1.0, "b", true));
        var deltas = exec.onStreamEvent("mixed", evt(1000, "mixed", "s", "1", "l", 1L, "d", 1.0, "b", true));

        // Same group both times ("1" -> 1L both rows) -> retract-then-assert with an updated count.
        assertEquals(2, deltas.size());
        assertEquals(2L, deltas.get(1).row().get("cnt"));
        assertEquals(1L, deltas.get(1).row().get("grp"));
    }

    // ------------------------------------------------------------------
    // Motivating case: SUM(TO_DOUBLE(price_str)) — a String-declared price column, summed
    // ------------------------------------------------------------------

    @Test
    void sumOfToDoubleOverStringPriceColumn_PipelineMode() {
        var sql = "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, PRICE_FEED);

        exec.onEvent("pricefeed", evt(1000, "pricefeed", "symbol", "AAPL", "price_str", "10.5"));
        exec.onEvent("pricefeed", evt(2000, "pricefeed", "symbol", "AAPL", "price_str", "5.25"));

        var closed = exec.advanceWatermark(12000);
        assertEquals(1, closed.size());
        var row = closed.get(0);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(15.75, row.get("total"));
    }

    @Test
    void sumOfToDoubleOverStringPriceColumn_TableMode() {
        var exec = compileTableAndCreate(
            "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed GROUP BY symbol", PRICE_FEED);

        exec.onStreamEvent("pricefeed", evt(1000, "pricefeed", "symbol", "AAPL", "price_str", "10.5"));
        exec.onStreamEvent("pricefeed", evt(1000, "pricefeed", "symbol", "AAPL", "price_str", "5.25"));

        var snapshot = exec.snapshot();
        assertEquals(1, snapshot.size());
        var current = snapshot.values().iterator().next();
        assertEquals("AAPL", current.row().get("symbol"));
        assertEquals(15.75, current.row().get("total"));
    }

    @Test
    void sumOfToDoubleSkipsUnconvertibleRowsAsZeroContribution() {
        // A row whose price_str doesn't parse contributes NULL, which SUM treats as "no contribution"
        // (existing aggregate NULL-skip semantics) — not a thrown exception, not a poisoned window.
        var sql = "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, PRICE_FEED);

        exec.onEvent("pricefeed", evt(1000, "pricefeed", "symbol", "AAPL", "price_str", "10.5"));
        exec.onEvent("pricefeed", evt(2000, "pricefeed", "symbol", "AAPL", "price_str", "garbage"));

        var closed = exec.advanceWatermark(12000);
        assertEquals(1, closed.size());
        assertEquals(10.5, closed.get(0).get("total"));
    }
}
