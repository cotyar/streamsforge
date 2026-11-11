package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TokenizerAndParserTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    @Test
    void simpleSelectCompiles() {
        var r = compile("SELECT symbol, price FROM trades", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(List.of("trades"), r.sourceNames());
    }

    @Test
    void selectStarCompiles() {
        var r = compile("SELECT * FROM trades", TRADES);
        assertTrue(r.ok());
    }

    @Test
    void lineCommentIsSkipped() {
        var sql = "-- this is a header comment\nSELECT symbol -- trailing comment\nFROM trades";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void stringLiteralWithEscapedQuoteEvaluates() {
        var exec = compileAndCreate("SELECT 'it''s' AS x FROM trades", TRADES);
        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0));
        assertEquals(1, results.size());
        assertEquals("it's", results.get(0).get("x"));
    }

    @Test
    void unexpectedCharacterProducesDiagnosticWithPosition() {
        var r = compile("SELECT symbol FROM trades WHERE price # 1", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.line() == 1 && d.column() == 39));
    }

    @Test
    void unterminatedStringProducesDiagnostic() {
        var r = compile("SELECT 'oops FROM trades", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("Unterminated")));
    }

    @Test
    void multiLinePositionsAreTracked() {
        var sql = "SELECT symbol\nFROM trades\nWHERE @@\n";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        var d = r.diagnostics().get(0);
        assertEquals(3, d.line());
        assertEquals(7, d.column());
    }

    @Test
    void missingFromKeywordIsParseError() {
        var r = compile("SELECT symbol trades", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("FROM")));
    }

    @Test
    void trailingGarbageIsParseError() {
        // "trades EXTRA" alone is a valid implicit alias, so use punctuation that can never start a clause.
        var r = compile("SELECT symbol FROM trades )", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("Unexpected token")));
    }

    @Test
    void badDurationUnitIsParseError() {
        var r = compile("SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 FORTNIGHTS ON t.symbol = q.symbol", TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("duration unit")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"INNER", "LEFT", "LEFT OUTER", "RIGHT", "RIGHT OUTER", "FULL", "FULL OUTER"})
    void allJoinKindsParse(String kind) {
        var sql = "SELECT t.symbol FROM trades t " + kind + " JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void bareJoinKeywordDefaultsToInner() {
        var r = compile("SELECT t.symbol FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        assertTrue(r.ok());
        assertTrue(r.planSummary().contains("INNER"));
    }

    @Test
    void crossJoinParsesAndForbidsOn() {
        var r = compile("SELECT t.symbol FROM trades t CROSS JOIN quotes q WITHIN 5 SECONDS", TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));

        var withOn = compile("SELECT t.symbol FROM trades t CROSS JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", TRADES, QUOTES);
        assertFalse(withOn.ok());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "WINDOW TUMBLING(SIZE 10 SECONDS)",
        "WINDOW HOPPING(SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)",
        "WINDOW SESSION(GAP 5 SECONDS)"})
    void allWindowKindsParse(String windowClause) {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol " + windowClause + " EMIT FINAL";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"EMIT CHANGES", "EMIT FINAL", ""})
    void emitVariantsParse(String emit) {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) " + emit;
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void singularDurationUnitsAreAccepted() {
        var r = compile("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 1 SECOND)", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void operatorPrecedenceParsesWithoutError() {
        // OR < AND < NOT < comparisons < + - < * / % < unary minus < primary
        var sql = "SELECT symbol FROM trades WHERE price + 1 * 2 > 3 AND NOT active OR qty - -1 = 5";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void groupByMultipleColumnsParses() {
        var sql = "SELECT t.symbol, q.bid, COUNT(*) AS cnt FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "GROUP BY t.symbol, q.bid WINDOW TUMBLING(SIZE 10 SECONDS)";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void planSummaryDescribesPipeline() {
        var sql = "SELECT t.symbol AS symbol, COUNT(*) AS cnt FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "WHERE t.price > 0 GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS) EMIT FINAL";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.planSummary().contains("trades AS t"));
        assertTrue(r.planSummary().contains("⋈[INNER,5s] quotes AS q"));
        assertTrue(r.planSummary().contains("WHERE"));
        assertTrue(r.planSummary().contains("TUMBLING(5s)"));
        assertTrue(r.planSummary().contains("GROUP BY t_symbol"));
        assertTrue(r.planSummary().contains("SELECT 2"));
    }

    @Test
    void compileNeverThrowsOnGarbageInput() {
        for (var sql : new String[] {"", "   ", "SELECT", "))))((((", "SELECT FROM FROM FROM", "🚀🚀🚀"}) {
            var r = compile(sql, TRADES);
            assertFalse(r.ok());
            assertFalse(r.diagnostics().isEmpty());
        }
    }

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') — tokenizer disambiguation and parser precedence.
    // ------------------------------------------------------------------

    @Test
    void minusAndGreaterThanTokenizeCorrectlyWithNoWhitespace() {
        var exec = compileAndCreate("SELECT qty-price AS diff FROM trades WHERE qty>0", TRADES);
        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 3.0, "qty", 10L, "active", true));
        assertEquals(1, results.size());
        assertEquals(7.0, results.get(0).get("diff"));
    }

    @Test
    void arrowAndArrowArrowLexCorrectlyWithAndWithoutWhitespace() {
        var r = compile("SELECT payload->'k' AS viaArrow, payload ->> 'k' AS viaArrowArrow FROM events", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void arrowArrowIsMatchedBeforeArrowSoItIsNotSplit() {
        var r = compile("SELECT payload ->> 'k' AS x FROM events", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void chainedJsonAccessParsesLeftAssociativeAndBindsTighterThanComparison() {
        var exec = compileAndCreate("SELECT eventType AS x FROM events WHERE payload -> 'k' ->> 'x' = 'y'", EVENTS);

        Map<String, Object> matches = new LinkedHashMap<>();
        matches.put("k", new LinkedHashMap<>(Map.of("x", "y")));
        var matchResult = exec.onEvent("events", evt(1000, "events", "eventType", "e1", "payload", matches));
        assertEquals(1, matchResult.size());

        Map<String, Object> mismatches = new LinkedHashMap<>();
        mismatches.put("k", new LinkedHashMap<>(Map.of("x", "z")));
        var mismatchResult = exec.onEvent("events", evt(2000, "events", "eventType", "e2", "payload", mismatches));
        assertTrue(mismatchResult.isEmpty());
    }

    @Test
    void jsonAccessBindsTighterThanUnaryMinus() {
        var exec = compileAndCreate("SELECT -payload -> 'k' AS x FROM events", EVENTS);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("k", 5L);
        var results = exec.onEvent("events", evt(1000, "events", "eventType", "e", "payload", payload));
        assertEquals(1, results.size());
        assertEquals(-5L, results.get(0).get("x"));
    }

    // ------------------------------------------------------------------
    // Qualified star `alias.*` in the select list
    // ------------------------------------------------------------------

    @Test
    void qualifiedStarParses() {
        var r = compile("SELECT t.* FROM trades t", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void qualifiedStarAlongsideOtherColumnParses() {
        var sql = "SELECT t.*, q.bid FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void qualifiedStarWithAsAliasIsParseError() {
        var r = compile("SELECT t.* AS x FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("alias.*")));
    }

    @Test
    void qualifiedStarWithImplicitAliasIsParseError() {
        var r = compile("SELECT t.* x FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("alias.*")));
    }

    @Test
    void originalBugQueryCompilesOk() {
        var r = compile("SELECT t.* FROM trades t", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void jsonAccessKeyMustBeStringOrIntegerLiteral() {
        var parenExpr = compile("SELECT payload -> (1 + 1) AS x FROM events", EVENTS);
        assertFalse(parenExpr.ok());
        assertTrue(parenExpr.diagnostics().stream().anyMatch(d -> d.message().contains("string literal") && d.message().contains("integer literal")));

        var doubleKey = compile("SELECT payload -> 1.5 AS x FROM events", EVENTS);
        assertFalse(doubleKey.ok());
        assertTrue(doubleKey.diagnostics().stream().anyMatch(d -> d.message().contains("string literal") && d.message().contains("integer literal")));
    }
}
