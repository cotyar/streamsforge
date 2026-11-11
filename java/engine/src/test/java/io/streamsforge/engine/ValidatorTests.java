package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ValidatorTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    /** Assert.Contains(diagnostics, d =&gt; d.Message.Contains(text)). */
    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    @Test
    void unknownSourceListsAvailableSources() {
        var r = compile("SELECT symbol FROM ticks", TRADES, QUOTES);
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("Unknown source 'ticks'")).toList();
        assertEquals(1, matching.size());
        var d = matching.get(0);
        assertTrue(d.message().contains("quotes"));
        assertTrue(d.message().contains("trades"));
    }

    @Test
    void unknownColumnHasPosition() {
        var r = compile("SELECT bogus FROM trades", TRADES);
        assertFalse(r.ok());
        assertEquals(1, r.diagnostics().size());
        var d = r.diagnostics().get(0);
        assertTrue(d.message().contains("Unknown column 'bogus'"));
        assertEquals(1, d.line());
        assertEquals(8, d.column());
    }

    @Test
    void ambiguousUnqualifiedColumnAcrossJoinedSourcesIsError() {
        // both trades and quotes declare "symbol"
        var sql = "SELECT symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Ambiguous column 'symbol'"));
    }

    @Test
    void qualifiedColumnResolvesAliasUnambiguously() {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void unknownAliasQualifierIsError() {
        var r = compile("SELECT z.symbol FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown source 'z'"));
    }

    @Test
    void reservedColumnsAreAccessiblePerAlias() {
        var sql = "SELECT t._ts AS tts, t._source AS tsrc FROM trades t";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void aggregateWithoutWindowIsError() {
        var r = compile("SELECT COUNT(*) AS cnt FROM trades", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Aggregate functions require a WINDOW clause"));
    }

    @Test
    void groupByWithoutWindowIsError() {
        var r = compile("SELECT symbol FROM trades GROUP BY symbol", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "GROUP BY requires a WINDOW clause"));
    }

    @Test
    void emitWithoutWindowIsError() {
        var r = compile("SELECT symbol FROM trades EMIT CHANGES", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "EMIT requires a WINDOW clause"));
    }

    @Test
    void nonAggregateSelectItemMustAppearInGroupBy() {
        var sql = "SELECT symbol, price, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Non-aggregate select item must appear in GROUP BY"));
    }

    @Test
    void nestedAggregateIsError() {
        var sql = "SELECT SUM(COUNT(*)) AS x FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "cannot be nested"));
    }

    @Test
    void joinMissingWithinIsError() {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q ON t.symbol = q.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "WITHIN"));
    }

    @Test
    void joinWithoutEquiComparisonIsError() {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.price > q.bid";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "equi-comparison"));
    }

    @Test
    void joinEquiComparisonWithResidualCompiles() {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol AND t.price > q.bid";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void duplicateAliasIsError() {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes t WITHIN 5 SECONDS ON t.symbol = t.symbol";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Duplicate alias"));
    }

    @Test
    void unknownFunctionIsError() {
        var r = compile("SELECT NOPE(price) AS x FROM trades", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown function"));
    }

    @Test
    void functionArityMismatchIsError() {
        var r = compile("SELECT ABS(price, qty) AS x FROM trades", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "wrong number of arguments"));
    }

    @Test
    void selfJoinUnderDifferentAliasesIsValid() {
        var sql = "SELECT a.symbol, b.symbol FROM trades a INNER JOIN trades b WITHIN 5 SECONDS ON a.symbol = b.symbol";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(List.of("trades"), r.sourceNames());
    }

    @Test
    void selectStarWithJoinPrefixesColumnsWithAlias() {
        var sql = "SELECT * FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var exec = compileAndCreate(sql, TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var results = exec.onEvent("quotes", evt(1100, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));
        assertEquals(1, results.size());
        assertTrue(results.get(0).containsKey("t_symbol"));
        assertTrue(results.get(0).containsKey("q_bid"));
        assertFalse(results.get(0).containsKey("t__ts")); // reserved fields not included in *
    }

    @Test
    void selectStarWithoutJoinUsesPlainNames() {
        var exec = compileAndCreate("SELECT * FROM trades", TRADES);
        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        assertEquals(1, results.size());
        assertTrue(results.get(0).containsKey("symbol"));
        assertTrue(results.get(0).containsKey("price"));
    }

    // ------------------------------------------------------------------
    // Qualified star `alias.*` in the select list
    // ------------------------------------------------------------------

    @Test
    void qualifiedStarResolvesKnownAlias() {
        var r = compile("SELECT t.* FROM trades t", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void qualifiedStarUnknownAliasIsError() {
        var r = compile("SELECT z.* FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown source/alias 'z'"));
    }

    @Test
    void qualifiedStarWithGroupByIsError() {
        var sql = "SELECT t.*, COUNT(*) AS cnt FROM trades t GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "star is not allowed with GROUP BY/aggregates"));
    }

    @Test
    void qualifiedStarWithAggregateNoGroupByIsErrorInTableMode() {
        var sql = "SELECT t.*, COUNT(*) AS cnt FROM trades t";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "star is not allowed with GROUP BY/aggregates"));
    }

    @Test
    void qualifiedStarWithJoinExpandsAllTradeColumnsPrefixedPlusExtraColumn() {
        var sql = "SELECT t.*, q.bid FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var exec = compileAndCreate(sql, TRADES, QUOTES);
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var results = exec.onEvent("quotes", evt(1100, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));

        assertEquals(1, results.size());
        var row = results.get(0);
        assertTrue(row.containsKey("t_symbol"));
        assertTrue(row.containsKey("t_price"));
        assertTrue(row.containsKey("t_qty"));
        assertTrue(row.containsKey("t_active"));
        // q.bid keeps its ordinary (unqualified) default column name — only the star-expanded t.* columns
        // are alias-prefixed; a plain select item's default name has always been just the field name.
        assertTrue(row.containsKey("bid"));
        assertFalse(row.containsKey("q_ask")); // only q.bid was projected, not q.*
        assertFalse(row.containsKey("ask"));
        assertEquals("AAPL", row.get("t_symbol"));
        assertEquals(99.0, row.get("bid"));
    }

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') validator rules
    // ------------------------------------------------------------------

    @Test
    void arrowOnNonJsonColumnIsError() {
        var r = compile("SELECT t.price -> 'x' AS x FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("'->'") && d.message().contains("must be a JSON column")));
    }

    @Test
    void arrowArrowOnNonJsonColumnIsError() {
        var r = compile("SELECT t.symbol ->> 'x' AS x FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("'->>'") && d.message().contains("must be a JSON column")));
    }

    @Test
    void arrowOnAnotherArrowResultIsValid() {
        // Chains validate: each '->' result is Json-kind, which is a valid left operand for the next '->'.
        var r = compile("SELECT payload -> 'order' -> 'symbol' AS x FROM events", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void bareJsonColumnInComparisonIsErrorWithArrowArrowHint() {
        var r = compile("SELECT eventType FROM events WHERE payload = 'x'", EVENTS);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "->>"));
    }

    @Test
    void bareJsonColumnInArithmeticIsError() {
        var r = compile("SELECT payload + 1 AS x FROM events", EVENTS);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "->>"));
    }

    @Test
    void bareJsonColumnEqualsNullIsAllowed() {
        var r = compile("SELECT eventType FROM events WHERE payload = NULL", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));

        var neq = compile("SELECT eventType FROM events WHERE payload != NULL", EVENTS);
        assertTrue(neq.ok(), join(neq.diagnostics()));
    }

    @Test
    void terminalArrowInComparisonIsErrorAndHintsArrowArrow() {
        // A '->' chain that never gets its final '->>' is still Json-kind, so using it directly in a
        // comparison hits the same bare-Json rule — the diagnostic should nudge the user toward '->>'.
        var r = compile("SELECT eventType FROM events WHERE payload -> 'order' = 'x'", EVENTS);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "->>"));
    }

    @Test
    void arrowArrowResultComparesAsStringWithoutError() {
        var r = compile("SELECT eventType FROM events WHERE payload ->> 'order' = 'x'", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void bareJsonColumnInSelectProjectsAsIsWithoutError() {
        var r = compile("SELECT payload FROM events", EVENTS);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void jsonEquiKeyJoinOnClauseValidates() {
        // The equi-key extractor must traverse through JsonAccessExpr nodes to see that both operands
        // reference the join's two sides.
        var sql = "SELECT e.eventType, t.price FROM events e " +
                  "JOIN trades t WITHIN 5 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol";
        var r = compile(sql, EVENTS, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void seededJsonPayloadJoinDemoPipelineCompiles() {
        // Exact SQL seeded as the "JSON payload join" demo pipeline (RegistryGrain.SeedPipelines) —
        // app_events' schema here is exactly TestHelpers.Events (eventType String, payload Json).
        var sql = "SELECT e.eventType, e.payload -> 'user' ->> 'tier' AS tier, e.payload -> 'order' ->> 'symbol' AS symbol, t.price FROM events e " +
                  "JOIN trades t WITHIN 10 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol";
        var r = compile(sql, EVENTS, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }
}
