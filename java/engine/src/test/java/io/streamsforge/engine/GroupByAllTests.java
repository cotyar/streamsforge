package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.sql.Ast.Identifier;
import io.streamsforge.engine.sql.Ast.SelectQuery;
import io.streamsforge.engine.sql.Parser;
import io.streamsforge.engine.sql.Tokenizer;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — {@code GROUP BY ALL} (DuckDB/Snowflake sugar): expands to every non-aggregate select-list
 * expression, in select-list order. The expansion lives entirely in the parser; everything downstream consumes the
 * group-by list as an opaque {@code List<Expr>}. The first block exercises the parser directly to pin down exactly
 * which Expr nodes end up in {@code SelectQuery.groupBy} and that they are the SAME instances as the matching select
 * items; the second block exercises full compile/execute behavior (table mode and pipeline mode).
 */
class GroupByAllTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    // ------------------------------------------------------------------
    // Parser-level: exactly which Expr nodes GROUP BY ALL expands to.
    // ------------------------------------------------------------------

    private static SelectQuery parseOk(String sql) {
        var tok = new Tokenizer(sql).tokenize();
        assertTrue(tok.diagnostics().isEmpty());
        var parsed = Parser.parse(tok.tokens());
        assertTrue(parsed.query() != null, join(parsed.diagnostics()));
        assertTrue(parsed.diagnostics().isEmpty());
        return parsed.query();
    }

    @Test
    void expansionPicksNonAggregateItemsInSelectListOrder() {
        // Select-list order is side, symbol — deliberately NOT the schema's declared field order — to
        // prove expansion follows the SELECT list, not the source schema.
        var q = parseOk("SELECT side, symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL");

        assertNotNull(q.groupBy);
        assertEquals(2, q.groupBy.size());
        var side = assertInstanceOf(Identifier.class, q.groupBy.get(0));
        var symbol = assertInstanceOf(Identifier.class, q.groupBy.get(1));
        assertEquals("side", side.name);
        assertEquals("symbol", symbol.name);

        // The sharing is deliberate — assert it directly, not just structurally.
        assertSame(q.select.items.get(0).expression, q.groupBy.get(0));
        assertSame(q.select.items.get(1).expression, q.groupBy.get(1));
    }

    @Test
    void aggregateSelectItemIsExcludedFromExpansion() {
        var q = parseOk("SELECT symbol, COUNT(*) AS cnt, SUM(price) AS total FROM trades GROUP BY ALL");

        assertEquals(1, q.groupBy.size());
        assertSame(q.select.items.get(0).expression, q.groupBy.get(0));
    }

    @Test
    void scalarSubqueryItemIsExcludedFromExpansion() {
        // ContainsAggregate deliberately treats a ScalarSubqueryExpr as aggregate-like (plan 004 N3/N4) —
        // GROUP BY ALL reuses that exact predicate, so a bare scalar subquery in the select list must NOT
        // show up in the expanded GROUP BY, same as a real aggregate wouldn't.
        var q = parseOk("SELECT symbol, (SELECT COUNT(*) FROM trades) AS total FROM trades GROUP BY ALL");

        assertEquals(1, q.groupBy.size());
        var only = assertInstanceOf(Identifier.class, q.groupBy.get(0));
        assertEquals("symbol", only.name);
    }

    @Test
    void emptyExpansionYieldsNullGroupByNotAnEmptyList() {
        // Every select item is an aggregate -> nothing to group by -> null (the implicit global group),
        // never an empty non-null list (that would trip the `GroupBy is not null` gates downstream).
        var q = parseOk("SELECT COUNT(*) AS cnt FROM trades GROUP BY ALL");
        assertNull(q.groupBy);
    }

    @Test
    void starWithAllIsRejectedAtParseTimeWithAPosition() {
        var sql = "SELECT * FROM trades GROUP BY ALL";
        var tok = new Tokenizer(sql).tokenize();
        var parsed = Parser.parse(tok.tokens());

        assertNull(parsed.query());
        assertEquals(1, parsed.diagnostics().size());
        var d = parsed.diagnostics().get(0);
        assertTrue(d.message().contains("GROUP BY ALL"));
        assertEquals(1, d.line());
        assertEquals(sql.indexOf("ALL") + 1, d.column());
    }

    @Test
    void qualifiedStarMixedWithOtherItemsStillCompilesButIsRejectedByTheExistingStarRule() {
        // `alias.*` isn't a bare `SELECT *`, so the parser happily includes it in the expansion (it's not
        // an aggregate) — but the Validator's pre-existing "star is not allowed with GROUP BY/aggregates"
        // rule still fires on it exactly as it would for an explicit GROUP BY.
        var sql = "SELECT t.*, COUNT(*) AS cnt FROM trades t GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "star is not allowed with GROUP BY/aggregates"));
    }

    @Test
    void allIsNotAReservedWordSelectAllStillParsesAsAColumnReference() {
        // ALL is only special-cased immediately after the GROUP BY keyword — it must not become globally
        // reserved, so a column literally named "all" still works everywhere else.
        var withAllColumn = schema("t", "all", FieldKind.STRING);
        var r = compile("SELECT all FROM t", withAllColumn);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    // ------------------------------------------------------------------
    // Validator idempotence: GROUP BY ALL resolves the shared nodes twice but must not double-report.
    // ------------------------------------------------------------------

    @Test
    void unknownColumnInAnExpandedGroupByItemIsReportedOnlyOnce() {
        var sql = "SELECT bogus, COUNT(*) AS cnt FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        var matches = r.diagnostics().stream().filter(d -> d.message().contains("Unknown column 'bogus'")).toList();
        assertEquals(1, matches.size());
    }

    @Test
    void ambiguousColumnInAnExpandedGroupByItemIsReportedOnlyOnce() {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades t JOIN ref r WITHIN 1 SECONDS ON t.symbol = r.symbol " +
                  "GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES, REF);
        assertFalse(r.ok());
        var matches = r.diagnostics().stream().filter(d -> d.message().contains("Ambiguous column 'symbol'")).toList();
        assertEquals(1, matches.size());
    }

    // ------------------------------------------------------------------
    // Table mode: end-to-end compile + execute, ALL vs. explicit twin.
    // ------------------------------------------------------------------

    private static final SourceSchema TRADES_WITH_SIDE =
        schema("trades", "symbol", FieldKind.STRING, "side", FieldKind.STRING, "price", FieldKind.DOUBLE);

    private static EventRecord row(String symbol, String side) {
        return evt(1000, "trades", "symbol", symbol, "side", side, "price", 1.0);
    }

    @Test
    void tableModeAllExpansionMatchesExplicitGroupByPlanSummaryAndResults() {
        var sqlAll = "SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL";
        var sqlExplicit = "SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY symbol, side";

        var rAll = compileTable(sqlAll, TRADES_WITH_SIDE);
        var rExplicit = compileTable(sqlExplicit, TRADES_WITH_SIDE);
        assertTrue(rAll.ok(), join(rAll.diagnostics()));
        assertTrue(rExplicit.ok(), join(rExplicit.diagnostics()));
        assertEquals(rExplicit.planSummary(), rAll.planSummary());

        var execAll = rAll.plan().createExecutor();
        var execExplicit = rExplicit.plan().createExecutor();

        var deltasAll = execAll.onStreamEvent("trades", row("AAPL", "BUY"));
        var deltasExplicit = execExplicit.onStreamEvent("trades", row("AAPL", "BUY"));
        assertSameDeltas(deltasExplicit, deltasAll);

        // A second contribution to the same group (retract-then-assert) must line up too.
        deltasAll = execAll.onStreamEvent("trades", row("AAPL", "BUY"));
        deltasExplicit = execExplicit.onStreamEvent("trades", row("AAPL", "BUY"));
        assertSameDeltas(deltasExplicit, deltasAll);

        // A distinct group (different side) must produce its own, separately-tracked row.
        deltasAll = execAll.onStreamEvent("trades", row("AAPL", "SELL"));
        deltasExplicit = execExplicit.onStreamEvent("trades", row("AAPL", "SELL"));
        assertSameDeltas(deltasExplicit, deltasAll);
    }

    private static void assertSameDeltas(List<TableDelta> expected, List<TableDelta> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).weight(), actual.get(i).weight());
            assertEquals(expected.get(i).row().get("symbol"), actual.get(i).row().get("symbol"));
            assertEquals(expected.get(i).row().get("side"), actual.get(i).row().get("side"));
            assertEquals(expected.get(i).row().get("cnt"), actual.get(i).row().get("cnt"));
        }
    }

    @Test
    void emptyExpansionProducesSameResultsAsWritingNoGroupByAtAllInTableMode() {
        var sqlAll = "SELECT COUNT(*) AS cnt FROM trades GROUP BY ALL";
        var sqlNone = "SELECT COUNT(*) AS cnt FROM trades";

        var rAll = compileTable(sqlAll, TRADES);
        var rNone = compileTable(sqlNone, TRADES);
        assertTrue(rAll.ok(), join(rAll.diagnostics()));
        assertTrue(rNone.ok(), join(rNone.diagnostics()));
        assertEquals(rNone.planSummary(), rAll.planSummary());

        var execAll = rAll.plan().createExecutor();
        var execNone = rNone.plan().createExecutor();

        var evt1 = evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 1L, "active", true);
        var evt2 = evt(1100, "trades", "symbol", "MSFT", "price", 2.0, "qty", 1L, "active", true);

        var d1AllList = execAll.onStreamEvent("trades", evt1);
        var d1NoneList = execNone.onStreamEvent("trades", evt1);
        assertEquals(1, d1AllList.size());
        assertEquals(1, d1NoneList.size());
        var d1All = d1AllList.get(0);
        var d1None = d1NoneList.get(0);
        assertEquals(d1None.weight(), d1All.weight());
        assertEquals(d1None.row().get("cnt"), d1All.row().get("cnt"));

        // Both rows land in the SAME single global group (no GROUP BY key at all) — a second, differently-
        // symbol'd row still retracts and re-asserts the one row rather than starting a second group.
        var d2All = execAll.onStreamEvent("trades", evt2);
        var d2None = execNone.onStreamEvent("trades", evt2);
        assertEquals(d2None.size(), d2All.size());
        for (int i = 0; i < d2None.size(); i++) {
            assertEquals(d2None.get(i).weight(), d2All.get(i).weight());
            assertEquals(d2None.get(i).row().get("cnt"), d2All.get(i).row().get("cnt"));
        }
    }

    @Test
    void groupByAllInsideACteCompilesAndAggregatesCorrectly() {
        var sql = "WITH agg AS (SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL) " +
                  "SELECT agg.symbol, agg.side, agg.cnt FROM agg";
        var r = compileTable(sql, TRADES_WITH_SIDE);
        assertTrue(r.ok(), join(r.diagnostics()));

        var exec = r.plan().createExecutor();
        var deltas = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "side", "BUY", "price", 1.0));
        assertEquals(1, deltas.size());
        var d = deltas.get(0);
        assertEquals(1L, d.weight());
        assertEquals("AAPL", d.row().get("symbol"));
        assertEquals("BUY", d.row().get("side"));
        assertEquals(1L, d.row().get("cnt"));
    }

    @Test
    void groupByAllInsideADerivedTableCompilesAndAggregatesCorrectly() {
        var sql = "SELECT d.symbol, d.side, d.cnt FROM (SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL) d";
        var r = compileTable(sql, TRADES_WITH_SIDE);
        assertTrue(r.ok(), join(r.diagnostics()));

        var exec = r.plan().createExecutor();
        var deltas = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "side", "BUY", "price", 1.0));
        assertEquals(1, deltas.size());
        var d = deltas.get(0);
        assertEquals(1L, d.weight());
        assertEquals("AAPL", d.row().get("symbol"));
        assertEquals(1L, d.row().get("cnt"));
    }

    // ------------------------------------------------------------------
    // Pipeline mode: same expansion, plus the WINDOW requirement the existing rule enforces.
    // ------------------------------------------------------------------

    @Test
    void pipelineModeGroupByAllWithoutWindowIsStillAnError() {
        // GroupByWithoutWindowIsError (ValidatorTests) covers the explicit form — this proves the same
        // rule fires for the expanded form (q.GroupBy is non-null either way by the time the check runs).
        var r = compile("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "GROUP BY requires a WINDOW clause"));
    }

    @Test
    void pipelineModeAllExpansionMatchesExplicitGroupByPlanSummaryAndResults() {
        var sqlAll = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var sqlExplicit = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";

        var rAll = compile(sqlAll, TRADES);
        var rExplicit = compile(sqlExplicit, TRADES);
        assertTrue(rAll.ok(), join(rAll.diagnostics()));
        assertTrue(rExplicit.ok(), join(rExplicit.diagnostics()));
        assertEquals(rExplicit.planSummary(), rAll.planSummary());

        var execAll = rAll.plan().createExecutor();
        var execExplicit = rExplicit.plan().createExecutor();

        execAll.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L, "active", true));
        execAll.onEvent("trades", evt(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 20L, "active", true));
        execExplicit.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 10L, "active", true));
        execExplicit.onEvent("trades", evt(2000, "trades", "symbol", "AAPL", "price", 1.0, "qty", 20L, "active", true));

        var allClosed = execAll.advanceWatermark(12000);
        var explicitClosed = execExplicit.advanceWatermark(12000);
        assertEquals(1, allClosed.size());
        assertEquals(1, explicitClosed.size());
        var closedAll = allClosed.get(0);
        var closedExplicit = explicitClosed.get(0);
        assertEquals(closedExplicit.get("symbol"), closedAll.get("symbol"));
        assertEquals(closedExplicit.get("cnt"), closedAll.get("cnt"));
        assertEquals(closedExplicit.get("window_start"), closedAll.get("window_start"));
        assertEquals(closedExplicit.get("window_end"), closedAll.get("window_end"));
    }

    @Test
    void groupByAllInsideAPipelineDerivedTableCompiles() {
        var sql = "SELECT d.avg_px FROM (SELECT AVG(price) AS avg_px FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("avg_px"));
    }
}
