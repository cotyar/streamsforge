package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TableValidatorTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    @Test
    void windowClauseIsForbiddenInTableMode() {
        var r = compileTable("SELECT symbol FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "WINDOW clause not allowed in table mode"));
    }

    @Test
    void emitIsForbiddenInTableMode() {
        var r = compileTable("SELECT symbol FROM trades EMIT CHANGES", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "EMIT not allowed in table mode"));
    }

    @Test
    void aggregatesAndGroupByAreAllowedWithoutWindowInTableMode() {
        var r = compileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void globalAggregateWithoutGroupByIsAllowedInTableMode() {
        var r = compileTable("SELECT COUNT(*) AS cnt, AVG(price) AS avgp FROM trades", TRADES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void ambiguousNamePresentInBothStreamsAndTablesIsError() {
        var tradesAsTable = schema("trades", "symbol", FieldKind.STRING);
        var r = compileTable("SELECT symbol FROM trades", new SourceSchema[] {TRADES}, new SourceSchema[] {tradesAsTable});
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Ambiguous name 'trades'"));
    }

    @Test
    void unknownSourceListsBothStreamsAndTables() {
        var positions = schema("positions", "symbol", FieldKind.STRING);
        var r = compileTable("SELECT symbol FROM bogus", new SourceSchema[] {TRADES}, new SourceSchema[] {positions});
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("Unknown source 'bogus'")).toList();
        assertEquals(1, matching.size());
        var d = matching.get(0);
        assertTrue(d.message().contains("trades"));
        assertTrue(d.message().contains("positions"));
    }

    @Test
    void withinClauseOnJoinIsForbiddenInTableMode() {
        var sql = "SELECT t.symbol FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "WITHIN clause in table mode"));
    }

    @Test
    void innerJoinRequiresEquiComparisonInTableMode() {
        var sql = "SELECT t.symbol FROM trades t JOIN quotes q ON t.price > q.bid";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "equi-comparison"));
    }

    @Test
    void innerJoinWithoutWithinCompilesInTableMode() {
        var sql = "SELECT t.symbol, q.bid FROM trades t JOIN quotes q ON t.symbol = q.symbol";
        var r = compileTable(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.streamInputs().contains("trades"));
        assertTrue(r.streamInputs().contains("quotes"));
    }

    @Test
    void nonAggregateSelectItemMustAppearInGroupByWithoutWindow() {
        var sql = "SELECT symbol, price, COUNT(*) AS cnt FROM trades GROUP BY symbol";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Non-aggregate select item must appear in GROUP BY"));
    }

    @Test
    void qualifiedStarResolvesInTableMode() {
        var positions = schema("positions", "symbol", FieldKind.STRING, "trades", FieldKind.LONG);
        var sql = "SELECT p.* FROM positions p";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {positions});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.outputSchema().fields().containsKey("symbol"));
        assertTrue(r.outputSchema().fields().containsKey("trades"));
    }

    @Test
    void qualifiedStarUnknownAliasIsErrorInTableMode() {
        var r = compileTable("SELECT z.* FROM trades t", TRADES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown source/alias 'z'"));
    }

    @Test
    void tableInputResolvesSeparatelyFromStreamInput() {
        var positions = schema("positions", "symbol", FieldKind.STRING, "trades", FieldKind.LONG);
        var sql = "SELECT p.symbol, p.trades FROM positions p WHERE p.trades > 50";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {positions});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.streamInputs().isEmpty());
        assertTrue(r.tableInputs().contains("positions"));
    }
}
