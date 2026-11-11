package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 008 wave 2b: the table-mode outer-join validator gate is lifted — LEFT, RIGHT, FULL, and CROSS all compile
 * now. What must NOT have moved: the WITHIN ban, the ON-clause requirement, and the equi-comparison requirement
 * all still fire, for every kind (including the newly-allowed ones) that isn't CROSS.
 */
class TableOuterJoinValidatorTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
    void outerJoinCompilesInTableMode(String kind) {
        var sql = "SELECT t.symbol, q.bid FROM trades t " + kind + " JOIN quotes q ON t.symbol = q.symbol";
        var r = compileTable(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void crossJoinCompilesInTableMode() {
        var sql = "SELECT t.symbol, q.bid FROM trades t CROSS JOIN quotes q";
        var r = compileTable(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL", ""}) // "" = plain INNER — same restriction, unaffected by plan 008
    void withinClauseStillForbiddenInTableMode(String kind) {
        var joinKw = kind.isEmpty() ? "JOIN" : kind + " JOIN";
        var sql = "SELECT t.symbol FROM trades t " + joinKw + " quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "WITHIN clause in table mode"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
    void outerJoinStillRequiresAnOnClause(String kind) {
        // Grammar-level for every non-CROSS join kind — omitting ON is caught by the parser itself
        // ("Expected 'ON'") before the Validator's own "requires an ON clause" diagnostic would even run.
        var sql = "SELECT t.symbol FROM trades t " + kind + " JOIN quotes q";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "ON"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
    void outerJoinStillRequiresAnEquiComparison(String kind) {
        var sql = "SELECT t.symbol FROM trades t " + kind + " JOIN quotes q ON t.price > q.bid";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "equi-comparison"));
    }

    @Test
    void crossJoinHasNoOnClauseAtAll_GrammarLevel() {
        // CROSS JOIN's grammar forbids ON outright — asserted only as a boundary marker.
        var sql = "SELECT t.symbol FROM trades t CROSS JOIN quotes q ON t.symbol = q.symbol";
        var r = compileTable(sql, TRADES, QUOTES);
        assertFalse(r.ok());
    }
}
