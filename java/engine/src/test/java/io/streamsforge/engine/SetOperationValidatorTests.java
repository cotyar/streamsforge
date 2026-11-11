package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — branch-compatibility validation for {@code UNION}/{@code UNION ALL}: arity, positional kind
 * unification (Long+Double -&gt; Double, everything else must match exactly, Json/Timestamp only with themselves),
 * output column naming from branch 0, pipeline-mode UNION (distinct) rejection naming UNION ALL as the fix, and
 * end-to-end confirmation that IN/EXISTS/scalar-subquery position rejects a set operation.
 */
class SetOperationValidatorTests {
    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    private static final SourceSchema[] NONE = new SourceSchema[0];

    // Local fixtures — deliberately different shapes from TestHelpers' own schemas.
    private static final SourceSchema ONE_STRING_COL = schema("one_col", "a", FieldKind.STRING);
    private static final SourceSchema TWO_STRING_COLS = schema("two_col", "a", FieldKind.STRING, "b", FieldKind.STRING);
    private static final SourceSchema LONG_COL = schema("long_src", "a", FieldKind.LONG);
    private static final SourceSchema DOUBLE_COL = schema("double_src", "a", FieldKind.DOUBLE);
    private static final SourceSchema STRING_COL = schema("string_src", "a", FieldKind.STRING);
    private static final SourceSchema BOOL_COL = schema("bool_src", "a", FieldKind.BOOL);
    private static final SourceSchema JSON_COL = schema("json_src", "a", FieldKind.JSON);
    private static final SourceSchema TIMESTAMP_COL = schema("ts_src", "a", FieldKind.TIMESTAMP);

    // ------------------------------------------------------------------
    // Arity
    // ------------------------------------------------------------------

    @Test
    void arityMismatchIsRejected() {
        var r = compileTable("SELECT a FROM one_col UNION ALL SELECT a, b FROM two_col", NONE, new SourceSchema[] {ONE_STRING_COL, TWO_STRING_COLS});
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "same number of columns"));
    }

    @Test
    void equalAritySucceeds() {
        var r = compileTable("SELECT a FROM long_src UNION ALL SELECT a FROM long_src", NONE, new SourceSchema[] {LONG_COL});
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    // ------------------------------------------------------------------
    // Kind unification
    // ------------------------------------------------------------------

    @Test
    void longAndDoubleUnifyToDouble() {
        var r = compileTable("SELECT a FROM long_src UNION ALL SELECT a FROM double_src", NONE, new SourceSchema[] {LONG_COL, DOUBLE_COL});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("a"));
    }

    @Test
    void doubleAndLongUnifyToDoubleRegardlessOfBranchOrder() {
        var r = compileTable("SELECT a FROM double_src UNION ALL SELECT a FROM long_src", NONE, new SourceSchema[] {DOUBLE_COL, LONG_COL});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("a"));
    }

    @Test
    void identicalKindsUnifyToThemselves() {
        var r = compileTable("SELECT a FROM string_src UNION ALL SELECT a FROM string_src", NONE, new SourceSchema[] {STRING_COL});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("a"));
    }

    @Test
    void stringAndLongDoNotUnify() {
        var r = compileTable("SELECT a FROM string_src UNION ALL SELECT a FROM long_src", NONE, new SourceSchema[] {STRING_COL, LONG_COL});
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "not compatible"));
    }

    @Test
    void boolAndStringDoNotUnify() {
        var r = compileTable("SELECT a FROM bool_src UNION ALL SELECT a FROM string_src", NONE, new SourceSchema[] {BOOL_COL, STRING_COL});
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "not compatible"));
    }

    @Test
    void jsonOnlyUnifiesWithJson() {
        var ok = compileTable("SELECT a FROM json_src UNION ALL SELECT a FROM json_src", NONE, new SourceSchema[] {JSON_COL});
        assertTrue(ok.ok(), join(ok.diagnostics()));

        var bad = compileTable("SELECT a FROM json_src UNION ALL SELECT a FROM string_src", NONE, new SourceSchema[] {JSON_COL, STRING_COL});
        assertFalse(bad.ok());
        assertTrue(has(bad.diagnostics(), "not compatible"));
    }

    @Test
    void timestampOnlyUnifiesWithTimestamp() {
        var ok = compileTable("SELECT a FROM ts_src UNION ALL SELECT a FROM ts_src", NONE, new SourceSchema[] {TIMESTAMP_COL});
        assertTrue(ok.ok(), join(ok.diagnostics()));

        var bad = compileTable("SELECT a FROM ts_src UNION ALL SELECT a FROM long_src", NONE, new SourceSchema[] {TIMESTAMP_COL, LONG_COL});
        assertFalse(bad.ok());
        assertTrue(has(bad.diagnostics(), "not compatible"));
    }

    // ------------------------------------------------------------------
    // Output column names come from branch 0
    // ------------------------------------------------------------------

    @Test
    void outputColumnNameIsTakenFromBranchZero() {
        var sql = "SELECT a AS renamed FROM string_src UNION ALL SELECT a FROM string_src";
        var r = compileTable(sql, NONE, new SourceSchema[] {STRING_COL});
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.outputSchema().fields().containsKey("renamed"));
        assertFalse(r.outputSchema().fields().containsKey("a"));
    }

    // ------------------------------------------------------------------
    // Pipeline-mode UNION (distinct) is rejected, naming UNION ALL as the fix
    // ------------------------------------------------------------------

    @Test
    void pipelineModeUnionWithoutAllIsRejected() {
        var r = compile("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", TRADES, QUOTES);
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("pipeline mode")).toList();
        assertEquals(1, matching.size());
        assertTrue(matching.get(0).message().contains("UNION ALL"));
    }

    @Test
    void pipelineModeUnionAllSucceeds() {
        var r = compile("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void tableModeUnionWithoutAllSucceeds() {
        var r = compileTable("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void derivedTablePositionUnionInPipelineModeAlsoRejectsWithoutAll() {
        var sql = "SELECT u.symbol FROM (SELECT symbol FROM trades UNION SELECT symbol FROM quotes) u";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("pipeline mode") && d.message().contains("UNION ALL")));
    }

    // ------------------------------------------------------------------
    // Plan summary names the set operation
    // ------------------------------------------------------------------

    @Test
    void planSummaryDescribesTheSetOperation() {
        var r = compile("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.planSummary().contains("UNION ALL"));
        assertTrue(r.planSummary().contains("SELECT 1"));
    }

    @Test
    void tableModePlanSummaryDistinguishesPlainUnionFromUnionAll() {
        var r = compileTable("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
        assertTrue(r.planSummary().contains("UNION"));
        assertFalse(r.planSummary().contains("UNION ALL"));
    }

    // ------------------------------------------------------------------
    // Subquery-position rejection, end to end (Ok=false with a positioned diagnostic)
    // ------------------------------------------------------------------

    @Test
    void unionInSubqueryPositionMakesTheWholeCompileFail() {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = compile(sql, TRADES, QUOTES, REF);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("UNION") && d.line() >= 1 && d.column() >= 1));
    }
}
