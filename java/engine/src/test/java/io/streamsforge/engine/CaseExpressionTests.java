package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Searched {@code CASE WHEN … THEN … [ELSE …] END}, which the Parser desugars into nested three-argument
 * {@code IF} calls rather than a new AST node. These tests deliberately cover BOTH spellings for the same
 * behaviors.
 */
class CaseExpressionTests {
    private static final SourceSchema MIXED = schema(
        "mixed",
        "s", FieldKind.STRING, "l", FieldKind.LONG, "d", FieldKind.DOUBLE, "b", FieldKind.BOOL);

    private static Object evalSingle(String selectExpr, Object... fields) {
        var exec = compileAndCreate("SELECT " + selectExpr + " AS x FROM mixed", MIXED);
        var results = exec.onEvent("mixed", evt(1000, "mixed", fields));
        assertEquals(1, results.size());
        return results.get(0).get("x");
    }

    private static Object[] row(long l) {
        return new Object[] {"s", "a", "l", l, "d", 1.5, "b", true};
    }

    private static String msgs(CompileResult r) {
        return r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining(";"));
    }

    // ------------------------------------------------------------------
    // Evaluation
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"0,zero", "1,one", "9,many"})
    void first_matching_branch_wins_and_else_catches_the_rest(long l, String expected) {
        assertEquals(expected, evalSingle(
            "CASE WHEN l = 0 THEN 'zero' WHEN l = 1 THEN 'one' ELSE 'many' END", row(l)));
    }

    @Test
    void an_omitted_ELSE_yields_NULL_rather_than_a_missing_column() {
        assertNull(evalSingle("CASE WHEN l = 0 THEN 'zero' END", row(7)));
    }

    @Test
    void IF_is_the_same_function_the_desugar_targets() {
        assertEquals("hit", evalSingle("IF(l = 3, 'hit', 'miss')", row(3)));
        assertEquals("miss", evalSingle("IF(l = 3, 'hit', 'miss')", row(4)));
    }

    /** A NULL condition is not true, so it takes the else-branch. */
    @Test
    void a_null_condition_takes_the_else_branch() {
        var exec = compileAndCreate("SELECT CASE WHEN l = 1 THEN 'yes' ELSE 'no' END AS x FROM mixed", MIXED);
        var results = exec.onEvent("mixed", evt(1000, "mixed", "s", "a", "l", null, "d", 1.5, "b", true));
        assertEquals(1, results.size());
        assertEquals("no", results.get(0).get("x"));
    }

    /** The demo's actual shape: a numeric distance turned into a status label in SQL instead of in every client. */
    @Test
    void nested_branches_fold_right_and_evaluate_in_written_order() {
        final String sql = "CASE WHEN l <= 0 THEN 'BREACHED' WHEN l <= 2 THEN 'WATCH' ELSE 'OK' END";
        assertEquals("BREACHED", evalSingle(sql, row(-1)));
        assertEquals("WATCH", evalSingle(sql, row(2)));
        assertEquals("OK", evalSingle(sql, row(3)));
    }

    // ------------------------------------------------------------------
    // Typing
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
        "CASE WHEN b THEN s ELSE 'x' END|STRING",
        "CASE WHEN b THEN l ELSE 0 END|LONG",
        "CASE WHEN b THEN d ELSE 0.0 END|DOUBLE",
        "CASE WHEN b THEN l ELSE d END|DOUBLE", // widens, as mixed arithmetic does
        "CASE WHEN b THEN l END|LONG"})         // implicit NULL else defers
    void result_kind_is_the_branches_common_kind(String expr, FieldKind expected) {
        var r = compile("SELECT " + expr + " AS x FROM mixed", MIXED);
        assertTrue(r.ok(), msgs(r));
        assertEquals(expected, r.outputSchema().fields().get("x"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CASE WHEN b THEN s ELSE l END", "IF(b, s, l)"})
    void branches_that_disagree_on_type_are_rejected(String expr) {
        var r = compile("SELECT " + expr + " AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("same type")));
    }

    @Test
    void a_non_boolean_condition_is_rejected_rather_than_always_taking_the_else_branch() {
        var r = compile("SELECT CASE WHEN s THEN 1 ELSE 2 END AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("must be a boolean")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"IF(b, 1)", "IF(b, 1, 2, 3)"})
    void IF_takes_exactly_three_arguments(String expr) {
        var r = compile("SELECT " + expr + " AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("wrong number of arguments")));
    }

    // ------------------------------------------------------------------
    // Parsing edges
    // ------------------------------------------------------------------

    /** CASE is intercepted only when the very next token is WHEN, so a column named "case" still resolves. */
    @Test
    void a_column_named_case_still_parses_as_an_identifier() {
        var schema = schema("t", "case", FieldKind.STRING);
        var r = compile("SELECT case AS x FROM t", schema);
        assertTrue(r.ok(), msgs(r));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("x"));
    }

    @Test
    void an_unterminated_CASE_is_a_diagnostic_not_a_crash() {
        var r = compile("SELECT CASE WHEN b THEN 1 AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertFalse(r.diagnostics().isEmpty());
    }
}
