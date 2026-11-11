package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 009 Round C wave C1 — Validator-level coverage of TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/TO_STRING: they're
 * KnownFunctions, arity-checked (exactly one argument), and each has a fixed result FieldKind independent of the
 * argument's own kind — plus the CAST(expr AS type) sugar's parser-level acceptance/rejection. Runtime evaluation
 * semantics live in {@link TypeConversionFunctionsTests}; this file is result-kind/arity/diagnostics only.
 */
class TypeConversionValidatorTests {
    private static final SourceSchema MIXED = schema(
        "mixed",
        "s", FieldKind.STRING, "l", FieldKind.LONG, "d", FieldKind.DOUBLE,
        "b", FieldKind.BOOL, "j", FieldKind.JSON);

    private static String msgs(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::message).collect(Collectors.joining(";"));
    }

    private static boolean has(List<SqlDiagnostic> ds, String text) {
        return ds.stream().anyMatch(d -> d.message().contains(text));
    }

    @ParameterizedTest
    @CsvSource({
        "TO_LONG(s),LONG",
        "TO_DOUBLE(s),DOUBLE",
        "TO_BOOL(s),BOOL",
        "TO_TIMESTAMP(s),TIMESTAMP",
        "TO_STRING(l),STRING"})
    void resultKindIsFixedByFunctionNameNotArgumentKind(String expr, FieldKind expectedKind) {
        var r = compile("SELECT " + expr + " AS x FROM mixed", MIXED);
        assertTrue(r.ok(), msgs(r.diagnostics()));
        assertEquals(expectedKind, r.outputSchema().fields().get("x"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"TO_LONG", "TO_DOUBLE", "TO_BOOL", "TO_TIMESTAMP", "TO_STRING"})
    void eachFunctionRequiresExactlyOneArgument(String fn) {
        var zero = compile("SELECT " + fn + "() AS x FROM mixed", MIXED);
        assertFalse(zero.ok());
        assertTrue(has(zero.diagnostics(), "wrong number of arguments"));

        var two = compile("SELECT " + fn + "(s, l) AS x FROM mixed", MIXED);
        assertFalse(two.ok());
        assertTrue(has(two.diagnostics(), "wrong number of arguments"));
    }

    @Test
    void functionsAcceptAJsonKindArgumentFromAnArrowAccess() {
        // Plan's own motivating case: a terminal '->' result is FieldKind.Json — these functions must
        // accept it directly (no "extract with ->> first" diagnostic the way bare comparisons get).
        var r = compile("SELECT TO_DOUBLE(j -> 'qty') AS x FROM mixed", MIXED);
        assertTrue(r.ok(), msgs(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("x"));
    }

    @Test
    void tableModeAcceptsTheSameFunctions() {
        var r = compileTable("SELECT TO_LONG(s) AS x FROM mixed", MIXED);
        assertTrue(r.ok(), msgs(r.diagnostics()));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("x"));
    }

    // ------------------------------------------------------------------
    // CAST(expr AS type) sugar — desugars to the same FunctionCallExpr the TO_* spelling produces
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "CAST(s AS STRING),STRING",
        "CAST(s AS TEXT),STRING",
        "CAST(s AS DOUBLE),DOUBLE",
        "CAST(s AS DOUBLE PRECISION),DOUBLE",
        "CAST(s AS LONG),LONG",
        "CAST(s AS BIGINT),LONG",
        "CAST(s AS INT),LONG",
        "CAST(s AS BOOL),BOOL",
        "CAST(s AS BOOLEAN),BOOL",
        "CAST(s AS TIMESTAMP),TIMESTAMP"})
    void castSugarAcceptsDialectAndCommonSqlSpellings(String expr, FieldKind expectedKind) {
        var r = compile("SELECT " + expr + " AS x FROM mixed", MIXED);
        assertTrue(r.ok(), msgs(r.diagnostics()));
        assertEquals(expectedKind, r.outputSchema().fields().get("x"));
    }

    @Test
    void castSugarRejectsAnUnknownTargetType() {
        var r = compile("SELECT CAST(s AS BANANA) AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown CAST target type"));
    }

    @Test
    void castSugarRejectsJsonAsATarget() {
        // No TO_JSON function exists in this wave — JSON only ever arises from '->' access — so JSON
        // is not a legal CAST target; the parser reports it the same way as any other unknown type.
        var r = compile("SELECT CAST(s AS JSON) AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(has(r.diagnostics(), "Unknown CAST target type"));
    }

    @Test
    void bareCastIdentifierWithoutParensIsStillAPlainColumnReference() {
        // "CAST" is only intercepted when immediately followed by '(' — this proves it doesn't become
        // a reserved word that breaks a hypothetical column/alias literally named "cast".
        var castNamed = schema("castNamed", "cast", FieldKind.STRING);
        var r = compile("SELECT cast FROM castNamed", castNamed);
        assertTrue(r.ok(), msgs(r.diagnostics()));
    }
}
