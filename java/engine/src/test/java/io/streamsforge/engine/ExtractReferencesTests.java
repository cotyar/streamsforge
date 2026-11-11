package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 016 wave 3-A — {@link SqlCompiler#extractReferences}. Every relation the statement READS FROM, as authored,
 * distinct by ordinal comparison, in first-appearance (reading) order; empty on any parse failure and empty on
 * "reads nothing"; and never an exception, for any input.
 */
class ExtractReferencesTests {
    private static void refs(List<String> expected, String sql) {
        assertEquals(expected, SqlCompiler.extractReferences(sql));
    }

    // ------------------------------------------------------------------
    // FROM / JOIN
    // ------------------------------------------------------------------

    @Test
    void plainFromReturnsTheOneRelation() {
        refs(List.of("trades"), "SELECT symbol FROM trades");
    }

    @Test
    void aliasIsNotTheReference() {
        refs(List.of("trades"), "SELECT t.symbol FROM trades t");
    }

    @Test
    void joinSourceIsReported() {
        refs(List.of("trades", "instruments"),
            "SELECT t.symbol FROM trades t JOIN instruments i WITHIN 1 MINUTE ON t.symbol = i.symbol");
    }

    @Test
    void everyJoinInAChainIsReported() {
        refs(List.of("trades", "instruments", "quotes"),
            "SELECT t.symbol FROM trades t " +
            "LEFT JOIN instruments i ON t.symbol = i.symbol " +
            "JOIN quotes q ON t.symbol = q.symbol");
    }

    @Test
    void crossJoinIsReported() {
        refs(List.of("trades", "instruments"), "SELECT * FROM trades CROSS JOIN instruments");
    }

    @Test
    void unnestJoinAddsNoRelationOfItsOwn() {
        refs(List.of("trades"), "SELECT t.symbol FROM trades t, UNNEST(t.legs) AS leg");
    }

    // ------------------------------------------------------------------
    // Subqueries — the defect the FROM-only regex had
    // ------------------------------------------------------------------

    @Test
    void derivedTableInFromReportsTheInnerRelationNotTheAlias() {
        refs(List.of("trades"), "SELECT d.symbol FROM ( SELECT symbol FROM trades ) d");
    }

    @Test
    void nestedDerivedTablesRecurse() {
        refs(List.of("trades"), "SELECT o.symbol FROM ( SELECT i.symbol FROM ( SELECT symbol FROM trades ) i ) o");
    }

    @Test
    void derivedTableJoinedToANamedSourceReportsBoth() {
        refs(List.of("trades", "instruments"),
            "SELECT t.symbol FROM trades t JOIN ( SELECT symbol FROM instruments ) i ON t.symbol = i.symbol");
    }

    @Test
    void inSubqueryInWhereIsReported() {
        refs(List.of("trades", "hot"), "SELECT symbol FROM trades WHERE symbol IN ( SELECT symbol FROM hot )");
    }

    @Test
    void notInSubqueryInWhereIsReported() {
        refs(List.of("trades", "banned"), "SELECT symbol FROM trades WHERE symbol NOT IN ( SELECT symbol FROM banned )");
    }

    @Test
    void existsSubqueryInWhereIsReported() {
        refs(List.of("trades", "hot"), "SELECT symbol FROM trades WHERE EXISTS ( SELECT symbol FROM hot )");
    }

    @Test
    void notExistsSubqueryInWhereIsReported() {
        refs(List.of("trades", "hot"), "SELECT symbol FROM trades WHERE NOT EXISTS ( SELECT symbol FROM hot )");
    }

    @Test
    void scalarSubqueryInWhereIsReported() {
        refs(List.of("trades", "limits"), "SELECT symbol FROM trades WHERE price > ( SELECT MAX(price) FROM limits )");
    }

    @Test
    void scalarSubqueryInSelectListIsReported() {
        refs(List.of("trades", "limits"), "SELECT symbol, ( SELECT MAX(price) FROM limits ) AS cap FROM trades");
    }

    // ------------------------------------------------------------------
    // Set operations — UNION family only; INTERSECT/EXCEPT do not exist in this grammar
    // ------------------------------------------------------------------

    @Test
    void unionAllBranchesAreAllReported() {
        refs(List.of("trades", "quotes"), "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes");
    }

    @Test
    void unionDistinctBranchesAreAllReported() {
        refs(List.of("trades", "quotes"), "SELECT symbol FROM trades UNION SELECT symbol FROM quotes");
    }

    @Test
    void threeBranchUnionReportsAllThree() {
        refs(List.of("a", "b", "c"), "SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c");
    }

    @Test
    void setOperationInDerivedTablePositionIsReported() {
        refs(List.of("trades", "quotes"),
            "SELECT u.symbol FROM ( SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes ) u");
    }

    /** This dialect has no INTERSECT and no EXCEPT — both fail to parse and come back EMPTY. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT x FROM a INTERSECT SELECT x FROM b",
        "SELECT x FROM a EXCEPT SELECT x FROM b"})
    void intersectAndExceptAreNotInThisGrammarAndYieldEmpty(String sql) {
        assertTrue(SqlCompiler.extractReferences(sql).isEmpty());
    }

    /** Mixing UNION and UNION ALL is a parser diagnostic, so it takes the empty path too. */
    @Test
    void mixedUnionKindsYieldEmpty() {
        assertTrue(SqlCompiler.extractReferences("SELECT x FROM a UNION ALL SELECT x FROM b UNION SELECT x FROM c").isEmpty());
    }

    // ------------------------------------------------------------------
    // CTEs — the parser substitutes them at parse time, so a CTE name is never a reference
    // ------------------------------------------------------------------

    @Test
    void cteNameIsNotAReferenceButItsBodyIs() {
        refs(List.of("trades"), "WITH recent AS ( SELECT symbol FROM trades ) SELECT * FROM recent");
    }

    @Test
    void chainedCtesResolveToTheirLeafRelations() {
        refs(List.of("trades"),
            "WITH a AS ( SELECT symbol FROM trades ), b AS ( SELECT symbol FROM a ) SELECT * FROM b");
    }

    @Test
    void cteReferencedTwiceIsStillOneReferenceToItsBody() {
        refs(List.of("trades"),
            "WITH r AS ( SELECT symbol FROM trades ) " +
            "SELECT l.symbol FROM r l JOIN r x ON l.symbol = x.symbol");
    }

    @Test
    void cteUnderAUnionIsSubstitutedInEveryBranch() {
        refs(List.of("trades", "quotes"),
            "WITH r AS ( SELECT symbol FROM trades ) " +
            "SELECT symbol FROM r UNION ALL SELECT symbol FROM quotes");
    }

    /** Documented edge: an UNREFERENCED CTE is dropped by the parser's substitution pass. */
    @Test
    void unreferencedCteBodyIsNotReported() {
        refs(List.of("quotes"), "WITH unused AS ( SELECT symbol FROM trades ) SELECT symbol FROM quotes");
    }

    // ------------------------------------------------------------------
    // Dedup / ordering
    // ------------------------------------------------------------------

    @Test
    void selfJoinReportsTheNameOnce() {
        refs(List.of("trades"), "SELECT a.symbol FROM trades a JOIN trades b ON a.symbol = b.symbol");
    }

    @Test
    void nameRepeatedAcrossUnionBranchesIsReportedOnce() {
        refs(List.of("trades", "quotes"),
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION ALL SELECT symbol FROM trades");
    }

    @Test
    void orderIsFirstAppearanceInReadingOrderFromThenJoinThenWhere() {
        refs(List.of("trades", "instruments", "hot"),
            "SELECT t.symbol FROM trades t JOIN instruments i ON t.symbol = i.symbol " +
            "WHERE t.symbol IN ( SELECT symbol FROM hot )");
    }

    /** Distinctness is ORDINAL: two spellings of the same relation both come back, exactly as authored. */
    @Test
    void distinctnessIsOrdinalSoCasingVariantsBothAppear() {
        refs(List.of("Trades", "trades"), "SELECT a.symbol FROM Trades a JOIN trades b ON a.symbol = b.symbol");
    }

    // ------------------------------------------------------------------
    // Never throws — empty and parse failure are the same answer, by design
    // ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "   ",
        "\t\n  ",
        "SELECT",
        "SELECT * FROM",
        "SELECT * FROM trades WHERE",
        "this is not sql at all",
        "SELECT * FROM trades ) ) )",
        "DROP TABLE trades",
        // A trailing ';' is an "Unexpected character" to this tokenizer — the dialect has no statement terminator.
        "SELECT symbol FROM trades;",
        "SELECT * FROM trades; SELECT * FROM quotes",
        "SELECT 'unterminated FROM trades",
        "INSERT INTO warehouse SELECT symbol FROM trades",
        "WITH r AS ( SELECT symbol FROM r ) SELECT * FROM r"})
    void badOrUnsupportedInputReturnsEmptyAndDoesNotThrow(String sql) {
        assertTrue(SqlCompiler.extractReferences(sql).isEmpty());
    }

    @Test
    void nullReturnsEmptyAndDoesNotThrow() {
        assertTrue(SqlCompiler.extractReferences(null).isEmpty());
    }

    /** The conflation stated as a test: a parse failure and a genuine "no references" are the same value. */
    @Test
    void parseFailureAndEmptyResultAreIndistinguishable() {
        var broken = SqlCompiler.extractReferences("SELECT * FROM");
        var empty = SqlCompiler.extractReferences("");
        assertEquals(empty, broken);
    }
}
