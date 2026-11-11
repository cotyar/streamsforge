package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.sql.Ast;
import io.streamsforge.engine.sql.Parser;
import io.streamsforge.engine.sql.Tokenizer;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — {@code UNION} / {@code UNION ALL} grammar. Parser-level tests exercise
 * {@link Parser#parseStatement} directly to pin down exactly which AST shape a set-operation chain parses to.
 */
class SetOperationParserTests {
    private static Parser.StatementResult parseStatement(String sql) {
        var tok = new Tokenizer(sql).tokenize();
        assertTrue(tok.diagnostics().isEmpty());
        return Parser.parseStatement(tok.tokens());
    }

    private static String join(List<SqlDiagnostic> ds) {
        return ds.stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    // ------------------------------------------------------------------
    // Grammar shape
    // ------------------------------------------------------------------

    @Test
    void unionAllParsesToATwoBranchSetOperationQuery() {
        var r = parseStatement("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes");

        assertTrue(r.diagnostics().isEmpty());
        assertNull(r.query());
        assertNotNull(r.setOp());
        assertTrue(r.setOp().all);
        assertEquals(2, r.setOp().branches.size());
    }

    @Test
    void plainUnionParsesWithAllFalse() {
        var r = parseStatement("SELECT symbol FROM trades UNION SELECT symbol FROM quotes");

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.setOp());
        assertFalse(r.setOp().all);
    }

    @Test
    void unionDistinctIsSugarForPlainUnion() {
        var r = parseStatement("SELECT symbol FROM trades UNION DISTINCT SELECT symbol FROM quotes");

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.setOp());
        assertFalse(r.setOp().all);
    }

    @Test
    void threeBranchChainFlattensIntoOneSetOperationQuery() {
        var r = parseStatement(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION ALL SELECT symbol FROM ref");

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.setOp());
        assertEquals(3, r.setOp().branches.size());
    }

    @Test
    void noUnionKeywordParsesToAPlainSelectQueryNotASetOp() {
        var r = parseStatement("SELECT symbol FROM trades");

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.query());
        assertNull(r.setOp());
    }

    // ------------------------------------------------------------------
    // The silent-mis-parse bug this wave's ClauseKeywords fix prevents
    // ------------------------------------------------------------------

    @Test
    void trailingUnionKeywordIsNotSwallowedAsAnAsLessAlias() {
        var r = parseStatement("SELECT symbol FROM trades UNION SELECT symbol FROM quotes");

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.setOp());
        assertEquals(2, r.setOp().branches.size());
        assertNull(r.setOp().branches.get(0).select.items.get(0).alias);
    }

    // ------------------------------------------------------------------
    // Mixed ALL-ness is rejected, not silently resolved one way
    // ------------------------------------------------------------------

    @Test
    void mixingUnionAllAndPlainUnionInOneChainIsRejected() {
        var r = parseStatement(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION SELECT symbol FROM ref");

        assertNull(r.setOp());
        assertEquals(1, r.diagnostics().size());
        assertTrue(r.diagnostics().get(0).message().contains("Mixing UNION and UNION ALL"));
    }

    // ------------------------------------------------------------------
    // WITH ... UNION ... — CTE substitution must recurse into every branch
    // ------------------------------------------------------------------

    @Test
    void withCteMainQueryCanBeAUnionChain() {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT hot.symbol FROM hot UNION ALL SELECT symbol FROM quotes";
        var r = parseStatement(sql);

        assertTrue(r.diagnostics().isEmpty());
        assertNotNull(r.setOp());
        assertEquals(2, r.setOp().branches.size());
        // Branch 0's FROM item was substituted from a NamedSource("hot") into a DerivedSource wrapping the
        // CTE's own body — exactly the same desugaring a non-union WITH query gets.
        assertEquals(Ast.DerivedSource.class, r.setOp().branches.get(0).from.source.getClass());
    }

    @Test
    void withCteReferencedFromASecondBranchAlsoDesugars() {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM quotes UNION ALL SELECT hot.symbol FROM hot";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    // ------------------------------------------------------------------
    // Derived-table position: FROM ( ... UNION ... ) alias
    // ------------------------------------------------------------------

    @Test
    void derivedTablePositionAcceptsAUnionAll() {
        var r = parseStatement(
            "SELECT u.symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes) u");

        assertTrue(r.diagnostics().isEmpty());
        assertNull(r.setOp()); // the OUTER statement is a plain SelectQuery — the union sits inside its FROM
    }

    @Test
    void derivedTablePositionUnionEndToEndCompiles() {
        var sql = "SELECT u.symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes) u";
        var r = compile(sql, TRADES, QUOTES);
        assertTrue(r.ok(), join(r.diagnostics()));
    }

    @Test
    void derivedTablePositionUnionRequiresAnAliasLikeAnyOtherDerivedTable() {
        var sql = "SELECT symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes)";
        var r = compile(sql, TRADES, QUOTES);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("requires an alias")));
    }

    // ------------------------------------------------------------------
    // Out-of-scope positions: IN / EXISTS / scalar subquery — rejected with a positioned diagnostic
    // ------------------------------------------------------------------

    @Test
    void unionInsideInSubqueryIsRejected() {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = compile(sql, TRADES, QUOTES, REF);
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("UNION")).toList();
        assertEquals(1, matching.size());
        var d = matching.get(0);
        assertTrue(d.message().contains("IN (...)"));
        assertTrue(d.line() >= 1 && d.column() >= 1);
    }

    @Test
    void unionInsideExistsIsRejected() {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = compile(sql, TRADES, QUOTES, REF);
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("UNION")).toList();
        assertEquals(1, matching.size());
        assertTrue(matching.get(0).message().contains("EXISTS (...)"));
    }

    @Test
    void unionInsideScalarSubqueryIsRejected() {
        var sql = "SELECT symbol, (SELECT COUNT(*) FROM quotes UNION SELECT COUNT(*) FROM ref) AS c FROM trades";
        var r = compile(sql, TRADES, QUOTES, REF);
        assertFalse(r.ok());
        var matching = r.diagnostics().stream().filter(d -> d.message().contains("UNION")).toList();
        assertEquals(1, matching.size());
        assertTrue(matching.get(0).message().contains("scalar subquery"));
    }
}
