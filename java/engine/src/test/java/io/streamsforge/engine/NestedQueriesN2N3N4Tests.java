package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.containsMatch;
import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Plan 004 N2 ({@code [NOT] IN}/{@code [NOT] EXISTS} → semi/anti join), N3 (uncorrelated scalar subqueries), and N4
 * (single-level equality-correlated scalar subqueries, decorrelated to a GROUP-BY-k join) — grammar, validator
 * diagnostics, table-mode retraction-correct executor semantics, and pipeline-mode rolling-snapshot semantics.
 */
class NestedQueriesN2N3N4Tests {
    // ------------------------------------------------------------------
    // Parser / basic compile shape
    // ------------------------------------------------------------------

    @Test
    void inSubqueryParsesAndCompilesInTableMode() {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void notInSubqueryParsesAndCompiles() {
        var sql = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void existsParsesAndCompiles() {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void notExistsParsesAndCompiles() {
        var sql = "SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void scalarSubqueryInSelectListParsesAndCompiles() {
        var sql = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";
        var r = compileTable(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("rel"));
    }

    @Test
    void scalarSubqueryInWhereParsesAndCompiles() {
        var sql = "SELECT symbol FROM trades WHERE price > (SELECT AVG(price) FROM trades)";
        var r = compileTable(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void doublyParenthesizedScalarSubqueryCompiles() {
        var sql = "SELECT symbol FROM trades WHERE price > ((SELECT AVG(price) FROM trades))";
        var r = compileTable(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    // ------------------------------------------------------------------
    // Validator diagnostics
    // ------------------------------------------------------------------

    @Test
    void inSubqueryInSelectListIsRejected() {
        var sql = "SELECT symbol IN (SELECT symbol FROM ref) AS flag FROM trades";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("only allowed in WHERE"));
    }

    @Test
    void inSubqueryNestedInsideOrIsRejected() {
        var sql = "SELECT symbol FROM trades WHERE active = true OR symbol IN (SELECT symbol FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("top-level AND-connected"));
    }

    @Test
    void existsInOnClauseIsRejected() {
        var sql = "SELECT t.symbol FROM trades t JOIN ref r ON EXISTS (SELECT 1 FROM ref) AND t.symbol = r.symbol";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("only allowed in WHERE"));
    }

    @Test
    void nonWindowedInSubqueryInPipelineIsRejected() {
        var sql = "SELECT symbol, COUNT(*) AS c FROM trades WHERE symbol IN (SELECT symbol FROM ref) GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES, REF);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("must be windowed"));
    }

    @Test
    void nonAggregateScalarSubqueryIsRejected() {
        var sql = "SELECT symbol, (SELECT symbol FROM ref) AS x FROM trades";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("single-row aggregate query"));
    }

    @Test
    void scalarSubqueryWithOwnGroupByIsRejected() {
        var sql = "SELECT symbol, (SELECT AVG(price) FROM trades GROUP BY symbol) AS x FROM trades";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("may not have its own GROUP BY"));
    }

    @Test
    void correlationBeyondEqualityIsRejected() {
        var sql = "SELECT t.symbol, (SELECT AVG(r.price) FROM trades r WHERE r.symbol > t.symbol) AS x FROM trades t";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("rewrite as a JOIN"));
    }

    @Test
    void unknownCorrelatedColumnIsRejected() {
        var sql = "SELECT t.symbol, (SELECT AVG(r.price) FROM trades r WHERE r.symbol = t.bogus) AS x FROM trades t";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Unknown column 'bogus' on 't'"));
    }

    @Test
    void scalarSubqueryInGroupedSelectListIsRejected() {
        var sql = "SELECT symbol, SUM(qty), (SELECT AVG(price) FROM trades) AS avgall FROM trades GROUP BY symbol";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("grouped/windowed/aggregated"));
    }

    @Test
    void inSubqueryMustSelectExactlyOneColumnIsEnforced() {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol, tag FROM ref)";
        var r = compileTable(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("must select exactly one column"));
    }

    // ------------------------------------------------------------------
    // Table mode — semi/anti join (N2) retraction-correct executor semantics
    // ------------------------------------------------------------------

    @Test
    void semiJoinPassesRowsWhoseKeyIsPresentInB() {
        var sql = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "x"), 1));

        var passed = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var row = single(passed);
        assertEquals(1L, row.weight());
        assertEquals("AAPL", row.row().get("symbol"));

        var filtered = exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 1L, "active", true));
        assertTrue(filtered.isEmpty());
    }

    @Test
    void semiJoinBSideDuplicatesDoNotFanOutARows() {
        // Plan 004 N2: "duplicates in B must not duplicate A's rows — presence, not join fan-out."
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "x"), 1));
        var second = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "y"), 1));
        assertTrue(second.isEmpty()); // presence didn't flip (already present) — no re-emission from the 2nd B row either

        var passed = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var row = single(passed); // exactly ONE row despite B holding 2 AAPL contributions
        assertEquals(1L, row.weight());
    }

    @Test
    void semiJoinBRetractionCascadesFlippingARows() {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "x"), 1));
        var asserted = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        single(asserted, d -> d.weight() == 1);

        // Retracting B's last row for key "AAPL" must retract every currently-matching A row, even though
        // no NEW trades event arrived.
        var cascaded = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "x"), -1));
        var retraction = single(cascaded);
        assertEquals(-1L, retraction.weight());
        assertEquals("AAPL", retraction.row().get("symbol"));
    }

    @Test
    void notInWithANullInTheSubqueryFiltersNonMatchingRows() {
        // PostgreSQL three-valued logic: `'MSFT' NOT IN ('AAPL', NULL)` is NULL, so the row is filtered.
        // (Changed 2026-11-11 at the user's request: this used to pin plan 004's deviation, which ignored NULLs
        // in the subquery and let MSFT through. See PostgresNullSemanticsTests.)
        var sql = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "x"), 1));
        exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", null, "tag", "y"), 1));

        var result = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 1L, "active", true));
        assertTrue(result.isEmpty());
    }

    @Test
    void existsRetroactivelyAssertsPreviouslyFilteredRowsWhenBBecomesNonEmpty() {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        var beforeB = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        assertTrue(beforeB.isEmpty()); // ref is empty -> EXISTS is false -> filtered

        var afterB = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "ANYTHING", "tag", "x"), 1));
        var row = single(afterB);
        assertEquals(1L, row.weight());
        assertEquals("AAPL", row.row().get("symbol"));
    }

    @Test
    void notExistsPassesWhenBIsEmptyAndRetractsOnceBGetsARow() {
        var sql = "SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        var passed = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        single(passed, d -> d.weight() == 1);

        var retracted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "ANYTHING", "tag", "x"), 1));
        var row = single(retracted);
        assertEquals(-1L, row.weight());
        assertEquals("AAPL", row.row().get("symbol"));
    }

    // ------------------------------------------------------------------
    // Table mode — scalar subquery (N3) retraction-correct executor semantics
    // ------------------------------------------------------------------

    @Test
    void scalarSubqueryChangeCascadesRetractAssertOfEveryAffectedRow() {
        var sql = "SELECT q.symbol, q.bid - (SELECT AVG(price) FROM trades) AS rel FROM quotes q";
        var exec = compileTableAndCreate(sql, TRADES, QUOTES);

        // No trades yet -> the scalar subquery has no rows -> the quotes row finds no match (plain
        // singleton equi-join, no null-padding) and is dropped for now.
        var beforeAgg = exec.onStreamEvent("quotes", evt(1000, "quotes", "symbol", "AAPL", "bid", 100.0, "ask", 101.0));
        assertTrue(beforeAgg.isEmpty());

        // First trades row establishes the aggregate's first value (AVG=100) — this retroactively matches
        // the already-indexed quotes row.
        var firstAgg = exec.onStreamEvent("trades", evt(1100, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var firstMatch = single(firstAgg);
        assertEquals(1L, firstMatch.weight());
        assertEquals(0.0, firstMatch.row().get("rel"));

        // A second trades row changes the aggregate (AVG becomes 150) -> every affected output row must
        // retract its old value and assert the new one.
        var changed = exec.onStreamEvent("trades", evt(1200, "trades", "symbol", "MSFT", "price", 200.0, "qty", 1L, "active", true));
        assertEquals(2, changed.size());
        var retraction = single(changed, d -> d.weight() == -1);
        assertEquals(0.0, retraction.row().get("rel"));
        var assertion = single(changed, d -> d.weight() == 1);
        assertEquals(-50.0, assertion.row().get("rel")); // 100 - 150
    }

    // ------------------------------------------------------------------
    // Table mode — N4 decorrelation equivalence
    // ------------------------------------------------------------------

    private static String canonSnapshot(TableExecutor e) {
        return e.snapshot().values().stream().map(v -> {
            var withoutSource = new EventRecord(v.row());
            withoutSource.remove(EventRecord.SOURCE_FIELD);
            return v.weight() + ":" + JsonText.serializeCanonicalRow(withoutSource);
        }).sorted(Comparator.naturalOrder()).collect(Collectors.joining("|"));
    }

    @Test
    void n4DecorrelatedScalarSubqueryMatchesHandWrittenEquivalentJoin() {
        final String n4Sql = "SELECT t.symbol, t.price, (SELECT AVG(r.price) FROM trades r WHERE r.symbol = t.symbol) AS avgbysym FROM trades t";
        final String handWrittenSql = "WITH bysym AS (SELECT symbol, AVG(price) AS avgbysym FROM trades GROUP BY symbol) " +
                                      "SELECT t.symbol, t.price, b.avgbysym FROM trades t JOIN bysym b ON t.symbol = b.symbol";

        var n4Result = compileTable(n4Sql, TRADES);
        assertTrue(n4Result.ok(), () -> diag(n4Result.diagnostics()));
        var handWrittenResult = compileTable(handWrittenSql, TRADES);
        assertTrue(handWrittenResult.ok(), () -> diag(handWrittenResult.diagnostics()));

        var n4Exec = n4Result.plan().createExecutor();
        var handWrittenExec = handWrittenResult.plan().createExecutor();

        var events = List.of(
            evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true),
            evt(2000, "trades", "symbol", "AAPL", "price", 200.0, "qty", 1L, "active", true),
            evt(3000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 1L, "active", true));
        for (var evt : events) {
            n4Exec.onStreamEvent("trades", evt);
            handWrittenExec.onStreamEvent("trades", evt);
        }

        // _source differs by construction (N4's decorrelated join uses an internal-only synthetic alias
        // that never appears in the hand-written version's SourceLabel) — compare everything else.
        assertEquals(3, n4Exec.snapshot().size());
        assertEquals(canonSnapshot(handWrittenExec), canonSnapshot(n4Exec));
    }

    // ------------------------------------------------------------------
    // Pipeline mode — rolling-snapshot semantics (N2 membership, N3 scalar value)
    // ------------------------------------------------------------------

    @Test
    void pipelineSemiJoinCompileResultSourceNamesReportsRealLeafStreamsOnly() {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol IN (SELECT symbol FROM hot) GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(List.of("trades"), r.sourceNames());
    }

    @Test
    void acceptanceQuery_WithHotIn_NowFullyGreen() {
        // Plan 004's pinned acceptance example.
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t " +
                  "WHERE t.symbol IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = compileAndCreate(sql, TRADES);

        // Round 1: establish hot's membership snapshot (these rows themselves are filtered — hot hasn't
        // closed yet, so its membership snapshot is still empty when they arrive; the plan 004 N2 "rolling
        // snapshot" rule is explicit that arrival is tested against whatever is CURRENTLY live, not a
        // future snapshot).
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(1500, "trades", "symbol", "AAPL", "price", 200.0, "qty", 1L, "active", true));
        var duringClose = exec.advanceWatermark(11000); // closes hot's [0,10000) window -> snapshot = {AAPL}
        assertTrue(duringClose.isEmpty()); // nothing was ever admitted into the outer's own window state yet

        // Round 2: NOW test against the populated snapshot.
        exec.onEvent("trades", evt(12000, "trades", "symbol", "AAPL", "price", 300.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(12500, "trades", "symbol", "MSFT", "price", 999.0, "qty", 1L, "active", true)); // MSFT never in hot's snapshot
        var results = exec.advanceWatermark(20000); // closes outer [10000,15000)

        var row = single(results);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(300.0, row.get("px"));
    }

    @Test
    void pipelineMembershipSnapshotIsReplacedWholesaleOnEachWindowClose() {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = compileAndCreate(sql, TRADES);

        // Window 1 [0,10000): only AAPL present.
        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.advanceWatermark(11000); // snapshot = {AAPL}

        // Window 2 [10000,20000): only MSFT present — the snapshot after this close must be REPLACED
        // (AAPL is no longer a member), not merged with window 1's membership.
        exec.onEvent("trades", evt(15000, "trades", "symbol", "MSFT", "price", 40.0, "qty", 1L, "active", true));
        exec.advanceWatermark(21000); // snapshot = {MSFT} (replaces {AAPL})

        exec.onEvent("trades", evt(22000, "trades", "symbol", "AAPL", "price", 500.0, "qty", 1L, "active", true)); // AAPL no longer a member -> filtered
        exec.onEvent("trades", evt(22500, "trades", "symbol", "MSFT", "price", 60.0, "qty", 1L, "active", true)); // MSFT still a member -> passes
        var results = exec.advanceWatermark(30000); // closes outer [20000,25000)

        var row = single(results);
        assertEquals("MSFT", row.get("symbol"));
        assertEquals(60.0, row.get("px"));
    }

    @Test
    void pipelineAntiJoinSnapshotFiltersMembersAndPassesNonMembers() {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol NOT IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.advanceWatermark(11000); // hot snapshot = {AAPL}

        exec.onEvent("trades", evt(12000, "trades", "symbol", "AAPL", "price", 300.0, "qty", 1L, "active", true)); // member -> filtered by NOT IN
        exec.onEvent("trades", evt(12500, "trades", "symbol", "MSFT", "price", 70.0, "qty", 1L, "active", true)); // non-member -> passes
        var results = exec.advanceWatermark(20000);

        var row = single(results);
        assertEquals("MSFT", row.get("symbol"));
        assertEquals(70.0, row.get("px"));
    }

    @Test
    void pipelineScalarSubquerySnapshotValueUsedAtArrival() {
        // Deliberately unwindowed at the OUTER level: a scalar subquery can't sit in the SELECT list of a
        // grouped/windowed/aggregated pipeline, but an UNWINDOWED outer pipeline projects immediately per
        // event and is fine.
        var sql = "SELECT q.symbol, q.bid - (SELECT AVG(price) FROM trades WINDOW TUMBLING(SIZE 10 SECONDS)) AS rel FROM quotes q";
        var r = compile(sql, QUOTES, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        var exec = r.plan().createExecutor();

        // Before the inner window ever closes, the scalar's snapshot is empty -> NULL-padded (still emits;
        // a scalar subquery's value being NULL is not a row filter).
        var beforeClose = exec.onEvent("quotes", evt(500, "quotes", "symbol", "AAPL", "bid", 150.0, "ask", 151.0));
        var beforeRow = single(beforeClose);
        assertNull(beforeRow.get("rel"));

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.advanceWatermark(11000); // closes trades' [0,10000) window -> scalar snapshot value = 100

        var results = exec.onEvent("quotes", evt(12000, "quotes", "symbol", "AAPL", "bid", 150.0, "ask", 151.0));
        var row = single(results);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(50.0, row.get("rel")); // 150 - 100
    }

    // ------------------------------------------------------------------
    // Determinism replay — table mode, semi-join + scalar subquery combined
    // ------------------------------------------------------------------

    private static final String DETERMINISM_SQL =
        "SELECT t.symbol, t.price, (SELECT AVG(price) FROM trades) AS avgall FROM trades t WHERE t.symbol IN (SELECT symbol FROM ref)";

    private static List<List<TableDelta>> runDeterminismBatchSequence(TableExecutor exec) {
        var out = new ArrayList<List<TableDelta>>();
        out.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1)));
        out.add(exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true)));
        out.add(exec.onStreamEvent("trades", evt(1500, "trades", "symbol", "MSFT", "price", 5.0, "qty", 1L, "active", true))); // filtered by IN (MSFT not in ref)
        out.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1)));
        out.add(exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 3L, "active", true)));
        out.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // retraction cascades through the semi-join
        return out;
    }

    private static String canonDelta(List<TableDelta> deltas) {
        return deltas.stream().map(d -> d.weight() + ":" + JsonText.serializeCanonicalRow(d.row())).collect(Collectors.joining("|"));
    }

    @Test
    void determinismReplay_SemiJoinPlusScalarSubquery_ReplaysIdenticallyAcrossFreshExecutors() {
        var compileResult = compileTable(DETERMINISM_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileResult.ok(), () -> diag(compileResult.diagnostics()));
        var plan = compileResult.plan();

        var runs = IntStream.range(0, 3).mapToObj(_i -> runDeterminismBatchSequence(plan.createExecutor())).toList();

        var canonical = runs.stream()
            .map(r -> r.stream().map(NestedQueriesN2N3N4Tests::canonDelta).collect(Collectors.joining(";")))
            .distinct().toList();
        assertEquals(1, canonical.size());
    }
}
