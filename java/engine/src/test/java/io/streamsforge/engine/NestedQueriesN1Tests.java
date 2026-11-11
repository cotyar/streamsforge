package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.containsMatch;
import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Plan 004 N1 — derived tables ({@code FROM ( SELECT ... ) alias}) and WITH (CTEs, desugared to derived tables
 * at parse time). Covers grammar (parser), scope-stack validation, and end-to-end executor semantics for both
 * pipeline (windowed) and table (retraction-propagating) modes.
 */
class NestedQueriesN1Tests {
    // ------------------------------------------------------------------
    // Parser
    // ------------------------------------------------------------------

    @Test
    void derivedTableWithoutAliasIsRejected() {
        var r = compile("SELECT symbol FROM (SELECT symbol FROM trades)", TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("requires an alias"));
    }

    @Test
    void derivedTableInFromWithAliasCompiles() {
        var r = compile("SELECT d.symbol FROM (SELECT symbol FROM trades) d", TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void derivedTableCanAppearInJoinPosition() {
        var sql = "SELECT t.symbol, d.tag FROM trades t JOIN (SELECT symbol, tag FROM ref) d WITHIN 1 SECONDS ON t.symbol = d.symbol";
        var r = compile(sql, TRADES, REF);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void withSingleCteDesugarsToDerivedTable() {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT hot.symbol FROM hot";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void withMultipleCtesCanReferenceEarlierOnes() {
        var sql = "WITH a AS (SELECT symbol FROM trades), b AS (SELECT a.symbol FROM a) SELECT b.symbol FROM b";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void withSelfReferenceIsRejectedAsRecursion() {
        var sql = "WITH x AS (SELECT symbol FROM x) SELECT symbol FROM x";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Recursive or forward CTE reference"));
    }

    @Test
    void withForwardReferenceIsRejectedAsRecursion() {
        // 'a' references 'b', but 'b' is declared AFTER 'a' in the WITH list — forward reference.
        var sql = "WITH a AS (SELECT symbol FROM b), b AS (SELECT symbol FROM trades) SELECT symbol FROM a";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Recursive or forward CTE reference 'b'"));
    }

    @Test
    void duplicateCteNameInWithListIsRejected() {
        var sql = "WITH a AS (SELECT symbol FROM trades), a AS (SELECT symbol FROM trades) SELECT symbol FROM a";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Duplicate CTE name"));
    }

    // ------------------------------------------------------------------
    // Validator: scope stack
    // ------------------------------------------------------------------

    @Test
    void derivedAliasDuplicatingAnotherAliasIsError() {
        var sql = "SELECT * FROM trades t JOIN (SELECT symbol FROM ref) t ON true";
        var r = compile(sql, TRADES, REF);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Duplicate alias 't'"));
    }

    @Test
    void unknownColumnInsideDerivedQueryKeepsInnerPosition() {
        // 'bogus' is on line 1; its column offset is inside the parenthesized inner SELECT — inner
        // diagnostics must keep their own (already-absolute, single-token-stream) position, not get
        // rewritten to point at the outer FROM.
        var sql = "SELECT d.symbol FROM (SELECT bogus FROM trades) d";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        var diag = single(r.diagnostics(), d -> d.message().contains("Unknown column 'bogus'"));
        assertEquals(1, diag.line());
        assertEquals(30, diag.column()); // position of 'bogus' itself, not the derived table's '('
    }

    @Test
    void outerCannotSeeInnerDerivedTablesOwnAliases() {
        // 't' is an alias local to the derived query's own FROM — invisible at the outer level.
        var sql = "SELECT t.symbol FROM (SELECT symbol FROM trades t) d";
        var r = compile(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Unknown source 't'"));
    }

    @Test
    void derivedSubqueryIsUncorrelatedAndCannotSeeOuterAliases() {
        // N1 derived tables are uncorrelated: the inner query cannot reference the outer FROM's alias.
        var sql = "SELECT t.symbol FROM trades t JOIN (SELECT symbol FROM ref WHERE ref.symbol = t.symbol) d ON true";
        var r = compile(sql, TRADES, REF);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Unknown source 't'") || d.message().contains("Unknown column"));
    }

    @Test
    void qualifiedStarOverDerivedAliasExpandsInnerProjectionColumns() {
        var sql = "SELECT d.* FROM (SELECT symbol, price FROM trades) d";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("price"));
    }

    @Test
    void derivedTableOutputSchemaFeedsOuterColumnResolutionWithRenamedColumn() {
        var sql = "SELECT d.avg_px FROM (SELECT AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("avg_px"));
    }

    @Test
    void tableModeDerivedTableForbidsWindow() {
        var sql = "SELECT d.symbol FROM (SELECT symbol FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = compileTable(sql, TRADES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("WINDOW clause not allowed in table mode"));
    }

    // ------------------------------------------------------------------
    // Executor — pipeline mode
    // ------------------------------------------------------------------

    @Test
    void twoLevelWindowedPipelineThroughTheParser() {
        // Upgrade of plan 003 M1's hand-built PipelineComposabilityTests smoke test: same scenario, same
        // expected result, but now compiled from ONE piece of SQL through WITH — no manual row-forwarding.
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL) " +
                  "SELECT symbol, avg_px FROM hot WHERE avg_px > 50";
        var exec = compileAndCreate(sql, TRADES);

        var results = new ArrayList<EventRecord>();
        results.addAll(exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true)));
        results.addAll(exec.onEvent("trades", evt(1500, "trades", "symbol", "AAPL", "price", 200.0, "qty", 1L, "active", true)));
        results.addAll(exec.onEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 10.0, "qty", 1L, "active", true)));
        results.addAll(exec.onEvent("trades", evt(2500, "trades", "symbol", "MSFT", "price", 20.0, "qty", 1L, "active", true)));
        results.addAll(exec.advanceWatermark(12000));

        var aapl = single(results);
        assertEquals("AAPL", aapl.get("symbol"));
        assertEquals(150.0, aapl.get("avg_px"));
    }

    @Test
    void derivedCteFeedsAJoinInTheOuterChain() {
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL) " +
                  "SELECT h.symbol, h.avg_px, q.bid FROM hot h JOIN quotes q WITHIN 1 SECONDS ON h.symbol = q.symbol";
        var exec = compileAndCreate(sql, TRADES, QUOTES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        // Advance by the MINIMUM nowMs that closes hot's inner window (end=10000, 1s allowed lateness ->
        // nowMs=11000): this single AdvanceWatermark call drives both the inner executor's watermark
        // (closing its window) AND the outer's own (shared nowMs) — picking the minimum keeps the outer's
        // watermark from outrunning the still-to-arrive quotes event below, same trap any interval join
        // has always had (advancing too far makes a legitimately-on-time event look late).
        // No quotes have arrived yet, so the closed-window emission finds no match: same "no matching
        // quote yet" shape as plan 003's hand-built PipelineComposabilityTests's analogous join test.
        var closed = exec.advanceWatermark(11000);
        assertTrue(closed.isEmpty());

        var matched = exec.onEvent("quotes", evt(10200, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));
        var row = single(matched);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(100.0, row.get("avg_px"));
        assertEquals(99.0, row.get("bid"));
    }

    @Test
    void windowsInWindowsBucketsInnerEmissionsByWindowEndTimestamp() {
        // Inner: 5s tumbling windows close at t=5000 and t=10000. Outer: a wider 20s tumbling window over
        // the inner's emissions — both inner closes (ts 5000 and 10000) fall inside the SAME outer window
        // [0, 20000), so the outer COUNT(*) over hot's emissions should be 2 once both are closed and the
        // outer window itself closes.
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS) EMIT FINAL) " +
                  "SELECT symbol, COUNT(*) AS cnt FROM hot GROUP BY symbol WINDOW TUMBLING(SIZE 20 SECONDS) EMIT FINAL";
        var exec = compileAndCreate(sql, TRADES);

        exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        exec.onEvent("trades", evt(6000, "trades", "symbol", "AAPL", "price", 200.0, "qty", 1L, "active", true));

        var results = exec.advanceWatermark(30000); // closes both inner windows AND the outer window

        var row = single(results);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(2L, row.get("cnt"));
    }

    @Test
    void pipelineCompileResultSourceNamesReportsRealLeafStreamsNotSyntheticDerivedMarker() {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM hot";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(List.of("trades"), r.sourceNames());
    }

    // ------------------------------------------------------------------
    // Executor — table mode (retraction-correct)
    // ------------------------------------------------------------------

    @Test
    void tableModeDerivedJoinTargetPropagatesRetraction() {
        var sql = "SELECT t.symbol, d.tag FROM trades t JOIN (SELECT symbol, tag FROM ref) d ON t.symbol = d.symbol";
        var exec = compileTableAndCreate(sql, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});

        exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));

        var asserted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), 1));
        var assertedDelta = single(asserted);
        assertEquals(1L, assertedDelta.weight());
        assertEquals("watchlist", assertedDelta.row().get("tag"));
        assertEquals(1, exec.snapshot().size());

        var retracted = exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "watchlist"), -1));
        var retractedDelta = single(retracted);
        assertEquals(-1L, retractedDelta.weight());
        assertEquals("watchlist", retractedDelta.row().get("tag"));
        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void tableModeDerivedTablesMayNestTwoLevelsDeep() {
        var sql = "SELECT x.symbol FROM (SELECT y.symbol FROM (SELECT symbol FROM trades) y) x";
        var exec = compileTableAndCreate(sql, TRADES);

        var results = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true));
        var row = single(results);
        assertEquals("AAPL", row.row().get("symbol"));
        assertEquals(1L, row.weight());
    }

    @Test
    void tableModeDerivedGroupByRetractsOldGroupRowOnChange() {
        // The derived subquery itself runs a running GROUP BY — its own retraction/assertion pairs must
        // propagate through the outer (here: a trivial passthrough) unchanged.
        var sql = "SELECT d.symbol, d.total FROM (SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol) d";
        var exec = compileTableAndCreate(sql, TRADES);

        var first = exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true));
        var firstDelta = single(first);
        assertEquals(1L, firstDelta.weight());
        assertEquals(10L, firstDelta.row().get("total"));

        var second = exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "AAPL", "price", 101.0, "qty", 5L, "active", true));
        assertEquals(2, second.size());
        var retraction = single(second, d -> d.weight() == -1);
        assertEquals(10L, retraction.row().get("total"));
        var assertion = single(second, d -> d.weight() == 1);
        assertEquals(15L, assertion.row().get("total"));

        var snapshot = single(exec.snapshot().values());
        assertEquals(15L, snapshot.row().get("total"));
    }

    // ------------------------------------------------------------------
    // Determinism replay on a nested table plan (plan 003 M1's replay guarantee, re-proven for N1's
    // nested-TableExecutor derived-node wiring — a fresh outer TableExecutor also means a fresh nested
    // derived TableExecutor each run; nothing shared/static should leak between runs).
    // ------------------------------------------------------------------

    private static final String NESTED_TABLE_SQL = "SELECT t.symbol, d.tag, t.price FROM trades t JOIN (SELECT symbol, tag FROM ref) d ON t.symbol = d.symbol WHERE t.price > 10";

    private static List<List<TableDelta>> runNestedTableBatchSequence(TableExecutor exec) {
        var perCallOutputs = new ArrayList<List<TableDelta>>();
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 10L, "active", true)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(1500, "trades", "symbol", "AAPL", "price", 5.0, "qty", 1L, "active", true))); // fails WHERE
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 2)));
        perCallOutputs.add(exec.onStreamEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 50.0, "qty", 3L, "active", true)));
        perCallOutputs.add(exec.onTableDelta("ref", new TableDelta(evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1))); // retraction cascades through the derived source's join
        return perCallOutputs;
    }

    private static String canonDelta(List<TableDelta> deltas) {
        return deltas.stream().map(d -> d.weight() + ":" + JsonText.serializeCanonicalRow(d.row())).collect(Collectors.joining("|"));
    }

    @Test
    void nestedTablePlanReplaysDeterministicallyAcrossFreshExecutors() {
        var compileResult = compileTable(NESTED_TABLE_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileResult.ok(), () -> diag(compileResult.diagnostics()));
        var plan = compileResult.plan();

        var runs = IntStream.range(0, 3).mapToObj(_i -> runNestedTableBatchSequence(plan.createExecutor())).toList();

        var canonical = runs.stream()
            .map(r -> r.stream().map(NestedQueriesN1Tests::canonDelta).collect(Collectors.joining(";")))
            .distinct().toList();
        assertEquals(1, canonical.size()); // all three independent runs collapse to the same canonical sequence
    }

    @Test
    void acceptanceQueryN1Slice_WithHotJoinedAgainstOuterWindow() {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t JOIN hot h WITHIN 5 SECONDS ON t.symbol = h.symbol " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = compile(sql, TRADES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("px"));
    }
}
