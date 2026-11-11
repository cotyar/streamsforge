package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — {@code UNION ALL} end to end in PIPELINE mode. A union-root PipelineExecutor is structurally "N
 * nested executors, no join/filter/project/window stage of its own, concatenate emissions"; these tests exercise
 * that fan-out/concatenation shape live: three branches, the same source feeding two of them, and no dedup.
 */
class UnionAllPipelineTests {
    // Branch 0: trades.price AS amt. Branch 1: quotes.bid AS amt. Branch 2: quotes.ask AS amt — the SAME
    // source ("quotes") feeds two different branches, proving fan-out; three branches overall proves the
    // chain isn't hardcoded to two.
    private static final String THREE_BRANCH_SQL =
        "SELECT symbol, price AS amt FROM trades " +
        "UNION ALL SELECT symbol, bid AS amt FROM quotes " +
        "UNION ALL SELECT symbol, ask AS amt FROM quotes";

    @Test
    void compilesAndReportsAUnifiedTwoColumnOutputSchema() {
        var r = compile(THREE_BRANCH_SQL, TRADES, QUOTES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(2, r.outputSchema().fields().size());
        assertEquals(FieldKind.STRING, r.outputSchema().fields().get("symbol"));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("amt"));
    }

    @Test
    void eventOnASourceUsedByOnlyOneBranchProducesExactlyOneRow() {
        var exec = compileAndCreate(THREE_BRANCH_SQL, TRADES, QUOTES);

        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 101.5, "qty", 1L, "active", true));

        var row = single(results);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(101.5, row.get("amt"));
    }

    @Test
    void eventOnASourceSharedByTwoBranchesFansOutToBoth() {
        // "quotes" feeds branch 1 (bid AS amt) AND branch 2 (ask AS amt) — a SINGLE incoming quotes event
        // must reach both nested branch executors and produce one row from each.
        var exec = compileAndCreate(THREE_BRANCH_SQL, TRADES, QUOTES);

        var results = exec.onEvent("quotes", evt(1000, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));

        assertEquals(2, results.size());
        var amounts = results.stream().map(r -> (double) r.get("amt")).sorted().toList();
        assertEquals(List.of(99.0, 101.0), amounts);
        results.forEach(r -> assertEquals("AAPL", r.get("symbol")));
    }

    @Test
    void noDedupIdenticalRowsFromDifferentBranchesBothAppear() {
        // Two branches over the SAME underlying source, with WHERE clauses that both admit the same row —
        // UNION ALL in pipeline mode never dedups (there is no Z-set weight to dedup with).
        var sql = "SELECT symbol FROM trades WHERE price > 0 UNION ALL SELECT symbol FROM trades WHERE qty > 0";
        var exec = compileAndCreate(sql, TRADES);

        var results = exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true));

        assertEquals(2, results.size());
        results.forEach(r -> assertEquals("AAPL", r.get("symbol")));
    }

    @Test
    void advanceWatermarkDrivesEveryBranchAndClosesWindowedBranches() {
        // Branch 0 is windowed (needs AdvanceWatermark to close and emit); branch 1 is a plain unwindowed
        // passthrough (emits immediately on OnEvent). The union root must drive BOTH branches' own
        // AdvanceWatermark and concatenate whatever each one emits.
        var sql =
            "SELECT symbol, AVG(price) AS amt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL " +
            "UNION ALL SELECT symbol, bid AS amt FROM quotes";
        var exec = compileAndCreate(sql, TRADES, QUOTES);

        assertTrue(exec.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true)).isEmpty());
        var quoteResult = exec.onEvent("quotes", evt(1500, "quotes", "symbol", "AAPL", "bid", 50.0, "ask", 51.0));
        var quoteRow = single(quoteResult);
        assertEquals(50.0, quoteRow.get("amt"));

        var closed = exec.advanceWatermark(12000);
        var windowRow = single(closed);
        assertEquals("AAPL", windowRow.get("symbol"));
        assertEquals(100.0, windowRow.get("amt"));
    }

    @Test
    void planSummaryNamesUnionAllAndTheOutputColumnCount() {
        var r = compile(THREE_BRANCH_SQL, TRADES, QUOTES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertTrue(r.planSummary().contains("UNION ALL"));
        assertTrue(r.planSummary().contains("SELECT 2"));
    }

    @Test
    void sourceNamesUnionAcrossBranchesWithoutDuplicates() {
        // "quotes" is a source in TWO branches — CompileResult.SourceNames must still list it once.
        var r = compile(THREE_BRANCH_SQL, TRADES, QUOTES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(List.of("quotes", "trades"), r.sourceNames().stream().sorted().toList());
    }
}
