package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 002 L2/L3 — deterministic-replay proof (same shape as TableDeterministicReplayTests, extended to a
 * plan that combines UNNEST (1-to-N row expansion, weight-linear retraction) AND LATEST BY (per-key
 * running argmax-by-_ts, retract/assert on replacement) in one query: feeds an identical batch sequence —
 * including an out-of-order (ignored) arrival, a same-key replacement, and an upstream retraction that
 * only affects ONE of an unnested row's several element-keys — into two fresh op chains built from the
 * same compiled plan, and asserts byte-identical output sequences.
 */
class UnnestLatestByDeterministicReplayTests {
    private static final String SQL =
        "SELECT s.trade_id AS id, l ->> 'ccy' AS ccy, l -> 'notional' AS notional " +
        "FROM structures s, UNNEST(s.legs) AS l " +
        "LATEST BY (s.trade_id, l ->> 'ccy')";

    /** {@code legs} is alternating (String ccy, double notional) pairs. */
    private static List<Object> legs(Object... ccyAndNotional) {
        var result = new ArrayList<Object>();
        for (int i = 0; i < ccyAndNotional.length; i += 2) {
            Map<String, Object> leg = new LinkedHashMap<>();
            leg.put("ccy", ccyAndNotional[i]);
            leg.put("notional", ccyAndNotional[i + 1]);
            result.add(leg);
        }
        return result;
    }

    private static List<List<TableDelta>> runBatchSequence(TableExecutor exec) {
        var perCallOutputs = new ArrayList<List<TableDelta>>();

        var row1 = evt(1000, "structures", "trade_id", "T1", "legs", legs("USD", 100.0, "EUR", 200.0), "payload", null);
        perCallOutputs.add(exec.onStreamEvent("structures", row1)); // 2 new keys: assert, assert

        var olderRow = evt(500, "structures", "trade_id", "T1", "legs", legs("USD", 999.0), "payload", null);
        perCallOutputs.add(exec.onStreamEvent("structures", olderRow)); // strictly older _ts for (T1,USD): ignored

        var newerRow = evt(2000, "structures", "trade_id", "T1", "legs", legs("USD", 150.0), "payload", null);
        perCallOutputs.add(exec.onStreamEvent("structures", newerRow)); // (T1,USD) replaces: retract, assert

        // Retract row1 upstream (e.g. a filtered-out WHERE flip further up a real chain) — its (T1,EUR)
        // element still matches what's currently retained (drop that key); its (T1,USD) element does NOT
        // match what's currently retained (newerRow superseded it) — a documented no-op for that one.
        perCallOutputs.add(exec.onTableDelta("structures", new TableDelta(row1, -1)));

        return perCallOutputs;
    }

    private static String canon(List<TableDelta> deltas) {
        return deltas.stream().map(d -> d.weight() + ":" + JsonText.serializeCanonicalRow(d.row())).collect(Collectors.joining("|"));
    }

    @Test
    void unnestPlusLatestByPlanReplaysDeterministically() {
        var compileResult = compileTable(SQL, STRUCTURES);
        assertTrue(compileResult.ok(), String.valueOf(compileResult.diagnostics()));
        var plan = compileResult.plan();

        var run1 = runBatchSequence(plan.createExecutor());
        var run2 = runBatchSequence(plan.createExecutor());

        assertEquals(run1.size(), run2.size());
        for (int i = 0; i < run1.size(); i++) {
            assertEquals(run1.get(i).size(), run2.get(i).size());
            assertEquals(canon(run1.get(i)), canon(run2.get(i)));
        }

        // Sanity: the batch actually exercised replacement/retraction/no-op, not a degenerate all-empty run.
        assertTrue(run1.stream().mapToInt(List::size).sum() > 0);
    }

    @Test
    void thirdIndependentRunAlsoMatches_NotJustPairwise() {
        var compileResult = compileTable(SQL, STRUCTURES);
        var plan = compileResult.plan();

        var runs = new ArrayList<List<List<TableDelta>>>();
        for (int i = 0; i < 3; i++) runs.add(runBatchSequence(plan.createExecutor()));

        var canonical = runs.stream()
            .map(r -> r.stream().map(UnnestLatestByDeterministicReplayTests::canon).collect(Collectors.joining(";")))
            .distinct().collect(Collectors.toList());
        assertEquals(1, canonical.size());
    }
}
