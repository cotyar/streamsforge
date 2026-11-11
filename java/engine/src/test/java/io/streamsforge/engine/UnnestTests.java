package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.containsMatch;
import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Plan 002 L2 — {@code UNNEST(expr) AS alias}: both grammar forms (comma-form sugar and JOIN/CROSS JOIN form),
 * element binding (a Json-kind pseudo-column addressed only via '->'/'->>'), and both runtime modes
 * (pipeline 1-to-N row expansion, table Z-set-linear weight passthrough).
 */
class UnnestTests {
    @SafeVarargs
    private static List<Object> legs(Map<String, Object>... legs) { return new ArrayList<Object>(List.of(legs)); }

    private static Map<String, Object> leg(String ccy, double notional) {
        var m = new LinkedHashMap<String, Object>();
        m.put("ccy", ccy);
        m.put("notional", notional);
        return m;
    }

    // ------------------------------------------------------------------
    // Parser
    // ------------------------------------------------------------------

    @Test
    void commaFormUnnestCompiles() {
        var r = compile("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void joinFormUnnestCompiles() {
        var r = compile("SELECT l ->> 'ccy' AS ccy FROM structures s JOIN UNNEST(s.legs) AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void crossJoinFormUnnestCompiles() {
        var r = compile("SELECT l ->> 'ccy' AS ccy FROM structures s CROSS JOIN UNNEST(s.legs) AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void multipleUnnestsInCommaFormBothReferencingTheBaseSourceCompile() {
        var r = compile("SELECT l1 ->> 'ccy' AS c1, l2 ->> 'ccy' AS c2 FROM structures s, UNNEST(s.legs) AS l1, UNNEST(s.legs) AS l2", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    @Test
    void unnestWithoutAliasIsRejected() {
        var r = compile("SELECT * FROM structures s, UNNEST(s.legs)", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("UNNEST requires an alias"));
    }

    @Test
    void unnestAsFirstFromItemIsRejected() {
        var r = compile("SELECT * FROM UNNEST(s.legs) AS l", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("UNNEST cannot be the first FROM item"));
    }

    @Test
    void leftJoinUnnestIsRejected() {
        var r = compile("SELECT * FROM structures s LEFT JOIN UNNEST(s.legs) AS l", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("no LEFT UNNEST"));
    }

    @Test
    void unnestOfAnotherUnnestAliasIsRejectedWithDiagnostic() {
        var r = compile("SELECT * FROM structures s, UNNEST(s.legs) AS l1, UNNEST(l1) AS l2", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("may not reference another UNNEST alias"));
    }

    // ------------------------------------------------------------------
    // Validator
    // ------------------------------------------------------------------

    @Test
    void unknownColumnInUnnestExprIsError() {
        var r = compile("SELECT * FROM structures s, UNNEST(s.bogus) AS l", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("Unknown column 'bogus'"));
    }

    @Test
    void unnestOfNonJsonExprIsError() {
        var r = compile("SELECT * FROM structures s, UNNEST(s.trade_id) AS l", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("UNNEST argument must be a JSON value"));
    }

    @Test
    void unnestAliasDotFieldGivesTailoredDiagnostic() {
        var r = compile("SELECT l.ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("has no field 'ccy'") && d.message().contains("->>"));
    }

    @Test
    void bareUnnestAliasInSelectEmitsElementAsJsonKind() {
        var r = compile("SELECT l FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        assertEquals(FieldKind.JSON, r.outputSchema().fields().get("l"));
    }

    @Test
    void qualifiedStarOverUnnestAliasExpands() {
        var r = compile("SELECT l.* FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
        single(r.outputSchema().fields().entrySet());
    }

    @Test
    void jsonPathExprAsUnnestArgumentCompiles() {
        var r = compile("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.payload -> 'legs') AS l", STRUCTURES);
        assertTrue(r.ok(), () -> diag(r.diagnostics()));
    }

    // ------------------------------------------------------------------
    // Executor — pipelines
    // ------------------------------------------------------------------

    @Test
    void unnestEmitsOneRowPerElementInArrayOrder() {
        var exec = compileAndCreate("SELECT s.trade_id AS id, l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        var legs = legs(leg("USD", 100), leg("EUR", 200), leg("GBP", 300));
        var results = exec.onEvent("structures", evt(1000, "structures", "trade_id", "T1", "legs", legs, "payload", null));

        assertEquals(3, results.size());
        assertEquals(List.of("USD", "EUR", "GBP"), results.stream().map(r -> r.get("ccy")).toList());
        assertTrue(results.stream().allMatch(r -> "T1".equals((String) r.get("id"))));
    }

    @Test
    void unnestOfNullArrayEmitsZeroRows() {
        var exec = compileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        var results = exec.onEvent("structures", evt(1000, "structures", "trade_id", "T1", "legs", null, "payload", null));
        assertTrue(results.isEmpty());
    }

    @Test
    void unnestOfEmptyArrayEmitsZeroRows() {
        var exec = compileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        var results = exec.onEvent("structures", evt(1000, "structures", "trade_id", "T1", "legs", legs(), "payload", null));
        assertTrue(results.isEmpty());
    }

    @Test
    void unnestOfNonArrayValueEmitsZeroRows() {
        var exec = compileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        // legs holds a scalar, not a JSON array/list — still a Json-kind field statically, but the runtime
        // value isn't a List<object?>, so it's the dynamic "non-array value" case documented for UNNEST.
        var results = exec.onEvent("structures", evt(1000, "structures", "trade_id", "T1", "legs", 42L, "payload", null));
        assertTrue(results.isEmpty());
    }

    @Test
    void joinAfterUnnestWithWithinMatchesMarketData() {
        var sql = "SELECT l ->> 'ccy' AS ccy, q.bid AS bid " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "JOIN quotes q WITHIN 5 SECONDS ON (l ->> 'ccy') = q.symbol";
        var exec = compileAndCreate(sql, STRUCTURES, QUOTES);

        exec.onEvent("quotes", evt(900, "quotes", "symbol", "USD", "bid", 1.1, "ask", 1.2));
        var results = exec.onEvent("structures", evt(1000, "structures",
            "trade_id", "T1", "legs", legs(leg("USD", 100), leg("EUR", 200)), "payload", null));

        var match = single(results);
        assertEquals("USD", match.get("ccy"));
        assertEquals(1.1, match.get("bid"));
    }

    @Test
    void acceptancePlan002L2_SumOverLegsPerCcyTumblingWindow() {
        // Plan 002 L2 acceptance query, adapted to this dialect's pinned rules:
        //  - GROUP BY must repeat the exact grouped EXPRESSION (this dialect has no group-by-alias sugar)
        //  - numeric aggregation over a JSON leaf uses '->' (raw numeric node), NOT '->>' (stringifies)
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS total_notional " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "GROUP BY l ->> 'ccy' " +
                  "WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = compileAndCreate(sql, STRUCTURES);

        exec.onEvent("structures", evt(1000, "structures",
            "trade_id", "T1", "legs", legs(leg("USD", 100), leg("EUR", 200), leg("USD", 50)), "payload", null));
        var results = exec.advanceWatermark(10_000);

        containsMatch(results, r -> "USD".equals((String) r.get("ccy")) && (double) r.get("total_notional") == 150.0);
        containsMatch(results, r -> "EUR".equals((String) r.get("ccy")) && (double) r.get("total_notional") == 200.0);
    }

    @Test
    void sumOfArrowArrowTextDoesNotAccumulateNumerically() {
        // Documents the pinned coercion rule: '->>' always returns TEXT, and SumAggregator/AvgAggregator only
        // recognize long/double inputs — a TEXT value silently contributes nothing. Use '->' for numeric
        // aggregation instead (see the acceptance test above).
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l ->> 'notional') AS total_notional " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "GROUP BY l ->> 'ccy' " +
                  "WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = compileAndCreate(sql, STRUCTURES);

        exec.onEvent("structures", evt(1000, "structures",
            "trade_id", "T1", "legs", legs(leg("USD", 100)), "payload", null));
        var results = exec.advanceWatermark(10_000);

        var row = single(results, r -> "USD".equals((String) r.get("ccy")));
        assertEquals(0L, row.get("total_notional")); // NOT 100 — the documented '->>' aggregation trap
    }

    // ------------------------------------------------------------------
    // Executor — tables
    // ------------------------------------------------------------------

    @Test
    void tableUnnestRetractionRetractsAllElementRows() {
        var exec = compileTableAndCreate("SELECT s.trade_id AS id, l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", STRUCTURES);
        var row = evt(0, "structures", "trade_id", "T1", "legs", legs(leg("USD", 100), leg("EUR", 200), leg("GBP", 300)), "payload", null);

        var asserted = exec.onStreamEvent("structures", row);
        assertEquals(3, asserted.size());
        assertTrue(asserted.stream().allMatch(d -> d.weight() == 1));

        var retracted = exec.onTableDelta("structures", new TableDelta(row, -1));
        assertEquals(3, retracted.size());
        assertTrue(retracted.stream().allMatch(d -> d.weight() == -1));
        assertEquals(
            asserted.stream().map(d -> (String) d.row().get("ccy")).sorted().toList(),
            retracted.stream().map(d -> (String) d.row().get("ccy")).sorted().toList());

        assertTrue(exec.snapshot().isEmpty());
    }

    @Test
    void tableUnnestUnderGroupByEmitsRetractAssertPerAggregateChange() {
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS total FROM structures s, UNNEST(s.legs) AS l GROUP BY l ->> 'ccy'";
        var exec = compileTableAndCreate(sql, STRUCTURES);

        var row1 = evt(0, "structures", "trade_id", "T1", "legs", legs(leg("USD", 100)), "payload", null);
        var first = exec.onStreamEvent("structures", row1);
        var assertion = single(first);
        assertEquals(1L, assertion.weight());
        assertEquals(100.0, assertion.row().get("total"));

        var row2 = evt(0, "structures", "trade_id", "T2", "legs", legs(leg("USD", 50)), "payload", null);
        var second = exec.onStreamEvent("structures", row2);
        assertEquals(2, second.size());
        assertEquals(-1L, second.get(0).weight());
        assertEquals(100.0, second.get(0).row().get("total"));
        assertEquals(1L, second.get(1).weight());
        assertEquals(150.0, second.get(1).row().get("total"));
    }

    @Test
    void nestedUnnestInsideDerivedTableWorks() {
        var sql = "SELECT d.ccy FROM (SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l) d";
        var exec = compileTableAndCreate(sql, STRUCTURES);

        var row = evt(0, "structures", "trade_id", "T1", "legs", legs(leg("USD", 100), leg("EUR", 200)), "payload", null);
        var deltas = exec.onStreamEvent("structures", row);

        assertEquals(2, deltas.size());
        assertEquals(List.of("EUR", "USD"), deltas.stream().map(d -> (String) d.row().get("ccy")).sorted().toList());
    }
}
