package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 011 wave C2 — the opt-in per-table ROW RETENTION policy (see TableRetentionPolicy and
 * runtime/TableRetentionSupport).
 *
 * <p>The load-bearing assertion in this file is NOT "the row count plateaus" — a policy that only trimmed
 * the consolidated output would pass that while every structure holding memory kept growing. It is
 * TableExecutor.retainedStateCount(): the size of the state that actually OWNS the rows
 * (TableLatestByOp.current for a LATEST BY plan, the ledger for a plain projection). snapshot() is
 * checked too, but as a consequence, not as the proof.
 */
class TableRetentionTests {
    private static EventRecord orderEvt(long ts, String orderId, String stage) {
        return evt(ts, "order_events", "order_id", orderId, "stage", stage, "filled_qty", 0L);
    }

    private static final String LATEST_BY_SQL = "SELECT order_id, stage FROM order_events LATEST BY (order_id)";

    private static List<String> orderIds(TableExecutor exec) {
        var l = new ArrayList<String>();
        for (var v : exec.snapshot().values()) l.add((String) v.row().get("order_id"));
        l.sort(null);
        return l;
    }

    // ------------------------------------------------------------------
    // Default off — the property that lets this ship without touching a single existing test.
    // ------------------------------------------------------------------

    @Test
    void retentionIsOffByDefault_UnboundedTableKeepsEveryKey() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);

        for (int i = 0; i < 50; i++) exec.onStreamEvent("order_events", orderEvt(1000 + i, "o" + i, "NEW"));

        assertEquals(50, exec.snapshot().size());
        assertEquals(-1, exec.retainedStateCount()); // no scope installed at all
    }

    @Test
    void configuringDisabledPolicyIsANoOp() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(TableRetentionPolicy.NONE);

        for (int i = 0; i < 20; i++) exec.onStreamEvent("order_events", orderEvt(1000 + i, "o" + i, "NEW"));

        assertEquals(20, exec.snapshot().size());
    }

    // ------------------------------------------------------------------
    // MaxRows on a LATEST BY plan — the motivating case (an unbounded key space).
    // ------------------------------------------------------------------

    @Test
    void maxRows_EvictsOldestByEventTime_AndReclaimsTheOperatorsOwnPerKeyState() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(3, 0));

        for (int i = 0; i < 10; i++) exec.onStreamEvent("order_events", orderEvt(1000 + i, "o" + i, "NEW"));

        // The operator's per-key map — the structure that would otherwise grow forever — is bounded.
        assertEquals(3, exec.retainedStateCount());
        // ...and so is the consolidated output, which follows from it rather than being trimmed separately.
        assertEquals(3, exec.snapshot().size());

        var surviving = orderIds(exec);
        assertEquals(List.of("o7", "o8", "o9"), surviving); // oldest _ts evicted first
    }

    @Test
    void eviction_EmitsARealRetraction_MarkedAsRetention() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(2, 0));

        exec.onStreamEvent("order_events", orderEvt(1000, "a", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1001, "b", "NEW"));
        var deltas = exec.onStreamEvent("order_events", orderEvt(1002, "c", "NEW"));

        // The admission's own assertion, THEN the eviction retraction — one list, one path, so every
        // consumer of this return value stays consistent without knowing retention exists.
        assertEquals(2, deltas.size());
        assertEquals(1L, deltas.get(0).weight());
        assertFalse(deltas.get(0).retention());
        assertEquals(-1L, deltas.get(1).weight());
        assertTrue(deltas.get(1).retention());
        assertEquals("a", deltas.get(1).row().get("order_id")); // the oldest key, by event time
    }

    @Test
    void eviction_LeavesNoResidueInTheConsolidationLedger() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(5, 0));

        for (int i = 0; i < 200; i++) exec.onStreamEvent("order_events", orderEvt(1000 + i, "o" + i, "NEW"));

        assertEquals(5, exec.snapshot().size());
        // An eviction that netted a key to a NEGATIVE weight would park it in the ledger's debt side-table
        // forever — a leak wearing a bound's clothes. Retracting the row's full running weight is what
        // makes this zero.
        assertEquals(0, exec.debtCount());
    }

    @Test
    void lateUpdateToARetainedKeyRefreshesItsPositionInTheEvictionOrder() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(2, 0));

        exec.onStreamEvent("order_events", orderEvt(1000, "a", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1001, "b", "NEW"));
        // "a" progresses — it is now the NEWEST key, so the next arrival must evict "b", not "a".
        exec.onStreamEvent("order_events", orderEvt(1002, "a", "FILLED"));
        exec.onStreamEvent("order_events", orderEvt(1003, "c", "NEW"));

        var surviving = orderIds(exec);
        assertEquals(List.of("a", "c"), surviving);
        var a = single(exec.snapshot().values(), v -> "a".equals(v.row().get("order_id")));
        assertEquals("FILLED", a.row().get("stage"));
    }

    // ------------------------------------------------------------------
    // TTL — event time, not wall clock.
    // ------------------------------------------------------------------

    @Test
    void ttl_EvictsByEventTimeMeasuredFromTheNewestAdmittedRow() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(0, 100));

        // These timestamps are far in the PAST in wall-clock terms; a wall-clock TTL would evict all of
        // them immediately. An event-time TTL keeps everything within 100ms of the newest row.
        exec.onStreamEvent("order_events", orderEvt(1_000, "old", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1_050, "mid", "NEW"));
        assertEquals(2, exec.snapshot().size());

        exec.onStreamEvent("order_events", orderEvt(1_200, "new", "NEW")); // cutoff becomes 1100
        assertEquals(1, exec.retainedStateCount());
        assertEquals(1, exec.snapshot().size());
        assertEquals("new", exec.snapshot().values().iterator().next().row().get("order_id"));
    }

    @Test
    void ttl_DoesNotAgeAnythingOutWhileTheInputIsStalled() throws InterruptedException {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(0, 10));

        exec.onStreamEvent("order_events", orderEvt(1_000, "a", "NEW"));
        Thread.sleep(50); // real time passes; event time does not

        // The documented consequence of an event-time TTL, asserted rather than left as prose: with no
        // further input there is no new high-water mark, so nothing expires.
        assertEquals(1, exec.snapshot().size());
    }

    @Test
    void ttlAndMaxRowsCompose_AgeFirstThenCount() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(2, 50));

        exec.onStreamEvent("order_events", orderEvt(1_000, "a", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1_010, "b", "NEW"));
        exec.onStreamEvent("order_events", orderEvt(1_020, "c", "NEW")); // count bound trims "a"
        assertEquals(2, exec.snapshot().size());

        exec.onStreamEvent("order_events", orderEvt(1_100, "d", "NEW")); // age bound trims b and c
        assertEquals(1, exec.retainedStateCount());
        assertEquals("d", exec.snapshot().values().iterator().next().row().get("order_id"));
    }

    // ------------------------------------------------------------------
    // Determinism — the invariant this codebase treats as testable, not aspirational.
    // ------------------------------------------------------------------

    private static List<String> runDeterminism() {
        var exec = compileTableAndCreate(LATEST_BY_SQL, ORDER_EVENTS);
        exec.configureRetention(new TableRetentionPolicy(7, 0));
        // Deliberately includes repeated timestamps, so the tie-break (and not arrival luck) is what
        // decides who goes.
        for (int i = 0; i < 60; i++) exec.onStreamEvent("order_events", orderEvt(1000 + (i / 3), "o" + i, "NEW"));
        return orderIds(exec);
    }

    @Test
    void replayingTheSameInputProducesTheSameBoundedTable() {
        assertEquals(runDeterminism(), runDeterminism());
        assertEquals(7, runDeterminism().size());
    }

    // ------------------------------------------------------------------
    // The plain-projection scope (no LATEST BY): the ledger IS the state.
    // ------------------------------------------------------------------

    @Test
    void projectionTable_BoundsTheLedgerItself() {
        var exec = compileTableAndCreate("SELECT symbol, price FROM trades", TRADES);
        exec.configureRetention(new TableRetentionPolicy(4, 0));

        for (int i = 0; i < 25; i++) {
            exec.onStreamEvent("trades", evt(2000 + i, "trades", "symbol", "S" + i, "price", 1.0 + i, "qty", 1L, "active", true));
        }

        assertEquals(4, exec.retainedStateCount());
        assertEquals(4, exec.snapshot().size());
        assertEquals(0, exec.debtCount());
        var survivors = new ArrayList<String>();
        for (var v : exec.snapshot().values()) survivors.add((String) v.row().get("symbol"));
        survivors.sort(null);
        assertEquals(List.of("S21", "S22", "S23", "S24"), survivors);
    }

    // ------------------------------------------------------------------
    // Refusal, not silent under-delivery, for shapes whose state retention cannot reclaim.
    // ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol",
        "SELECT COUNT(*) AS n FROM trades"
    })
    void aggregatePlansRefuseRetention(String sql) {
        var result = compileTable(sql, TRADES);
        assertTrue(result.ok(), String.valueOf(result.diagnostics()));
        assertFalse(result.plan().supportsRetention());
        assertThrows(IllegalStateException.class, () -> result.plan().createExecutor().configureRetention(new TableRetentionPolicy(10, 0)));
    }

    @Test
    void joinPlansRefuseRetention() {
        var result = compileTable("SELECT t.symbol, q.bid FROM trades t JOIN quotes q ON t.symbol = q.symbol", TRADES, QUOTES);
        assertTrue(result.ok(), String.valueOf(result.diagnostics()));
        assertFalse(result.plan().supportsRetention());
    }

    @Test
    void setOperationPlansRefuseRetention() {
        var result = compileTable("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", TRADES, QUOTES);
        assertTrue(result.ok(), String.valueOf(result.diagnostics()));
        assertFalse(result.plan().supportsRetention());
    }

    @Test
    void derivedSourcePlansRefuseRetention() {
        var result = compileTable("SELECT symbol FROM (SELECT symbol FROM trades) d", TRADES);
        assertTrue(result.ok(), String.valueOf(result.diagnostics()));
        assertFalse(result.plan().supportsRetention());
    }

    @Test
    void supportedShapesReportThemselvesAsSupported() {
        assertTrue(compileTable(LATEST_BY_SQL, ORDER_EVENTS).plan().supportsRetention());
        assertTrue(compileTable("SELECT symbol, price FROM trades WHERE price > 1", TRADES).plan().supportsRetention());
    }
}
