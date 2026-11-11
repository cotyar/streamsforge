package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.runtime.ConsolidationLedger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Plan 009 wave D: direct unit tests for the extracted ConsolidationLedger itself. TableConsolidationLedgerTests
 * already proves the same order-independence property end-to-end through TableExecutor's public SQL surface; this
 * file instead drives ConsolidationLedger directly, with no SQL/plan/op machinery at all — the narrowest possible
 * proof that the extracted type itself is correct, independent of any one of its call sites.
 */
class ConsolidationLedgerTests {
    private static EventRecord row(String symbol) {
        var r = new EventRecord();
        r.put("symbol", symbol);
        return r;
    }

    @Test
    void retractionArrivingBeforeItsAssertion_NetsCorrectly() {
        var ledger = new ConsolidationLedger();

        // The retraction arrives FIRST — no matching assertion has ever been folded in for this key.
        ledger.apply("AAPL", row("AAPL"), -1);
        assertTrue(ledger.visible().isEmpty()); // never visible — a non-positive weight is never exposed
        assertEquals(1, ledger.debtCount()); // but retained as debt, not silently dropped

        // The assertion the retraction was always going to cancel, folded in second.
        ledger.apply("AAPL", row("AAPL"), 1);

        // Correct net across the whole sequence is 0 — must never resurface at a spurious +1 the way
        // discarding the out-of-order negative delta (the bug commit 9de443e fixed) would have produced.
        assertTrue(ledger.visible().isEmpty());
        assertEquals(0, ledger.debtCount());
    }

    @Test
    void zeroCrossing_PositiveToNegative_MovesFromVisibleToDebt() {
        var ledger = new ConsolidationLedger();

        ledger.apply("AAPL", row("AAPL"), 2);
        assertTrue(ledger.visible().containsKey("AAPL"));
        assertEquals(2L, ledger.visible().get("AAPL").weight());
        assertEquals(0, ledger.debtCount());

        // -3 crosses zero: 2 + (-3) = -1, strictly negative.
        ledger.apply("AAPL", row("AAPL"), -3);
        assertFalse(ledger.visible().containsKey("AAPL"));
        assertEquals(1, ledger.debtCount());
    }

    @Test
    void zeroCrossing_NegativeToPositive_MovesFromDebtToVisible() {
        var ledger = new ConsolidationLedger();

        ledger.apply("AAPL", row("AAPL"), -1);
        assertEquals(1, ledger.debtCount());
        assertTrue(ledger.visible().isEmpty());

        // +4 crosses zero: -1 + 4 = 3, strictly positive.
        ledger.apply("AAPL", row("AAPL"), 4);
        assertEquals(0, ledger.debtCount());
        assertEquals(3L, ledger.visible().get("AAPL").weight());
    }

    @Test
    void fullyCancelledKey_LeavesNoResidueInEitherMap() {
        var ledger = new ConsolidationLedger();

        ledger.apply("AAPL", row("AAPL"), 1);
        ledger.apply("AAPL", row("AAPL"), 1);
        ledger.apply("AAPL", row("AAPL"), -2); // nets to exactly 0

        assertTrue(ledger.visible().isEmpty());
        assertEquals(0, ledger.debtCount());

        // A fresh assertion afterward must start from a clean slate (0), not resurrect any leftover debt.
        ledger.apply("AAPL", row("AAPL"), 1);
        assertEquals(1L, ledger.visible().get("AAPL").weight());
    }

    @Test
    void visible_IsTheSameDictionaryInstanceAcrossCalls() {
        // TableExecutor.snapshot() depends on this: the `/rows` read path holds onto the reference rather
        // than copying, so visible() must return the SAME instance every time, live-updated in place.
        var ledger = new ConsolidationLedger();
        var first = ledger.visible();
        ledger.apply("AAPL", row("AAPL"), 1);
        var second = ledger.visible();

        assertSame(first, second);
        assertTrue(second.containsKey("AAPL")); // the mutation is visible through the held-onto reference
    }

    @Test
    void seed_PopulatesVisibleWithoutTouchingDebtOrRunningArithmetic() {
        // ArrangementGrain's rebuild-from-checkpoint path: the checkpoint only ever holds positive-weight
        // rows, so seed bypasses apply's weight-folding entirely.
        var ledger = new ConsolidationLedger();
        ledger.seed("AAPL", row("AAPL"), 5);

        assertEquals(5L, ledger.visible().get("AAPL").weight());
        assertEquals(0, ledger.debtCount());

        // A subsequent apply folds against the seeded weight exactly like it would against one reached via
        // ordinary apply calls.
        ledger.apply("AAPL", row("AAPL"), -5);
        assertTrue(ledger.visible().isEmpty());
        assertEquals(0, ledger.debtCount());
    }

    @Test
    void clear_ResetsBothMapsToEmpty() {
        var ledger = new ConsolidationLedger();
        ledger.apply("AAPL", row("AAPL"), 1);
        ledger.apply("MSFT", row("MSFT"), -1);
        assertFalse(ledger.visible().isEmpty());
        assertNotEquals(0, ledger.debtCount());

        ledger.clear();

        assertTrue(ledger.visible().isEmpty());
        assertEquals(0, ledger.debtCount());
    }

    static Stream<Arguments> weightSequences() {
        return Stream.of(
            Arguments.of(new long[] {3, -1, -2}, 0L), // forward: +3 then -1 then -2 — nets to 0
            Arguments.of(new long[] {-2, -1, 3}, 0L), // fully reversed order of the same multiset
            Arguments.of(new long[] {3, -1, -2, 5}, 5L), // same shape, plus a trailing +5 — nets to +5
            Arguments.of(new long[] {5, -2, -1, 3}, 5L)); // reversed order of that multiset
    }

    @ParameterizedTest
    @MethodSource("weightSequences")
    void arrivalOrder_NeverAffectsTheFinalWeight_OnlyTheSumOfDeltasDoes(long[] weightsInOrder, long expectedFinalWeight) {
        var ledger = new ConsolidationLedger();
        for (var w : weightsInOrder) ledger.apply("AAPL", row("AAPL"), w);

        if (expectedFinalWeight == 0) {
            assertTrue(ledger.visible().isEmpty());
        } else {
            assertEquals(expectedFinalWeight, ledger.visible().get("AAPL").weight());
        }

        assertEquals(0, ledger.debtCount()); // every sequence here fully resolves — none end mid-debt
    }
}
