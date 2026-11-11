package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Direct, join/aggregate-free proof of TableExecutorImpl.ApplyConsolidation's order-independence fix.
 * This file isolates the ledger itself: every test here drives a plain passthrough table-mode plan
 * ("SELECT symbol, price FROM prices", no JOIN, no GROUP BY) so a call to onTableDelta's weight flows
 * straight through TableIngestOp and TableFilterProjectOp.onBatchTerminal unchanged and lands in the
 * consolidation exactly as given — full, direct arithmetic control over the ledger with none of a join or
 * aggregate's own retraction/assertion re-emission in the way.
 */
class TableConsolidationLedgerTests {
    private static final SourceSchema PRICES = schema("prices", "symbol", FieldKind.STRING, "price", FieldKind.DOUBLE);

    private static TableExecutor createPassthrough() {
        return compileTableAndCreate("SELECT symbol, price FROM prices", new SourceSchema[0], new SourceSchema[] {PRICES});
    }

    private static EventRecord row(String symbol, double price) { return evt(0, "prices", "symbol", symbol, "price", price); }

    private static TableDelta delta(String symbol, double price, long weight) { return new TableDelta(row(symbol, price), weight); }

    /** Ordinal key order (C# {@code OrderBy(kv =&gt; kv.Key)} over these ASCII keys). */
    private static String canon(TableExecutor exec) {
        var sorted = new TreeMap<String, RowWeight>(exec.snapshot());
        var parts = new ArrayList<String>();
        for (var kv : sorted.entrySet()) parts.add(kv.getKey() + "=" + kv.getValue().weight());
        return String.join("\n", parts);
    }

    private record W(String symbol, double price, long weight) {}

    // ------------------------------------------------------------------------------------------------

    @Test
    void negativeDeltaArrivingBeforeItsAssertion_NetsToNothing() {
        var exec = createPassthrough();

        // The retraction arrives FIRST — no matching assertion has ever been seen for this canonical row.
        exec.onTableDelta("prices", delta("AAPL", 100.0, -1));
        assertTrue(exec.snapshot().isEmpty()); // never exposed — snapshot() never shows a non-positive row
        assertEquals(1, exec.debtCount()); // but NOT silently dropped either — recorded as outstanding debt

        // The assertion the retraction was always going to cancel, admitted second.
        exec.onTableDelta("prices", delta("AAPL", 100.0, 1));

        // Correct net across the whole sequence is 0 — the row must never resurface at a spurious +1 the
        // way the old ("discard negative deltas for absent keys") ledger would have produced.
        assertTrue(exec.snapshot().isEmpty());
        assertEquals(0, exec.debtCount());
    }

    @Test
    void rowThatNetsToExactlyZero_LeavesNoEntryAndNoResidualDebt() {
        var exec = createPassthrough();

        exec.onTableDelta("prices", delta("AAPL", 100.0, 1)); // ordinary causal-order assertion
        assertEquals(1, exec.snapshot().size());
        assertEquals(0, exec.debtCount());

        exec.onTableDelta("prices", delta("AAPL", 100.0, -1)); // ordinary causal-order retraction

        // No entry in the positive ledger...
        assertTrue(exec.snapshot().isEmpty());
        // ...and no residual debt either — the fix must not trade the original correctness bug for a memory
        // leak in the new side table.
        assertEquals(0, exec.debtCount());
    }

    @Test
    void interleavingsOfSameMultiset_ConvergeToIdenticalSnapshot() {
        // AAPL nets to +1 (1 + 1 - 1), MSFT nets to 0 (vanishes entirely), GOOG's retraction arrives before
        // its own assertion (nets to +1: -1 + 2) — a mix deliberately chosen to exercise every ledger
        // transition (fresh positive, accumulate, net-to-zero, debt-then-cancel-into-positive) at once.
        W[] multiset = {
            new W("AAPL", 100.0, 1),
            new W("AAPL", 100.0, 1),
            new W("AAPL", 100.0, -1),
            new W("MSFT", 200.0, 1),
            new W("MSFT", 200.0, -1),
            new W("GOOG", 50.0, -1),
            new W("GOOG", 50.0, 2),
        };

        var forward = createPassthrough();
        for (var w : multiset) forward.onTableDelta("prices", delta(w.symbol(), w.price(), w.weight()));
        var forwardCanon = canon(forward);

        assertEquals(2, forward.snapshot().size()); // AAPL=+1, GOOG=+1 — MSFT fully cancelled out
        assertEquals(0, forward.debtCount());

        var reversed = createPassthrough();
        for (int i = multiset.length - 1; i >= 0; i--) reversed.onTableDelta("prices", delta(multiset[i].symbol(), multiset[i].price(), multiset[i].weight()));
        assertEquals(forwardCanon, canon(reversed));
        assertEquals(0, reversed.debtCount());

        // A third interleaving that is neither forward nor fully reversed: GOOG's retraction (index 5)
        // pulled to the very front, everything else left in its original relative order.
        var custom = new ArrayList<W>(List.of(multiset[5]));
        for (int i = 0; i < multiset.length; i++) if (i != 5) custom.add(multiset[i]);
        var interleaved = createPassthrough();
        for (var w : custom) interleaved.onTableDelta("prices", delta(w.symbol(), w.price(), w.weight()));
        assertEquals(forwardCanon, canon(interleaved));
        assertEquals(0, interleaved.debtCount());
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
    void multiWeightSequences_ConvergeToTheDeltaSum_RegardlessOfOrder(long[] weightsInOrder, long expectedFinalWeight) {
        var exec = createPassthrough();
        for (var w : weightsInOrder) exec.onTableDelta("prices", delta("AAPL", 100.0, w));

        if (expectedFinalWeight == 0) {
            assertTrue(exec.snapshot().isEmpty());
        } else {
            var key = exec.canonicalRowKey(row("AAPL", 100.0));
            assertEquals(expectedFinalWeight, exec.snapshot().get(key).weight());
        }

        // Every sequence here fully resolves (no still-pending negative running total) — none of them are
        // meant to end mid-debt, so the side table must always drain back to empty too.
        assertEquals(0, exec.debtCount());
    }

    @Test
    void snapshotNeverContainsANonPositiveWeight() {
        var exec = createPassthrough();
        W[] rows = {
            new W("AAPL", 100.0, -1), new W("AAPL", 100.0, 1), new W("AAPL", 100.0, 1), // net +1, reached via a debt-first path
            new W("MSFT", 200.0, 1), new W("MSFT", 200.0, -1), // net 0
            new W("GOOG", 50.0, -3), new W("GOOG", 50.0, 1), // net -2 — must stay OUT of the snapshot
            new W("TSLA", 10.0, 2), new W("TSLA", 10.0, -5), // net -3 — must stay OUT of the snapshot too
        };
        for (var w : rows) exec.onTableDelta("prices", delta(w.symbol(), w.price(), w.weight()));

        for (var v : exec.snapshot().values()) assertTrue(v.weight() > 0);

        // GOOG (-2) and TSLA (-3) never crossed into positive territory — confirm they were tracked as
        // debt, not silently lost the way the pre-fix ledger would have lost them.
        assertEquals(2, exec.debtCount());
        assertEquals(1, exec.snapshot().size()); // only AAPL (net +1) is user-visible
    }
}
