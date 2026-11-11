package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.Epoch;
import org.junit.jupiter.api.Test;

/** Epoch is a thin monotone-long wrapper; these tests pin its comparison, Min/Max, and NegativeInfinity-floor
 *  semantics that FrontierTracker and EpochBuffer both depend on. */
class DataflowEpochTests {
    @Test
    void orderingMatchesUnderlyingLongValue() {
        assertTrue(new Epoch(1).lessThan(new Epoch(2)));
        assertTrue(new Epoch(2).greaterThan(new Epoch(1)));
        assertTrue(new Epoch(2).lessOrEqual(new Epoch(2)));
        assertTrue(new Epoch(2).greaterOrEqual(new Epoch(2)));
        assertFalse(new Epoch(2).lessThan(new Epoch(2)));
    }

    @Test
    void equalityIsValueBased() {
        assertEquals(new Epoch(42), new Epoch(42));
        assertNotEquals(new Epoch(42), new Epoch(43));
    }

    @Test
    void nextIncrementsByOne() {
        assertEquals(new Epoch(6), new Epoch(5).next());
    }

    @Test
    void minAndMaxPickTheExpectedSide() {
        assertEquals(new Epoch(3), Epoch.min(new Epoch(3), new Epoch(9)));
        assertEquals(new Epoch(3), Epoch.min(new Epoch(9), new Epoch(3)));
        assertEquals(new Epoch(9), Epoch.max(new Epoch(3), new Epoch(9)));
        assertEquals(new Epoch(9), Epoch.max(new Epoch(9), new Epoch(3)));
    }

    @Test
    void negativeInfinityIsBelowEveryRealEpoch() {
        assertTrue(Epoch.NEGATIVE_INFINITY.lessThan(Epoch.ZERO));
        assertTrue(Epoch.NEGATIVE_INFINITY.lessThan(new Epoch(Long.MIN_VALUE + 1)));
        assertEquals(Epoch.NEGATIVE_INFINITY, Epoch.min(Epoch.NEGATIVE_INFINITY, Epoch.ZERO));
    }
}
