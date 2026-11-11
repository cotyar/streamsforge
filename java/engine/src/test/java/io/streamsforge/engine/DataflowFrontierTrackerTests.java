package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.DeltaBatch;
import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.dataflow.FrontierObserveResult;
import io.streamsforge.engine.dataflow.FrontierTracker;
import io.streamsforge.engine.dataflow.UpstreamId;
import java.util.List;
import org.junit.jupiter.api.Test;

/** FrontierTracker is the primitive every historical dataflow bug traces back to (plan's words) — these tests drive
 *  its min-combine, regression-detection, idempotency, and silent-upstream semantics directly, independent of any
 *  executor. */
class DataflowFrontierTrackerTests {
    private static final EdgeId EDGE0 = new EdgeId(0);
    private static final EdgeId EDGE1 = new EdgeId(1);
    private static final UpstreamId UA = new UpstreamId(EDGE0, 0);
    private static final UpstreamId UB = new UpstreamId(EDGE0, 1);

    @Test
    void singleUpstreamFrontierTracksItsObservations() {
        var tracker = new FrontierTracker(List.of(UA));

        var obs = tracker.observe(UA, new Epoch(5));

        assertTrue(obs.advanced());
        assertEquals(new Epoch(5), obs.frontier());
        assertEquals(new Epoch(5), tracker.frontier());
    }

    @Test
    void duplicateObservationIsIdempotentAndReportsNoChange() {
        var tracker = new FrontierTracker(List.of(UA));
        tracker.observe(UA, new Epoch(5));

        var repeat = tracker.observe(UA, new Epoch(5));

        assertEquals(FrontierObserveResult.NO_CHANGE, repeat.result());
        assertFalse(repeat.advanced());
        assertFalse(repeat.regressed());
        assertEquals(new Epoch(5), tracker.frontier());
    }

    @Test
    void multiUpstreamFrontierIsTheMinimum() {
        var tracker = new FrontierTracker(List.of(UA, UB));

        tracker.observe(UA, new Epoch(10));
        var afterB = tracker.observe(UB, new Epoch(3));

        assertTrue(afterB.advanced());
        assertEquals(new Epoch(3), tracker.frontier());

        var afterAAdvancesFurther = tracker.observe(UA, new Epoch(20));
        // B still holds it at 3 — advancing A past B must NOT move the combined frontier.
        assertEquals(FrontierObserveResult.NO_CHANGE, afterAAdvancesFurther.result());
        assertEquals(new Epoch(3), tracker.frontier());
    }

    @Test
    void oneSilentUpstreamHoldsTheFrontierAtNegativeInfinity() {
        var tracker = new FrontierTracker(List.of(UA, UB));

        var obs = tracker.observe(UA, new Epoch(100));

        // UB has never observed — the combined frontier cannot move past "no data yet".
        assertEquals(FrontierObserveResult.NO_CHANGE, obs.result());
        assertEquals(Epoch.NEGATIVE_INFINITY, tracker.frontier());

        var afterB = tracker.observe(UB, new Epoch(1));
        assertTrue(afterB.advanced());
        assertEquals(new Epoch(1), tracker.frontier());
    }

    @Test
    void regressionIsDetectedAndRejectedNotApplied() {
        var tracker = new FrontierTracker(List.of(UA));
        tracker.observe(UA, new Epoch(10));

        var regressed = tracker.observe(UA, new Epoch(4));

        assertTrue(regressed.regressed());
        assertEquals(FrontierObserveResult.REGRESSION, regressed.result());
        // Rejected, not applied: the tracker's frontier is unaffected by the bad observation.
        assertEquals(new Epoch(10), tracker.frontier());
        assertEquals(new Epoch(10), regressed.frontier());
    }

    @Test
    void regressionOnOneUpstreamDoesNotCorruptAnotherUpstreamsState() {
        var tracker = new FrontierTracker(List.of(UA, UB));
        tracker.observe(UA, new Epoch(10));
        tracker.observe(UB, new Epoch(10));
        assertEquals(new Epoch(10), tracker.frontier());

        tracker.observe(UA, new Epoch(2)); // regression, rejected

        // Frontier is untouched; A's high-water mark is still 10 (the rejected 2 was never applied), so advancing A
        // further is accepted as a normal (non-regression) update — it just doesn't move the combined frontier yet
        // because B still holds it at 10.
        assertEquals(new Epoch(10), tracker.frontier());
        var advanceA = tracker.observe(UA, new Epoch(15));
        assertFalse(advanceA.regressed());
        assertEquals(FrontierObserveResult.NO_CHANGE, advanceA.result());
        assertEquals(new Epoch(10), tracker.frontier());

        // Once B catches up past A's new mark, the combined frontier finally moves to A's mark.
        var advanceB = tracker.observe(UB, new Epoch(20));
        assertTrue(advanceB.advanced());
        assertEquals(new Epoch(15), tracker.frontier());
    }

    @Test
    void emptyEpochMarkerStillAdvancesTheFrontier() {
        // An epoch with zero deltas is still a real observation — DeltaBatch.IsMarker batches carry no rows but MUST
        // still move the frontier, otherwise a quiet upstream would wedge the whole operator.
        var tracker = new FrontierTracker(List.of(UA));
        var marker = new DeltaBatch(EDGE0, 0, new Epoch(7), List.of());

        assertTrue(marker.isMarker());
        var obs = tracker.observe(marker.upstream(), marker.epoch());

        assertTrue(obs.advanced());
        assertEquals(new Epoch(7), tracker.frontier());
    }

    @Test
    void unknownUpstreamThrows() {
        var tracker = new FrontierTracker(List.of(UA));
        assertThrows(IllegalArgumentException.class, () -> tracker.observe(UB, new Epoch(1)));
    }

    @Test
    void registeringDuplicateUpstreamThrows() {
        var tracker = new FrontierTracker(List.of(UA));
        assertThrows(IllegalArgumentException.class, () -> tracker.registerUpstream(UA));
    }

    @Test
    void registeringAfterObservationStartedThrows() {
        var tracker = new FrontierTracker(List.of(UA));
        tracker.observe(UA, new Epoch(1));

        assertThrows(IllegalStateException.class, () -> tracker.registerUpstream(UB));
    }

    @Test
    void frontierAcrossDifferentEdgesStillMinCombines() {
        var upstreamEdge0 = new UpstreamId(EDGE0, 0);
        var upstreamEdge1 = new UpstreamId(EDGE1, 0);
        var tracker = new FrontierTracker(List.of(upstreamEdge0, upstreamEdge1));

        tracker.observe(upstreamEdge0, new Epoch(50));
        var obs = tracker.observe(upstreamEdge1, new Epoch(2));

        assertTrue(obs.advanced());
        assertEquals(new Epoch(2), tracker.frontier());
    }

    @Test
    void seededRandomWalk_FrontierNeverExceedsTrueMinimumOfHighWaterMarks() {
        var rng = new DataflowTestRandom(20260718);
        var upstreams = new UpstreamId[5];
        for (int i = 0; i < upstreams.length; i++) upstreams[i] = new UpstreamId(EDGE0, i);
        var tracker = new FrontierTracker(List.of(upstreams));
        var highWater = new long[upstreams.length];
        var observed = new boolean[upstreams.length];

        for (int step = 0; step < 2000; step++) {
            int idx = rng.next(upstreams.length);
            // Bias toward advances but occasionally attempt (and expect rejection of) a regression.
            int delta = rng.next(0, 100) < 90 ? rng.next(0, 5) : -rng.next(1, 5);
            long candidate = (observed[idx] ? highWater[idx] : 0) + delta;

            var result = tracker.observe(upstreams[idx], new Epoch(candidate));

            if (observed[idx] && candidate < highWater[idx]) {
                assertTrue(result.regressed());
            } else {
                highWater[idx] = candidate;
                observed[idx] = true;
                assertFalse(result.regressed());
            }

            boolean allObserved = true;
            long min = Long.MAX_VALUE;
            for (int i = 0; i < upstreams.length; i++) {
                allObserved &= observed[i];
                min = Math.min(min, highWater[i]);
            }
            var expectedFrontier = allObserved ? new Epoch(min) : Epoch.NEGATIVE_INFINITY;
            assertEquals(expectedFrontier, tracker.frontier());
        }
    }
}
