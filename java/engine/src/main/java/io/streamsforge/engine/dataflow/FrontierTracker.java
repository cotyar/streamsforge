package io.streamsforge.engine.dataflow;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-upstream high-water marks with min-combine (plans/003-materialize-territory.md, "Protocol details"). An
 * operator's combined frontier is the minimum epoch reported across all of its registered upstreams — the operator
 * may only act as if it has seen everything up to the SLOWEST upstream.
 *
 * <p>INVARIANTS:
 * <ul>
 * <li>The combined frontier never regresses.</li>
 * <li>A single upstream's high-water mark never regresses either: {@link #observe} detects (and reports via
 *     {@link FrontierObserveResult#REGRESSION}, never applies) any epoch below what that upstream already reported —
 *     surfaced to the caller rather than thrown, so one misbehaving upstream doesn't take the operator down.</li>
 * <li>A registered upstream that has never called observe holds the combined frontier at
 *     {@link Epoch#NEGATIVE_INFINITY} ("silent upstream" semantics).</li>
 * <li>The upstream set is fixed once observation starts: {@link #registerUpstream} after the first observe throws.</li>
 * </ul>
 */
public final class FrontierTracker {
    private final Map<UpstreamId, Epoch> highWater = new LinkedHashMap<>();
    private boolean observing;
    private Epoch frontier = Epoch.NEGATIVE_INFINITY;

    public FrontierTracker() {}

    public FrontierTracker(Iterable<UpstreamId> upstreams) {
        for (var id : upstreams) registerUpstream(id);
    }

    /** The upstreams currently registered with this tracker. */
    public Collection<UpstreamId> upstreams() { return highWater.keySet(); }

    /** The combined frontier: min over every registered upstream's high-water mark. NEGATIVE_INFINITY until at
     *  least one upstream has been registered and every registered upstream has observed at least once. */
    public Epoch frontier() { return frontier; }

    /** Adds an upstream this tracker will combine into its frontier. Setup-only: throws once observe has been called. */
    public void registerUpstream(UpstreamId id) {
        if (observing) {
            throw new IllegalStateException("Cannot register upstream " + id + ": FrontierTracker's upstream set is fixed once observation starts.");
        }
        if (highWater.containsKey(id)) throw new IllegalArgumentException("Upstream " + id + " is already registered. (Parameter 'id')");
        highWater.put(id, Epoch.NEGATIVE_INFINITY);
    }

    /** Records that {@code id} has reached {@code epoch} and recomputes the combined frontier. Idempotent for a
     *  repeated observation of the same epoch (NO_CHANGE, not REGRESSION). Rejects any epoch below the current mark. */
    public FrontierObservation observe(UpstreamId id, Epoch epoch) {
        Epoch current = highWater.get(id);
        if (current == null) throw new IllegalArgumentException("Upstream " + id + " was never registered with this tracker. (Parameter 'id')");

        observing = true;

        if (epoch.lessThan(current)) return new FrontierObservation(FrontierObserveResult.REGRESSION, frontier);

        highWater.put(id, epoch); // no-op write when epoch == current

        Epoch combined = Epoch.NEGATIVE_INFINITY;
        boolean first = true;
        for (Epoch mark : highWater.values()) {
            combined = first ? mark : Epoch.min(combined, mark);
            first = false;
        }

        if (combined.greaterThan(frontier)) {
            frontier = combined;
            return new FrontierObservation(FrontierObserveResult.ADVANCED, frontier);
        }

        return new FrontierObservation(FrontierObserveResult.NO_CHANGE, frontier);
    }
}
