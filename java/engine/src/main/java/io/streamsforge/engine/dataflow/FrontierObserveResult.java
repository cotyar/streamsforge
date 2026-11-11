package io.streamsforge.engine.dataflow;

/** Outcome of {@link FrontierTracker#observe}. */
public enum FrontierObserveResult {
    /** The combined frontier moved forward as a result of this observation. */
    ADVANCED,

    /** The observation was accepted (recorded) but the combined frontier is unchanged — either a duplicate of the
     *  upstream's current high-water mark, or another upstream is still holding the combined frontier back. */
    NO_CHANGE,

    /** The observed epoch is below the upstream's current high-water mark. Rejected: the observation is NOT applied.
     *  Always a bug in the sender — epochs must be non-decreasing per upstream. */
    REGRESSION
}
