package io.streamsforge.engine.dataflow;

/**
 * A DBSP-style epoch marker (plans/003-materialize-territory.md, "Protocol details"): the partitioned dataflow
 * batches every delta into a totally-ordered, monotonically increasing epoch. Epochs are the unit of progress —
 * an operator's "frontier" (see {@link FrontierTracker}) is expressed in epochs, and a batch's epoch says "this
 * data belongs to tick N; nothing earlier will ever follow it from this sender."
 *
 * <p>INVARIANT: epochs only ever increase for a given sender — FrontierTracker's regression detection is the
 * enforcement point.
 */
public record Epoch(long value) implements Comparable<Epoch> {
    /** The epoch value assigned to the very first tick. */
    public static final Epoch ZERO = new Epoch(0);

    /** The floor below every real epoch; the starting value of a combined frontier before every upstream has
     *  reported at least once. */
    public static final Epoch NEGATIVE_INFINITY = new Epoch(Long.MIN_VALUE);

    @Override public int compareTo(Epoch other) { return Long.compare(value, other.value); }

    public boolean lessThan(Epoch o) { return value < o.value; }
    public boolean lessOrEqual(Epoch o) { return value <= o.value; }
    public boolean greaterThan(Epoch o) { return value > o.value; }
    public boolean greaterOrEqual(Epoch o) { return value >= o.value; }

    /** The next epoch after this one. */
    public Epoch next() { return new Epoch(value + 1); }

    public static Epoch min(Epoch a, Epoch b) { return a.value <= b.value ? a : b; }
    public static Epoch max(Epoch a, Epoch b) { return a.value >= b.value ? a : b; }

    @Override public String toString() { return value == Long.MIN_VALUE ? "Epoch(-inf)" : "Epoch(" + value + ")"; }
}
