package io.streamsforge.engine.runtime;

/** Subtractable (Z-set-aware) aggregate accumulator: add is replaced with apply(value, weight), where a
 *  negative weight retracts a previously-applied contribution. NULL inputs are skipped (except COUNT(*),
 *  which counts rows regardless of value). */
public interface IZAggregator {
    void apply(Object value, long weight);

    Object result();

    /**
     * ADDITIVE — true when this accumulator's state is indistinguishable from a freshly created one (every
     * contribution netted out). {@code TableReduceOp} drops a group at total weight zero ONLY when every
     * aggregator is also empty: zero weight alone does not mean the input multiset is empty when a retraction
     * arrived before its own assertion ({+r2, -r1} has weight 0 but SUM r2 - r1), and dropping that state made
     * GROUP BY depend on delivery order. The default ({@code true}) keeps a third-party aggregate exactly as it
     * behaved before; one that can carry a not-yet-asserted contribution should override it.
     */
    default boolean isEmpty() { return true; }
}
