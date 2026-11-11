package io.streamsforge.engine.runtime;

/** Subtractable (Z-set-aware) aggregate accumulator: add is replaced with apply(value, weight), where a
 *  negative weight retracts a previously-applied contribution. NULL inputs are skipped (except COUNT(*),
 *  which counts rows regardless of value). */
public interface IZAggregator {
    void apply(Object value, long weight);

    Object result();
}
