package io.streamsforge.dataflow;

import java.util.List;

/**
 * Per-key reduction. {@code input} holds the key's accumulated values with non-zero multiplicity (never empty); the
 * logic appends the desired output values with multiplicities to {@code output}. It must be a pure function of its
 * arguments. Input order is unspecified.
 */
@FunctionalInterface
public interface ReduceLogic<K, V, R> {
    void apply(K key, List<Weighted<V>> input, List<Weighted<R>> output);
}
