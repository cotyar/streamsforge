package io.streamsforge.dataflow;

/** Handle inside an iterative scope, used to bring outer collections in. */
public final class IterScope<T extends Time<T>> {
    IterScope() {}

    /** Brings an outer collection into the loop; its updates appear at iteration 0 of their outer time. */
    public <X> Collection<X, Product<T, LongTime>> enter(Collection<X, T> outer) {
        return outer.enter();
    }
}
