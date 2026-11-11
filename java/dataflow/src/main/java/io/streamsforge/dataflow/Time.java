package io.streamsforge.dataflow;

/**
 * A logical timestamp drawn from a lattice (a partial order with least upper bounds and greatest lower bounds).
 *
 * <p>{@code compareTo} must be a <em>linear extension</em> of the partial order: if {@code a.lessEqual(b)} then
 * {@code a.compareTo(b) <= 0}. Operators use it to process times in an order that respects causality.
 */
public interface Time<T extends Time<T>> extends Comparable<T> {
    /** The partial order. */
    boolean lessEqual(T other);

    /** Least upper bound. */
    T join(T other);

    /** Greatest lower bound. */
    T meet(T other);

    default boolean lessThan(T other) {
        return lessEqual(other) && !equals(other);
    }

    /**
     * Logical compaction: the least time that is indistinguishable from this one for every time at or beyond some
     * element of {@code frontier}, i.e. the meet over {@code f} of {@code this.join(f)}. An empty frontier leaves the
     * time alone.
     */
    @SuppressWarnings("unchecked")
    default T advanceBy(Antichain<T> frontier) {
        T self = (T) this;
        T result = null;
        for (T f : frontier.elements()) {
            T j = self.join(f);
            result = result == null ? j : result.meet(j);
        }
        return result == null ? self : result;
    }
}
