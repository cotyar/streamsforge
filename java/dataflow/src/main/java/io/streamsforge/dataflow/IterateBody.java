package io.streamsforge.dataflow;

/** The body of an {@link Collection#iterate iterate}: maps the loop variable to its next value. */
@FunctionalInterface
public interface IterateBody<D, T extends Time<T>> {
    Collection<D, Product<T, LongTime>> apply(IterScope<T> scope, Collection<D, Product<T, LongTime>> variable);
}
