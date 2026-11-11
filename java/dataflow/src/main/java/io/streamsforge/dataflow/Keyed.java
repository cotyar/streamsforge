package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;

/** A collection of {@code (key, value)} pairs with the keyed operators: join, semijoin, antijoin, reduce, count. */
public final class Keyed<K, V, T extends Time<T>> {
    private final Collection<Pair<K, V>, T> coll;

    private Keyed(Collection<Pair<K, V>, T> coll) { this.coll = coll; }

    public static <K, V, T extends Time<T>> Keyed<K, V, T> of(Collection<Pair<K, V>, T> c) { return new Keyed<>(c); }

    public Collection<Pair<K, V>, T> collection() { return coll; }

    /** Indexes this collection into a shareable arrangement. */
    public Arranged<K, V, T> arrange() { return new Arranged<>(coll.worker, coll.out); }

    public <V2, R> Collection<R, T> join(Keyed<K, V2, T> other, JoinFn<K, V, V2, R> fn) {
        return arrange().join(other.arrange(), fn);
    }

    public <V2> Collection<Pair<K, Pair<V, V2>>, T> join(Keyed<K, V2, T> other) {
        return join(other, (k, a, b) -> Pair.of(k, Pair.of(a, b)));
    }

    /** Keeps the pairs whose key is present in {@code keys} (multiplied by the key's multiplicity). */
    public Collection<Pair<K, V>, T> semijoin(Collection<K, T> keys) {
        return join(keys.<K, Unit>keyed(k -> k, k -> Unit.U), (k, v, u) -> Pair.of(k, v));
    }

    /** Keeps the pairs whose key is absent from {@code keys} (presence means a positive count). */
    public Collection<Pair<K, V>, T> antijoin(Collection<K, T> keys) {
        return coll.concat(semijoin(keys.distinct()).negate());
    }

    public <R> Collection<Pair<K, R>, T> reduce(ReduceLogic<K, V, R> logic) { return arrange().reduce(logic); }

    public Collection<Pair<K, Long>, T> count() { return arrange().count(); }
}
