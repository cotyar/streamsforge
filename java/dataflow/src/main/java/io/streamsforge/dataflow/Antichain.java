package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A set of mutually incomparable times, representing the frontier "everything at or beyond one of these". The
 * empty antichain is the end of time (nothing more can happen).
 */
public final class Antichain<T extends Time<T>> {
    private final ArrayList<T> els = new ArrayList<>(1);

    public Antichain() {}

    @SafeVarargs
    public static <T extends Time<T>> Antichain<T> of(T... ts) {
        Antichain<T> a = new Antichain<>();
        for (T t : ts) a.insert(t);
        return a;
    }

    /** Adds {@code t} unless some element is already {@code <= t}; removes elements {@code >= t}. */
    public boolean insert(T t) {
        for (int i = 0; i < els.size(); i++) if (els.get(i).lessEqual(t)) return false;
        els.removeIf(t::lessEqual);
        els.add(t);
        return true;
    }

    /** True if some element is {@code <= t}, i.e. time {@code t} may still receive updates. */
    public boolean lessEqual(T t) {
        for (int i = 0; i < els.size(); i++) if (els.get(i).lessEqual(t)) return true;
        return false;
    }

    /** True if some element is strictly {@code < t}. */
    public boolean lessThan(T t) {
        for (int i = 0; i < els.size(); i++) if (els.get(i).lessThan(t)) return true;
        return false;
    }

    /** {@code this <= other}: every element of {@code other} is at or beyond this frontier. */
    public boolean lessEqual(Antichain<T> other) {
        for (T e : other.els) if (!lessEqual(e)) return false;
        return true;
    }

    public boolean isEmpty() { return els.isEmpty(); }
    public int size() { return els.size(); }
    public List<T> elements() { return els; }
    public void clear() { els.clear(); }

    public Antichain<T> copy() {
        Antichain<T> a = new Antichain<>();
        a.els.addAll(els);
        return a;
    }

    @Override public boolean equals(Object o) {
        return o instanceof Antichain<?> a && new HashSet<>(els).equals(new HashSet<>(a.els));
    }
    @Override public int hashCode() { return new HashSet<>(els).hashCode(); }
    @Override public String toString() { return els.toString(); }
}
