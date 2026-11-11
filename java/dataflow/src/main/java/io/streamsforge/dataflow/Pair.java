package io.streamsforge.dataflow;

/** A two-tuple; the element type of keyed collections. */
public record Pair<A, B>(A first, B second) {
    public static <A, B> Pair<A, B> of(A a, B b) { return new Pair<>(a, b); }
}
