package io.streamsforge.engine.dataflow;

/** Identifies one edge of the dataflow graph (a producer stage → consumer stage link), assigned at plan time. */
public record EdgeId(int value) implements Comparable<EdgeId> {
    @Override public int compareTo(EdgeId other) { return Integer.compare(value, other.value); }

    @Override public String toString() { return "Edge(" + value + ")"; }
}
