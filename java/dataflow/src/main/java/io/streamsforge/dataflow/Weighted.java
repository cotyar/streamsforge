package io.streamsforge.dataflow;

/** A value with a multiplicity, as seen by {@link ReduceLogic}. */
public record Weighted<V>(V value, long weight) {}
