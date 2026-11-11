package io.streamsforge.dataflow;

/** One change to a collection: {@code data} gains {@code diff} (signed) copies at {@code time}. */
public record Update<D, T>(D data, T time, long diff) {}
