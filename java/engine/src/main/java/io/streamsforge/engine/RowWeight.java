package io.streamsforge.engine;

/** C# tuple {@code (EventRecord Row, long Weight)} — a consolidated output row with its running Z-set weight. */
public record RowWeight(EventRecord row, long weight) {}
