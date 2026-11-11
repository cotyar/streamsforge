package io.streamsforge.dataflow;

/** A totally ordered {@code long} timestamp (an epoch number). */
public record LongTime(long value) implements Time<LongTime> {
    public static final LongTime ZERO = new LongTime(0);

    public static LongTime of(long v) {
        return new LongTime(v);
    }

    @Override public boolean lessEqual(LongTime o) { return value <= o.value; }
    @Override public LongTime join(LongTime o) { return value >= o.value ? this : o; }
    @Override public LongTime meet(LongTime o) { return value <= o.value ? this : o; }
    @Override public int compareTo(LongTime o) { return Long.compare(value, o.value); }
    @Override public String toString() { return Long.toString(value); }
}
