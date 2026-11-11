package io.streamsforge.engine;

/**
 * One Z-set delta: a row entering (+1) or leaving (-1) a table's output. Weights other than ±1 arise from
 * join fan-out (weight multiplication) or upstream retraction cascades.
 *
 * <p>{@code retention} (plan 011 C2, additive, default false): true only for a retraction emitted because the
 * table's ROW RETENTION policy evicted this row, as opposed to one emitted because an upstream input retracted
 * it. An ordinary retraction means "this row is no longer true"; an eviction means "this table is a bounded
 * view and has stopped carrying this row", so history should drop the key's version list with it.
 */
public record TableDelta(EventRecord row, long weight, boolean retention) {
    public TableDelta(EventRecord row, long weight) { this(row, weight, false); }
}
