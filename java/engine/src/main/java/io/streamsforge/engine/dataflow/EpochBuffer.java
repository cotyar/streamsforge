package io.streamsforge.engine.dataflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Buffers incoming {@link DeltaBatch}es until the operator's combined frontier (see {@link FrontierTracker}) permits
 * flushing them: "buffer inputs per epoch → on frontier advance to E: process all batches &lt;= E in deterministic
 * order".
 *
 * <p>THE core replay guarantee: the flush sequence for a fixed set of batches depends ONLY on each batch's (epoch,
 * edgeId, fromPartition) — never on arrival order.
 *
 * <p>INVARIANTS: a batch is only ever returned once, by the first onFrontier call whose frontier is &gt;= its epoch;
 * within a flush, batches are ordered by (epoch, edgeId, fromPartition) ascending; no silent drops; epoch markers
 * (zero deltas) are buffered and flushed like any other batch.
 *
 * <p>THIS BUFFER IS UNBOUNDED, and nothing in this repo bounds it: while any ONE upstream partition is stalled the
 * combined frontier cannot advance, so onFrontier returns nothing and every batch from every OTHER partition
 * accumulates here — memory proportional to (stall duration x ingest rate), with no ceiling. The counters
 * ({@link #batchCount()}/{@link #deltaCount()}) are diagnostics only; wiring a real bound is a semantics decision
 * (block ingest, or drop and mark the table degraded), deliberately not made here. See orleans/DESIGN.md's "Known
 * ceilings".
 */
public final class EpochBuffer {
    private final List<DeltaBatch> pending = new ArrayList<>();

    /** Number of batches currently buffered (not yet flushed). */
    public int batchCount() { return pending.size(); }

    /** Total delta rows across all currently buffered batches. A diagnostic only: it bounds nothing. */
    public long deltaCount() {
        long total = 0;
        for (var batch : pending) total += batch.deltas().size();
        return total;
    }

    public void add(DeltaBatch batch) { pending.add(batch); }

    /** Removes and returns every buffered batch with epoch &lt;= {@code frontier}, ordered by (epoch, edgeId,
     *  fromPartition) ascending. Batches above the frontier are left buffered for a later call. */
    public List<DeltaBatch> onFrontier(Epoch frontier) {
        var ready = new ArrayList<DeltaBatch>();
        var remaining = new ArrayList<DeltaBatch>();

        for (var batch : pending) {
            if (batch.epoch().lessOrEqual(frontier)) ready.add(batch);
            else remaining.add(batch);
        }

        pending.clear();
        pending.addAll(remaining);

        ready.sort(Comparator.comparing(DeltaBatch::epoch).thenComparing(DeltaBatch::upstream));
        return ready;
    }
}
