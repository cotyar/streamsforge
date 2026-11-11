package io.streamsforge.engine.runtime;

import io.streamsforge.engine.TableDelta;
import java.util.List;

/**
 * Plan 011 wave C2 — per-table ROW RETENTION machinery, Engine side.
 *
 * <p>A retention scope is "one place a row is retained, that can hand back the oldest ones as ordinary retraction
 * deltas". Retention is defined at the state that actually owns the row (the LATEST BY op's per-key map, or the
 * consolidated output ledger), because trimming only derived copies would report success while every real
 * structure kept growing. Eviction produces deltas (negative weight, same row) so every consumer stays consistent;
 * {@link TableDelta#retention()} marks them.
 *
 * <p>ORDERING IS BY EVENT TIME, not wall clock or hash order: (ts, identity-string) is a total order with a
 * deterministic tie-break, which makes eviction replayable. TTL is event-time too: the cutoff is
 * {@link #maxObservedTs()} - ttlMs.
 */
public interface IRetentionScope {
    /** How many rows/keys this scope currently retains — the quantity a MaxRows bound bounds. */
    int retainedCount();

    /** The highest event timestamp admitted so far; the TTL cutoff is measured back from it. */
    long maxObservedTs();

    /** Removes the {@code count} oldest retained entries and returns their retraction deltas (retention = true). */
    List<TableDelta> evictOldest(int count);

    /** Removes every retained entry whose event timestamp is strictly older than {@code cutoffTs}. */
    List<TableDelta> evictOlderThan(long cutoffTs);
}
