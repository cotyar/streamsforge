package io.streamsforge.engine.runtime;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.RowWeight;
import java.util.HashMap;
import java.util.Map;

/**
 * Plan 009 wave D: the Z-set consolidation ledger, extracted to one place (TableExecutor's own ledger,
 * TableGrain's coordinator-mode snapshot, ArrangementGrain's per-partition index all share this arithmetic).
 *
 * <p>THE ALGORITHM: a row's net weight decides whether it is visible, and the final state depends only on the SUM
 * of the deltas applied so far for a key, never on arrival order. That order-independence is why an unmatched
 * NEGATIVE running weight is retained as debt rather than dropped (a retraction can legitimately arrive before its
 * assertion — out-of-order admission, replay, restart-resume fan-in, a FULL OUTER join's null-pad retraction).
 *
 * <p>Kept as two SEPARATE maps — {@link #visible()} (weight &gt; 0) and an internal debt map (weight &lt;= 0) — so
 * visible never needs filtering. The maps are disjoint by construction; a key that nets to exactly 0 is removed
 * from both.
 *
 * <p>Deliberately key-agnostic: callers supply their own already-computed canonical row key.
 */
public final class ConsolidationLedger {
    // Only ever holds weight > 0 entries. Exposed BY REFERENCE (never copied); .NET Dictionary enumeration order.
    private final Map<String, RowWeight> visible = new NetDictionary<>();

    // Outstanding NEGATIVE running weight per canonical key.
    private final Map<String, Long> debt = new HashMap<>();

    /** The current positive-weight rows, keyed by canonical row key. Returns the SAME live map on every call. */
    public Map<String, RowWeight> visible() { return visible; }

    /** Number of canonical keys currently holding outstanding negative debt. */
    public int debtCount() { return debt.size(); }

    /** Folds one more weighted row into the ledger for the given (externally-computed) canonical key. */
    public void apply(String key, EventRecord row, long weight) {
        var existing = visible.get(key);
        long currentWeight = existing != null ? existing.weight() : debt.getOrDefault(key, 0L);
        long newWeight = currentWeight + weight;

        if (newWeight > 0) {
            // Same canonical key => same row content; always store the incoming row.
            visible.put(key, new RowWeight(row, newWeight));
            debt.remove(key);
        } else if (newWeight < 0) {
            visible.remove(key);
            debt.put(key, newWeight);
        } else { // fully cancelled out — no residue in either structure.
            visible.remove(key);
            debt.remove(key);
        }
    }

    /** Seeds a row directly into {@link #visible()} without touching the debt side-table (rebuild-from-checkpoint). */
    public void seed(String key, EventRecord row, long weight) { visible.put(key, new RowWeight(row, weight)); }

    /** Clears both maps back to empty. */
    public void clear() {
        visible.clear();
        debt.clear();
    }
}
