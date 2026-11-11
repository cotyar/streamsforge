package io.streamsforge.engine.runtime;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.runtime.RetentionOrder.TsKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The retention scope for a plan whose terminal state IS the consolidated output ledger — a plain
 * projection/filter table (no LATEST BY, no GROUP BY). Kept as an ordered index BESIDE the ledger rather than as
 * an ordered ledger, because the ledger is on the hot read path and must not pay for an ordering nobody reads when
 * retention is off. The index is only built when retention is configured.
 */
public final class LedgerRetentionScope implements IRetentionScope {
    private final ConsolidationLedger ledger;
    private final TreeSet<TsKey> order = new TreeSet<>(RetentionOrder.COMPARATOR);
    private final Map<String, Long> tsByKey = new HashMap<>();
    private long maxObservedTs;

    public LedgerRetentionScope(ConsolidationLedger ledger) { this.ledger = ledger; }

    @Override public long maxObservedTs() { return maxObservedTs; }

    @Override public int retainedCount() { return order.size(); }

    /** Called after every {@link ConsolidationLedger#apply} — mirrors the ledger's visibility decision into the
     *  ordered index. Idempotent in both directions. */
    public void observe(String key) {
        var current = ledger.visible().get(key);
        if (current != null) {
            long ts = current.row().timestamp();
            Long previous = tsByKey.get(key);
            if (previous != null) {
                if (previous == ts) return;
                order.remove(new TsKey(previous, key));
            }
            order.add(new TsKey(ts, key));
            tsByKey.put(key, ts);
            if (ts > maxObservedTs) maxObservedTs = ts;
        } else {
            Long stale = tsByKey.remove(key);
            if (stale != null) order.remove(new TsKey(stale, key));
        }
    }

    @Override public List<TableDelta> evictOldest(int count) {
        var results = new ArrayList<TableDelta>();
        for (int i = 0; i < count && !order.isEmpty(); i++) evictOne(order.first(), results);
        return results;
    }

    @Override public List<TableDelta> evictOlderThan(long cutoffTs) {
        var results = new ArrayList<TableDelta>();
        while (!order.isEmpty() && order.first().ts() < cutoffTs) evictOne(order.first(), results);
        return results;
    }

    private void evictOne(TsKey entry, List<TableDelta> results) {
        order.remove(entry);
        tsByKey.remove(entry.key());
        var current = ledger.visible().get(entry.key());
        if (current == null) return;

        // Retract the row's ENTIRE running weight so consolidation nets it to exactly zero.
        results.add(new TableDelta(current.row(), -current.weight(), true));
    }
}
