package io.streamsforge.engine.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Z-set index for one side of a table-mode equi-join: join-key → (canonical row text → (row, weight)).
 *  Weight is accumulated per distinct row so repeated identical deltas net out instead of growing the bucket
 *  unboundedly; entries whose weight nets to zero are pruned immediately. */
public final class ZSetIndex {
    /** A row with its accumulated weight. */
    public record Entry(WorkingRow row, long weight) {}

    private final Map<String, Map<String, Entry>> byKey = new HashMap<>();

    /** Snapshot of the bucket (so callers may mutate the index while iterating). */
    public List<Entry> lookup(String key) {
        var bucket = byKey.get(key);
        var result = new ArrayList<Entry>();
        if (bucket == null) return result;
        for (var e : bucket.values()) if (e.weight() != 0) result.add(e);
        return result;
    }

    public void apply(String key, String rowCanonical, WorkingRow row, long weight) {
        var bucket = byKey.computeIfAbsent(key, k -> new NetDictionary<>());
        var existing = bucket.get(rowCanonical);
        if (existing != null) {
            long newWeight = existing.weight() + weight;
            if (newWeight == 0) bucket.remove(rowCanonical);
            else bucket.put(rowCanonical, new Entry(row, newWeight));
        } else if (weight != 0) {
            bucket.put(rowCanonical, new Entry(row, weight));
        }
    }
}
