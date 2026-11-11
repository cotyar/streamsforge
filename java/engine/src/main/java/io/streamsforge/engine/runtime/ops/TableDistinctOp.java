package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan 008 W3 UNION (distinct): per canonical row key, tracks the running net weight and emits a +1 assertion only
 * when the weight crosses zero upward, and a -1 retraction when it crosses back — set semantics over a Z-set.
 * STATE: canonical row key → running weight (entries netting to exactly 0 are dropped).
 */
public final class TableDistinctOp implements ITableOp {
    private final Map<String, Long> weights = new HashMap<>();

    public List<TableDelta> onBatch(Epoch epoch, List<TableDelta> input) {
        var results = new ArrayList<TableDelta>();
        for (var d : input) {
            String key = JsonText.serializeCanonicalRow(d.row());
            long before = weights.getOrDefault(key, 0L);
            long after = before + d.weight();

            if (after == 0) weights.remove(key);
            else weights.put(key, after);

            boolean wasPresent = before > 0;
            boolean isPresent = after > 0;
            if (!wasPresent && isPresent) results.add(new TableDelta(d.row(), 1));
            else if (wasPresent && !isPresent) results.add(new TableDelta(d.row(), -1));
        }
        return results;
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
