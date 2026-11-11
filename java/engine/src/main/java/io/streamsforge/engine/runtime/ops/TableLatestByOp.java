package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.IRetentionScope;
import io.streamsforge.engine.runtime.JsonText;
import io.streamsforge.engine.runtime.NetDictionary;
import io.streamsforge.engine.runtime.RetentionOrder;
import io.streamsforge.engine.runtime.RetentionOrder.TsKey;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Plan 002 L3: {@code LATEST BY (key...)} — keeps only the most recent (by event timestamp) row per key and emits
 * retract(old)+assert(new) as the key's row is superseded. A strictly older late arrival is ignored; a retraction
 * only counts when it retracts the row currently held for its key (content match), except a client KEY RETRACTION
 * ({@code _retract = true}) which drops whatever is held. Any positive weight is a single assert (multiplicity is
 * not tracked). Also the {@link IRetentionScope} for a LATEST BY plan (plan 011 C2).
 *
 * <p>STATE: {@link #current} — encoded key → retained row + its event ts.
 */
public final class TableLatestByOp implements ITableOp, IRetentionScope {
    /** The retained row for one key. */
    public static final class KeyState {
        public WorkingRow row;
        public long ts;
    }

    private final CompiledTablePlan plan;
    private final List<Expr> keys;

    public final Map<String, KeyState> current = new NetDictionary<>();

    // Plan 011 C2 — retention (inert until enableRetention): (retained row's event ts, encoded key) order.
    private boolean retentionEnabled;
    private TreeSet<TsKey> order;
    private long maxObservedTs;

    public TableLatestByOp(CompiledTablePlan plan, List<Expr> keys) {
        this.plan = plan;
        this.keys = keys;
    }

    public List<TableDelta> onBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableDelta>();
        for (var d : input) results.addAll(onDelta(d.row(), d.weight()));
        return results;
    }

    private List<TableDelta> onDelta(WorkingRow row, long weight) {
        var results = new ArrayList<TableDelta>();
        String key = encodeKey(row);

        // The event-time high-water mark a TTL cutoff is measured back from — taken over every ADMITTED assertion.
        if (weight > 0 && row.ts() > maxObservedTs) maxObservedTs = row.ts();

        if (weight > 0) {
            var existing = current.get(key);
            if (existing != null) {
                if (row.ts() < existing.ts) return results; // strictly older late arrival: ignored
                results.add(new TableDelta(projectRow(existing.row), -1));
                if (order != null) order.remove(new TsKey(existing.ts, key));
            }
            var ks = new KeyState();
            ks.row = row; ks.ts = row.ts();
            current.put(key, ks);
            if (order != null) order.add(new TsKey(row.ts(), key));
            results.add(new TableDelta(projectRow(row), 1));
            return results;
        }

        // KEY RETRACTION short-circuits the content-match rule below.
        if (isKeyRetraction(row)) {
            var retained = current.remove(key);
            if (retained != null) {
                if (order != null) order.remove(new TsKey(retained.ts, key));
                results.add(new TableDelta(projectRow(retained.row), -1));
            }
            return results;
        }

        // Retraction: only meaningful when it retracts the row THIS op currently holds for the key.
        var cur = current.get(key);
        if (cur != null && sameRow(cur.row, row)) {
            current.remove(key);
            if (order != null) order.remove(new TsKey(cur.ts, key));
            results.add(new TableDelta(projectRow(cur.row), -1));
        }
        return results;
    }

    // ------------------------------------------------------------------
    // Plan 011 C2 — IRetentionScope.
    // ------------------------------------------------------------------

    public void enableRetention() {
        if (retentionEnabled) return;
        retentionEnabled = true;
        order = new TreeSet<>(RetentionOrder.COMPARATOR);
        for (var kv : current.entrySet()) order.add(new TsKey(kv.getValue().ts, kv.getKey()));
    }

    @Override public int retainedCount() { return current.size(); }

    @Override public long maxObservedTs() { return maxObservedTs; }

    @Override public List<TableDelta> evictOldest(int count) {
        var results = new ArrayList<TableDelta>();
        for (int i = 0; i < count && order != null && !order.isEmpty(); i++) evictOne(order.first(), results);
        return results;
    }

    @Override public List<TableDelta> evictOlderThan(long cutoffTs) {
        var results = new ArrayList<TableDelta>();
        while (order != null && !order.isEmpty() && order.first().ts() < cutoffTs) evictOne(order.first(), results);
        return results;
    }

    private void evictOne(TsKey entry, List<TableDelta> results) {
        order.remove(entry);
        var state = current.remove(entry.key());
        if (state == null) return;
        results.add(new TableDelta(projectRow(state.row), -1, true));
    }

    private static boolean sameRow(WorkingRow a, WorkingRow b) {
        return JsonText.serializeCanonicalRow(a.fields()).equals(JsonText.serializeCanonicalRow(b.fields()));
    }

    private static boolean isKeyRetraction(WorkingRow row) {
        for (String alias : row.aliases()) {
            if (Boolean.TRUE.equals(row.fields().get(alias + "_" + TableIngestOp.RETRACT_FIELD))) return true;
        }
        return false;
    }

    private String encodeKey(WorkingRow row) {
        var ctx = new EvalContext(row, plan.bindings);
        var values = new Object[keys.size()];
        for (int i = 0; i < values.length; i++) values[i] = ExpressionEvaluator.eval(keys.get(i), ctx);
        return TableKeyEncoding.encodeGroupKey(values);
    }

    private EventRecord projectRow(WorkingRow row) {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, plan.bindings);
        for (var item : plan.output) evt.put(item.name, ExpressionEvaluator.eval(item.expression, ctx));
        evt.put(EventRecord.TIMESTAMP_FIELD, row.ts());
        evt.put(EventRecord.SOURCE_FIELD, plan.sourceLabel);
        return evt;
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
