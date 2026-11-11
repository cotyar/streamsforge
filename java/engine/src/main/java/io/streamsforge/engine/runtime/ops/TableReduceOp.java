package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.AggregateLookup;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.IZAggregator;
import io.streamsforge.engine.runtime.NetDictionary;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ZAggregator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GROUP BY + aggregates, unwindowed: running aggregates that emit retraction/assertion pairs as groups change.
 * STATE: {@link #groups} (group-key → running aggregate state). A group with total weight exactly 0 is dropped; a
 * NEGATIVE total weight is deliberately KEPT (a retraction that arrived for a group never asserted must not destroy
 * the state and let the next assert rebuild a group of one — see {@link #unmatchedRetractions}).
 */
public final class TableReduceOp implements ITableOp {
    /** Per-group running state. */
    public static final class GroupState {
        public Object[] groupValues;
        public IZAggregator[] aggregators;
        public long totalWeight;
    }

    private long unmatchedRetractions;

    /** How many retractions this GROUP BY has received for a group it had never asserted. Non-zero means the table
     *  was attached to an upstream that ALREADY HELD ROWS that were never replayed to it. */
    public long unmatchedRetractions() { return unmatchedRetractions; }

    private final CompiledTablePlan plan;

    public final Map<String, GroupState> groups = new NetDictionary<>();

    public TableReduceOp(CompiledTablePlan plan) { this.plan = plan; }

    public List<TableDelta> onBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableDelta>();
        for (var d : input) results.addAll(onDelta(d.row(), d.weight()));
        return results;
    }

    private List<TableDelta> onDelta(WorkingRow row, long weight) {
        Object[] groupValues = evalGroupValues(row);
        String key = TableKeyEncoding.encodeGroupKey(groupValues);

        var state = groups.get(key);
        if (state == null) {
            state = new GroupState();
            state.groupValues = groupValues;
            state.aggregators = createAggregators();
            groups.put(key, state);
        }

        var results = new ArrayList<TableDelta>();
        boolean hadOutput = state.totalWeight > 0;
        EventRecord oldRow = hadOutput ? buildRow(state) : null;

        if (weight < 0 && state.totalWeight <= 0) unmatchedRetractions++;

        for (int i = 0; i < plan.aggregateNodes.size(); i++) {
            var node = plan.aggregateNodes.get(i);
            Object val = node.isStar ? (Object) Boolean.TRUE : ExpressionEvaluator.eval(node.arg, new EvalContext(row, plan.bindings));
            state.aggregators[i].apply(val, weight);
        }
        state.totalWeight += weight;

        if (oldRow != null) results.add(new TableDelta(oldRow, -1));

        if (state.totalWeight > 0) {
            results.add(new TableDelta(buildRow(state), 1));
        } else if (state.totalWeight == 0 && java.util.Arrays.stream(state.aggregators).allMatch(IZAggregator::isEmpty)) {
            // Zero weight AND nothing left in any accumulator is a group that genuinely has no rows — drop it.
            // Zero weight alone is not enough: a retraction that arrives before its own assertion leaves
            // {+r2, -r1} (weight 0, SUM r2 - r1); dropping that made GROUP BY depend on delivery order. Such a
            // group is kept, emits nothing while its weight is not positive, and converges once the missing
            // assertion lands. Mirrors the C# fix (TableReduceOrderInsensitivityTests).
            groups.remove(key);
        }
        return results;
    }

    private IZAggregator[] createAggregators() {
        var arr = new IZAggregator[plan.aggregateNodes.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = ZAggregator.create(plan.aggregateNodes.get(i));
        return arr;
    }

    private Object[] evalGroupValues(WorkingRow row) {
        if (plan.groupBy == null) return new Object[0];
        var ctx = new EvalContext(row, plan.bindings);
        var arr = new Object[plan.groupBy.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = ExpressionEvaluator.eval(plan.groupBy.get(i), ctx);
        return arr;
    }

    private EventRecord buildRow(GroupState state) {
        var evt = new EventRecord();
        AggregateLookup lookup = node -> state.aggregators[plan.aggregateIndex.get(node)].result();
        var dummyRow = new WorkingRow(0, new ArrayList<>(), new LinkedHashMap<>());
        var ctx = new EvalContext(dummyRow, plan.bindings, lookup);

        for (var item : plan.output) {
            Object value = item.groupByIndex != null ? state.groupValues[item.groupByIndex] : ExpressionEvaluator.eval(item.expression, ctx);
            evt.put(item.name, value);
        }

        evt.put(EventRecord.TIMESTAMP_FIELD, 0L);
        evt.put(EventRecord.SOURCE_FIELD, plan.sourceLabel);
        return evt;
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
