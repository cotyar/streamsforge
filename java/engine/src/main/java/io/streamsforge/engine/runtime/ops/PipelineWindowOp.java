package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.planning.CompiledPlan;
import io.streamsforge.engine.runtime.Aggregator;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.AggregateLookup;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.NetDictionary;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains per-(window,group) aggregate state for TUMBLING/HOPPING/SESSION windows. EMIT CHANGES additionally
 * emits an update row (_final=false) on every contributing input row; closed windows always emit a final row
 * (watermark passing window end / session gap).
 *
 * <p>STATE: {@link #states} (window-key string → WindowState) and {@link #openSession} (group-key string → the
 * currently-open session for SESSION windows, a subset view of states for O(1) lookup — not independent state).
 */
public final class PipelineWindowOp {
    /** Running per-window aggregate state. */
    public static final class WindowState {
        public long start;
        public long end;
        public Object[] groupValues;
        public String groupKeyStr;
        public Aggregator[] aggregators;
    }

    private final CompiledPlan plan;

    /** This op's primary state: window-key string → running per-window aggregate state (.NET Dictionary order). */
    public final Map<String, WindowState> states = new NetDictionary<>();

    /** Derived index (group-key → currently-open session), SESSION windows only. */
    public final Map<String, WindowState> openSession = new NetDictionary<>();

    private long sessionSeq;

    public PipelineWindowOp(CompiledPlan plan) { this.plan = plan; }

    private static long ms(java.time.Duration d) { return (long) Math.rint(d.toNanos() / 1_000_000.0); }

    public List<EventRecord> onRow(WorkingRow row) {
        var results = new ArrayList<EventRecord>();
        Object[] groupValues = evalGroupValues(row);
        String groupKeyStr = TableKeyEncoding.encodeGroupKey(groupValues);
        var targets = resolveTargetWindows(row.ts(), groupKeyStr, groupValues);

        for (var state : targets) {
            for (int i = 0; i < plan.aggregateNodes.size(); i++) {
                var node = plan.aggregateNodes.get(i);
                Object val = node.isStar ? (Object) Boolean.TRUE : ExpressionEvaluator.eval(node.arg, new EvalContext(row, plan.bindings));
                state.aggregators[i].add(val);
            }
            if (plan.emit == EmitMode.CHANGES) results.add(buildRow(state, false));
        }
        return results;
    }

    public List<EventRecord> evict(long watermark) {
        var results = new ArrayList<EventRecord>();
        List<String> toClose = null;

        for (var kv : states.entrySet()) {
            var state = kv.getValue();
            boolean due = plan.window instanceof SessionWindowSpec sw
                ? watermark > state.end + ms(sw.gap)
                : watermark >= state.end;
            if (!due) continue;

            results.add(buildRow(state, true));
            if (toClose == null) toClose = new ArrayList<>();
            toClose.add(kv.getKey());
        }

        if (toClose != null) {
            for (String key : toClose) {
                var state = states.get(key);
                states.remove(key);
                var open = openSession.get(state.groupKeyStr);
                if (open != null && open == state) openSession.remove(state.groupKeyStr);
            }
        }
        return results;
    }

    private List<WindowState> resolveTargetWindows(long ts, String groupKeyStr, Object[] groupValues) {
        var result = new ArrayList<WindowState>();
        if (plan.window instanceof TumblingWindowSpec t) {
            long size = ms(t.size);
            long start = floorDiv(ts, size) * size;
            result.add(getOrCreate(start, start + size, groupKeyStr, groupValues));
        } else if (plan.window instanceof HoppingWindowSpec h) {
            long size = ms(h.size);
            long advance = ms(h.advance);
            long kMin = floorDiv(ts - size, advance) + 1;
            long kMax = floorDiv(ts, advance);
            for (long k = kMin; k <= kMax; k++) {
                long t0 = k * advance;
                result.add(getOrCreate(t0, t0 + size, groupKeyStr, groupValues));
            }
        } else if (plan.window instanceof SessionWindowSpec s) {
            long gap = ms(s.gap);
            var open = openSession.get(groupKeyStr);
            if (open != null && ts <= open.end + gap) {
                open.end = Math.max(open.end, ts);
                result.add(open);
                return result;
            }
            var created = new WindowState();
            created.start = ts;
            created.end = ts;
            created.groupValues = groupValues;
            created.groupKeyStr = groupKeyStr;
            created.aggregators = createAggregators();
            states.put("S|" + groupKeyStr + "|" + (sessionSeq++), created);
            openSession.put(groupKeyStr, created);
            result.add(created);
        }
        return result;
    }

    private WindowState getOrCreate(long start, long end, String groupKeyStr, Object[] groupValues) {
        String key = start + "|" + end + "|" + groupKeyStr;
        var existing = states.get(key);
        if (existing != null) return existing;
        var state = new WindowState();
        state.start = start;
        state.end = end;
        state.groupValues = groupValues;
        state.groupKeyStr = groupKeyStr;
        state.aggregators = createAggregators();
        states.put(key, state);
        return state;
    }

    private Aggregator[] createAggregators() {
        var arr = new Aggregator[plan.aggregateNodes.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = Aggregator.create(plan.aggregateNodes.get(i));
        return arr;
    }

    private Object[] evalGroupValues(WorkingRow row) {
        if (plan.groupBy == null) return new Object[0];
        var ctx = new EvalContext(row, plan.bindings);
        var arr = new Object[plan.groupBy.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = ExpressionEvaluator.eval(plan.groupBy.get(i), ctx);
        return arr;
    }

    private EventRecord buildRow(WindowState state, boolean isFinal) {
        var evt = new EventRecord();
        AggregateLookup lookup = node -> state.aggregators[plan.aggregateIndex.get(node)].result();
        var dummyRow = new WorkingRow(state.end, new ArrayList<>(), new LinkedHashMap<>());
        var ctx = new EvalContext(dummyRow, plan.bindings, lookup);

        for (var item : plan.output) {
            Object value = item.groupByIndex != null ? state.groupValues[item.groupByIndex] : ExpressionEvaluator.eval(item.expression, ctx);
            evt.put(item.name, value);
        }

        evt.put(EventRecord.TIMESTAMP_FIELD, state.end);
        evt.put(EventRecord.SOURCE_FIELD, plan.sourceLabel);
        evt.put("window_start", state.start);
        evt.put("window_end", state.end);
        if (plan.emit == EmitMode.CHANGES) evt.put("_final", isFinal);
        return evt;
    }

    private static long floorDiv(long a, long b) {
        long q = a / b;
        long r = a % b;
        if (r != 0 && (r < 0) != (b < 0)) q--;
        return q;
    }
}
