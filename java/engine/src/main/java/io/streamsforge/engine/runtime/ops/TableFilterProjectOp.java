package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.WorkingRow;
import java.util.ArrayList;
import java.util.List;

/**
 * WHERE filtering, plus terminal projection to the table's output row when no GROUP BY / LATEST BY sits downstream.
 * A key-retraction row (client {@code _retract}) bypasses WHERE — it never carries the fields WHERE would test.
 * STATE: none.
 */
public final class TableFilterProjectOp implements ITableOp {
    private final CompiledTablePlan plan;

    public TableFilterProjectOp(CompiledTablePlan plan) { this.plan = plan; }

    /** WHERE only — used when a reduce/latest-by op is downstream and will perform its own projection. */
    public List<TableRowDelta> onBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) {
            if (!isKeyRetraction(d.row()) && !passesWhere(d.row())) continue;
            results.add(d);
        }
        return results;
    }

    /** WHERE + projection — used when this op is the terminal stage. */
    public List<TableDelta> onBatchTerminal(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableDelta>();
        for (var d : input) {
            if (!isKeyRetraction(d.row()) && !passesWhere(d.row())) continue;
            results.add(new TableDelta(projectRow(d.row(), plan), d.weight()));
        }
        return results;
    }

    private boolean passesWhere(WorkingRow row) {
        if (plan.where == null) return true;
        return ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(plan.where, new EvalContext(row, plan.bindings)));
    }

    private static boolean isKeyRetraction(WorkingRow row) {
        for (String alias : row.aliases()) {
            if (Boolean.TRUE.equals(row.fields().get(alias + "_" + TableIngestOp.RETRACT_FIELD))) return true;
        }
        return false;
    }

    private static EventRecord projectRow(WorkingRow row, CompiledTablePlan plan) {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, plan.bindings);
        for (var item : plan.output) evt.put(item.name, ExpressionEvaluator.eval(item.expression, ctx));
        evt.put(EventRecord.TIMESTAMP_FIELD, row.ts());
        evt.put(EventRecord.SOURCE_FIELD, plan.sourceLabel);
        return evt;
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
