package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.planning.CompiledPlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.WorkingRow;
import java.util.ArrayList;
import java.util.List;

/**
 * WHERE filtering, with terminal projection to the pipeline's output row when no WINDOW sits downstream (the
 * pipeline-mode mirror of {@link TableFilterProjectOp}). A windowed plan hands filtered rows to
 * {@link PipelineWindowOp}, which performs its OWN projection; an unwindowed plan projects immediately here.
 * STATE: none.
 */
public final class PipelineFilterProjectOp {
    private final CompiledPlan plan;

    public PipelineFilterProjectOp(CompiledPlan plan) { this.plan = plan; }

    /** WHERE only — used when a PipelineWindowOp is downstream and will perform its own projection. */
    public List<WorkingRow> onBatch(List<WorkingRow> rows) {
        var results = new ArrayList<WorkingRow>();
        for (var row : rows) if (passesWhere(row)) results.add(row);
        return results;
    }

    /** WHERE + projection — used when this op is the terminal stage (no WINDOW). */
    public List<EventRecord> onBatchTerminal(List<WorkingRow> rows) {
        var results = new ArrayList<EventRecord>();
        for (var row : rows) if (passesWhere(row)) results.add(projectRow(row, plan));
        return results;
    }

    private boolean passesWhere(WorkingRow row) {
        if (plan.where == null) return true;
        var whereResult = ExpressionEvaluator.eval(plan.where, new EvalContext(row, plan.bindings));
        return ExpressionEvaluator.isTrue(whereResult);
    }

    private static EventRecord projectRow(WorkingRow row, CompiledPlan compiled) {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, compiled.bindings);
        for (var item : compiled.output) evt.put(item.name, ExpressionEvaluator.eval(item.expression, ctx));
        evt.put(EventRecord.TIMESTAMP_FIELD, row.ts());
        evt.put(EventRecord.SOURCE_FIELD, compiled.sourceLabel);
        return evt;
    }
}
