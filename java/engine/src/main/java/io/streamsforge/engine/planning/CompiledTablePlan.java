package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.sql.Ast.*;
import java.util.List;
import java.util.Map;

/** The fully resolved, executable form of a compiled TABLE query — the table-mode analogue of
 *  {@link CompiledPlan} (no window/emit: tables are unwindowed by construction). */
public final class CompiledTablePlan {
    public List<CompiledTableSource> sources; // [0] = FROM; [1..] mirror joins order
    public List<CompiledTableJoin> joins;
    public Expr where;
    public List<Expr> groupBy;
    public List<OutputItem> output;
    public List<AggregateCallExpr> aggregateNodes;
    /** Reference-identity map. */
    public Map<AggregateCallExpr, Integer> aggregateIndex;
    /** Reference-identity map. */
    public Map<Expr, Binding> bindings;
    public boolean hasAggregates;
    public String planSummary;
    public List<String> streamInputs;
    public List<String> tableInputs;
    public String sourceLabel; // comma-joined source names, used as output _source
    public SourceSchema outputSchema;
    /** Plan 002 L3: LATEST BY key expressions — null unless the query has a LATEST BY clause. */
    public List<Expr> latestBy;

    /** Plan 008 W3: non-null only for a set-operation root (same shape as CompiledPlan.unionBranches). */
    public List<CompiledTablePlan> unionBranches;

    /** Plan 008 W3: meaningful only when unionBranches is set — true for UNION ALL (the default), false for UNION. */
    public boolean unionAll = true;
}
