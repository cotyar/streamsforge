package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.sql.Ast.*;
import java.util.List;
import java.util.Map;

/** The fully resolved, executable form of a compiled query — everything the runtime needs, independent of the
 *  frozen public DTOs. Immutable once built; shared across all executors of a plan. */
public final class CompiledPlan {
    public List<CompiledSource> sources; // [0] = FROM; [1..] mirror joins order
    public List<CompiledJoin> joins;
    public Expr where;
    public List<Expr> groupBy;
    public WindowSpec window;
    public EmitMode emit = EmitMode.FINAL; // meaningful only when window != null
    public List<OutputItem> output;
    public List<AggregateCallExpr> aggregateNodes;
    /** Reference-identity map (AggregateCallExpr -> index into aggregateNodes). */
    public Map<AggregateCallExpr, Integer> aggregateIndex;
    /** Reference-identity map. */
    public Map<Expr, Binding> bindings;
    public boolean hasAggregates;
    public String planSummary;
    public SourceSchema outputSchema;
    public List<String> sourceNames;
    public String sourceLabel; // comma-joined source names, used as output _source

    /** Plan 008 W3: non-null only for a set-operation root. When set, EVERY other field above is a benign
     *  placeholder; the union-root plan has no FROM/JOIN chain of its own. */
    public List<CompiledPlan> unionBranches;
}
