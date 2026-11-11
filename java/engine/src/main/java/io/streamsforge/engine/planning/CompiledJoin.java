package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.time.Duration;
import java.util.List;

/** One compiled JOIN stage (pipeline mode). Plain data holder; fields set directly by the Planner. */
public final class CompiledJoin {
    public JoinKind kind;
    public String alias;
    public String sourceName;
    public SourceSchema schema;
    public Duration within = Duration.ZERO;
    /** Plan 008: first equi-key component only, with every OTHER component folded back into {@link #residual}. */
    public Expr leftKey;
    public Expr rightKey;
    public Expr residual;
    /** Plan 008: every equi-conjunct's left/right operand, in ON-clause order (composite keys). */
    public List<Expr> leftKeys;
    public List<Expr> rightKeys;
    /** Plan 004 N1: set when this JOIN's source is a derived table/CTE. */
    public CompiledPlan derivedPlan;
    /** Plan 002 L2: set only when kind == UNNEST. */
    public Expr unnestExpr;
}
