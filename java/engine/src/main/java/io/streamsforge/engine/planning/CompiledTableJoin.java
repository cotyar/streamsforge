package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.List;

/** One compiled JOIN stage (table mode). Plain data holder; fields set directly by the TablePlanner. */
public final class CompiledTableJoin {
    public JoinKind kind;
    public String alias;
    public String sourceName;
    public SourceSchema schema;
    public boolean isTable;
    /** Plan 008: for INNER/CROSS/SEMI/ANTI/SCALAR, the first equi-key component only, with every OTHER component
     *  folded back into {@link #residual}. For LEFT/RIGHT/FULL this is simply leftKeys[0] — TableOuterJoinOp
     *  reads leftKeys/rightKeys directly and residual is then the PURE residual. */
    public Expr leftKey;
    public Expr rightKey;
    public Expr residual;
    public List<Expr> leftKeys;
    public List<Expr> rightKeys;
    public CompiledTablePlan derivedPlan;
    /** Plan 002 L2: set only when kind == UNNEST. */
    public Expr unnestExpr;
}
