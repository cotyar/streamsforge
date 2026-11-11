package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.Expr;

/**
 * Plan 004 N4: one component of a scalar subquery's equality correlation —
 * {@code innerExpr = outerAlias.outerField} (or the reverse) found as a top-level WHERE conjunct of the
 * subquery. Planner uses {@code innerExpr} as (one component of) the decorrelated join's inner-side key
 * and {@code outerAlias}/{@code outerField} to build the outer-side key.
 * Constructor args in C# declaration order: innerExpr, outerAlias, outerField.
 */
public final class CorrelationKey {
    public final Expr innerExpr;
    public final String outerAlias;
    public final String outerField;

    public CorrelationKey(Expr innerExpr, String outerAlias, String outerField) {
        this.innerExpr = innerExpr;
        this.outerAlias = outerAlias;
        this.outerField = outerField;
    }
}
