package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.time.Duration;
import java.util.List;

/**
 * A resolved, validated JOIN: its equi-key component lists (null only when validation already failed, or this
 * kind has no equi-key concept — CROSS, UNNEST) and residual filter.
 * Constructor args in C# declaration order: kind, alias, sourceName, within, leftKeys, rightKeys, residual,
 * isTable, unnestExpr (shorter overloads without unnestExpr / isTable+unnestExpr are provided).
 * Plan 008: leftKeys/rightKeys hold every equi-conjunct's operands in ON-clause order (composite keys);
 * residual holds only the conjuncts that are NOT an equi-comparison between the two sides.
 */
public final class JoinBinding {
    public final JoinKind kind;
    public final String alias;
    public final String sourceName;
    public final Duration within;
    public final List<Expr> leftKeys;
    public final List<Expr> rightKeys;
    public final Expr residual;
    /** Table mode only: whether sourceName resolved against the table namespace (vs. streams). */
    public final boolean isTable;
    /** Plan 002 L2: set only when kind == UNNEST — the expression to unnest. */
    public final Expr unnestExpr;

    public JoinBinding(JoinKind kind, String alias, String sourceName, Duration within, List<Expr> leftKeys,
                       List<Expr> rightKeys, Expr residual, boolean isTable, Expr unnestExpr) {
        this.kind = kind;
        this.alias = alias;
        this.sourceName = sourceName;
        this.within = within;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.residual = residual;
        this.isTable = isTable;
        this.unnestExpr = unnestExpr;
    }

    public JoinBinding(JoinKind kind, String alias, String sourceName, Duration within, List<Expr> leftKeys,
                       List<Expr> rightKeys, Expr residual) {
        this(kind, alias, sourceName, within, leftKeys, rightKeys, residual, false, null);
    }

    public JoinBinding(JoinKind kind, String alias, String sourceName, Duration within, List<Expr> leftKeys,
                       List<Expr> rightKeys, Expr residual, boolean isTable) {
        this(kind, alias, sourceName, within, leftKeys, rightKeys, residual, isTable, null);
    }
}
