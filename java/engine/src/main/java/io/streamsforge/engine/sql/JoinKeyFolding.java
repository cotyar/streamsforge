package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.BinaryExpr;
import io.streamsforge.engine.sql.Ast.Expr;
import java.util.List;

/**
 * Plan 008: bridges JoinBinding's composite key LISTS back to the single-key "first key + residual" shape the
 * single-key ops (TableJoinOp, TableSemiAntiOp, PipelineJoinOp, PipelineSubqueryOp) still expect.
 */
public final class JoinKeyFolding {
    private JoinKeyFolding() {}

    /** Every key component AFTER the first becomes an extra {@code leftKeys[i] = rightKeys[i]} conjunct ANDed
     *  onto {@code residual}, in list order. A no-op when {@code leftKeys} is null or has exactly one element. */
    public static Expr foldExtraKeysIntoResidual(List<Expr> leftKeys, List<Expr> rightKeys, Expr residual) {
        if (leftKeys == null) return residual;
        for (int i = 1; i < leftKeys.size(); i++) {
            Expr l = leftKeys.get(i);
            var eq = new BinaryExpr("=", l, rightKeys.get(i), l.line, l.column);
            residual = residual == null ? eq : new BinaryExpr("AND", residual, eq, residual.line, residual.column);
        }
        return residual;
    }
}
