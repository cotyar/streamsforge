package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.JsonText;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.ZSetIndex;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plan 004 N2 table-mode semi/anti join: {@code [NOT] IN (subquery)} / {@code [NOT] EXISTS}. Output is the left rows
 * (not combined) whose key is present (SEMI) / absent (ANTI) on the right. A right-side presence flip re-emits every
 * currently-indexed left row under that key (negated when it stops passing). NULL left keys are dropped; NULL right
 * keys are ignored (the NOT IN NULL rule). STATE: {@link #left}, {@link #right}.
 */
public final class TableSemiAntiOp implements ITableJoinStage {
    private final boolean isAnti;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Map<Expr, Binding> bindings;

    public final ZSetIndex left = new ZSetIndex();
    public final ZSetIndex right = new ZSetIndex();

    public TableSemiAntiOp(JoinKind kind, Expr leftKey, Expr rightKey, Map<Expr, Binding> bindings) {
        this.isAnti = kind == JoinKind.ANTI;
        this.leftKey = leftKey; this.rightKey = rightKey; this.bindings = bindings;
    }

    @Override public List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) {
            Object keyVal = ExpressionEvaluator.eval(leftKey, new EvalContext(d.row(), bindings));
            if (keyVal == null) continue;

            String keyStr = TableKeyEncoding.encodeScalar(keyVal);
            String rowCanonical = JsonText.serializeCanonicalRow(d.row().fields());
            left.apply(keyStr, rowCanonical, d.row(), d.weight());

            boolean present = !right.lookup(keyStr).isEmpty();
            boolean passes = present != isAnti;
            if (passes) results.add(new TableRowDelta(d.row(), d.weight()));
        }
        return results;
    }

    @Override public List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) {
            Object keyVal = ExpressionEvaluator.eval(rightKey, new EvalContext(d.row(), bindings));
            if (keyVal == null) continue;

            String keyStr = TableKeyEncoding.encodeScalar(keyVal);
            boolean wasPresent = !right.lookup(keyStr).isEmpty();

            String rowCanonical = JsonText.serializeCanonicalRow(d.row().fields());
            right.apply(keyStr, rowCanonical, d.row(), d.weight());

            boolean isPresentNow = !right.lookup(keyStr).isEmpty();
            if (wasPresent == isPresentNow) continue;

            boolean nowPasses = isPresentNow != isAnti;
            int sign = nowPasses ? 1 : -1;
            for (var a : left.lookup(keyStr)) results.add(new TableRowDelta(a.row(), sign * a.weight()));
        }
        return results;
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
