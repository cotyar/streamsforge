package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.JsonText;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ZSetIndex;
import io.streamsforge.engine.sql.Ast.Expr;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table-mode equi-join of two Z-sets on a single key (further equi components are folded into the residual): each
 * arriving delta multiplies its weight against the matching opposite-side bucket, then is indexed on its own side.
 * Retractions fall out for free (weight -1 × matching weights). NULL never matches NULL and is not indexed.
 * STATE: {@link #left} and {@link #right} — unbounded consolidated Z-set indexes.
 */
public final class TableJoinOp implements ITableJoinStage {
    private final Expr leftKey;
    private final Expr rightKey;
    private final Expr residual;
    private final Map<Expr, Binding> bindings;

    public final ZSetIndex left = new ZSetIndex();
    public final ZSetIndex right = new ZSetIndex();

    public TableJoinOp(Expr leftKey, Expr rightKey, Expr residual, Map<Expr, Binding> bindings) {
        this.leftKey = leftKey; this.rightKey = rightKey; this.residual = residual; this.bindings = bindings;
    }

    @Override public List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input) { return onBatch(input, true); }

    @Override public List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input) { return onBatch(input, false); }

    private List<TableRowDelta> onBatch(List<TableRowDelta> input, boolean isLeft) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) onArrival(d.row(), d.weight(), isLeft, results);
        return results;
    }

    private void onArrival(WorkingRow row, long weight, boolean isLeft, List<TableRowDelta> results) {
        Expr keyExpr = isLeft ? leftKey : rightKey;
        Object keyVal = ExpressionEvaluator.eval(keyExpr, new EvalContext(row, bindings));
        if (keyVal == null) return; // SQL equi-join semantics: NULL never matches NULL

        String keyStr = TableKeyEncoding.encodeScalar(keyVal);
        var otherIndex = isLeft ? right : left;

        for (var other : otherIndex.lookup(keyStr)) {
            var combined = isLeft ? WorkingRow.combine(row, other.row()) : WorkingRow.combine(other.row(), row);
            if (residual != null) {
                boolean ok = ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(residual, new EvalContext(combined, bindings)));
                if (!ok) continue;
            }
            results.add(new TableRowDelta(combined, weight * other.weight()));
        }

        var ownIndex = isLeft ? left : right;
        String rowCanonical = JsonText.serializeCanonicalRow(row.fields());
        ownIndex.apply(keyStr, rowCanonical, row, weight);
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
