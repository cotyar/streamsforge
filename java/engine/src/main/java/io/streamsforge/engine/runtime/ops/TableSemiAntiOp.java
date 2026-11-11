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
 * currently-indexed left row under that key (negated when it stops passing).
 *
 * <p>NULLs follow PostgreSQL's three-valued logic (a WHERE conjunct keeps a row only when TRUE): {@code x IN (S)} is
 * TRUE only on a match, so NULL-keyed rows on either side never pass a SEMI. {@code x NOT IN (S)} is TRUE when S is
 * empty (even for a NULL x), FALSE on a match, and otherwise NULL (filtered) when x is NULL or S contains a NULL — one
 * NULL in S filters every non-matching outer row, and its retraction brings them back. NULL-keyed rows are therefore
 * tracked on both sides ({@link #leftNulls}, {@link #rightNulls}) and present right keys are counted. Before
 * 2026-11-11 NULL right keys were ignored (plan 004's deviation). The right side is broadcast to every partition, so
 * each partition's copy of this global state is complete. STATE: {@link #left}, {@link #right}, {@link #leftNulls},
 * {@link #rightNulls}.
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

    private static final String NULL_BUCKET = "";
    public final ZSetIndex leftNulls = new ZSetIndex();
    public final ZSetIndex rightNulls = new ZSetIndex();
    private int presentRightKeys;

    private boolean rightHasNull() { return !rightNulls.lookup(NULL_BUCKET).isEmpty(); }
    private boolean rightIsEmpty() { return presentRightKeys == 0 && !rightHasNull(); }
    private boolean passesKeyed(boolean present, boolean rightHasNull) { return isAnti ? !present && !rightHasNull : present; }
    private boolean passesNullKey(boolean rightIsEmpty) { return isAnti && rightIsEmpty; }

    @Override public List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) {
            Object keyVal = ExpressionEvaluator.eval(leftKey, new EvalContext(d.row(), bindings));
            String rowCanonical = JsonText.serializeCanonicalRow(d.row().fields());
            boolean passes;
            if (keyVal == null) {
                if (!isAnti) continue; // NULL IN (...) is never TRUE
                leftNulls.apply(NULL_BUCKET, rowCanonical, d.row(), d.weight());
                passes = passesNullKey(rightIsEmpty());
            } else {
                String keyStr = TableKeyEncoding.encodeScalar(keyVal);
                left.apply(keyStr, rowCanonical, d.row(), d.weight());
                passes = passesKeyed(!right.lookup(keyStr).isEmpty(), rightHasNull());
            }
            if (passes) results.add(new TableRowDelta(d.row(), d.weight()));
        }
        return results;
    }

    @Override public List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) {
            Object keyVal = ExpressionEvaluator.eval(rightKey, new EvalContext(d.row(), bindings));
            String rowCanonical = JsonText.serializeCanonicalRow(d.row().fields());
            boolean hadNull = rightHasNull(), wasEmpty = rightIsEmpty();

            if (keyVal == null) {
                if (!isAnti) continue; // a NULL in S never makes IN TRUE
                rightNulls.apply(NULL_BUCKET, rowCanonical, d.row(), d.weight());
                if (rightHasNull() != hadNull) {
                    for (String k : left.keys()) { // every keyed left row absent from S flips at once
                        boolean present = !right.lookup(k).isEmpty();
                        emitFlip(results, left.lookup(k), passesKeyed(present, hadNull), passesKeyed(present, !hadNull));
                    }
                }
            } else {
                String keyStr = TableKeyEncoding.encodeScalar(keyVal);
                boolean wasPresent = !right.lookup(keyStr).isEmpty();
                right.apply(keyStr, rowCanonical, d.row(), d.weight());
                boolean isPresentNow = !right.lookup(keyStr).isEmpty();
                if (wasPresent != isPresentNow) {
                    presentRightKeys += isPresentNow ? 1 : -1;
                    emitFlip(results, left.lookup(keyStr), passesKeyed(wasPresent, hadNull), passesKeyed(isPresentNow, hadNull));
                }
            }

            if (isAnti && rightIsEmpty() != wasEmpty) {
                emitFlip(results, leftNulls.lookup(NULL_BUCKET), passesNullKey(wasEmpty), passesNullKey(!wasEmpty));
            }
        }
        return results;
    }

    private static void emitFlip(List<TableRowDelta> results, List<ZSetIndex.Entry> rows, boolean passedBefore, boolean passesNow) {
        if (passedBefore == passesNow) return;
        int sign = passesNow ? 1 : -1;
        for (var e : rows) results.add(new TableRowDelta(e.row(), sign * e.weight()));
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
