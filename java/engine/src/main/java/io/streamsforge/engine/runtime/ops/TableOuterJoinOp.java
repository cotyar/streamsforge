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
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plan 008: table-mode LEFT/RIGHT/FULL OUTER join over Z-sets, with composite equi keys ({@code leftKeys}/
 * {@code rightKeys}) and a pure residual. An arriving row multiplies against the other side's bucket (matches that
 * pass the residual); the OTHER side's unmatched rows ("pads": row ⋈ all-NULL opposite side) flip to/from padded as
 * this row changes whether they have a match, and the arriving row pads itself when it has no match and its side
 * pads. A NULL key component is final and immediate: pad iff this side pads, never indexed. With a residual, the
 * pad-flip is computed per other-side row (OwnBucketHasMatch) instead of at the key level. STATE:
 * {@link #left}/{@link #right} — unbounded Z-set indexes.
 */
public final class TableOuterJoinOp implements ITableJoinStage {
    private final List<Expr> leftKeys;
    private final List<Expr> rightKeys;
    private final Expr residual;
    private final Map<Expr, Binding> bindings;
    private final boolean padLeftUnmatched;
    private final boolean padRightUnmatched;
    private final WorkingRow nullRight;
    private final WorkingRow nullLeft;

    public final ZSetIndex left = new ZSetIndex();
    public final ZSetIndex right = new ZSetIndex();

    public TableOuterJoinOp(JoinKind kind, List<Expr> leftKeys, List<Expr> rightKeys, Expr residual, Map<Expr, Binding> bindings,
                            List<WorkingRow.Side> leftAliasesSoFar, WorkingRow.Side rightSide) {
        if (kind != JoinKind.LEFT && kind != JoinKind.RIGHT && kind != JoinKind.FULL) {
            throw new IllegalArgumentException("TableOuterJoinOp only supports Left/Right/Full — Inner uses TableJoinOp, Semi/Anti use TableSemiAntiOp. (Parameter 'kind')");
        }
        if (leftKeys.isEmpty() || leftKeys.size() != rightKeys.size()) {
            throw new IllegalArgumentException("Left/right key lists must be the same non-zero length (got " + leftKeys.size() + " vs " + rightKeys.size() + "). (Parameter 'rightKeys')");
        }
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.residual = residual;
        this.bindings = bindings;
        this.padLeftUnmatched = kind == JoinKind.LEFT || kind == JoinKind.FULL;
        this.padRightUnmatched = kind == JoinKind.RIGHT || kind == JoinKind.FULL;
        this.nullRight = WorkingRow.nullSide(List.of(rightSide));
        this.nullLeft = WorkingRow.nullSide(leftAliasesSoFar);
    }

    @Override public List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) onArrival(d.row(), d.weight(), true, results);
        return results;
    }

    @Override public List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        for (var d : input) onArrival(d.row(), d.weight(), false, results);
        return results;
    }

    private record Before(WorkingRow row, long weight, boolean wasMatched) {}

    private void onArrival(WorkingRow row, long weight, boolean isLeftSide, List<TableRowDelta> results) {
        var ownKeys = isLeftSide ? leftKeys : rightKeys;
        var ownIndex = isLeftSide ? left : right;
        var otherIndex = isLeftSide ? right : left;
        boolean ownPads = isLeftSide ? padLeftUnmatched : padRightUnmatched;
        boolean otherPads = isLeftSide ? padRightUnmatched : padLeftUnmatched;

        Object[] keyValues = evalKeyValues(ownKeys, row);
        if (keyValues == null) {
            // NULL key is final and immediate — pad iff this side pads its own unmatched rows; never indexed.
            if (ownPads) results.add(new TableRowDelta(padOwn(row, isLeftSide), weight));
            return;
        }
        String keyStr = TableKeyEncoding.encodeGroupKey(keyValues);

        // T1 products against the OTHER side's current bucket, plus this row's own match count.
        int ownMatchCount = 0;
        for (var other : otherIndex.lookup(keyStr)) {
            var combined = isLeftSide ? WorkingRow.combine(row, other.row()) : WorkingRow.combine(other.row(), row);
            if (!passesResidual(combined)) continue;
            ownMatchCount++;
            results.add(new TableRowDelta(combined, weight * other.weight()));
        }

        // OTHER side's pad-flip: snapshot "before" state BEFORE this row is applied to the own index.
        boolean hasResidual = residual != null;
        boolean wasPresentCheap = false;
        List<Before> perRowBefore = null;
        if (otherPads) {
            if (!hasResidual) {
                wasPresentCheap = !ownIndex.lookup(keyStr).isEmpty();
            } else {
                perRowBefore = new ArrayList<>();
                for (var o : otherIndex.lookup(keyStr)) {
                    perRowBefore.add(new Before(o.row(), o.weight(), ownBucketHasMatch(ownIndex, keyStr, o.row(), isLeftSide)));
                }
            }
        }

        // Index self (unconditional).
        String rowCanonical = JsonText.serializeCanonicalRow(row.fields());
        ownIndex.apply(keyStr, rowCanonical, row, weight);

        // OTHER side's pad-flip: "after" state + re-emission.
        if (otherPads) {
            if (!hasResidual) {
                boolean isPresentNow = !ownIndex.lookup(keyStr).isEmpty();
                if (wasPresentCheap != isPresentNow) {
                    int sign = isPresentNow ? -1 : 1;
                    for (var other : otherIndex.lookup(keyStr)) {
                        results.add(new TableRowDelta(padOther(other.row(), isLeftSide), sign * other.weight()));
                    }
                }
            } else {
                for (var b : perRowBefore) {
                    boolean isMatched = ownBucketHasMatch(ownIndex, keyStr, b.row(), isLeftSide);
                    if (b.wasMatched() == isMatched) continue;
                    int sign = isMatched ? -1 : 1;
                    results.add(new TableRowDelta(padOther(b.row(), isLeftSide), sign * b.weight()));
                }
            }
        }

        // This row's own pad: only when unmatched and this side pads.
        if (ownPads && ownMatchCount == 0) {
            results.add(new TableRowDelta(padOwn(row, isLeftSide), weight));
        }
    }

    private Object[] evalKeyValues(List<Expr> keys, WorkingRow row) {
        var values = new Object[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            Object v = ExpressionEvaluator.eval(keys.get(i), new EvalContext(row, bindings));
            if (v == null) return null;
            values[i] = v;
        }
        return values;
    }

    private boolean ownBucketHasMatch(ZSetIndex ownIndex, String keyStr, WorkingRow otherRow, boolean isLeftSide) {
        for (var own : ownIndex.lookup(keyStr)) {
            var combined = isLeftSide ? WorkingRow.combine(own.row(), otherRow) : WorkingRow.combine(otherRow, own.row());
            if (passesResidual(combined)) return true;
        }
        return false;
    }

    private boolean passesResidual(WorkingRow combined) {
        return residual == null || ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(residual, new EvalContext(combined, bindings)));
    }

    private WorkingRow padOwn(WorkingRow row, boolean isLeftSide) {
        return isLeftSide ? WorkingRow.combine(row, nullRight) : WorkingRow.combine(nullLeft, row);
    }

    private WorkingRow padOther(WorkingRow otherRow, boolean isLeftSide) {
        return isLeftSide ? WorkingRow.combine(nullLeft, otherRow) : WorkingRow.combine(otherRow, nullRight);
    }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
