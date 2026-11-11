package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.SqlValues;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One stage of a left-to-right folded interval join: {@code (accumulatedLeft) JOIN rightAlias WITHIN d [ON ...]}.
 * Buffers both sides; on arrival matches against the opposite buffer immediately; on eviction (watermark passing
 * entry.ts + within) null-pads unmatched LEFT/RIGHT/FULL entries. CROSS matches every currently buffered
 * opposite-side row, ignoring key and residual.
 *
 * <p>STATE: {@link #left} and {@link #right} — buffered entries per side; bounded by the join's WITHIN window.
 */
public final class PipelineJoinOp implements IPipelineJoinStage {
    /** A buffered (row, join key, matched-flag) entry. */
    public static final class BufEntry {
        public WorkingRow row;
        public Object key;
        public boolean matched;
    }

    private final JoinKind kind;
    private final long withinMs;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Expr residual;
    private final Map<Expr, Binding> bindings;
    private final WorkingRow nullRight;
    private final WorkingRow nullLeft;

    /** This op's state, left side. */
    public final List<BufEntry> left = new ArrayList<>();
    /** This op's state, right side. */
    public final List<BufEntry> right = new ArrayList<>();

    public PipelineJoinOp(JoinKind kind, Duration within, Expr leftKey, Expr rightKey, Expr residual,
                          Map<Expr, Binding> bindings, List<WorkingRow.Side> leftAliasesSoFar, WorkingRow.Side rightSide) {
        this.kind = kind;
        this.withinMs = (long) Math.rint(within.toNanos() / 1_000_000.0); // Math.Round: banker's rounding
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.residual = residual;
        this.bindings = bindings;
        this.nullRight = WorkingRow.nullSide(List.of(rightSide));
        this.nullLeft = WorkingRow.nullSide(leftAliasesSoFar);
    }

    @Override public List<WorkingRow> onLeft(WorkingRow row) { return onArrival(row, true); }

    @Override public List<WorkingRow> onRight(WorkingRow row) { return onArrival(row, false); }

    private List<WorkingRow> onArrival(WorkingRow row, boolean isLeft) {
        var results = new ArrayList<WorkingRow>();
        var otherBuf = isLeft ? right : left;
        var ownBuf = isLeft ? left : right;

        if (kind == JoinKind.CROSS) {
            for (var other : otherBuf) {
                results.add(isLeft ? WorkingRow.combine(row, other.row) : WorkingRow.combine(other.row, row));
            }
            var e = new BufEntry();
            e.row = row; e.matched = false; e.key = null;
            ownBuf.add(e);
            return results;
        }

        Expr keyExpr = isLeft ? leftKey : rightKey;
        Object key = keyExpr == null ? null : ExpressionEvaluator.eval(keyExpr, new EvalContext(row, bindings));
        boolean matchedAny = false;

        for (var other : otherBuf) {
            if (!keyEquals(key, other.key)) continue;
            if (Math.abs(row.ts() - other.row.ts()) > withinMs) continue;

            var combined = isLeft ? WorkingRow.combine(row, other.row) : WorkingRow.combine(other.row, row);
            if (residual != null) {
                boolean ok = ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(residual, new EvalContext(combined, bindings)));
                if (!ok) continue;
            }

            matchedAny = true;
            other.matched = true;
            results.add(combined);
        }

        var e = new BufEntry();
        e.row = row; e.matched = matchedAny; e.key = key;
        ownBuf.add(e);
        return results;
    }

    @Override public List<WorkingRow> evict(long watermark) {
        var results = new ArrayList<WorkingRow>();

        left.removeIf(e -> {
            if (watermark <= e.row.ts() + withinMs) return false;
            if (!e.matched && (kind == JoinKind.LEFT || kind == JoinKind.FULL)) results.add(WorkingRow.combine(e.row, nullRight));
            return true;
        });

        right.removeIf(e -> {
            if (watermark <= e.row.ts() + withinMs) return false;
            if (!e.matched && (kind == JoinKind.RIGHT || kind == JoinKind.FULL)) results.add(WorkingRow.combine(nullLeft, e.row));
            return true;
        });

        return results;
    }

    private static boolean keyEquals(Object a, Object b) {
        if (a == null || b == null) return false;
        if (SqlValues.isNumber(a) && SqlValues.isNumber(b)) return SqlValues.toDouble(a) == SqlValues.toDouble(b);
        if (a instanceof String sa && b instanceof String sb) return sa.equals(sb);
        if (a instanceof Boolean ba && b instanceof Boolean bb) return ba.booleanValue() == bb.booleanValue();
        return a == b; // object.Equals on reference types (dictionaries/lists): reference identity
    }
}
