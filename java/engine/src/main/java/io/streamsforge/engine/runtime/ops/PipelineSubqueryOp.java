package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan 004 N2/N3/N4's pipeline-mode subquery join stage — semi/anti membership ({@code [NOT] IN}/{@code [NOT]
 * EXISTS}) and scalar value lookup (uncorrelated N3 / equality-correlated N4) share ONE mechanism: "the current
 * state of a windowed subquery's most recent emission, indexed by key, tested against at arrival".
 *
 * <p>THE ROLLING-SNAPSHOT RULE: the subquery side MUST be a windowed derived query. Its nested executor is driven
 * by the outer executor; what differs is what happens to what it emits — {@link #onRightBatch}: the WHOLE
 * emission batch from one onEvent/advanceWatermark call WHOLESALE REPLACES this op's snapshot (never merges).
 * An EMPTY batch leaves the snapshot untouched. A side-A row is tested against WHATEVER snapshot is live AT ITS
 * OWN ARRIVAL — a deliberate, documented approximation.
 *
 * <p>STATE: the snapshot — join-key string → the inner query's currently-live emitted row(s) under that key.
 */
public final class PipelineSubqueryOp implements IPipelineSnapshotJoinStage {
    private final JoinKind kind; // SEMI, ANTI, or SCALAR
    private final Expr leftKey;
    private final Expr rightKey;
    private final Expr residual;
    private final Map<Expr, Binding> bindings;
    private final WorkingRow nullRight;

    private Map<String, List<WorkingRow>> snapshot = new HashMap<>();

    public PipelineSubqueryOp(JoinKind kind, Expr leftKey, Expr rightKey, Expr residual, Map<Expr, Binding> bindings,
                              WorkingRow.Side rightSide) {
        this.kind = kind;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.residual = residual;
        this.bindings = bindings;
        this.nullRight = WorkingRow.nullSide(List.of(rightSide));
    }

    /** Replaces the whole snapshot from one inner-executor emission batch. A B-row whose key evaluates to NULL
     *  is skipped entirely. */
    @Override public void onRightBatch(List<WorkingRow> rows) {
        if (rows.isEmpty()) return; // rolling: an empty batch leaves the prior snapshot live

        var next = new HashMap<String, List<WorkingRow>>();
        for (var row : rows) {
            var keyVal = ExpressionEvaluator.eval(rightKey, new EvalContext(row, bindings));
            if (keyVal == null) continue;
            String keyStr = TableKeyEncoding.encodeScalar(keyVal);
            next.computeIfAbsent(keyStr, k -> new ArrayList<>()).add(row);
        }
        snapshot = next;
    }

    @Override public List<WorkingRow> onLeft(WorkingRow row) {
        var keyVal = ExpressionEvaluator.eval(leftKey, new EvalContext(row, bindings));
        var result = new ArrayList<WorkingRow>();
        if (keyVal == null) {
            // Semi/Anti: outer key NULL — dropped. Scalar: null-pad and keep the row.
            if (kind == JoinKind.SCALAR) result.add(WorkingRow.combine(row, nullRight));
            return result;
        }

        String keyStr = TableKeyEncoding.encodeScalar(keyVal);
        var candidates = snapshot.get(keyStr);
        boolean present = candidates != null && !candidates.isEmpty();

        if (kind == JoinKind.SCALAR) {
            if (present) {
                for (var candidate : candidates) {
                    var combined = WorkingRow.combine(row, candidate);
                    if (residual == null || ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(residual, new EvalContext(combined, bindings)))) {
                        result.add(combined);
                        return result;
                    }
                }
            }
            result.add(WorkingRow.combine(row, nullRight));
            return result;
        }

        boolean passes = present != (kind == JoinKind.ANTI); // Semi: passes when present; Anti: passes when absent
        if (passes) result.add(row);
        return result;
    }

    /** Never invoked under normal dispatch for a snapshot stage; implemented as a length-1 batch for safety. */
    @Override public List<WorkingRow> onRight(WorkingRow row) {
        onRightBatch(List.of(row));
        return new ArrayList<>();
    }

    /** No-op — no left-side buffering to evict. */
    @Override public List<WorkingRow> evict(long watermark) { return new ArrayList<>(); }
}
