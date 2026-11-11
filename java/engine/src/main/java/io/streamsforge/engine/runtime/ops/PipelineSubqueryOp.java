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

    /** Whether the snapshot holds a NULL-keyed row: PostgreSQL's "S contains a NULL" for NOT IN. */
    private boolean hasNull;
    /** Whether the snapshot holds any row at all: {@code x NOT IN (S)} is TRUE for an empty S, even for a NULL x. */
    private boolean nonEmpty;

    /** Replaces the whole snapshot from one inner-executor emission batch. A B-row whose key evaluates to NULL
     *  is not indexed (it never matches) but sets {@link #hasNull} — see TableSemiAntiOp's NULL rules. */
    @Override public void onRightBatch(List<WorkingRow> rows) {
        if (rows.isEmpty()) return; // rolling: an empty batch leaves the prior snapshot live

        var next = new HashMap<String, List<WorkingRow>>();
        boolean sawNull = false;
        for (var row : rows) {
            var keyVal = ExpressionEvaluator.eval(rightKey, new EvalContext(row, bindings));
            if (keyVal == null) { sawNull = true; continue; }
            String keyStr = TableKeyEncoding.encodeScalar(keyVal);
            next.computeIfAbsent(keyStr, k -> new ArrayList<>()).add(row);
        }
        snapshot = next;
        hasNull = sawNull;
        nonEmpty = true;
    }

    @Override public List<WorkingRow> onLeft(WorkingRow row) {
        var keyVal = ExpressionEvaluator.eval(leftKey, new EvalContext(row, bindings));
        var result = new ArrayList<WorkingRow>();
        if (keyVal == null) {
            // Scalar: null-pad and keep the row. Semi: NULL IN (S) is never TRUE. Anti: NULL NOT IN (S) is TRUE
            // only for an empty S.
            if (kind == JoinKind.SCALAR) result.add(WorkingRow.combine(row, nullRight));
            else if (kind == JoinKind.ANTI && !nonEmpty) result.add(row);
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

        // Semi: passes when present. Anti: passes when absent AND S holds no NULL (else the result is NULL).
        boolean passes = kind == JoinKind.ANTI ? !present && !hasNull : present;
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
