package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan 002 L2: {@code [CROSS] JOIN UNNEST(expr) AS alias}. Evaluates {@code expr} against the accumulated LEFT row
 * and emits ONE output row per array element, in array order, with {@code alias} bound to that element as a
 * single Json-kind value. A NULL, non-list or empty list emits ZERO rows (no LEFT UNNEST). Linear and stateless;
 * onRight/evict are dead code paths in practice.
 */
public final class PipelineUnnestOp implements IPipelineJoinStage {
    private final Expr expr;
    private final String alias;
    private final Map<Expr, Binding> bindings;

    public PipelineUnnestOp(Expr expr, String alias, Map<Expr, Binding> bindings) {
        this.expr = expr; this.alias = alias; this.bindings = bindings;
    }

    @Override public List<WorkingRow> onLeft(WorkingRow row) {
        var value = ExpressionEvaluator.eval(expr, new EvalContext(row, bindings));
        var results = new ArrayList<WorkingRow>();
        if (!(value instanceof List<?> list)) return results;

        String key = alias + "_" + alias;
        for (Object element : list) {
            var fields = new LinkedHashMap<String, Object>(row.fields());
            fields.put(key, element);
            var aliases = new ArrayList<String>(row.aliases());
            aliases.add(alias);
            results.add(new WorkingRow(row.ts(), aliases, fields));
        }
        return results;
    }

    @Override public List<WorkingRow> onRight(WorkingRow row) { return new ArrayList<>(); }

    @Override public List<WorkingRow> evict(long watermark) { return new ArrayList<>(); }
}
