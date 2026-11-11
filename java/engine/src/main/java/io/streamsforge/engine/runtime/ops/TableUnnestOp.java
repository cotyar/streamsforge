package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
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
 * Plan 002 L2 table-mode {@code JOIN UNNEST(expr) AS alias}: linear pass-through — each element-row inherits the
 * INPUT delta's weight unchanged (a retraction of a source row retracts exactly the element rows its assertion
 * produced, so the op needs no state). A NULL / non-array value expands to zero rows. STATE: none.
 */
public final class TableUnnestOp implements ITableJoinStage {
    private final Expr expr;
    private final String alias;
    private final Map<Expr, Binding> bindings;

    public TableUnnestOp(Expr expr, String alias, Map<Expr, Binding> bindings) {
        this.expr = expr; this.alias = alias; this.bindings = bindings;
    }

    @Override public List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input) {
        var results = new ArrayList<TableRowDelta>();
        String key = alias + "_" + alias;

        for (var d : input) {
            Object value = ExpressionEvaluator.eval(expr, new EvalContext(d.row(), bindings));
            if (!(value instanceof List<?> list)) continue;

            for (Object element : list) {
                var fields = new LinkedHashMap<String, Object>(d.row().fields());
                fields.put(key, element);
                var aliases = new ArrayList<String>(d.row().aliases());
                aliases.add(alias);
                results.add(new TableRowDelta(new WorkingRow(d.row().ts(), aliases, fields), d.weight()));
            }
        }
        return results;
    }

    @Override public List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input) { return new ArrayList<>(); }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
