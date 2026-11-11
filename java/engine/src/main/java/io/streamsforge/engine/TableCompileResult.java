package io.streamsforge.engine;

import java.util.List;

/**
 * Outcome of compiling table-mode SQL.
 *
 * @param streamInputs stream source names this table subscribes to directly; empty when !ok
 * @param tableInputs other table names this table subscribes to (table-over-table chaining); empty when !ok
 * @param outputSchema column names + kinds of the table's output row; null when !ok
 * @param plan executable plan; null when !ok
 */
public record TableCompileResult(boolean ok, List<SqlDiagnostic> diagnostics, String planSummary, List<String> streamInputs,
                                 List<String> tableInputs, SourceSchema outputSchema, TablePlan plan) {
    public static TableCompileResult failure(List<SqlDiagnostic> diagnostics) {
        return new TableCompileResult(false, diagnostics, null, List.of(), List.of(), null, null);
    }
}
