package io.streamsforge.engine;

import java.util.List;

/**
 * Outcome of compiling a streaming-SQL query.
 *
 * @param ok whether compilation succeeded
 * @param diagnostics all diagnostics (errors and warnings)
 * @param planSummary human-readable one-line plan description, e.g. "trades ⋈ quotes WITHIN 5s → WHERE → ...";
 *                    null when !ok
 * @param sourceNames source stream names the query subscribes to; empty when !ok
 * @param outputSchema column names + kinds of the pipeline's output row; null when !ok
 * @param plan executable plan; null when !ok. Call {@link PipelinePlan#createExecutor()} per running pipeline.
 */
public record CompileResult(boolean ok, List<SqlDiagnostic> diagnostics, String planSummary, List<String> sourceNames,
                            SourceSchema outputSchema, PipelinePlan plan) {}
