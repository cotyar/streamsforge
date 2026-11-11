package io.streamsforge.engine.sql;

import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlDiagnostic;
import java.util.List;

/**
 * Plan 008 W3: outcome of validating a top-level (or WITH-wrapped) set operation. {@code outputSchema} is null
 * only when branch validation/compatibility failed.
 * Constructor args in C# declaration order: diagnostics, branchValidations, outputSchema, streamInputs, tableInputs.
 */
public final class SetOperationValidationResult {
    public final List<SqlDiagnostic> diagnostics;
    public final List<ValidationResult> branchValidations;
    public final SourceSchema outputSchema;
    public final List<String> streamInputs;
    public final List<String> tableInputs;

    public SetOperationValidationResult(List<SqlDiagnostic> diagnostics, List<ValidationResult> branchValidations,
                                        SourceSchema outputSchema, List<String> streamInputs, List<String> tableInputs) {
        this.diagnostics = diagnostics;
        this.branchValidations = branchValidations;
        this.outputSchema = outputSchema;
        this.streamInputs = streamInputs;
        this.tableInputs = tableInputs;
    }

    public boolean hasErrors() {
        for (SqlDiagnostic d : diagnostics) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }
}
