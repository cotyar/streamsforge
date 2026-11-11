package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.SetOperationQuery;
import java.util.List;

/**
 * Plan 008 W3: set on a {@link ResolvedSource} when it came from a derived-table-position set operation
 * ({@code FROM ( SELECT ... UNION [ALL] SELECT ... ) alias}). Carries every branch's own independently
 * computed ValidationResult, in branch order.
 * Constructor args in C# declaration order: setOp, branchValidations.
 */
public final class UnionDerivedInfo {
    public final SetOperationQuery setOp;
    public final List<ValidationResult> branchValidations;

    public UnionDerivedInfo(SetOperationQuery setOp, List<ValidationResult> branchValidations) {
        this.setOp = setOp;
        this.branchValidations = branchValidations;
    }
}
