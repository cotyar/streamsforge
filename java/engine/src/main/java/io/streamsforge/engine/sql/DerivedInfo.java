package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.SelectQuery;

/**
 * Plan 004 N1: set on a {@link ResolvedSource} when it came from {@code FROM (SELECT ...) alias} or a
 * desugared WITH-list CTE. Carries the inner query plus its own independently-computed {@link ValidationResult}.
 * Constructor args in C# declaration order: query, validation.
 */
public final class DerivedInfo {
    public final SelectQuery query;
    public final ValidationResult validation;

    public DerivedInfo(SelectQuery query, ValidationResult validation) {
        this.query = query;
        this.validation = validation;
    }
}
