package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.SelectQuery;

/**
 * Plan 004 N2: one resolved {@code [NOT] IN (SELECT ...)} / {@code [NOT] EXISTS (SELECT ...)} WHERE predicate
 * (uncorrelated in this tier). Planner rewrites it into a Semi (negated=false) or Anti (negated=true) join stage.
 * Constructor args in C# declaration order: query, validation, negated, keyColumnName.
 */
public final class SubqueryPredicateInfo {
    public final SelectQuery query;
    public final ValidationResult validation;
    public final boolean negated;
    /** Null for EXISTS. For IN: the subquery's single projected output column's name. */
    public final String keyColumnName;

    public SubqueryPredicateInfo(SelectQuery query, ValidationResult validation, boolean negated, String keyColumnName) {
        this.query = query;
        this.validation = validation;
        this.negated = negated;
        this.keyColumnName = keyColumnName;
    }
}
