package io.streamsforge.engine.sql;

import io.streamsforge.engine.sql.Ast.SelectQuery;
import java.util.List;

/**
 * Plan 004 N3 (correlations empty) / N4 (non-empty): one resolved scalar subquery expression.
 * {@code residualQuery} is the subquery with its correlation conjuncts stripped from WHERE and — for N4 — a
 * synthesized GROUP BY + {@code __key{i}} projections added. Planner compiles it like a derived table and
 * wires it as a Scalar-kind join stage.
 * Constructor args in C# declaration order: residualQuery, validation, correlations, valueColumnName,
 * keyColumnNames.
 */
public final class ScalarSubqueryInfo {
    public final SelectQuery residualQuery;
    public final ValidationResult validation;
    /** Empty for N3 (uncorrelated — Planner joins on a constant key instead). */
    public final List<CorrelationKey> correlations;
    public final String valueColumnName;
    /** N4 only: residualQuery's synthesized {@code __key{i}} output column names, same order as correlations. */
    public final List<String> keyColumnNames;

    public ScalarSubqueryInfo(SelectQuery residualQuery, ValidationResult validation, List<CorrelationKey> correlations,
                              String valueColumnName, List<String> keyColumnNames) {
        this.residualQuery = residualQuery;
        this.validation = validation;
        this.correlations = correlations;
        this.valueColumnName = valueColumnName;
        this.keyColumnNames = keyColumnNames;
    }
}
