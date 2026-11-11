package io.streamsforge.engine.sql;

import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.sql.Ast.AggregateCallExpr;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.ScalarSubqueryExpr;
import java.util.List;
import java.util.Map;

/**
 * Outcome of {@link Validator#validate}. Constructor args in C# declaration order: diagnostics, sources,
 * bindings, joins, usedAggregates, exprKinds, streamInputs, tableInputs, subqueryPredicates,
 * scalarSubqueries, latestBy. {@code bindings}, {@code exprKinds}, {@code subqueryPredicates} and
 * {@code scalarSubqueries} are identity-keyed ({@link java.util.IdentityHashMap}) maps.
 */
public final class ValidationResult {
    public final List<SqlDiagnostic> diagnostics;
    public final List<ResolvedSource> sources;
    public final Map<Expr, ExpressionEvaluator.Binding> bindings;
    public final List<JoinBinding> joins;
    public final List<AggregateCallExpr> usedAggregates;
    /** Every expression node's inferred FieldKind — populated in both modes. */
    public final Map<Expr, FieldKind> exprKinds;
    /** Table mode only: distinct stream source names referenced (FROM/JOIN). */
    public final List<String> streamInputs;
    /** Table mode only: distinct other-table names referenced (FROM/JOIN). */
    public final List<String> tableInputs;
    /** Plan 004 N2: every resolved IN/EXISTS WHERE predicate, keyed by its own AST node (identity). */
    public final Map<Expr, SubqueryPredicateInfo> subqueryPredicates;
    /** Plan 004 N3/N4: every resolved scalar subquery expression, keyed by its own AST node (identity). */
    public final Map<ScalarSubqueryExpr, ScalarSubqueryInfo> scalarSubqueries;
    /** Plan 002 L3: the query's LATEST BY key expressions (table mode only); nullable. */
    public final List<Expr> latestBy;

    public ValidationResult(List<SqlDiagnostic> diagnostics, List<ResolvedSource> sources,
                            Map<Expr, ExpressionEvaluator.Binding> bindings, List<JoinBinding> joins,
                            List<AggregateCallExpr> usedAggregates, Map<Expr, FieldKind> exprKinds,
                            List<String> streamInputs, List<String> tableInputs,
                            Map<Expr, SubqueryPredicateInfo> subqueryPredicates,
                            Map<ScalarSubqueryExpr, ScalarSubqueryInfo> scalarSubqueries, List<Expr> latestBy) {
        this.diagnostics = diagnostics;
        this.sources = sources;
        this.bindings = bindings;
        this.joins = joins;
        this.usedAggregates = usedAggregates;
        this.exprKinds = exprKinds;
        this.streamInputs = streamInputs;
        this.tableInputs = tableInputs;
        this.subqueryPredicates = subqueryPredicates;
        this.scalarSubqueries = scalarSubqueries;
        this.latestBy = latestBy;
    }

    public boolean hasAggregates() { return !usedAggregates.isEmpty(); }

    public boolean hasErrors() {
        for (SqlDiagnostic d : diagnostics) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }
}
