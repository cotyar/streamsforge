package io.streamsforge.engine.sql;

import static io.streamsforge.engine.sql.Ast.*;

import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.runtime.AggregateParameters;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.StatAggregatorNames;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Semantic analysis: source/column/alias resolution, aggregate/window/emit structural rules, and equi-join
 * key extraction. Never throws — every problem becomes a {@link SqlDiagnostic}. Two modes: STREAM (windowed
 * pipelines — the original dialect) and TABLE (persistent materialized tables — unwindowed running aggregates
 * over a combined streams+tables namespace).
 */
public final class Validator {
    private final ValidationMode _mode;
    private final Map<String, SourceSchema> _schemas;
    private Set<String> _tableNames = new HashSet<>();
    private Set<String> _ambiguousNames = new HashSet<>();
    private final List<String> _streamInputs = new ArrayList<>();
    private final List<String> _tableInputs = new ArrayList<>();

    // Table mode only: the original (pre-merge) stream/table schema dicts, retained so a derived table's
    // recursive validateTable call gets the exact same streams-vs-tables split as the outer query.
    private final Map<String, SourceSchema> _streamSchemasForRecursion;
    private final Map<String, SourceSchema> _tableSchemasForRecursion;

    private final List<SqlDiagnostic> _diags = new ArrayList<>();
    private final Map<Expr, Binding> _bindings = new IdentityHashMap<>();
    private final List<AggregateCallExpr> _usedAggregates = new ArrayList<>();

    // Plan 008 W3: GROUP BY ALL reuses the SAME Expr instances as the select-list items, so every node is only
    // ever walked once, tracked here by reference identity (otherwise a failing node would diagnose twice).
    private final Set<Expr> _resolvedNodes = Collections.newSetFromMap(new IdentityHashMap<>());

    // Plan 004 N2: WHERE-position gating for IN/EXISTS subquery predicates.
    private boolean _resolvingWhere;
    private Set<Expr> _whereTopLevelConjuncts;

    private final Map<Expr, SubqueryPredicateInfo> _subqueryPredicates = new IdentityHashMap<>();
    private final Map<ScalarSubqueryExpr, ScalarSubqueryInfo> _scalarSubqueries = new IdentityHashMap<>();

    // Tracks the resolved FieldKind of expression nodes. Nodes absent from this map have no meaningful kind.
    private final Map<Expr, FieldKind> _exprKind = new IdentityHashMap<>();

    // Plan 002 L2: every UNNEST alias resolved so far, in this query's own FROM/JOIN chain.
    private final Set<String> _unnestAliases = new HashSet<>();

    /** (alias, schema) pair — the C# {@code (string Alias, SourceSchema Schema)} scope tuple. */
    private record Scope(String alias, SourceSchema schema) {}

    /** The C# ResolveFromItem tuple. */
    private record ResolvedFrom(boolean ok, String sourceName, SourceSchema schema, boolean isTable,
                                DerivedInfo derived, UnionDerivedInfo unionDerived) {}

    /** The C# ExtractEquiKey tuple. */
    private record EquiKey(List<Expr> leftKeys, List<Expr> rightKeys, Expr residual) {}

    /** Result of {@link #checkSetOperationBranchCompatibility}: diagnostics plus the unified schema (null on any
     *  incompatibility). */
    public record BranchCompatibility(List<SqlDiagnostic> diagnostics, SourceSchema unified) {}

    private record CorrelationTuple(Expr innerExpr, String outerAlias, String outerField) {}

    private Validator(Map<String, SourceSchema> schemas) {
        _mode = ValidationMode.STREAM;
        _schemas = schemas;
        _streamSchemasForRecursion = null;
        _tableSchemasForRecursion = null;
    }

    private Validator(Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        _mode = ValidationMode.TABLE;
        _tableNames = new HashSet<>(tableSchemas.keySet());
        _ambiguousNames = new HashSet<>();
        for (String k : streamSchemas.keySet()) if (tableSchemas.containsKey(k)) _ambiguousNames.add(k);

        var merged = new LinkedHashMap<String, SourceSchema>(streamSchemas);
        for (var kv : tableSchemas.entrySet()) merged.put(kv.getKey(), kv.getValue());
        _schemas = merged;

        _streamSchemasForRecursion = streamSchemas;
        _tableSchemasForRecursion = tableSchemas;
    }

    public static ValidationResult validate(SelectQuery query, Map<String, SourceSchema> schemas) {
        var v = new Validator(schemas);
        return v.run(query);
    }

    /** Table-mode validation: sql is a SELECT over streams AND/OR other tables, without windows. A FROM/JOIN
     *  identifier must exist in exactly one of streamSchemas/tableSchemas — present in both is an "ambiguous
     *  name" diagnostic. */
    public static ValidationResult validateTable(SelectQuery query, Map<String, SourceSchema> streamSchemas,
                                                 Map<String, SourceSchema> tableSchemas) {
        var v = new Validator(streamSchemas, tableSchemas);
        return v.run(query);
    }

    /** Plan 008 W3: message for "UNION (distinct) used in pipeline mode" — shared by the top-level set-operation
     *  path and the derived-table-position path so both report byte-identical wording. */
    private static final String PIPELINE_UNION_DISTINCT_MESSAGE =
        "UNION (distinct) is not supported in pipeline mode — pipeline mode has no Z-set weights to dedup " +
        "with, and an unbounded distinct over an unbounded stream would be unbounded state; use UNION ALL instead";

    /** Plan 008 W3: top-level (or WITH-wrapped) pipeline-mode set operation. Every branch validates
     *  independently and uncorrelated (own Validator instance per branch, same schemas). */
    public static SetOperationValidationResult validateSetOperation(SetOperationQuery setOp, Map<String, SourceSchema> schemas) {
        var branchValidations = new ArrayList<ValidationResult>();
        for (SelectQuery b : setOp.branches) branchValidations.add(validate(b, schemas));
        return finishSetOperationValidation(setOp, branchValidations, false);
    }

    /** Plan 008 W3: top-level (or WITH-wrapped) table-mode set operation. UNION (distinct) is allowed here. */
    public static SetOperationValidationResult validateSetOperationTable(SetOperationQuery setOp,
            Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        var branchValidations = new ArrayList<ValidationResult>();
        for (SelectQuery b : setOp.branches) branchValidations.add(validateTable(b, streamSchemas, tableSchemas));
        return finishSetOperationValidation(setOp, branchValidations, true);
    }

    private static boolean hasError(List<SqlDiagnostic> diags) {
        for (SqlDiagnostic d : diags) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }

    private static SetOperationValidationResult finishSetOperationValidation(SetOperationQuery setOp,
            List<ValidationResult> branchValidations, boolean isTable) {
        var diags = new ArrayList<SqlDiagnostic>();
        for (ValidationResult bv : branchValidations) diags.addAll(bv.diagnostics);

        if (!isTable && !setOp.all) {
            diags.add(new SqlDiagnostic(PIPELINE_UNION_DISTINCT_MESSAGE, setOp.line, setOp.column));
        }

        SourceSchema unified = null;
        List<String> streamInputs = new ArrayList<>();
        List<String> tableInputs = new ArrayList<>();

        if (!hasError(diags)) {
            var branchSchemas = new ArrayList<SourceSchema>();
            for (int i = 0; i < setOp.branches.size() && i < branchValidations.size(); i++) {
                branchSchemas.add(buildDerivedOutputSchema(setOp.branches.get(i), branchValidations.get(i)));
            }
            var compat = checkSetOperationBranchCompatibility(branchSchemas, setOp.line, setOp.column);
            diags.addAll(compat.diagnostics());
            unified = compat.unified();

            if (isTable) {
                var si = new LinkedHashSet<String>();
                var ti = new LinkedHashSet<String>();
                for (ValidationResult bv : branchValidations) {
                    si.addAll(bv.streamInputs);
                    ti.addAll(bv.tableInputs);
                }
                streamInputs = new ArrayList<>(si);
                tableInputs = new ArrayList<>(ti);
            }
        }

        return new SetOperationValidationResult(diags, branchValidations, unified, streamInputs, tableInputs);
    }

    private ValidationResult run(SelectQuery q) {
        var sources = new ArrayList<ResolvedSource>();
        Set<String> aliasSeen = new HashSet<>();
        var joins = new ArrayList<JoinBinding>();

        // FROM
        registerSource(q.from.source, sources, aliasSeen);

        // JOINs, left-to-right, each ON scoped to (aliases so far) ∪ (this join's alias).
        for (JoinClause j : q.from.joins) {
            // Plan 002 L2: UNNEST has no ON/WITHIN/equi-key concept at all — resolved by its own method.
            if (j.source instanceof UnnestSource unnestSrc) {
                resolveUnnestJoin(unnestSrc, sources, aliasSeen, joins);
                continue;
            }

            var leftAliasesBefore = new LinkedHashSet<String>();
            for (ResolvedSource s : sources) leftAliasesBefore.add(s.alias);
            var resolved = resolveFromItem(j.source, aliasSeen);
            boolean sourceOk = resolved.ok();
            var jSchema = resolved.schema();
            boolean jIsTable = resolved.isTable();

            if (_mode == ValidationMode.STREAM) {
                if (j.within == null) {
                    _diags.add(new SqlDiagnostic(joinLabel(j.kind) + " JOIN requires a WITHIN clause", j.line, j.column));
                }
            } else {
                if (j.within != null) {
                    _diags.add(new SqlDiagnostic(
                        joinLabel(j.kind) + " JOIN may not have a WITHIN clause in table mode — state is unbounded/consolidated, not time-bounded",
                        j.line, j.column));
                }
                // Plan 008: LEFT/RIGHT/FULL (and CROSS) compile in table mode too. WITHIN stays banned.
            }

            List<Expr> leftKeys = null, rightKeys = null;
            Expr residual = null;

            if (j.kind == JoinKind.CROSS) {
                // Grammar already forbids ON for CROSS; nothing further to resolve.
            } else if (j.on == null) {
                _diags.add(new SqlDiagnostic(joinLabel(j.kind) + " JOIN requires an ON clause", j.line, j.column));
            } else {
                var scope = new ArrayList<Scope>();
                for (ResolvedSource s : sources) scope.add(new Scope(s.alias, s.schema));
                if (sourceOk) scope.add(new Scope(j.source.alias, jSchema));
                resolveExpr(j.on, scope, 0);

                var extracted = extractEquiKey(j.on, leftAliasesBefore, j.source.alias);
                if (extracted == null) {
                    _diags.add(new SqlDiagnostic(
                        joinLabel(j.kind) + " JOIN's ON clause must contain an equi-comparison between " + String.join(", ", leftAliasesBefore) + " and " + j.source.alias,
                        j.on.line, j.on.column));
                } else {
                    leftKeys = extracted.leftKeys();
                    rightKeys = extracted.rightKeys();
                    residual = extracted.residual();
                }
            }

            if (sourceOk) {
                sources.add(new ResolvedSource(j.source.alias, resolved.sourceName(), jSchema, jIsTable, resolved.derived(), resolved.unionDerived()));
            }

            joins.add(new JoinBinding(j.kind, j.source.alias, resolved.sourceName(), j.within, leftKeys, rightKeys, residual, jIsTable));
        }

        var fullScope = new ArrayList<Scope>();
        for (ResolvedSource s : sources) fullScope.add(new Scope(s.alias, s.schema));

        if (q.where != null) {
            // Plan 004 N2: only a top-level (AND-connected) conjunct of WHERE may be an IN/EXISTS predicate.
            _whereTopLevelConjuncts = Collections.newSetFromMap(new IdentityHashMap<>());
            _whereTopLevelConjuncts.addAll(flattenAnd(q.where));
            _resolvingWhere = true;
            resolveExpr(q.where, fullScope, 0);
            _resolvingWhere = false;
        }

        if (q.groupBy != null) {
            for (Expr g : q.groupBy) resolveExpr(g, fullScope, 0);
        }

        // Plan 002 L3: LATEST BY key expressions resolve against the same fullScope GROUP BY does.
        if (q.latestBy != null) {
            for (Expr k : q.latestBy) resolveExpr(k, fullScope, 0);
        }

        if (!q.select.isStar) {
            for (SelectItem item : q.select.items) resolveExpr(item.expression, fullScope, 0);
        }

        boolean hasAggregates = !_usedAggregates.isEmpty();

        if (_mode == ValidationMode.STREAM) {
            if (hasAggregates && q.window == null) {
                var first = _usedAggregates.get(0);
                _diags.add(new SqlDiagnostic("Aggregate functions require a WINDOW clause", first.line, first.column));
            }
            if (q.groupBy != null && q.window == null) {
                _diags.add(new SqlDiagnostic("GROUP BY requires a WINDOW clause", orOne(q.groupByLine), orOne(q.groupByColumn)));
            }
            if (q.emit != null && q.window == null) {
                _diags.add(new SqlDiagnostic("EMIT requires a WINDOW clause", orOne(q.emitLine), orOne(q.emitColumn)));
            }
            if (q.latestBy != null) {
                _diags.add(new SqlDiagnostic("LATEST BY is table-mode only", orOne(q.latestByLine), orOne(q.latestByColumn)));
            }
        } else {
            // Table mode: aggregates + GROUP BY are allowed WITHOUT a window (running aggregates).
            // WINDOW and EMIT are structurally forbidden outright.
            if (q.window != null) {
                _diags.add(new SqlDiagnostic(
                    "WINDOW clause not allowed in table mode — tables maintain state continuously; windows belong to stream pipelines",
                    orOne(q.windowLine), orOne(q.windowColumn)));
            }
            if (q.emit != null) {
                _diags.add(new SqlDiagnostic("EMIT not allowed in table mode", orOne(q.emitLine), orOne(q.emitColumn)));
            }
            // Plan 002 L3: LATEST BY is mutually exclusive with GROUP BY/aggregates.
            if (q.latestBy != null) {
                if (q.groupBy != null) {
                    _diags.add(new SqlDiagnostic("LATEST BY may not be combined with GROUP BY", orOne(q.latestByLine), orOne(q.latestByColumn)));
                }
                if (hasAggregates) {
                    _diags.add(new SqlDiagnostic("LATEST BY may not be combined with aggregate functions", orOne(q.latestByLine), orOne(q.latestByColumn)));
                }
            }
        }

        if ((q.window != null || q.groupBy != null || hasAggregates) && !q.select.isStar) {
            // Every non-aggregate select item must be one of the GROUP BY columns.
            List<Expr> groupByList = q.groupBy != null ? q.groupBy : List.of();
            for (SelectItem item : q.select.items) {
                if (item.expression instanceof QualifiedStarExpr qStar) {
                    // alias.* is disallowed outright alongside GROUP BY/aggregates.
                    _diags.add(new SqlDiagnostic("star is not allowed with GROUP BY/aggregates", qStar.line, qStar.column));
                    continue;
                }
                // Plan 004 N3/N4: a scalar subquery outside any aggregate's own argument can't sit in a
                // grouped/windowed/aggregated query's SELECT list in this engine.
                var badScalarSubquery = findScalarSubqueryOutsideAggregate(item.expression, false);
                if (badScalarSubquery != null) {
                    _diags.add(new SqlDiagnostic(
                        "Scalar subquery in the SELECT list of a grouped/windowed/aggregated query is not supported in this tier — move it to WHERE, or restructure without GROUP BY/WINDOW/aggregates (plan 004 N3/N4)",
                        badScalarSubquery.line, badScalarSubquery.column));
                }

                if (containsAggregate(item.expression)) continue;
                boolean matchesGroupBy = false;
                for (Expr g : groupByList) {
                    if (structurallyEqual(item.expression, g)) { matchesGroupBy = true; break; }
                }
                if (!matchesGroupBy) {
                    _diags.add(new SqlDiagnostic(
                        "Non-aggregate select item must appear in GROUP BY",
                        item.expression.line, item.expression.column));
                }
            }
        }

        return new ValidationResult(
            _diags,
            sources,
            _bindings,
            joins,
            _usedAggregates,
            _exprKind,
            new ArrayList<>(new LinkedHashSet<>(_streamInputs)),
            new ArrayList<>(new LinkedHashSet<>(_tableInputs)),
            _subqueryPredicates,
            _scalarSubqueries,
            q.latestBy);
    }

    private static int orOne(Integer v) { return v != null ? v : 1; }

    private void registerSource(FromItem item, List<ResolvedSource> sources, Set<String> aliasSeen) {
        var r = resolveFromItem(item, aliasSeen);
        if (r.ok()) {
            sources.add(new ResolvedSource(item.alias, r.sourceName(), r.schema(), r.isTable(), r.derived(), r.unionDerived()));
        }
    }

    /** Resolves one FROM/JOIN item — a plain named source, a derived table/CTE (plan 004 N1: recursively
     *  validates the inner query against the SAME stream/table namespace — uncorrelated — then synthesizes
     *  this alias's schema from the inner query's own output), or (plan 008 W3) a derived-table-position set
     *  operation. Does NOT add to sources itself, but DOES perform the duplicate-alias check. */
    private ResolvedFrom resolveFromItem(FromItem item, Set<String> aliasSeen) {
        if (!aliasSeen.add(item.alias)) {
            _diags.add(new SqlDiagnostic("Duplicate alias '" + item.alias + "'", item.line, item.column));
        }

        if (item instanceof DerivedSource ds) {
            ValidationResult inner = validateNestedUncorrelated(ds.query);
            _diags.addAll(inner.diagnostics);
            foldNestedInputs(inner);
            var schema = buildDerivedOutputSchema(ds.query, inner);
            return new ResolvedFrom(true, "(derived)", schema, false, new DerivedInfo(ds.query, inner), null);
        }

        if (item instanceof DerivedSetOperationSource dus) {
            var branchValidations = new ArrayList<ValidationResult>();
            for (SelectQuery b : dus.setOp.branches) branchValidations.add(validateNestedUncorrelated(b));
            for (ValidationResult bv : branchValidations) {
                _diags.addAll(bv.diagnostics);
                foldNestedInputs(bv);
            }

            if (_mode == ValidationMode.STREAM && !dus.setOp.all) {
                _diags.add(new SqlDiagnostic(PIPELINE_UNION_DISTINCT_MESSAGE, dus.setOp.line, dus.setOp.column));
            }

            var branchSchemas = new ArrayList<SourceSchema>();
            for (int i = 0; i < dus.setOp.branches.size() && i < branchValidations.size(); i++) {
                branchSchemas.add(buildDerivedOutputSchema(dus.setOp.branches.get(i), branchValidations.get(i)));
            }
            var compat = checkSetOperationBranchCompatibility(branchSchemas, dus.setOp.line, dus.setOp.column);
            _diags.addAll(compat.diagnostics());

            var schema = compat.unified() != null ? compat.unified() : new SourceSchema("(derived)", new LinkedHashMap<>());
            return new ResolvedFrom(true, "(derived)", schema, false, null, new UnionDerivedInfo(dus.setOp, branchValidations));
        }

        var sref = (NamedSource) item;

        if (_mode == ValidationMode.TABLE && _ambiguousNames.contains(sref.name)) {
            _diags.add(new SqlDiagnostic("Ambiguous name '" + sref.name + "' — present in both streams and tables", sref.line, sref.column));
            return new ResolvedFrom(false, sref.name, null, false, null, null);
        }

        var namedSchema = _schemas.get(sref.name);
        if (namedSchema == null) {
            var keys = new ArrayList<>(_schemas.keySet());
            Collections.sort(keys);
            var available = String.join(", ", keys);
            _diags.add(new SqlDiagnostic("Unknown source '" + sref.name + "' — available: " + available, sref.line, sref.column));
            return new ResolvedFrom(false, sref.name, null, false, null, null);
        }

        boolean isTable = _mode == ValidationMode.TABLE && _tableNames.contains(sref.name);
        if (_mode == ValidationMode.TABLE) {
            if (isTable) _tableInputs.add(sref.name); else _streamInputs.add(sref.name);
        }

        return new ResolvedFrom(true, sref.name, namedSchema, isTable, null, null);
    }

    // ------------------------------------------------------------------
    // Plan 002 L2 — UNNEST(expr) AS alias
    // ------------------------------------------------------------------

    /** Resolves one UNNEST join item. No ON/WITHIN/equi-key to extract: the alias's "schema" is a synthetic
     *  one-field element schema (field name == alias name, kind JSON), and the UNNEST expr is resolved against
     *  only the REAL (non-UNNEST) sources registered so far. */
    private void resolveUnnestJoin(UnnestSource unnestSrc, List<ResolvedSource> sources, Set<String> aliasSeen, List<JoinBinding> joins) {
        if (!aliasSeen.add(unnestSrc.alias)) {
            _diags.add(new SqlDiagnostic("Duplicate alias '" + unnestSrc.alias + "'", unnestSrc.line, unnestSrc.column));
        }

        var referencedUnnestAlias = findUnnestAliasReference(unnestSrc.expr);
        if (referencedUnnestAlias != null) {
            _diags.add(new SqlDiagnostic(
                "UNNEST argument may not reference another UNNEST alias '" + referencedUnnestAlias + "' — only real FROM sources are allowed (UNNEST-of-UNNEST is not supported; plan 002 L2)",
                unnestSrc.expr.line, unnestSrc.expr.column));
        } else {
            var realScope = new ArrayList<Scope>();
            for (ResolvedSource s : sources) if (!s.isUnnest) realScope.add(new Scope(s.alias, s.schema));
            resolveExpr(unnestSrc.expr, realScope, 0);

            var exprKind = getExprKind(unnestSrc.expr);
            if (exprKind != null && exprKind != FieldKind.JSON) {
                _diags.add(new SqlDiagnostic(
                    "UNNEST argument must be a JSON value (a JSON column, or a '->' JSON access expression) — got " + describeKind(exprKind) + ", which can never be a JSON array",
                    unnestSrc.expr.line, unnestSrc.expr.column));
            }
        }

        // Synthetic one-field element schema: the field NAME equals the alias itself.
        var elemFields = new LinkedHashMap<String, FieldKind>();
        elemFields.put(unnestSrc.alias, FieldKind.JSON);
        var elementSchema = new SourceSchema("(unnest)", elemFields);
        sources.add(new ResolvedSource(unnestSrc.alias, "(unnest)", elementSchema, false, null, null, true));
        _unnestAliases.add(unnestSrc.alias);

        joins.add(new JoinBinding(JoinKind.UNNEST, unnestSrc.alias, "(unnest)", null, null, null, null, false, unnestSrc.expr));
    }

    /** Walks e for the first Identifier/QualifiedIdentifier that names an already-resolved UNNEST alias. Doesn't
     *  descend into a nested subquery expression's own SelectQuery. */
    private String findUnnestAliasReference(Expr e) {
        if (e instanceof Identifier id && _unnestAliases.contains(id.name)) return id.name;
        if (e instanceof QualifiedIdentifier qid && _unnestAliases.contains(qid.qualifier)) return qid.qualifier;
        if (e instanceof UnaryExpr u) return findUnnestAliasReference(u.operand);
        if (e instanceof BinaryExpr b) {
            var l = findUnnestAliasReference(b.left);
            return l != null ? l : findUnnestAliasReference(b.right);
        }
        if (e instanceof FunctionCallExpr f) {
            for (Expr a : f.args) {
                var r = findUnnestAliasReference(a);
                if (r != null) return r;
            }
            return null;
        }
        if (e instanceof JsonAccessExpr j) return findUnnestAliasReference(j.left);
        return null;
    }

    /** Validates q as a fully independent, uncorrelated query against this Validator's own namespace — the one
     *  mechanism N1's derived tables, N2's IN/EXISTS subqueries, and N3/N4's residual scalar subqueries share. */
    private ValidationResult validateNestedUncorrelated(SelectQuery q) {
        return _mode == ValidationMode.TABLE
            ? validateTable(q, _streamSchemasForRecursion, _tableSchemasForRecursion)
            : validate(q, _schemas);
    }

    /** Table mode only: folds a nested (uncorrelated) validation's own leaf StreamInputs/TableInputs into this
     *  query's (transitive rather than the synthetic "(derived)" marker). */
    private void foldNestedInputs(ValidationResult inner) {
        if (_mode != ValidationMode.TABLE) return;
        _streamInputs.addAll(inner.streamInputs);
        _tableInputs.addAll(inner.tableInputs);
    }

    /** Plan 008 W3: checks that every branch of a set operation has the same output arity and positionally
     *  compatible column kinds, and computes the unified output schema — branch 0's column NAMES, with each
     *  column's KIND unified pairwise (Long+Double unify to Double; every other pair must be identical).
     *  An arity mismatch is reported (comparing every branch to branch 0's count) and short-circuits. Returns
     *  (diagnostics, null) on any incompatibility; (empty, non-null) on success. */
    public static BranchCompatibility checkSetOperationBranchCompatibility(List<SourceSchema> branchSchemas, int line, int column) {
        var diags = new ArrayList<SqlDiagnostic>();
        if (branchSchemas.isEmpty()) return new BranchCompatibility(diags, null);

        var firstNames = new ArrayList<>(branchSchemas.get(0).fields().keySet());
        var firstKinds = new ArrayList<>(branchSchemas.get(0).fields().values());
        int arity = firstNames.size();

        for (int b = 1; b < branchSchemas.size(); b++) {
            if (branchSchemas.get(b).fields().size() != arity) {
                diags.add(new SqlDiagnostic(
                    "UNION branch " + (b + 1) + " has " + branchSchemas.get(b).fields().size() + " output column(s), branch 1 has " + arity + " — every branch of a set operation must have the same number of columns",
                    line, column));
            }
        }
        if (!diags.isEmpty()) return new BranchCompatibility(diags, null);

        var unifiedKinds = firstKinds.toArray(new FieldKind[0]);
        for (int b = 1; b < branchSchemas.size(); b++) {
            var kinds = new ArrayList<>(branchSchemas.get(b).fields().values());
            for (int i = 0; i < arity; i++) {
                var u = unifiedKinds[i];
                var k = kinds.get(i);
                if (u == k) continue;
                if ((u == FieldKind.LONG && k == FieldKind.DOUBLE) || (u == FieldKind.DOUBLE && k == FieldKind.LONG)) {
                    unifiedKinds[i] = FieldKind.DOUBLE;
                    continue;
                }
                diags.add(new SqlDiagnostic(
                    "UNION branch " + (b + 1) + "'s column " + (i + 1) + " ('" + firstNames.get(i) + "') has kind " + kindName(k) + ", which is not compatible with branch 1's kind " + kindName(u),
                    line, column));
            }
        }
        if (!diags.isEmpty()) return new BranchCompatibility(diags, null);

        var fields = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < arity; i++) fields.put(firstNames.get(i), unifiedKinds[i]);
        return new BranchCompatibility(diags, new SourceSchema("(union)", fields));
    }

    /** Plan 004 N1: "a derived table's output schema is the synthetic source schema one level up." Mirrors
     *  Planner/TablePlanner's BuildOutputSchema (necessarily duplicated — Sql/ sits below Planning/). Same
     *  star/qualified-star/default-naming rules. */
    public static SourceSchema buildDerivedOutputSchema(SelectQuery q, ValidationResult inner) {
        var fields = new LinkedHashMap<String, FieldKind>();
        boolean prefixed = inner.sources.size() > 1;

        if (q.select.isStar) {
            for (ResolvedSource src : inner.sources) addSourceFields(fields, src, prefixed);
            return new SourceSchema("(derived)", fields);
        }

        for (int i = 0; i < q.select.items.size(); i++) {
            var item = q.select.items.get(i);
            if (item.expression instanceof QualifiedStarExpr qs) {
                ResolvedSource src = null;
                for (ResolvedSource s : inner.sources) {
                    if (s.alias.equals(qs.alias)) { src = s; break; }
                }
                if (src != null) addSourceFields(fields, src, prefixed);
                continue;
            }
            var name = item.alias != null ? item.alias : derivedDefaultName(item.expression, i);
            var kind = inner.exprKinds.get(item.expression);
            fields.put(name, kind != null ? kind : FieldKind.STRING);
        }
        return new SourceSchema("(derived)", fields);
    }

    private static void addSourceFields(Map<String, FieldKind> fields, ResolvedSource src, boolean prefixed) {
        for (var e : src.schema.fields().entrySet()) {
            fields.put(prefixed ? src.alias + "_" + e.getKey() : e.getKey(), e.getValue());
        }
    }

    private static String derivedDefaultName(Expr e, int index) {
        if (e instanceof Identifier id) return id.name;
        if (e instanceof QualifiedIdentifier q) return q.name;
        if (e instanceof AggregateCallExpr agg) return agg.isStar ? "count_star" : agg.name.toLowerCase(Locale.ROOT);
        if (e instanceof FunctionCallExpr f) return f.name.toLowerCase(Locale.ROOT);
        if (e instanceof JsonAccessExpr j) {
            if (j.key instanceof StringLiteral s) return s.value;
            if (j.key instanceof NumberLiteral n && n.longValue != null) return Long.toString(n.longValue);
            return "json";
        }
        return "col" + (index + 1);
    }

    private static String joinLabel(JoinKind kind) {
        switch (kind) {
            case INNER: return "INNER";
            case LEFT: return "LEFT";
            case RIGHT: return "RIGHT";
            case FULL: return "FULL";
            case CROSS: return "CROSS";
            default: return pascal(kind.name());
        }
    }

    /** C# enum-name rendering ({@code Enum.ToString()}): PascalCase of the Java constant. */
    private static String pascal(String upper) {
        return upper.isEmpty() ? upper : upper.substring(0, 1).toUpperCase(Locale.ROOT) + upper.substring(1).toLowerCase(Locale.ROOT);
    }

    /** The C# {@code FieldKind.ToString()} rendering used in diagnostics: String, Double, Long, Bool, Timestamp, Json. */
    private static String kindName(FieldKind k) { return pascal(k.name()); }

    private static boolean isReservedName(String name) {
        return name.equalsIgnoreCase(EventRecord.TIMESTAMP_FIELD) || name.equalsIgnoreCase(EventRecord.SOURCE_FIELD);
    }

    // ------------------------------------------------------------------
    // Column / alias resolution
    // ------------------------------------------------------------------

    private void resolveExpr(Expr e, List<Scope> scope, int aggDepth) {
        if (!_resolvedNodes.add(e)) return;

        if (e instanceof NumberLiteral nl) {
            _exprKind.put(e, nl.isDouble() ? FieldKind.DOUBLE : FieldKind.LONG);
        } else if (e instanceof StringLiteral) {
            _exprKind.put(e, FieldKind.STRING);
        } else if (e instanceof BoolLiteral) {
            _exprKind.put(e, FieldKind.BOOL);
        } else if (e instanceof NullLiteral || e instanceof StarExpr) {
            // no kind
        } else if (e instanceof Identifier id) {
            resolveBareIdentifier(id, scope);
        } else if (e instanceof QualifiedIdentifier qid) {
            resolveQualifiedIdentifier(qid, scope);
        } else if (e instanceof QualifiedStarExpr qs) {
            resolveQualifiedStar(qs, scope);
        } else if (e instanceof UnaryExpr u) {
            resolveExpr(u.operand, scope, aggDepth);
            recordUnaryKind(e, u);
        } else if (e instanceof BinaryExpr b) {
            resolveExpr(b.left, scope, aggDepth);
            resolveExpr(b.right, scope, aggDepth);
            if (ARITHMETIC_OR_COMPARISON_OPS.contains(b.op)) {
                checkBareJsonOperand(b.left, b.op, b.right);
                checkBareJsonOperand(b.right, b.op, b.left);
            }
            recordBinaryKind(e, b);
        } else if (e instanceof JsonAccessExpr j) {
            resolveJsonAccess(j, scope, aggDepth);
        } else if (e instanceof FunctionCallExpr f) {
            validateFunctionArity(f);
            for (Expr a : f.args) resolveExpr(a, scope, aggDepth);
            recordFunctionKind(e, f);
        } else if (e instanceof AggregateCallExpr agg) {
            if (aggDepth > 0) {
                _diags.add(new SqlDiagnostic("Aggregate functions cannot be nested", agg.line, agg.column));
            }
            _usedAggregates.add(agg);
            validateAggregateShape(agg);
            if (!agg.isStar && agg.arg != null) resolveExpr(agg.arg, scope, aggDepth + 1);
            recordAggregateKind(e, agg);
        } else if (e instanceof InSubqueryExpr ins) {
            resolveInSubquery(ins, scope, aggDepth);
        } else if (e instanceof ExistsExpr ex) {
            resolveExists(ex);
        } else if (e instanceof ScalarSubqueryExpr sse) {
            resolveScalarSubquery(sse, scope);
        }
    }

    private void recordUnaryKind(Expr node, UnaryExpr u) {
        if (u.op.equals("NOT")) { _exprKind.put(node, FieldKind.BOOL); return; }
        var k = getExprKind(u.operand);
        if (k != null && k != FieldKind.JSON) _exprKind.put(node, k);
    }

    private static final Set<String> COMPARISON_OPS = Set.of("=", "!=", "<>", "<", "<=", ">", ">=", "AND", "OR");

    private void recordBinaryKind(Expr node, BinaryExpr b) {
        if (COMPARISON_OPS.contains(b.op)) { _exprKind.put(node, FieldKind.BOOL); return; }
        if (b.op.equals("/")) { _exprKind.put(node, FieldKind.DOUBLE); return; }
        var lk = getExprKind(b.left);
        var rk = getExprKind(b.right);
        if (lk == FieldKind.LONG && rk == FieldKind.LONG) _exprKind.put(node, FieldKind.LONG);
        else if ((lk == FieldKind.LONG || lk == FieldKind.DOUBLE) && (rk == FieldKind.LONG || rk == FieldKind.DOUBLE)) _exprKind.put(node, FieldKind.DOUBLE);
    }

    private void recordFunctionKind(Expr node, FunctionCallExpr f) {
        switch (f.name.toUpperCase(Locale.ROOT)) {
            case "ABS":
            case "ROUND":
                _exprKind.put(node, !f.args.isEmpty() && getExprKind(f.args.get(0)) == FieldKind.LONG ? FieldKind.LONG : FieldKind.DOUBLE);
                break;
            case "UPPER":
            case "LOWER":
                _exprKind.put(node, FieldKind.STRING);
                break;
            case "COALESCE": {
                if (!f.args.isEmpty()) {
                    var k = getExprKind(f.args.get(0));
                    if (k != null) _exprKind.put(node, k);
                }
                break;
            }
            // Plan 009 Round C wave C1: type-conversion functions — result kind is fixed by the function name.
            case "TO_LONG":
                _exprKind.put(node, FieldKind.LONG);
                break;
            case "TO_DOUBLE":
                _exprKind.put(node, FieldKind.DOUBLE);
                break;
            case "TO_BOOL":
                _exprKind.put(node, FieldKind.BOOL);
                break;
            case "TO_TIMESTAMP":
                _exprKind.put(node, FieldKind.TIMESTAMP);
                break;
            case "TO_STRING":
                _exprKind.put(node, FieldKind.STRING);
                break;
            case "IF":
                recordIfKind(node, f);
                break;
            default: {
                // A registered scalar (SqlFunctions) — reached only for a name none of the built-in arms claimed.
                var ext = SqlFunctions.findScalar(f.name);
                if (ext != null) {
                    var argKinds = new ArrayList<FieldKind>();
                    for (Expr a : f.args) argKinds.add(getExprKind(a));
                    var extKind = ext.resultKind(argKinds);
                    if (extKind != null) _exprKind.put(node, extKind);
                }
                break;
            }
        }
    }

    /** {@code IF(cond, then, else)} — and therefore every searched CASE. The two branches have to agree;
     *  Long/Double mix widens to Double; anything else that disagrees is a diagnostic positioned on the call.
     *  An unknown branch kind is deferred to, not guessed at. */
    private void recordIfKind(Expr node, FunctionCallExpr f) {
        if (f.args.size() != 3) return; // arity already diagnosed

        var condKind = getExprKind(f.args.get(0));
        if (condKind != null && condKind != FieldKind.BOOL) {
            _diags.add(new SqlDiagnostic(
                "CASE/IF condition must be a boolean expression, got " + kindName(condKind), f.args.get(0).line, f.args.get(0).column));
        }

        var thenKind = getExprKind(f.args.get(1));
        var elseKind = getExprKind(f.args.get(2));
        if (thenKind == null || elseKind == null) {
            var known = thenKind != null ? thenKind : elseKind;
            if (known != null) _exprKind.put(node, known);
            return;
        }
        if (thenKind == elseKind) {
            _exprKind.put(node, thenKind);
            return;
        }
        if ((thenKind == FieldKind.LONG || thenKind == FieldKind.DOUBLE) && (elseKind == FieldKind.LONG || elseKind == FieldKind.DOUBLE)) {
            _exprKind.put(node, FieldKind.DOUBLE);
            return;
        }
        _diags.add(new SqlDiagnostic(
            "CASE/IF branches must produce the same type, got " + kindName(thenKind) + " and " + kindName(elseKind), f.line, f.column));
    }

    /** Argument count, DISTINCT placement and the parameterised aggregates' constant. */
    private void validateAggregateShape(AggregateCallExpr agg) {
        boolean takesParameter = StatAggregatorNames.takesParameter(agg.name);

        if (!agg.isStar) {
            int expected = takesParameter ? 2 : 1;
            if (agg.argCount != expected) {
                _diags.add(new SqlDiagnostic(
                    takesParameter
                        ? "'" + agg.name + "' takes exactly two arguments — a constant probability and the value, e.g. " + agg.name + "(0.95, x)"
                        : "Aggregate '" + agg.name + "' called with wrong number of arguments",
                    agg.line, agg.column));
            }
        }

        if (takesParameter && agg.argCount == 2 && !AggregateParameters.isUsableProbability(agg.parameter)) {
            // A literal, not an expression: the parameter configures the aggregate for the whole group.
            Expr at = agg.parameter != null ? agg.parameter : agg;
            _diags.add(new SqlDiagnostic(
                "'" + agg.name + "'s first argument must be a constant number between 0 and 1",
                at.line, at.column));
        }

        if (agg.isDistinct) {
            if (!agg.name.equalsIgnoreCase("COUNT")) {
                _diags.add(new SqlDiagnostic(
                    "DISTINCT is only supported on COUNT, not on '" + agg.name + "'", agg.line, agg.column));
            } else if (agg.arg == null) {
                _diags.add(new SqlDiagnostic("COUNT(DISTINCT …) needs an expression to count", agg.line, agg.column));
            }
        }
    }

    private void recordAggregateKind(Expr node, AggregateCallExpr agg) {
        var argKind = agg.arg != null ? getExprKind(agg.arg) : null;
        FieldKind result;
        switch (agg.name) {
            case "COUNT":
                result = FieldKind.LONG;
                break;
            case "SUM":
                result = argKind == FieldKind.LONG ? FieldKind.LONG : FieldKind.DOUBLE;
                break;
            case "AVG":
                result = FieldKind.DOUBLE;
                break;
            case "MIN":
            case "MAX":
                result = argKind != null ? argKind : FieldKind.DOUBLE;
                break;
            default: {
                if (StatAggregatorNames.canonical(agg.name) != null) {
                    // Every statistical aggregate is a real number even over integer input.
                    result = FieldKind.DOUBLE;
                } else {
                    // A registered aggregate states its own; Double stays the fallback.
                    var reg = SqlFunctions.findAggregate(agg.name);
                    var rk = reg != null ? reg.resultKind(argKind) : null;
                    result = rk != null ? rk : FieldKind.DOUBLE;
                }
                break;
            }
        }
        _exprKind.put(node, result);
    }

    private static String findFieldIgnoreCase(SourceSchema schema, String name) {
        for (String k : schema.fields().keySet()) if (k.equalsIgnoreCase(name)) return k;
        return null;
    }

    private void resolveBareIdentifier(Identifier id, List<Scope> scope) {
        boolean reserved = isReservedName(id.name);
        var matches = new ArrayList<Binding>();
        for (Scope s : scope) {
            if (reserved) {
                matches.add(new Binding(s.alias(), canonicalReserved(id.name)));
                continue;
            }
            var field = findFieldIgnoreCase(s.schema(), id.name);
            if (field != null) matches.add(new Binding(s.alias(), field));
        }

        if (matches.isEmpty()) {
            _diags.add(new SqlDiagnostic("Unknown column '" + id.name + "'", id.line, id.column));
            return;
        }
        if (matches.size() > 1) {
            var aliases = new ArrayList<String>();
            for (Binding m : matches) aliases.add(m.alias());
            _diags.add(new SqlDiagnostic(
                "Ambiguous column '" + id.name + "' — present in: " + String.join(", ", aliases),
                id.line, id.column));
            return;
        }
        _bindings.put(id, matches.get(0));
        if (reserved) _exprKind.put(id, reservedKind(id.name));
        else recordColumnKind(id, scope, matches.get(0));
    }

    private void resolveQualifiedIdentifier(QualifiedIdentifier qid, List<Scope> scope) {
        int entryIndex = -1;
        for (int i = 0; i < scope.size(); i++) {
            if (scope.get(i).alias().equals(qid.qualifier)) { entryIndex = i; break; }
        }
        if (entryIndex < 0) {
            var aliases = new ArrayList<String>();
            for (Scope s : scope) aliases.add(s.alias());
            _diags.add(new SqlDiagnostic("Unknown source '" + qid.qualifier + "' — available: " + String.join(", ", aliases), qid.line, qid.column));
            return;
        }
        var entry = scope.get(entryIndex);
        String alias = entry.alias();
        var schema = entry.schema();
        if (isReservedName(qid.name)) {
            _bindings.put(qid, new Binding(alias, canonicalReserved(qid.name)));
            _exprKind.put(qid, reservedKind(qid.name));
            return;
        }
        // Plan 002 L2: `unnestAlias.field` — an error unconditionally, pointing at the '->>' syntax instead.
        if (_unnestAliases.contains(alias)) {
            _diags.add(new SqlDiagnostic(
                "UNNEST alias '" + alias + "' has no field '" + qid.name + "' — its value is a single JSON array element, not a row; use a JSON operator instead, e.g. " + alias + " ->> '" + qid.name + "'",
                qid.line, qid.column));
            return;
        }
        var field = findFieldIgnoreCase(schema, qid.name);
        if (field == null) {
            _diags.add(new SqlDiagnostic("Unknown column '" + qid.name + "' on '" + alias + "'", qid.line, qid.column));
            return;
        }
        _bindings.put(qid, new Binding(alias, field));
        var kind = schema.fields().get(field);
        if (kind != null) _exprKind.put(qid, kind);
    }

    /** Resolves {@code alias.*}'s qualifier against the query's sources; there's no single field/kind to
     *  record — the Planner re-walks the alias at plan time. */
    private void resolveQualifiedStar(QualifiedStarExpr qs, List<Scope> scope) {
        boolean found = false;
        for (Scope s : scope) if (s.alias().equals(qs.alias)) { found = true; break; }
        if (!found) {
            var aliases = new ArrayList<String>();
            for (Scope s : scope) aliases.add(s.alias());
            _diags.add(new SqlDiagnostic("Unknown source/alias '" + qs.alias + "' — available: " + String.join(", ", aliases), qs.line, qs.column));
        }
    }

    private void recordColumnKind(Expr node, List<Scope> scope, Binding resolved) {
        SourceSchema schema = null;
        for (Scope s : scope) if (s.alias().equals(resolved.alias())) { schema = s.schema(); break; }
        // C# scope.First(...) throws if absent; resolved came from this scope so it is always present.
        var kind = schema.fields().get(resolved.field());
        if (kind != null) _exprKind.put(node, kind);
    }

    private static FieldKind reservedKind(String name) {
        return name.equalsIgnoreCase(EventRecord.TIMESTAMP_FIELD) ? FieldKind.LONG : FieldKind.STRING;
    }

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') validation
    // ------------------------------------------------------------------

    private static final Set<String> ARITHMETIC_OR_COMPARISON_OPS =
        Set.of("+", "-", "*", "/", "%", "=", "!=", "<>", "<", "<=", ">", ">=");

    private FieldKind getExprKind(Expr e) { return _exprKind.get(e); }

    /** Bare-JSON guard for comparisons/arithmetic: a Json-kind operand can't be compared/computed on directly
     *  — except {@code = NULL} / {@code != NULL}. */
    void checkBareJsonOperand(Expr operand, String op, Expr otherOperand) {
        if (getExprKind(operand) != FieldKind.JSON) return;
        boolean isNullEquality = (op.equals("=") || op.equals("!=") || op.equals("<>")) && otherOperand instanceof NullLiteral;
        if (isNullEquality) return;

        _diags.add(new SqlDiagnostic(
            "JSON value used directly in '" + op + "' — extract a value first with '->>' (e.g. col ->> 'key' " + op + " ...); " +
            "a bare JSON column (or a '->' chain without a final '->>') can only be compared to NULL",
            operand.line, operand.column));
    }

    private void resolveJsonAccess(JsonAccessExpr j, List<Scope> scope, int aggDepth) {
        resolveExpr(j.left, scope, aggDepth);

        var leftKind = getExprKind(j.left);
        if (leftKind != FieldKind.JSON) {
            String op = j.returnText ? "->>" : "->";
            _diags.add(new SqlDiagnostic(
                "'" + op + "' left operand must be a JSON column (or another '->' result), not " + describeKind(leftKind),
                j.line, j.column));
        }

        // '->>' always yields TEXT; a terminal '->' yields Json.
        _exprKind.put(j, j.returnText ? FieldKind.STRING : FieldKind.JSON);
    }

    private static String describeKind(FieldKind kind) {
        if (kind == null) return "a non-JSON value";
        if (kind == FieldKind.JSON) return "JSON";
        return kindName(kind);
    }

    private static String canonicalReserved(String name) {
        return name.equalsIgnoreCase(EventRecord.TIMESTAMP_FIELD) ? EventRecord.TIMESTAMP_FIELD : EventRecord.SOURCE_FIELD;
    }

    // Plan 009 Round C wave C1: TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/TO_STRING — total, never-throwing
    // type-conversion functions; CAST(expr AS type) desugars to these same names.
    /** The Validator's built-in scalar-function names (case-insensitive). */
    public static final Set<String> KNOWN_FUNCTIONS;
    static {
        var s = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        Collections.addAll(s,
            "ABS", "ROUND", "UPPER", "LOWER", "COALESCE",
            "TO_LONG", "TO_DOUBLE", "TO_BOOL", "TO_TIMESTAMP", "TO_STRING",
            // The searched-CASE desugar target; callable directly as IF(cond, then, else) too.
            "IF");
        KNOWN_FUNCTIONS = Collections.unmodifiableSet(s);
    }

    private void validateFunctionArity(FunctionCallExpr f) {
        if (!KNOWN_FUNCTIONS.contains(f.name)) {
            var ext = SqlFunctions.findScalar(f.name);
            if (ext != null) {
                if (!ext.isValidArity(f.args.size())) {
                    _diags.add(new SqlDiagnostic("Function '" + f.name + "' called with wrong number of arguments", f.line, f.column));
                }
                return;
            }
            _diags.add(new SqlDiagnostic("Unknown function '" + f.name + "'", f.line, f.column));
            return;
        }
        int n = f.args.size();
        boolean ok;
        switch (f.name.toUpperCase(Locale.ROOT)) {
            case "ABS":
            case "UPPER":
            case "LOWER":
                ok = n == 1;
                break;
            case "ROUND":
                ok = n == 1 || n == 2;
                break;
            case "COALESCE":
                ok = n >= 1;
                break;
            case "TO_LONG":
            case "TO_DOUBLE":
            case "TO_BOOL":
            case "TO_TIMESTAMP":
            case "TO_STRING":
                ok = n == 1;
                break;
            case "IF":
                ok = n == 3;
                break;
            default:
                ok = true;
                break;
        }
        if (!ok) {
            _diags.add(new SqlDiagnostic("Function '" + f.name + "' called with wrong number of arguments", f.line, f.column));
        }
    }

    // ------------------------------------------------------------------
    // Plan 004 N2 — IN / EXISTS subquery predicates (WHERE, top-level AND-conjunct only)
    // ------------------------------------------------------------------

    private boolean isAllowedSubqueryPredicatePosition(Expr e) {
        return _resolvingWhere && _whereTopLevelConjuncts != null && _whereTopLevelConjuncts.contains(e);
    }

    private String subqueryPredicatePositionMessage() {
        return _resolvingWhere
            ? "IN/EXISTS subquery predicates must be a top-level AND-connected condition in WHERE (not nested inside OR/NOT/another expression) — plan 004 N2"
            : "IN/EXISTS subquery predicates are only allowed in WHERE (not SELECT list, ON, or GROUP BY) — plan 004 N2";
    }

    /** Plan 004 N2, pipelines only: true when q itself windows, OR (recursively) any of its FROM/JOIN sources
     *  is a derived table/CTE whose own inner query is transitively windowed. */
    private static boolean isTransitivelyWindowed(SelectQuery q, ValidationResult v) {
        if (q.window != null) return true;
        for (ResolvedSource s : v.sources) {
            if (s.derived != null && isTransitivelyWindowed(s.derived.query, s.derived.validation)) return true;
        }
        return false;
    }

    private void resolveExists(ExistsExpr ex) {
        if (!isAllowedSubqueryPredicatePosition(ex)) {
            _diags.add(new SqlDiagnostic(subqueryPredicatePositionMessage(), ex.line, ex.column));
            return;
        }

        var inner = validateNestedUncorrelated(ex.subquery);
        _diags.addAll(inner.diagnostics);
        foldNestedInputs(inner);

        // EXISTS/NOT EXISTS is uncorrelated in this tier: an outer-alias reference already surfaced as the
        // SAME "Unknown source"/"Unknown column" diagnostic an uncorrelated N1 derived table would produce.
        if (_mode == ValidationMode.STREAM && !isTransitivelyWindowed(ex.subquery, inner)) {
            _diags.add(new SqlDiagnostic(
                "EXISTS subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its membership is well-defined — plan 004 N2",
                ex.line, ex.column));
        }

        _exprKind.put(ex, FieldKind.BOOL);
        _subqueryPredicates.put(ex, new SubqueryPredicateInfo(ex.subquery, inner, ex.negated, null));
    }

    private void resolveInSubquery(InSubqueryExpr ie, List<Scope> scope, int aggDepth) {
        resolveExpr(ie.left, scope, aggDepth);

        if (!isAllowedSubqueryPredicatePosition(ie)) {
            _diags.add(new SqlDiagnostic(subqueryPredicatePositionMessage(), ie.line, ie.column));
            return;
        }

        var inner = validateNestedUncorrelated(ie.subquery);
        _diags.addAll(inner.diagnostics);
        foldNestedInputs(inner);

        boolean singleColumn = !ie.subquery.select.isStar && ie.subquery.select.items.size() == 1
            && !(ie.subquery.select.items.get(0).expression instanceof QualifiedStarExpr);
        if (!singleColumn) {
            _diags.add(new SqlDiagnostic("IN subquery must select exactly one column", ie.line, ie.column));
        }

        if (_mode == ValidationMode.STREAM && !isTransitivelyWindowed(ie.subquery, inner)) {
            _diags.add(new SqlDiagnostic(
                "IN subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its membership set is well-defined — plan 004 N2",
                ie.line, ie.column));
        }

        _exprKind.put(ie, FieldKind.BOOL);
        String keyColumn = null;
        if (singleColumn) {
            var first = ie.subquery.select.items.get(0);
            keyColumn = first.alias != null ? first.alias : derivedDefaultName(first.expression, 0);
        }
        _subqueryPredicates.put(ie, new SubqueryPredicateInfo(ie.subquery, inner, ie.negated, keyColumn));
    }

    // ------------------------------------------------------------------
    // Plan 004 N3/N4 — scalar subquery expressions
    // ------------------------------------------------------------------

    private static Set<String> collectFromAliases(SelectQuery q) {
        var result = new HashSet<String>();
        result.add(q.from.source.alias);
        for (JoinClause j : q.from.joins) result.add(j.source.alias);
        return result;
    }

    /** True when e references an alias that resolves against outerAliases and is NOT shadowed by innerAliases.
     *  Only qualified references can be correlation refs. Does not descend into a nested subquery's own query. */
    private static boolean containsOuterRef(Expr e, Set<String> innerAliases, Set<String> outerAliases) {
        if (e instanceof QualifiedIdentifier qid) return !innerAliases.contains(qid.qualifier) && outerAliases.contains(qid.qualifier);
        if (e instanceof QualifiedStarExpr qs) return !innerAliases.contains(qs.alias) && outerAliases.contains(qs.alias);
        if (e instanceof UnaryExpr u) return containsOuterRef(u.operand, innerAliases, outerAliases);
        if (e instanceof BinaryExpr b) return containsOuterRef(b.left, innerAliases, outerAliases) || containsOuterRef(b.right, innerAliases, outerAliases);
        if (e instanceof FunctionCallExpr f) {
            for (Expr a : f.args) if (containsOuterRef(a, innerAliases, outerAliases)) return true;
            return false;
        }
        if (e instanceof AggregateCallExpr agg) return agg.arg != null && containsOuterRef(agg.arg, innerAliases, outerAliases);
        if (e instanceof JsonAccessExpr j) return containsOuterRef(j.left, innerAliases, outerAliases);
        if (e instanceof InSubqueryExpr ins) return containsOuterRef(ins.left, innerAliases, outerAliases); // .subquery is independently scoped
        return false;
    }

    private static boolean queryHasOuterRefOutsideWhere(SelectQuery q, Set<String> innerAliases, Set<String> outerAliases) {
        if (!q.select.isStar) {
            for (SelectItem i : q.select.items) if (containsOuterRef(i.expression, innerAliases, outerAliases)) return true;
        }
        if (q.groupBy != null) {
            for (Expr g : q.groupBy) if (containsOuterRef(g, innerAliases, outerAliases)) return true;
        }
        return false;
    }

    /** If eq is exactly {@code innerExpr = outerAlias.outerField} or the reverse, resolves the outer field
     *  against outerScope and returns the split; null (not this shape) for anything looser. */
    private CorrelationTuple trySplitCorrelationEquality(BinaryExpr eq, Set<String> innerAliases, Set<String> outerAliases, List<Scope> outerScope) {
        var r = tryOuterSide(eq.left, eq.right, innerAliases, outerAliases, outerScope);
        if (r != null) return r;
        return tryOuterSide(eq.right, eq.left, innerAliases, outerAliases, outerScope);
    }

    private CorrelationTuple tryOuterSide(Expr outerSide, Expr otherSide, Set<String> innerAliases, Set<String> outerAliases, List<Scope> outerScope) {
        if (!(outerSide instanceof QualifiedIdentifier q) || innerAliases.contains(q.qualifier) || !outerAliases.contains(q.qualifier)) return null;
        if (containsOuterRef(otherSide, innerAliases, outerAliases)) return null; // both sides reference outer — not a clean split

        SourceSchema outerSourceSchema = null;
        for (Scope s : outerScope) if (s.alias().equals(q.qualifier)) { outerSourceSchema = s.schema(); break; }
        var canonicalField = findFieldIgnoreCase(outerSourceSchema, q.name);
        if (canonicalField == null) {
            _diags.add(new SqlDiagnostic("Unknown column '" + q.name + "' on '" + q.qualifier + "'", q.line, q.column));
            return null;
        }

        return new CorrelationTuple(otherSide, q.qualifier, canonicalField);
    }

    /** Resolves a scalar subquery expression (plan 004 N3 uncorrelated / N4 equality-correlated). N4's
     *  decorrelation happens right here: correlation conjuncts are stripped out of a residual copy of the inner
     *  query's WHERE, and (when any correlation was found) a GROUP BY over every correlation's inner expression
     *  is synthesized, each also projected under a synthetic {@code __key{i}} name. The residual query is then
     *  validated exactly like an N1 derived table. */
    private void resolveScalarSubquery(ScalarSubqueryExpr sse, List<Scope> outerScope) {
        var innerAliases = collectFromAliases(sse.query);
        var outerAliases = new HashSet<String>();
        for (Scope s : outerScope) outerAliases.add(s.alias());

        var pureInnerConjuncts = new ArrayList<Expr>();
        var correlations = new ArrayList<CorrelationTuple>();
        boolean invalidCorrelation = false;

        if (sse.query.where != null) {
            for (Expr conjunct : flattenAnd(sse.query.where)) {
                if (!containsOuterRef(conjunct, innerAliases, outerAliases)) {
                    pureInnerConjuncts.add(conjunct);
                    continue;
                }
                if (conjunct instanceof BinaryExpr eq && eq.op.equals("=")) {
                    var split = trySplitCorrelationEquality(eq, innerAliases, outerAliases, outerScope);
                    if (split != null) {
                        correlations.add(split);
                        continue;
                    }
                }
                invalidCorrelation = true;
            }
        }

        if (!invalidCorrelation && queryHasOuterRefOutsideWhere(sse.query, innerAliases, outerAliases)) {
            invalidCorrelation = true;
        }

        if (invalidCorrelation) {
            _diags.add(new SqlDiagnostic("Correlated subqueries beyond equality are not supported — rewrite as a JOIN", sse.line, sse.column));
            return;
        }

        if (sse.query.groupBy != null) {
            _diags.add(new SqlDiagnostic(
                "Scalar subquery may not have its own GROUP BY — use equality correlation instead (plan 004 N4) or drop it for an uncorrelated single-row aggregate (N3)",
                sse.line, sse.column));
            return;
        }
        if (sse.query.select.isStar || sse.query.select.items.size() != 1 || sse.query.select.items.get(0).expression instanceof QualifiedStarExpr) {
            _diags.add(new SqlDiagnostic("Scalar subquery must select exactly one column", sse.line, sse.column));
            return;
        }
        var valueItem = sse.query.select.items.get(0);
        if (!containsAggregate(valueItem.expression)) {
            _diags.add(new SqlDiagnostic(
                "Scalar subquery must be a single-row aggregate query (an aggregate with no GROUP BY) — anything else is not provably single-row",
                sse.line, sse.column));
            return;
        }

        Expr residualWhere = andFold(pureInnerConjuncts);

        var selectItems = new ArrayList<SelectItem>(sse.query.select.items);
        var keyColumnNames = new ArrayList<String>();
        List<Expr> groupBy = null;
        if (!correlations.isEmpty()) {
            groupBy = new ArrayList<>();
            for (int i = 0; i < correlations.size(); i++) {
                var name = "__key" + i;
                keyColumnNames.add(name);
                groupBy.add(correlations.get(i).innerExpr());
                selectItems.add(new SelectItem(correlations.get(i).innerExpr(), name));
            }
        }

        var residualQuery = new SelectQuery(
            new SelectClause(false, selectItems),
            sse.query.from,
            residualWhere,
            groupBy,
            sse.query.window,
            sse.query.emit,
            sse.query.emitLine, sse.query.emitColumn, sse.query.groupByLine, sse.query.groupByColumn, sse.query.windowLine, sse.query.windowColumn,
            null, null, null);

        var inner = validateNestedUncorrelated(residualQuery);
        _diags.addAll(inner.diagnostics);
        foldNestedInputs(inner);

        if (_mode == ValidationMode.STREAM && !isTransitivelyWindowed(residualQuery, inner)) {
            _diags.add(new SqlDiagnostic(
                "Scalar subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its value is well-defined — plan 004 N3/N4",
                sse.line, sse.column));
        }

        var valueColumnName = valueItem.alias != null ? valueItem.alias : derivedDefaultName(valueItem.expression, 0);
        var k = inner.exprKinds.get(valueItem.expression);
        var kind = k != null ? k : FieldKind.DOUBLE;
        _exprKind.put(sse, kind);
        var corrKeys = new ArrayList<CorrelationKey>();
        for (CorrelationTuple c : correlations) corrKeys.add(new CorrelationKey(c.innerExpr(), c.outerAlias(), c.outerField()));
        _scalarSubqueries.put(sse, new ScalarSubqueryInfo(residualQuery, inner, corrKeys, valueColumnName, keyColumnNames));
    }

    /** {@code list.Aggregate((a, b) => new BinaryExpr("AND", a, b, a.Line, a.Column))}; null for an empty list. */
    private static Expr andFold(List<Expr> list) {
        if (list.isEmpty()) return null;
        Expr acc = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            acc = new BinaryExpr("AND", acc, list.get(i), acc.line, acc.column);
        }
        return acc;
    }

    // ------------------------------------------------------------------
    // Equi-join key extraction
    // ------------------------------------------------------------------

    private static List<Expr> flattenAnd(Expr e) {
        var result = new ArrayList<Expr>();
        flattenAndInto(e, result);
        return result;
    }

    private static void flattenAndInto(Expr e, List<Expr> out) {
        if (e instanceof BinaryExpr b && b.op.equals("AND")) {
            flattenAndInto(b.left, out);
            flattenAndInto(b.right, out);
        } else {
            out.add(e);
        }
    }

    private Set<String> collectAliases(Expr e) {
        var result = new HashSet<String>();
        collectAliasesWalk(e, result);
        return result;
    }

    private void collectAliasesWalk(Expr node, Set<String> result) {
        if (node instanceof Identifier || node instanceof QualifiedIdentifier) {
            var b = _bindings.get(node);
            if (b != null) result.add(b.alias());
        } else if (node instanceof UnaryExpr u) {
            collectAliasesWalk(u.operand, result);
        } else if (node instanceof BinaryExpr bin) {
            collectAliasesWalk(bin.left, result);
            collectAliasesWalk(bin.right, result);
        } else if (node instanceof FunctionCallExpr f) {
            for (Expr a : f.args) collectAliasesWalk(a, result);
        } else if (node instanceof AggregateCallExpr agg) {
            if (agg.arg != null) collectAliasesWalk(agg.arg, result);
        } else if (node instanceof JsonAccessExpr j) {
            collectAliasesWalk(j.left, result); // Key is always a literal — no aliases there
        }
    }

    /** Plan 008: collects EVERY equi-conjunct spanning the two sides into an ordered pair of key lists
     *  (composite keys). Only a conjunct that is NOT an equi-comparison between exactly these two sides remains
     *  in the residual. Null when there is no equi-conjunct at all. */
    private EquiKey extractEquiKey(Expr onExpr, Set<String> leftAliases, String rightAlias) {
        var conjuncts = flattenAnd(onExpr);
        var leftKeys = new ArrayList<Expr>();
        var rightKeys = new ArrayList<Expr>();
        var residuals = new ArrayList<Expr>();

        for (Expr c : conjuncts) {
            if (c instanceof BinaryExpr be && be.op.equals("=")) {
                var la = collectAliases(be.left);
                var ra = collectAliases(be.right);
                if (!la.isEmpty() && leftAliases.containsAll(la) && ra.size() == 1 && ra.contains(rightAlias)) {
                    leftKeys.add(be.left);
                    rightKeys.add(be.right);
                    continue;
                }
                if (!ra.isEmpty() && leftAliases.containsAll(ra) && la.size() == 1 && la.contains(rightAlias)) {
                    leftKeys.add(be.right);
                    rightKeys.add(be.left);
                    continue;
                }
            }
            residuals.add(c);
        }

        if (leftKeys.isEmpty()) return null;
        return new EquiKey(leftKeys, rightKeys, andFold(residuals));
    }

    // ------------------------------------------------------------------
    // Structural helpers
    // ------------------------------------------------------------------

    public static boolean containsAggregate(Expr e) {
        if (e instanceof AggregateCallExpr) return true;
        // Plan 004 N3/N4: a scalar subquery's value doesn't vary across an outer GROUP BY group any differently
        // than an aggregate's would — exempt it from "non-aggregate select item must appear in GROUP BY".
        if (e instanceof ScalarSubqueryExpr) return true;
        if (e instanceof UnaryExpr u) return containsAggregate(u.operand);
        if (e instanceof BinaryExpr b) return containsAggregate(b.left) || containsAggregate(b.right);
        if (e instanceof FunctionCallExpr f) {
            for (Expr a : f.args) if (containsAggregate(a)) return true;
            return false;
        }
        if (e instanceof JsonAccessExpr j) return containsAggregate(j.left);
        return false;
    }

    /** Plan 004 N3/N4: finds the first ScalarSubqueryExpr in e that is NOT nested inside an aggregate call's own
     *  argument — returns its node (for position), or null if none. */
    private static Expr findScalarSubqueryOutsideAggregate(Expr e, boolean insideAggregate) {
        if (e instanceof ScalarSubqueryExpr && !insideAggregate) return e;
        if (e instanceof AggregateCallExpr agg) return agg.arg != null ? findScalarSubqueryOutsideAggregate(agg.arg, true) : null;
        if (e instanceof UnaryExpr u) return findScalarSubqueryOutsideAggregate(u.operand, insideAggregate);
        if (e instanceof BinaryExpr b) {
            var l = findScalarSubqueryOutsideAggregate(b.left, insideAggregate);
            return l != null ? l : findScalarSubqueryOutsideAggregate(b.right, insideAggregate);
        }
        if (e instanceof FunctionCallExpr f) {
            for (Expr a : f.args) {
                var r = findScalarSubqueryOutsideAggregate(a, insideAggregate);
                if (r != null) return r;
            }
            return null;
        }
        if (e instanceof JsonAccessExpr j) return findScalarSubqueryOutsideAggregate(j.left, insideAggregate);
        return null;
    }

    private boolean structurallyEqual(Expr a, Expr b) { return structurallyEqual(a, b, _bindings); }

    public static boolean structurallyEqual(Expr a, Expr b, Map<Expr, Binding> bindings) {
        if (a instanceof Identifier || a instanceof QualifiedIdentifier) {
            if (!(b instanceof Identifier || b instanceof QualifiedIdentifier)) return false;
            var ba = bindings.get(a);
            var bb = bindings.get(b);
            return ba != null && bb != null && ba.equals(bb);
        }
        if (a instanceof NumberLiteral na && b instanceof NumberLiteral nb) {
            if (na.isDouble() != nb.isDouble()) return false;
            if (na.isDouble()) {
                double x = na.doubleValue, y = nb.doubleValue;
                return x == y || (Double.isNaN(x) && Double.isNaN(y));
            }
            return na.longValue.equals(nb.longValue);
        }
        if (a instanceof StringLiteral sa && b instanceof StringLiteral sb) return sa.value.equals(sb.value);
        if (a instanceof BoolLiteral ba2 && b instanceof BoolLiteral bb2) return ba2.value == bb2.value;
        if (a instanceof NullLiteral && b instanceof NullLiteral) return true;
        if (a instanceof StarExpr && b instanceof StarExpr) return true;
        if (a instanceof UnaryExpr ua && b instanceof UnaryExpr ub) return ua.op.equals(ub.op) && structurallyEqual(ua.operand, ub.operand, bindings);
        if (a instanceof BinaryExpr xa && b instanceof BinaryExpr xb) {
            return xa.op.equals(xb.op) && structurallyEqual(xa.left, xb.left, bindings) && structurallyEqual(xa.right, xb.right, bindings);
        }
        if (a instanceof FunctionCallExpr fa && b instanceof FunctionCallExpr fb) {
            if (!fa.name.equalsIgnoreCase(fb.name) || fa.args.size() != fb.args.size()) return false;
            for (int i = 0; i < fa.args.size(); i++) {
                if (!structurallyEqual(fa.args.get(i), fb.args.get(i), bindings)) return false;
            }
            return true;
        }
        if (a instanceof AggregateCallExpr aa && b instanceof AggregateCallExpr ab) {
            return aa.name.equalsIgnoreCase(ab.name) && aa.isStar == ab.isStar
                && ((aa.arg == null && ab.arg == null) || (aa.arg != null && ab.arg != null && structurallyEqual(aa.arg, ab.arg, bindings)));
        }
        if (a instanceof JsonAccessExpr ja && b instanceof JsonAccessExpr jb) {
            return ja.returnText == jb.returnText
                && structurallyEqual(ja.left, jb.left, bindings) && structurallyEqual(ja.key, jb.key, bindings);
        }
        return false;
    }
}
