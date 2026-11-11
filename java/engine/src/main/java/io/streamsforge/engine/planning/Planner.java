package io.streamsforge.engine.planning;

import static io.streamsforge.engine.sql.Ast.*;

import io.streamsforge.engine.CompileResult;
import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.PipelinePlan;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.sql.JoinKeyFolding;
import io.streamsforge.engine.sql.Parser;
import io.streamsforge.engine.sql.ResolvedSource;
import io.streamsforge.engine.sql.ScalarSubqueryInfo;
import io.streamsforge.engine.sql.SubqueryPredicateInfo;
import io.streamsforge.engine.sql.Tokenizer;
import io.streamsforge.engine.sql.ValidationResult;
import io.streamsforge.engine.sql.Validator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Tokenize → parse → validate → plan a pipeline-mode query. */
public final class Planner {
    private Planner() {}

    public static CompileResult compile(String sql, Map<String, SourceSchema> schemas) {
        try {
            return compileCore(sql, schemas);
        } catch (Exception ex) {
            var d = new ArrayList<SqlDiagnostic>();
            d.add(new SqlDiagnostic("Internal compiler error: " + ex.getMessage(), 1, 1));
            return new CompileResult(false, d, null, List.of(), null, null);
        }
    }

    private static boolean hasError(List<SqlDiagnostic> diagnostics) {
        for (var d : diagnostics) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }

    private static CompileResult compileCore(String sql, Map<String, SourceSchema> schemas) {
        var diagnostics = new ArrayList<SqlDiagnostic>();

        var tok = new Tokenizer(sql).tokenize();
        diagnostics.addAll(tok.diagnostics());

        var parsed = Parser.parseStatement(tok.tokens());
        diagnostics.addAll(parsed.diagnostics());

        if (parsed.query() == null && parsed.setOp() == null) {
            return new CompileResult(false, diagnostics, null, List.of(), null, null);
        }
        if (parsed.setOp() != null) return compileSetOperation(parsed.setOp(), schemas, diagnostics);

        var query = parsed.query();
        var validation = Validator.validate(query, schemas);
        diagnostics.addAll(validation.diagnostics);

        if (hasError(diagnostics)) return new CompileResult(false, diagnostics, null, List.of(), null, null);

        var compiled = buildCompiledPlan(query, validation);
        return new CompileResult(true, diagnostics, compiled.planSummary, compiled.sourceNames, compiled.outputSchema, new PipelinePlan(compiled));
    }

    /** Plan 008 W3: top-level (or WITH-wrapped) {@code SELECT ... UNION [ALL] SELECT ...} chain. */
    private static CompileResult compileSetOperation(SetOperationQuery setOp, Map<String, SourceSchema> schemas, List<SqlDiagnostic> diagnostics) {
        var v = Validator.validateSetOperation(setOp, schemas);
        diagnostics.addAll(v.diagnostics);

        if (hasError(diagnostics)) return new CompileResult(false, diagnostics, null, List.of(), null, null);

        var compiled = buildCompiledUnionPlan(setOp, v.branchValidations, v.outputSchema);
        return new CompileResult(true, diagnostics, compiled.planSummary, compiled.sourceNames, compiled.outputSchema, new PipelinePlan(compiled));
    }

    /** Builds a union-root CompiledPlan — every branch compiled via the ordinary, recursive buildCompiledPlan,
     *  then EVERY branch (0 included) normalized to the SAME output column names (branch 0's own) AND the SAME
     *  {@code _source} stamp (the union's own joined label). */
    private static CompiledPlan buildCompiledUnionPlan(SetOperationQuery setOp, List<ValidationResult> branchValidations, SourceSchema unifiedSchema) {
        var branchNames = new ArrayList<>(unifiedSchema.fields().keySet());
        var rawBranchPlans = new ArrayList<CompiledPlan>();
        for (int i = 0; i < setOp.branches.size(); i++) rawBranchPlans.add(buildCompiledPlan(setOp.branches.get(i), branchValidations.get(i)));

        var sourceNames = new LinkedHashSet<String>();
        var labels = new ArrayList<String>();
        for (var b : rawBranchPlans) { sourceNames.addAll(b.sourceNames); labels.add(b.sourceLabel); }
        String sourceLabel = String.join(",", labels);
        String opText = setOp.all ? "UNION ALL" : "UNION";
        String summary = String.join(" " + opText + " ", labels) + " → SELECT " + unifiedSchema.fields().size() + " cols";

        var branchPlans = new ArrayList<CompiledPlan>();
        for (var bp : rawBranchPlans) branchPlans.add(normalizeOutputColumns(bp, branchNames, sourceLabel));

        var plan = new CompiledPlan();
        plan.sources = new ArrayList<>();
        plan.joins = new ArrayList<>();
        plan.where = null;
        plan.groupBy = null;
        plan.window = null;
        plan.emit = EmitMode.FINAL;
        plan.output = new ArrayList<>();
        plan.aggregateNodes = new ArrayList<>();
        plan.aggregateIndex = new IdentityHashMap<>();
        plan.bindings = new IdentityHashMap<>();
        plan.hasAggregates = false;
        plan.planSummary = summary;
        plan.outputSchema = unifiedSchema;
        plan.sourceNames = new ArrayList<>(sourceNames);
        plan.sourceLabel = sourceLabel;
        plan.unionBranches = branchPlans;
        return plan;
    }

    private static CompiledPlan normalizeOutputColumns(CompiledPlan plan, List<String> names, String sourceLabel) {
        var newOutput = new ArrayList<OutputItem>();
        for (int i = 0; i < plan.output.size(); i++) {
            var item = plan.output.get(i);
            newOutput.add(new OutputItem(i < names.size() ? names.get(i) : item.name, item.expression, item.groupByIndex));
        }
        var oldKinds = new ArrayList<>(plan.outputSchema.fields().values());
        var newFields = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < newOutput.size(); i++) newFields.put(newOutput.get(i).name, i < oldKinds.size() ? oldKinds.get(i) : FieldKind.STRING);

        var p = new CompiledPlan();
        p.sources = plan.sources; p.joins = plan.joins; p.where = plan.where; p.groupBy = plan.groupBy; p.window = plan.window;
        p.emit = plan.emit; p.output = newOutput; p.aggregateNodes = plan.aggregateNodes; p.aggregateIndex = plan.aggregateIndex;
        p.bindings = plan.bindings; p.hasAggregates = plan.hasAggregates; p.planSummary = plan.planSummary;
        p.outputSchema = new SourceSchema(plan.outputSchema.name(), newFields);
        p.sourceNames = plan.sourceNames; p.sourceLabel = sourceLabel; p.unionBranches = plan.unionBranches;
        return p;
    }

    /** Builds the executable plan from a validated query — recursive (derived tables/CTEs carry their own
     *  already-validated inner query + ValidationResult). */
    private static CompiledPlan buildCompiledPlan(SelectQuery q, ValidationResult v) {
        var sources = new ArrayList<CompiledSource>();
        for (ResolvedSource s : v.sources) {
            CompiledPlan derived = s.derived != null
                ? buildCompiledPlan(s.derived.query, s.derived.validation)
                : s.unionDerived != null ? buildCompiledUnionPlan(s.unionDerived.setOp, s.unionDerived.branchValidations, s.schema) : null;
            sources.add(new CompiledSource(s.alias, s.sourceName, s.schema, derived));
        }
        var joins = new ArrayList<CompiledJoin>();
        for (var j : v.joins) {
            ResolvedSource srcEntry = null;
            for (var s : v.sources) if (s.alias.equals(j.alias)) { srcEntry = s; break; }
            if (srcEntry == null) throw new IllegalStateException("Sequence contains no matching element");
            // Pipeline mode has no composite-key-aware op: the fold is UNCONDITIONAL here.
            var residual = JoinKeyFolding.foldExtraKeysIntoResidual(j.leftKeys, j.rightKeys, j.residual);
            var cj = new CompiledJoin();
            cj.kind = j.kind;
            cj.alias = j.alias;
            cj.sourceName = j.sourceName;
            cj.schema = srcEntry.schema;
            cj.within = j.within != null ? j.within : Duration.ZERO;
            cj.leftKey = j.leftKeys != null ? j.leftKeys.get(0) : null;
            cj.rightKey = j.rightKeys != null ? j.rightKeys.get(0) : null;
            cj.residual = residual;
            cj.leftKeys = j.leftKeys;
            cj.rightKeys = j.rightKeys;
            for (var s : sources) if (s.alias.equals(j.alias)) { cj.derivedPlan = s.derivedPlan; break; }
            cj.unnestExpr = j.unnestExpr;
            joins.add(cj);
        }

        var bindings = v.bindings;

        // Plan 004 N2/N3/N4: rewrite WHERE and the projection; each substitution appends one Semi/Anti/Scalar join.
        Expr where = rewriteWhereForSubqueryPredicates(q.where, v, bindings, joins);
        var selectItems = new ArrayList<SelectItem>();
        for (var item : q.select.items) selectItems.add(new SelectItem(rewriteScalarSubqueries(item.expression, v, bindings, joins), item.alias));
        var qForOutput = new SelectQuery(new SelectClause(q.select.isStar, selectItems), q.from, q.where, q.groupBy, q.window, q.emit,
            q.emitLine, q.emitColumn, q.groupByLine, q.groupByColumn, q.windowLine, q.windowColumn, null, null, null);

        var output = buildOutput(qForOutput, sources, bindings);

        if (q.window != null) assignGroupByIndexes(output, q.groupBy, bindings);

        var exprs = new ArrayList<Expr>();
        for (var o : output) exprs.add(o.expression);
        var aggregateNodes = collectAggregateNodes(exprs);
        var aggregateIndex = new IdentityHashMap<AggregateCallExpr, Integer>();
        for (int i = 0; i < aggregateNodes.size(); i++) aggregateIndex.put(aggregateNodes.get(i), i);

        // Real, externally-feedable leaf source names (see C# doc): derived sources contribute their own child
        // SourceNames; synthesized Semi/Anti/Scalar joins likewise; UNNEST aliases are excluded.
        Set<String> unnestAliases = new LinkedHashSet<>();
        for (var j : joins) if (j.kind == JoinKind.UNNEST) unnestAliases.add(j.alias);
        var sourceNames = new LinkedHashSet<String>();
        for (var s : sources) {
            if (unnestAliases.contains(s.alias)) continue;
            if (s.derivedPlan != null) sourceNames.addAll(s.derivedPlan.sourceNames); else sourceNames.add(s.sourceName);
        }
        for (var j : joins) {
            if (j.kind == JoinKind.SEMI || j.kind == JoinKind.ANTI || j.kind == JoinKind.SCALAR) sourceNames.addAll(j.derivedPlan.sourceNames);
        }
        var aliases = new ArrayList<String>();
        for (var s : sources) aliases.add(s.alias);

        var plan = new CompiledPlan();
        plan.sources = sources;
        plan.joins = joins;
        plan.where = where;
        plan.groupBy = q.groupBy;
        plan.window = q.window;
        plan.emit = q.emit != null ? q.emit : EmitMode.FINAL;
        plan.output = output;
        plan.aggregateNodes = aggregateNodes;
        plan.aggregateIndex = aggregateIndex;
        plan.bindings = bindings;
        plan.hasAggregates = !aggregateNodes.isEmpty();
        plan.planSummary = buildPlanSummary(q, sources, joins, output, bindings);
        plan.outputSchema = buildOutputSchema(output, sources, v.exprKinds);
        plan.sourceNames = new ArrayList<>(sourceNames);
        plan.sourceLabel = String.join(",", aliases);
        return plan;
    }

    /** Derives the pipeline's output row schema (column name → kind) from the projection. */
    private static SourceSchema buildOutputSchema(List<OutputItem> output, List<CompiledSource> sources, Map<Expr, FieldKind> exprKinds) {
        var fields = new LinkedHashMap<String, FieldKind>();
        for (var item : output) {
            FieldKind kind;
            CompiledSource src = null;
            if (item.expression instanceof QualifiedIdentifier qid) {
                for (var s : sources) if (s.alias.equals(qid.qualifier)) { src = s; break; }
            }
            if (src != null) {
                // Star-expansion synthetic node: not part of the validated AST, so exprKinds won't have it.
                var k = src.schema.fields().get(((QualifiedIdentifier) item.expression).name);
                kind = k != null ? k : FieldKind.STRING;
            } else if (exprKinds.containsKey(item.expression)) {
                kind = exprKinds.get(item.expression);
            } else {
                kind = FieldKind.STRING;
            }
            fields.put(item.name, kind);
        }
        return new SourceSchema("(pipeline)", fields);
    }

    private static List<OutputItem> buildOutput(SelectQuery q, List<CompiledSource> sources, Map<Expr, Binding> bindings) {
        var output = new ArrayList<OutputItem>();

        if (q.select.isStar) {
            boolean prefixed = sources.size() > 1;
            for (var src : sources) {
                for (String field : src.schema.fields().keySet()) {
                    var node = new QualifiedIdentifier(src.alias, field, 0, 0);
                    bindings.put(node, new Binding(src.alias, field));
                    String name = prefixed ? src.alias + "_" + field : field;
                    output.add(new OutputItem(name, node));
                }
            }
            return output;
        }

        for (int i = 0; i < q.select.items.size(); i++) {
            var item = q.select.items.get(i);
            if (item.expression instanceof QualifiedStarExpr qs) {
                expandQualifiedStar(qs, sources, bindings, output);
                continue;
            }
            String name = item.alias != null ? item.alias : defaultName(item.expression, i);
            output.add(new OutputItem(name, item.expression));
        }
        return output;
    }

    private static void expandQualifiedStar(QualifiedStarExpr qs, List<CompiledSource> sources, Map<Expr, Binding> bindings, List<OutputItem> output) {
        CompiledSource src = null;
        for (var s : sources) if (s.alias.equals(qs.alias)) { src = s; break; }
        if (src == null) throw new IllegalStateException("Sequence contains no matching element");
        boolean prefixed = sources.size() > 1;
        for (String field : src.schema.fields().keySet()) {
            var node = new QualifiedIdentifier(src.alias, field, qs.line, qs.column);
            bindings.put(node, new Binding(src.alias, field));
            String name = prefixed ? src.alias + "_" + field : field;
            output.add(new OutputItem(name, node));
        }
    }

    static String defaultName(Expr e, int index) {
        if (e instanceof Identifier id) return id.name;
        if (e instanceof QualifiedIdentifier q) return q.name;
        if (e instanceof AggregateCallExpr agg) return agg.isStar ? "count_star" : agg.name.toLowerCase(Locale.ROOT);
        if (e instanceof FunctionCallExpr f) return f.name.toLowerCase(Locale.ROOT);
        if (e instanceof JsonAccessExpr j) return jsonKeyText(j.key);
        return "col" + (index + 1);
    }

    static String jsonKeyText(Expr key) {
        if (key instanceof StringLiteral s) return s.value;
        if (key instanceof NumberLiteral n && n.longValue != null) return Long.toString(n.longValue);
        return "json";
    }

    private static void assignGroupByIndexes(List<OutputItem> output, List<Expr> groupBy, Map<Expr, Binding> bindings) {
        if (groupBy == null) return;
        for (var item : output) {
            if (Validator.containsAggregate(item.expression)) continue;
            int idx = -1;
            for (int i = 0; i < groupBy.size(); i++) {
                if (Validator.structurallyEqual(item.expression, groupBy.get(i), bindings)) { idx = i; break; }
            }
            if (idx >= 0) item.groupByIndex = idx;
        }
    }

    static List<AggregateCallExpr> collectAggregateNodes(Iterable<Expr> roots) {
        var list = new ArrayList<AggregateCallExpr>();
        for (var root : roots) walkAggregates(root, list);
        return list;
    }

    private static void walkAggregates(Expr e, List<AggregateCallExpr> list) {
        if (e instanceof AggregateCallExpr agg) {
            list.add(agg);
        } else if (e instanceof UnaryExpr u) {
            walkAggregates(u.operand, list);
        } else if (e instanceof BinaryExpr b) {
            walkAggregates(b.left, list);
            walkAggregates(b.right, list);
        } else if (e instanceof FunctionCallExpr f) {
            for (var a : f.args) walkAggregates(a, list);
        } else if (e instanceof JsonAccessExpr j) {
            walkAggregates(j.left, list);
        }
    }

    static String formatDuration(Duration d) {
        double totalMs = d.toNanos() / 1_000_000.0;
        if (totalMs % 1000 == 0) return (long) (d.toNanos() / 1_000_000_000.0) + "s";
        return (long) totalMs + "ms";
    }

    static String columnText(Expr e, Map<Expr, Binding> bindings) {
        if (e instanceof Identifier || e instanceof QualifiedIdentifier) {
            var b = bindings.get(e);
            if (b != null) return b.alias() + "_" + b.field();
        }
        if (e instanceof Identifier id) return id.name;
        if (e instanceof QualifiedIdentifier q) return q.qualifier + "_" + q.name;
        if (e instanceof JsonAccessExpr j) return columnText(j.left, bindings) + (j.returnText ? "->>" : "->") + jsonKeyText(j.key);
        return "expr";
    }

    /** Enum name as C# prints it (PascalCase), for plan summaries. */
    static String pascal(Enum<?> e) {
        String n = e.name();
        return n.charAt(0) + n.substring(1).toLowerCase(Locale.ROOT);
    }

    private static String buildPlanSummary(SelectQuery q, List<CompiledSource> sources, List<CompiledJoin> joins, List<OutputItem> output, Map<Expr, Binding> bindings) {
        var sb = new StringBuilder();
        sb.append(sources.get(0).sourceName).append(" AS ").append(sources.get(0).alias);

        for (var j : joins) {
            if (j.kind == JoinKind.UNNEST) {
                sb.append(" ⇶[UNNEST] ").append(columnText(j.unnestExpr, bindings)).append(" AS ").append(j.alias);
                continue;
            }
            String symbol = j.kind == JoinKind.CROSS ? "×" : "⋈";
            String label;
            switch (j.kind) {
                case INNER: label = "INNER"; break;
                case LEFT: label = "LEFT"; break;
                case RIGHT: label = "RIGHT"; break;
                case FULL: label = "FULL"; break;
                case CROSS: label = "CROSS"; break;
                default: label = pascal(j.kind); break;
            }
            sb.append(" ").append(symbol).append("[").append(label).append(",").append(formatDuration(j.within)).append("] ")
                .append(j.sourceName).append(" AS ").append(j.alias);
        }

        var parts = new ArrayList<String>();
        parts.add(sb.toString());

        if (q.where != null) parts.add("WHERE");

        if (q.window != null) {
            String windowText;
            if (q.window instanceof TumblingWindowSpec t) windowText = "TUMBLING(" + formatDuration(t.size) + ")";
            else if (q.window instanceof HoppingWindowSpec h) windowText = "HOPPING(" + formatDuration(h.size) + "," + formatDuration(h.advance) + ")";
            else if (q.window instanceof SessionWindowSpec s) windowText = "SESSION(" + formatDuration(s.gap) + ")";
            else windowText = "WINDOW";
            if (q.groupBy != null) {
                var cols = new ArrayList<String>();
                for (var g : q.groupBy) cols.add(columnText(g, bindings));
                windowText += " GROUP BY " + String.join(", ", cols);
            }
            parts.add(windowText);
        }

        parts.add("SELECT " + output.size());
        return String.join(" → ", parts);
    }

    // ------------------------------------------------------------------
    // Plan 004 N2/N3/N4 — subquery predicate/expression rewriting.
    // ------------------------------------------------------------------

    static void flattenAndForRewrite(Expr e, List<Expr> out) {
        if (e instanceof BinaryExpr b && b.op.equals("AND")) {
            flattenAndForRewrite(b.left, out);
            flattenAndForRewrite(b.right, out);
        } else {
            out.add(e);
        }
    }

    private static Expr rewriteWhereForSubqueryPredicates(Expr where, ValidationResult v, Map<Expr, Binding> bindings, List<CompiledJoin> joins) {
        if (where == null) return null;

        var conjuncts = new ArrayList<Expr>();
        flattenAndForRewrite(where, conjuncts);
        var residual = new ArrayList<Expr>();
        for (var conjunct : conjuncts) {
            if (conjunct instanceof InSubqueryExpr ins && v.subqueryPredicates.containsKey(ins)) {
                joins.add(buildSemiAntiJoin(v.subqueryPredicates.get(ins), ins.left, bindings, joins.size()));
                continue;
            }
            if (conjunct instanceof ExistsExpr ex && v.subqueryPredicates.containsKey(ex)) {
                joins.add(buildSemiAntiJoin(v.subqueryPredicates.get(ex), null, bindings, joins.size()));
                continue;
            }
            residual.add(rewriteScalarSubqueries(conjunct, v, bindings, joins));
        }

        if (residual.isEmpty()) return null;
        Expr acc = residual.get(0);
        for (int i = 1; i < residual.size(); i++) acc = new BinaryExpr("AND", acc, residual.get(i), acc.line, acc.column);
        return acc;
    }

    private static CompiledJoin buildSemiAntiJoin(SubqueryPredicateInfo info, Expr insLeft, Map<Expr, Binding> bindings, int index) {
        String alias = "__sq" + index;
        var derivedPlan = buildCompiledPlan(info.query, info.validation);

        Expr leftKey, rightKey;
        if (insLeft != null) {
            leftKey = insLeft;
            var rightNode = new QualifiedIdentifier(alias, info.keyColumnName, 0, 0);
            bindings.put(rightNode, new Binding(alias, info.keyColumnName));
            rightKey = rightNode;
        } else {
            leftKey = new NumberLiteral(null, 0L, 0, 0);
            rightKey = new NumberLiteral(null, 0L, 0, 0);
        }

        var cj = new CompiledJoin();
        cj.kind = info.negated ? JoinKind.ANTI : JoinKind.SEMI;
        cj.alias = alias;
        cj.sourceName = "(derived)";
        cj.schema = derivedPlan.outputSchema;
        cj.within = Duration.ZERO;
        cj.leftKey = leftKey;
        cj.rightKey = rightKey;
        cj.residual = null;
        cj.leftKeys = List.of(leftKey);
        cj.rightKeys = List.of(rightKey);
        cj.derivedPlan = derivedPlan;
        return cj;
    }

    private static CompiledJoin buildScalarJoin(ScalarSubqueryInfo info, Map<Expr, Binding> bindings, int index) {
        String alias = "__sq" + index;
        var derivedPlan = buildCompiledPlan(info.residualQuery, info.validation);

        Expr leftKey, rightKey;
        Expr residual = null;

        if (info.correlations.isEmpty()) {
            leftKey = new NumberLiteral(null, 0L, 0, 0);
            rightKey = new NumberLiteral(null, 0L, 0, 0);
        } else {
            var first = info.correlations.get(0);
            var outerNode = new QualifiedIdentifier(first.outerAlias, first.outerField, 0, 0);
            bindings.put(outerNode, new Binding(first.outerAlias, first.outerField));
            leftKey = outerNode;

            var innerNode = new QualifiedIdentifier(alias, info.keyColumnNames.get(0), 0, 0);
            bindings.put(innerNode, new Binding(alias, info.keyColumnNames.get(0)));
            rightKey = innerNode;

            for (int i = 1; i < info.correlations.size(); i++) {
                var c = info.correlations.get(i);
                var outerN = new QualifiedIdentifier(c.outerAlias, c.outerField, 0, 0);
                bindings.put(outerN, new Binding(c.outerAlias, c.outerField));
                var innerN = new QualifiedIdentifier(alias, info.keyColumnNames.get(i), 0, 0);
                bindings.put(innerN, new Binding(alias, info.keyColumnNames.get(i)));
                var eq = new BinaryExpr("=", outerN, innerN, 0, 0);
                residual = residual == null ? eq : new BinaryExpr("AND", residual, eq, 0, 0);
            }
        }

        var cj = new CompiledJoin();
        cj.kind = JoinKind.SCALAR;
        cj.alias = alias;
        cj.sourceName = "(derived)";
        cj.schema = derivedPlan.outputSchema;
        cj.within = Duration.ZERO;
        cj.leftKey = leftKey;
        cj.rightKey = rightKey;
        cj.residual = residual;
        cj.leftKeys = List.of(leftKey);
        cj.rightKeys = List.of(rightKey);
        cj.derivedPlan = derivedPlan;
        return cj;
    }

    /** Replaces every ScalarSubqueryExpr in {@code e} with a bound reference to its synthesized Scalar join
     *  stage's output column. Reference-preserving: an unchanged subtree is returned as the same object. */
    private static Expr rewriteScalarSubqueries(Expr e, ValidationResult v, Map<Expr, Binding> bindings, List<CompiledJoin> joins) {
        if (e instanceof ScalarSubqueryExpr sse && v.scalarSubqueries.containsKey(sse)) {
            var info = v.scalarSubqueries.get(sse);
            var join = buildScalarJoin(info, bindings, joins.size());
            joins.add(join);
            var node = new QualifiedIdentifier(join.alias, info.valueColumnName, sse.line, sse.column);
            bindings.put(node, new Binding(join.alias, info.valueColumnName));
            if (v.exprKinds.containsKey(sse)) v.exprKinds.put(node, v.exprKinds.get(sse));
            return node;
        }
        if (e instanceof UnaryExpr u) {
            var operand = rewriteScalarSubqueries(u.operand, v, bindings, joins);
            if (operand == u.operand) return e;
            var rebuilt = new UnaryExpr(u.op, operand, u.line, u.column);
            copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof BinaryExpr b) {
            var left = rewriteScalarSubqueries(b.left, v, bindings, joins);
            var right = rewriteScalarSubqueries(b.right, v, bindings, joins);
            if (left == b.left && right == b.right) return e;
            var rebuilt = new BinaryExpr(b.op, left, right, b.line, b.column);
            copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof FunctionCallExpr f) {
            var args = new ArrayList<Expr>();
            boolean same = true;
            for (var a : f.args) {
                var r = rewriteScalarSubqueries(a, v, bindings, joins);
                if (r != a) same = false;
                args.add(r);
            }
            if (same) return e;
            var rebuilt = new FunctionCallExpr(f.name, args, f.line, f.column);
            copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof AggregateCallExpr agg) {
            if (agg.arg == null) return e;
            var arg = rewriteScalarSubqueries(agg.arg, v, bindings, joins);
            if (arg == agg.arg) return e;
            var rebuilt = new AggregateCallExpr(agg.name, arg, agg.isStar, agg.line, agg.column);
            copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof JsonAccessExpr j) {
            var left = rewriteScalarSubqueries(j.left, v, bindings, joins);
            if (left == j.left) return e;
            var rebuilt = new JsonAccessExpr(left, j.returnText, j.key, j.line, j.column);
            copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        return e;
    }

    static void copyExprKind(ValidationResult v, Expr from, Expr to) {
        var k = v.exprKinds.get(from);
        if (k != null) v.exprKinds.put(to, k);
    }
}
