package io.streamsforge.engine.planning;

import static io.streamsforge.engine.sql.Ast.*;

import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.TableCompileResult;
import io.streamsforge.engine.TablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.sql.JoinKeyFolding;
import io.streamsforge.engine.sql.Parser;
import io.streamsforge.engine.sql.ResolvedSource;
import io.streamsforge.engine.sql.ScalarSubqueryInfo;
import io.streamsforge.engine.sql.SubqueryPredicateInfo;
import io.streamsforge.engine.sql.Tokenizer;
import io.streamsforge.engine.sql.ValidationResult;
import io.streamsforge.engine.sql.Validator;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Tokenize → parse → validate (table-mode grammar) → plan a materialized TABLE's SELECT. */
public final class TablePlanner {
    private TablePlanner() {}

    public static TableCompileResult compile(String sql, Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        try {
            return compileCore(sql, streamSchemas, tableSchemas);
        } catch (Exception ex) {
            var d = new ArrayList<SqlDiagnostic>();
            d.add(new SqlDiagnostic("Internal compiler error: " + ex.getMessage(), 1, 1));
            return TableCompileResult.failure(d);
        }
    }

    private static boolean hasError(List<SqlDiagnostic> diagnostics) {
        for (var d : diagnostics) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }

    private static TableCompileResult compileCore(String sql, Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        var diagnostics = new ArrayList<SqlDiagnostic>();

        var tok = new Tokenizer(sql).tokenize();
        diagnostics.addAll(tok.diagnostics());

        var parsed = Parser.parseStatement(tok.tokens());
        diagnostics.addAll(parsed.diagnostics());

        if (parsed.query() == null && parsed.setOp() == null) return TableCompileResult.failure(diagnostics);

        if (parsed.setOp() != null) return compileSetOperation(parsed.setOp(), streamSchemas, tableSchemas, diagnostics);

        var validation = Validator.validateTable(parsed.query(), streamSchemas, tableSchemas);
        diagnostics.addAll(validation.diagnostics);

        if (hasError(diagnostics)) return TableCompileResult.failure(diagnostics);

        var compiled = buildCompiledTablePlan(parsed.query(), validation);
        return new TableCompileResult(true, diagnostics, compiled.planSummary, compiled.streamInputs, compiled.tableInputs,
            compiled.outputSchema, new TablePlan(compiled));
    }

    /** Plan 008 W3: compiles a top-level (or WITH-wrapped) table-mode set operation. UNION (distinct) is allowed here. */
    private static TableCompileResult compileSetOperation(SetOperationQuery setOp, Map<String, SourceSchema> streamSchemas,
                                                          Map<String, SourceSchema> tableSchemas, List<SqlDiagnostic> diagnostics) {
        var v = Validator.validateSetOperationTable(setOp, streamSchemas, tableSchemas);
        diagnostics.addAll(v.diagnostics);

        if (hasError(diagnostics)) return TableCompileResult.failure(diagnostics);

        var compiled = buildCompiledUnionPlan(setOp, v.branchValidations, v.outputSchema, v.streamInputs, v.tableInputs);
        return new TableCompileResult(true, diagnostics, compiled.planSummary, compiled.streamInputs, compiled.tableInputs,
            compiled.outputSchema, new TablePlan(compiled));
    }

    private static CompiledTablePlan buildCompiledUnionPlan(SetOperationQuery setOp, List<ValidationResult> branchValidations,
                                                            SourceSchema unifiedSchema, List<String> streamInputs, List<String> tableInputs) {
        var branchNames = new ArrayList<>(unifiedSchema.fields().keySet());
        var rawBranchPlans = new ArrayList<CompiledTablePlan>();
        for (int i = 0; i < setOp.branches.size(); i++) rawBranchPlans.add(buildCompiledTablePlan(setOp.branches.get(i), branchValidations.get(i)));

        String opText = setOp.all ? "UNION ALL" : "UNION";
        var labels = new ArrayList<String>();
        for (var b : rawBranchPlans) labels.add(b.sourceLabel);
        String sourceLabel = String.join(",", labels);
        String summary = String.join(" " + opText + " ", labels) + " → SELECT " + unifiedSchema.fields().size() + " cols";

        // EVERY branch (0 included) is normalized to the SAME output column names AND the SAME `_source` stamp.
        var branchPlans = new ArrayList<CompiledTablePlan>();
        for (var bp : rawBranchPlans) branchPlans.add(normalizeBranchPlan(bp, branchNames, sourceLabel));

        var p = new CompiledTablePlan();
        p.sources = new ArrayList<>();
        p.joins = new ArrayList<>();
        p.where = null;
        p.groupBy = null;
        p.output = new ArrayList<>();
        p.aggregateNodes = new ArrayList<>();
        p.aggregateIndex = new IdentityHashMap<>();
        p.bindings = new IdentityHashMap<>();
        p.hasAggregates = false;
        p.planSummary = summary;
        p.streamInputs = streamInputs;
        p.tableInputs = tableInputs;
        p.sourceLabel = sourceLabel;
        p.outputSchema = unifiedSchema;
        p.latestBy = null;
        p.unionBranches = branchPlans;
        p.unionAll = setOp.all;
        return p;
    }

    private static CompiledTablePlan normalizeBranchPlan(CompiledTablePlan plan, List<String> names, String sourceLabel) {
        var newOutput = new ArrayList<OutputItem>();
        for (int i = 0; i < plan.output.size(); i++) {
            var item = plan.output.get(i);
            newOutput.add(new OutputItem(i < names.size() ? names.get(i) : item.name, item.expression, item.groupByIndex));
        }
        var oldKinds = new ArrayList<>(plan.outputSchema.fields().values());
        var newFields = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < newOutput.size(); i++) newFields.put(newOutput.get(i).name, i < oldKinds.size() ? oldKinds.get(i) : FieldKind.STRING);

        var p = new CompiledTablePlan();
        p.sources = plan.sources; p.joins = plan.joins; p.where = plan.where; p.groupBy = plan.groupBy; p.output = newOutput;
        p.aggregateNodes = plan.aggregateNodes; p.aggregateIndex = plan.aggregateIndex; p.bindings = plan.bindings;
        p.hasAggregates = plan.hasAggregates; p.planSummary = plan.planSummary; p.streamInputs = plan.streamInputs;
        p.tableInputs = plan.tableInputs; p.sourceLabel = sourceLabel;
        p.outputSchema = new SourceSchema(plan.outputSchema.name(), newFields);
        p.latestBy = plan.latestBy; p.unionBranches = plan.unionBranches; p.unionAll = plan.unionAll;
        return p;
    }

    private static CompiledTablePlan buildCompiledTablePlan(SelectQuery q, ValidationResult v) {
        var sources = new ArrayList<CompiledTableSource>();
        for (ResolvedSource s : v.sources) {
            CompiledTablePlan derived = null;
            if (s.derived != null) {
                derived = buildCompiledTablePlan(s.derived.query, s.derived.validation);
            } else if (s.unionDerived != null) {
                var si = new LinkedHashSet<String>();
                var ti = new LinkedHashSet<String>();
                for (var bv : s.unionDerived.branchValidations) { si.addAll(bv.streamInputs); ti.addAll(bv.tableInputs); }
                derived = buildCompiledUnionPlan(s.unionDerived.setOp, s.unionDerived.branchValidations, s.schema, new ArrayList<>(si), new ArrayList<>(ti));
            }
            sources.add(new CompiledTableSource(s.alias, s.sourceName, s.schema, s.isTable, derived));
        }
        var joins = new ArrayList<CompiledTableJoin>();
        for (var j : v.joins) {
            ResolvedSource srcEntry = null;
            for (var s : v.sources) if (s.alias.equals(j.alias)) { srcEntry = s; break; }
            if (srcEntry == null) throw new IllegalStateException("Sequence contains no matching element");

            // CROSS JOIN has no ON clause: constant-key trick (NumberLiteral(0) both sides) gives the full product.
            List<Expr> leftKeys, rightKeys;
            if (j.kind == JoinKind.CROSS) {
                leftKeys = new ArrayList<>(List.of(new NumberLiteral(null, 0L, 0, 0)));
                rightKeys = new ArrayList<>(List.of(new NumberLiteral(null, 0L, 0, 0)));
            } else {
                leftKeys = j.leftKeys != null ? new ArrayList<>(j.leftKeys) : null;
                rightKeys = j.rightKeys != null ? new ArrayList<>(j.rightKeys) : null;
            }

            // TableOuterJoinOp (LEFT/RIGHT/FULL) reads leftKeys/rightKeys directly and needs the PURE residual;
            // every other kind folds extra equi-key components back into residual.
            Expr residual = (j.kind == JoinKind.LEFT || j.kind == JoinKind.RIGHT || j.kind == JoinKind.FULL)
                ? j.residual
                : JoinKeyFolding.foldExtraKeysIntoResidual(leftKeys, rightKeys, j.residual);

            var cj = new CompiledTableJoin();
            cj.kind = j.kind;
            cj.alias = j.alias;
            cj.sourceName = j.sourceName;
            cj.schema = srcEntry.schema;
            cj.isTable = j.isTable;
            cj.leftKey = leftKeys != null ? leftKeys.get(0) : null;
            cj.rightKey = rightKeys != null ? rightKeys.get(0) : null;
            cj.residual = residual;
            cj.leftKeys = leftKeys;
            cj.rightKeys = rightKeys;
            for (var s : sources) if (s.alias.equals(j.alias)) { cj.derivedPlan = s.derivedPlan; break; }
            cj.unnestExpr = j.unnestExpr;
            joins.add(cj);
        }

        var bindings = v.bindings;

        Expr where = rewriteWhereForSubqueryPredicates(q.where, v, bindings, joins);
        var selectItems = new ArrayList<SelectItem>();
        for (var item : q.select.items) selectItems.add(new SelectItem(rewriteScalarSubqueries(item.expression, v, bindings, joins), item.alias));
        var qForOutput = new SelectQuery(new SelectClause(q.select.isStar, selectItems), q.from, q.where, q.groupBy, q.window, q.emit,
            q.emitLine, q.emitColumn, q.groupByLine, q.groupByColumn, q.windowLine, q.windowColumn, q.latestBy, q.latestByLine, q.latestByColumn);

        var output = buildOutput(qForOutput, sources, bindings);

        if (q.groupBy != null) assignGroupByIndexes(output, q.groupBy, bindings);

        var exprs = new ArrayList<Expr>();
        for (var o : output) exprs.add(o.expression);
        var aggregateNodes = Planner.collectAggregateNodes(exprs);
        var aggregateIndex = new IdentityHashMap<AggregateCallExpr, Integer>();
        for (int i = 0; i < aggregateNodes.size(); i++) aggregateIndex.put(aggregateNodes.get(i), i);

        var aliases = new ArrayList<String>();
        for (var s : sources) aliases.add(s.alias);
        var outputSchema = buildOutputSchema(output, sources, v.exprKinds);

        var plan = new CompiledTablePlan();
        plan.sources = sources;
        plan.joins = joins;
        plan.where = where;
        plan.groupBy = q.groupBy;
        plan.output = output;
        plan.aggregateNodes = aggregateNodes;
        plan.aggregateIndex = aggregateIndex;
        plan.bindings = bindings;
        plan.hasAggregates = !aggregateNodes.isEmpty();
        plan.planSummary = buildPlanSummary(q, sources, joins, output, bindings);
        plan.streamInputs = v.streamInputs;
        plan.tableInputs = v.tableInputs;
        plan.sourceLabel = String.join(",", aliases);
        plan.outputSchema = outputSchema;
        plan.latestBy = q.latestBy;
        return plan;
    }

    private static List<OutputItem> buildOutput(SelectQuery q, List<CompiledTableSource> sources, Map<Expr, Binding> bindings) {
        var output = new ArrayList<OutputItem>();

        if (q.select.isStar) {
            boolean prefixed = sources.size() > 1;
            for (var src : sources) {
                for (String field : src.schema.fields().keySet()) {
                    var node = new QualifiedIdentifier(src.alias, field, 0, 0);
                    bindings.put(node, new Binding(src.alias, field));
                    output.add(new OutputItem(prefixed ? src.alias + "_" + field : field, node));
                }
            }
            return output;
        }

        for (int i = 0; i < q.select.items.size(); i++) {
            var item = q.select.items.get(i);
            if (item.expression instanceof QualifiedStarExpr qs) {
                CompiledTableSource src = null;
                for (var s : sources) if (s.alias.equals(qs.alias)) { src = s; break; }
                if (src == null) throw new IllegalStateException("Sequence contains no matching element");
                boolean prefixed = sources.size() > 1;
                for (String field : src.schema.fields().keySet()) {
                    var node = new QualifiedIdentifier(src.alias, field, qs.line, qs.column);
                    bindings.put(node, new Binding(src.alias, field));
                    output.add(new OutputItem(prefixed ? src.alias + "_" + field : field, node));
                }
                continue;
            }
            String name = item.alias != null ? item.alias : Planner.defaultName(item.expression, i);
            output.add(new OutputItem(name, item.expression));
        }
        return output;
    }

    private static void assignGroupByIndexes(List<OutputItem> output, List<Expr> groupBy, Map<Expr, Binding> bindings) {
        for (var item : output) {
            if (Validator.containsAggregate(item.expression)) continue;
            int idx = -1;
            for (int i = 0; i < groupBy.size(); i++) {
                if (Validator.structurallyEqual(item.expression, groupBy.get(i), bindings)) { idx = i; break; }
            }
            if (idx >= 0) item.groupByIndex = idx;
        }
    }

    private static SourceSchema buildOutputSchema(List<OutputItem> output, List<CompiledTableSource> sources, Map<Expr, FieldKind> exprKinds) {
        var fields = new LinkedHashMap<String, FieldKind>();
        for (var item : output) {
            FieldKind kind;
            CompiledTableSource src = null;
            if (item.expression instanceof QualifiedIdentifier qid) {
                for (var s : sources) if (s.alias.equals(qid.qualifier)) { src = s; break; }
            }
            if (src != null) {
                var k = src.schema.fields().get(((QualifiedIdentifier) item.expression).name);
                kind = k != null ? k : FieldKind.STRING;
            } else if (exprKinds.containsKey(item.expression)) {
                kind = exprKinds.get(item.expression);
            } else {
                kind = FieldKind.STRING;
            }
            fields.put(item.name, kind);
        }
        return new SourceSchema("(table)", fields);
    }

    private static String buildPlanSummary(SelectQuery q, List<CompiledTableSource> sources, List<CompiledTableJoin> joins,
                                           List<OutputItem> output, Map<Expr, Binding> bindings) {
        var sb = new StringBuilder();
        sb.append(sources.get(0).sourceName).append(" AS ").append(sources.get(0).alias);

        for (var j : joins) {
            if (j.kind == JoinKind.UNNEST) {
                sb.append(" ⇶[UNNEST] ").append(Planner.columnText(j.unnestExpr, bindings)).append(" AS ").append(j.alias);
                continue;
            }
            String symbol = j.kind == JoinKind.CROSS ? "×" : "⋈";
            String label = j.kind == JoinKind.INNER ? "INNER" : j.kind == JoinKind.CROSS ? "CROSS" : Planner.pascal(j.kind);
            sb.append(" ").append(symbol).append("[").append(label).append("] ").append(j.sourceName).append(" AS ").append(j.alias);
        }

        var parts = new ArrayList<String>();
        parts.add(sb.toString());

        if (q.where != null) parts.add("WHERE");

        if (q.groupBy != null) {
            var cols = new ArrayList<String>();
            for (var g : q.groupBy) cols.add(Planner.columnText(g, bindings));
            parts.add("GROUP BY " + String.join(", ", cols));
        }

        if (q.latestBy != null) {
            var cols = new ArrayList<String>();
            for (var k : q.latestBy) cols.add(Planner.columnText(k, bindings));
            parts.add("LATEST BY " + String.join(", ", cols));
        }

        parts.add("SELECT " + output.size());
        return String.join(" → ", parts);
    }

    // ------------------------------------------------------------------
    // Plan 004 N2/N3/N4 — subquery predicate/expression rewriting (table-mode mirror of Planner).
    // ------------------------------------------------------------------

    private static Expr rewriteWhereForSubqueryPredicates(Expr where, ValidationResult v, Map<Expr, Binding> bindings, List<CompiledTableJoin> joins) {
        if (where == null) return null;

        var conjuncts = new ArrayList<Expr>();
        Planner.flattenAndForRewrite(where, conjuncts);
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

    private static CompiledTableJoin buildSemiAntiJoin(SubqueryPredicateInfo info, Expr insLeft, Map<Expr, Binding> bindings, int index) {
        String alias = "__sq" + index;
        var derivedPlan = buildCompiledTablePlan(info.query, info.validation);

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

        var cj = new CompiledTableJoin();
        cj.kind = info.negated ? JoinKind.ANTI : JoinKind.SEMI;
        cj.alias = alias;
        cj.sourceName = "(derived)";
        cj.schema = derivedPlan.outputSchema;
        cj.isTable = false;
        cj.leftKey = leftKey;
        cj.rightKey = rightKey;
        cj.residual = null;
        cj.leftKeys = List.of(leftKey);
        cj.rightKeys = List.of(rightKey);
        cj.derivedPlan = derivedPlan;
        return cj;
    }

    private static CompiledTableJoin buildScalarJoin(ScalarSubqueryInfo info, Map<Expr, Binding> bindings, int index) {
        String alias = "__sq" + index;
        var derivedPlan = buildCompiledTablePlan(info.residualQuery, info.validation);

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

        var cj = new CompiledTableJoin();
        cj.kind = JoinKind.SCALAR;
        cj.alias = alias;
        cj.sourceName = "(derived)";
        cj.schema = derivedPlan.outputSchema;
        cj.isTable = false;
        cj.leftKey = leftKey;
        cj.rightKey = rightKey;
        cj.residual = residual;
        cj.leftKeys = List.of(leftKey);
        cj.rightKeys = List.of(rightKey);
        cj.derivedPlan = derivedPlan;
        return cj;
    }

    private static Expr rewriteScalarSubqueries(Expr e, ValidationResult v, Map<Expr, Binding> bindings, List<CompiledTableJoin> joins) {
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
            Planner.copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof BinaryExpr b) {
            var left = rewriteScalarSubqueries(b.left, v, bindings, joins);
            var right = rewriteScalarSubqueries(b.right, v, bindings, joins);
            if (left == b.left && right == b.right) return e;
            var rebuilt = new BinaryExpr(b.op, left, right, b.line, b.column);
            Planner.copyExprKind(v, e, rebuilt);
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
            Planner.copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof AggregateCallExpr agg) {
            if (agg.arg == null) return e;
            var arg = rewriteScalarSubqueries(agg.arg, v, bindings, joins);
            if (arg == agg.arg) return e;
            var rebuilt = new AggregateCallExpr(agg.name, arg, agg.isStar, agg.line, agg.column);
            Planner.copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        if (e instanceof JsonAccessExpr j) {
            var left = rewriteScalarSubqueries(j.left, v, bindings, joins);
            if (left == j.left) return e;
            var rebuilt = new JsonAccessExpr(left, j.returnText, j.key, j.line, j.column);
            Planner.copyExprKind(v, e, rebuilt);
            return rebuilt;
        }
        return e;
    }
}
