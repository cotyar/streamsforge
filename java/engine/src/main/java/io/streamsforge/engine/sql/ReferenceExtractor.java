package io.streamsforge.engine.sql;

import io.streamsforge.engine.DiagnosticSeverity;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.sql.Ast.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Plan 016 wave 3-A — the implementation behind {@code SqlCompiler.extractReferences}: parse the statement and
 * collect the name of every {@link NamedSource} reachable from it, i.e. every relation the SQL READS FROM.
 * A NAME SCAN, not a resolution: nothing here consults a schema dictionary. CTEs get no special handling —
 * the parser substitutes every WITH-list CTE reference in place at PARSE time. The walk's default is "no
 * children worth walking", failing by under-reporting rather than by throwing.
 */
public final class ReferenceExtractor {
    private ReferenceExtractor() {}

    /** Names in first-appearance order, ordinal-distinct; empty on any tokenize/parse failure and on
     *  null/blank input. */
    public static List<String> extract(String sql) {
        if (sql == null || sql.isBlank()) {
            return new ArrayList<>();
        }

        var names = new ArrayList<String>();
        Set<String> seen = new HashSet<>();

        try {
            var tok = new Tokenizer(sql).tokenize();
            if (hasError(tok.diagnostics())) {
                return new ArrayList<>();
            }

            var parsed = Parser.parseStatement(tok.tokens());
            if (hasError(parsed.diagnostics())) {
                return new ArrayList<>();
            }

            if (parsed.setOp() != null) {
                for (SelectQuery branch : parsed.setOp().branches) walkQuery(branch, names, seen);
            } else if (parsed.query() != null) {
                walkQuery(parsed.query(), names, seen);
            }
        } catch (RuntimeException ex) {
            // A tokenizer/parser bug on hostile input must not become the caller's exception.
            return new ArrayList<>();
        }

        return names;
    }

    private static boolean hasError(List<SqlDiagnostic> diags) {
        for (SqlDiagnostic d : diags) if (d.severity() == DiagnosticSeverity.ERROR) return true;
        return false;
    }

    /** Clause order: FROM source, then each JOIN (its source, then its ON), then WHERE, then the SELECT list,
     *  then GROUP BY / LATEST BY. */
    private static void walkQuery(SelectQuery query, List<String> names, Set<String> seen) {
        walkFromItem(query.from.source, names, seen);
        for (JoinClause join : query.from.joins) {
            walkFromItem(join.source, names, seen);
            walkExpr(join.on, names, seen);
        }

        walkExpr(query.where, names, seen);
        for (SelectItem item : query.select.items) walkExpr(item.expression, names, seen);
        if (query.groupBy != null) for (Expr g : query.groupBy) walkExpr(g, names, seen);
        if (query.latestBy != null) for (Expr l : query.latestBy) walkExpr(l, names, seen);
    }

    private static void walkFromItem(FromItem item, List<String> names, Set<String> seen) {
        if (item instanceof NamedSource ns) {
            if (seen.add(ns.name)) names.add(ns.name);
        } else if (item instanceof DerivedSource ds) {
            walkQuery(ds.query, names, seen);
        } else if (item instanceof DerivedSetOperationSource dso) {
            for (SelectQuery branch : dso.setOp.branches) walkQuery(branch, names, seen);
        } else if (item instanceof UnnestSource us) {
            walkExpr(us.expr, names, seen);
        }
    }

    private static void walkExpr(Expr e, List<String> names, Set<String> seen) {
        if (e == null) return;
        if (e instanceof InSubqueryExpr ins) {
            walkExpr(ins.left, names, seen);
            walkQuery(ins.subquery, names, seen);
        } else if (e instanceof ExistsExpr ex) {
            walkQuery(ex.subquery, names, seen);
        } else if (e instanceof ScalarSubqueryExpr sse) {
            walkQuery(sse.query, names, seen);
        } else if (e instanceof UnaryExpr u) {
            walkExpr(u.operand, names, seen);
        } else if (e instanceof BinaryExpr b) {
            walkExpr(b.left, names, seen);
            walkExpr(b.right, names, seen);
        } else if (e instanceof JsonAccessExpr j) {
            walkExpr(j.left, names, seen);
            walkExpr(j.key, names, seen);
        } else if (e instanceof FunctionCallExpr f) {
            for (Expr a : f.args) walkExpr(a, names, seen);
        } else if (e instanceof AggregateCallExpr agg) {
            walkExpr(agg.arg, names, seen);
            walkExpr(agg.parameter, names, seen);
        }
    }
}
