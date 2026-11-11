package io.streamsforge.engine.dd;

import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Compiles table-mode SQL onto the differential dataflow library. Tokenizing, parsing, validation and planning are the
 * engine's own ({@link SqlCompiler#compileTable}); this class only decides whether the planned query is inside the
 * supported subset and, if so, hands back a {@link DataflowTablePlan}.
 *
 * <p>Supported: WHERE / projection, INNER / CROSS / LEFT / RIGHT / FULL equi-joins (any number of inputs),
 * {@code [NOT] IN (subquery)} and {@code [NOT] EXISTS} (semi / anti join), {@code JOIN UNNEST}, GROUP BY with
 * COUNT / COUNT(DISTINCT) / SUM / AVG / MIN / MAX (and global aggregates), derived tables and CTEs, UNION ALL and
 * UNION (distinct). Refused as {@link Status#UNSUPPORTED}: LATEST BY, scalar subqueries, the statistical /
 * percentile / user aggregates.
 */
public final class DataflowTableCompiler {
    private DataflowTableCompiler() {}

    public enum Status { OK, INVALID, UNSUPPORTED }

    /** Result of {@link #compile}. {@code plan} is non-null iff {@code status == OK}. */
    public record Outcome(Status status, String message, List<SqlDiagnostic> diagnostics, DataflowTablePlan plan) {
        public boolean ok() { return status == Status.OK; }
    }

    /**
     * Dialect knobs. {@code sqlStandardEmptySum}: SUM over a group whose argument is NULL in every row is NULL in ANSI
     * SQL (and AVG/MIN/MAX of the same group are NULL in this engine), but the engine's SUM returns 0 there. Default
     * {@code false} = match the engine (so the two can be compared row for row); see README "Discrepancies".
     */
    public record Options(boolean sqlStandardEmptySum) {
        public static final Options ENGINE_COMPATIBLE = new Options(false);
    }

    public static Outcome compile(String sql, Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        return compile(sql, streamSchemas, tableSchemas, Options.ENGINE_COMPATIBLE);
    }

    public static Outcome compile(String sql, Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas, Options options) {
        var r = SqlCompiler.compileTable(sql, streamSchemas, tableSchemas == null ? Map.of() : tableSchemas);
        if (!r.ok() || r.plan() == null) return new Outcome(Status.INVALID, "engine rejected the query", r.diagnostics(), null);
        var compiled = r.plan().compiled();
        var problems = new ArrayList<String>();
        check(compiled, problems);
        if (!problems.isEmpty()) {
            return new Outcome(Status.UNSUPPORTED, "unsupported by the dataflow compiler: " + String.join("; ", problems), r.diagnostics(), null);
        }
        return new Outcome(Status.OK, "ok", r.diagnostics(), new DataflowTablePlan(compiled, options));
    }

    private static void check(CompiledTablePlan p, List<String> problems) {
        if (p.unionBranches != null) {
            for (var b : p.unionBranches) check(b, problems);
            return;
        }
        if (p.latestBy != null) problems.add("LATEST BY");
        for (var a : p.aggregateNodes) {
            if (!Aggs.supported(a)) problems.add("aggregate " + a.name + (a.isDistinct ? "(DISTINCT ...)" : ""));
        }
        for (var s : p.sources) if (s.derivedPlan != null) check(s.derivedPlan, problems);
        for (var j : p.joins) {
            if (j.kind == JoinKind.SCALAR) problems.add("scalar subquery");
            if (j.derivedPlan != null) check(j.derivedPlan, problems);
        }
    }
}
