package io.streamsforge.engine;

import io.streamsforge.engine.planning.Planner;
import io.streamsforge.engine.planning.TablePlanner;
import io.streamsforge.engine.sql.ReferenceExtractor;
import java.util.List;
import java.util.Map;

/** Entry points of the streaming-SQL compiler. */
public final class SqlCompiler {
    private SqlCompiler() {}

    /** Tokenize → parse → validate → plan. Never throws on bad SQL; returns diagnostics. */
    public static CompileResult compile(String sql, Map<String, SourceSchema> schemas) {
        return Planner.compile(sql, schemas);
    }

    /** Tokenize → parse → validate (table-mode grammar) → plan a materialized TABLE's SELECT. streamSchemas and
     *  tableSchemas together form the SQL namespace a FROM/JOIN identifier resolves against; a name present in both
     *  is an "ambiguous name" diagnostic. Never throws. */
    public static TableCompileResult compileTable(String sql, Map<String, SourceSchema> streamSchemas, Map<String, SourceSchema> tableSchemas) {
        return TablePlanner.compile(sql, streamSchemas, tableSchemas);
    }

    /**
     * Plan 016 wave 3-A — ADDITIVE. Every relation name {@code sql} READS FROM, as authored: the FROM source, every
     * JOIN source, every derived table / subquery source, every subquery inside WHERE (IN / NOT IN / EXISTS / NOT
     * EXISTS / scalar), and every branch of a UNION [ALL] chain, at any nesting depth. Tokenize + parse only — no
     * schema resolution, which is the point: it answers "what does this statement mention?" for entities that DO NOT
     * EXIST YET (a config document being import-planned).
     *
     * <p><b>CTEs are not references</b> (the parser substitutes CTE bodies in place); a declared-but-unused CTE
     * contributes nothing. <b>Ordering and duplicates:</b> distinct by ORDINAL comparison, in first-appearance order
     * (FROM, then each JOIN's source and ON, then WHERE, then SELECT, then GROUP BY / LATEST BY, recursing into nested
     * queries as met). <b>Never throws, for any input</b> — malformed SQL, empty, null, whitespace all return an EMPTY
     * list, deliberately indistinguishable from "parsed fine, reads nothing". Pre-parse sugar (INSERT INTO sink) is not
     * stripped here.
     *
     * @param sql statement text; null, empty and whitespace are accepted and return an empty list
     * @return distinct relation names in first-appearance order
     */
    public static List<String> extractReferences(String sql) { return ReferenceExtractor.extract(sql); }
}
