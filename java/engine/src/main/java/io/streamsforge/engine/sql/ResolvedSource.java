package io.streamsforge.engine.sql;

import io.streamsforge.engine.SourceSchema;

/**
 * One resolved FROM/JOIN source — a plain named stream/table (derived/unionDerived both null), a derived
 * table/CTE (derived set), or a derived-table-position set operation (unionDerived set). Mutually exclusive.
 * Constructor args in C# declaration order: alias, sourceName, schema, isTable, derived, unionDerived, isUnnest.
 * (A convenience constructor without isUnnest defaults it to false.)
 */
public final class ResolvedSource {
    public final String alias;
    public final String sourceName;
    public final SourceSchema schema;
    public final boolean isTable;
    public final DerivedInfo derived;
    public final UnionDerivedInfo unionDerived;
    /** Plan 002 L2: true when this source is an UNNEST alias (synthetic one-field element schema). */
    public final boolean isUnnest;

    public ResolvedSource(String alias, String sourceName, SourceSchema schema, boolean isTable,
                          DerivedInfo derived, UnionDerivedInfo unionDerived, boolean isUnnest) {
        this.alias = alias;
        this.sourceName = sourceName;
        this.schema = schema;
        this.isTable = isTable;
        this.derived = derived;
        this.unionDerived = unionDerived;
        this.isUnnest = isUnnest;
    }

    public ResolvedSource(String alias, String sourceName, SourceSchema schema, boolean isTable,
                          DerivedInfo derived, UnionDerivedInfo unionDerived) {
        this(alias, sourceName, schema, isTable, derived, unionDerived, false);
    }
}
