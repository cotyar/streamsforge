package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;

/** One resolved FROM source of a compiled table plan. */
public final class CompiledTableSource {
    public final String alias;
    public final String sourceName;
    public final SourceSchema schema;
    public final boolean isTable;
    /** Plan 004 N1: set when this FROM source is a derived table/CTE — the nested child table plan. */
    public final CompiledTablePlan derivedPlan;

    public CompiledTableSource(String alias, String sourceName, SourceSchema schema, boolean isTable, CompiledTablePlan derivedPlan) {
        this.alias = alias; this.sourceName = sourceName; this.schema = schema; this.isTable = isTable; this.derivedPlan = derivedPlan;
    }
}
