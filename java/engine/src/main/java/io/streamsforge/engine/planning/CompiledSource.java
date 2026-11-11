package io.streamsforge.engine.planning;

import io.streamsforge.engine.SourceSchema;

/** One resolved FROM source of a compiled pipeline plan. {@code derivedPlan} (plan 004 N1) is set when this
 *  FROM source is a derived table/CTE — the nested, fully compiled child plan it wraps. */
public final class CompiledSource {
    public final String alias;
    public final String sourceName;
    public final SourceSchema schema;
    public final CompiledPlan derivedPlan;

    public CompiledSource(String alias, String sourceName, SourceSchema schema, CompiledPlan derivedPlan) {
        this.alias = alias; this.sourceName = sourceName; this.schema = schema; this.derivedPlan = derivedPlan;
    }
}
