package io.streamsforge.engine.dd;

import io.streamsforge.engine.planning.CompiledTablePlan;

/** A table query that passed the engine's validation and this compiler's subset check. Instantiate as often as
 *  needed; every {@link DataflowTableQuery} owns an independent dataflow graph. */
public final class DataflowTablePlan {
    private final CompiledTablePlan compiled;

    private final DataflowTableCompiler.Options options;

    DataflowTablePlan(CompiledTablePlan compiled, DataflowTableCompiler.Options options) { this.compiled = compiled; this.options = options; }

    public CompiledTablePlan compiled() { return compiled; }

    public DataflowTableQuery newQuery() {
        var g = new GraphBuilder(options);
        var out = g.build(compiled);
        return new DataflowTableQuery(g, out.capture());
    }
}
