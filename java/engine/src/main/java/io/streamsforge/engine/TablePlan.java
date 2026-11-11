package io.streamsforge.engine;

import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.TableRetentionSupport;

/** Compiled, immutable table plan. Thread-safe to share; executors hold the mutable state. */
public final class TablePlan {
    private final CompiledTablePlan compiled;

    /** Engine-internal (C# {@code internal}): built by the TablePlanner. */
    public TablePlan(CompiledTablePlan compiled) { this.compiled = compiled; }

    /** Engine-internal accessor (C# {@code internal CompiledTablePlan Compiled}). */
    public CompiledTablePlan compiled() { return compiled; }

    public TableExecutor createExecutor() { return new TableExecutor(this); }

    /**
     * Plan 011 C2 — whether a {@link TableRetentionPolicy} can be honestly applied to a table with THIS plan shape.
     * True for a plan that reads one leaf source with no joins, no set operation, no derived/CTE source and no GROUP
     * BY/aggregate, i.e. exactly the shapes whose entire per-row state is reachable from the terminal stage (LATEST
     * BY's per-key map, or the consolidated ledger for a plain projection).
     *
     * <p>WHY THE OTHERS ARE EXCLUDED RATHER THAN "SUPPORTED, PARTIALLY". A join's ZSet indexes hold the INPUT rows
     * of both sides; evicting an output row leaves both indexes untouched and growing, so a policy there would bound
     * the number the console shows while bounding nothing that occupies memory. A GROUP BY is excluded for a stronger
     * reason: evicting a group would drop its running aggregate state, so the next contributing delta would restart
     * that group's SUM/COUNT from zero and silently emit a WRONG value.
     */
    public boolean supportsRetention() { return TableRetentionSupport.isSupported(compiled); }

    /**
     * Wishlist #18 — true when this plan is an UNKEYED GLOBAL AGGREGATE: one or more aggregate functions with no
     * GROUP BY (e.g. {@code SELECT COUNT(*) FROM x}), which the executor collapses to exactly one output row. A plan
     * WITH a GROUP BY also has aggregates but is keyed, so both conditions are checked.
     */
    public boolean isGlobalAggregate() { return compiled.hasAggregates && compiled.groupBy == null; }

    /** Plan 003 M2: builds the compiled plan's partitioned dataflow graph. */
    public TableDataflowPlan createDataflow(int partitionCount) { return new TableDataflowPlan(compiled, partitionCount); }
}
