package io.streamsforge.engine.runtime;

import io.streamsforge.engine.planning.CompiledTablePlan;

/** Plan 011 C2 — the single place that decides whether a compiled plan's shape can honestly carry a retention
 *  policy. Backs the public {@code TablePlan.supportsRetention()}. */
public final class TableRetentionSupport {
    private TableRetentionSupport() {}

    public static boolean isSupported(CompiledTablePlan plan) {
        return plan.unionBranches == null
            && plan.joins.isEmpty()
            && plan.groupBy == null
            && !plan.hasAggregates
            && !plan.sources.isEmpty()
            && plan.sources.get(0).derivedPlan == null;
    }
}
