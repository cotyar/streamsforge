package io.streamsforge.engine;

import io.streamsforge.engine.planning.CompiledPlan;

/** Compiled, immutable query plan. Thread-safe to share; executors hold the mutable state. */
public final class PipelinePlan {
    private final CompiledPlan compiled;

    /** Engine-internal (C# {@code internal}): built by the Planner. */
    public PipelinePlan(CompiledPlan compiled) { this.compiled = compiled; }

    /** Engine-internal accessor for the compiled plan (C# {@code internal CompiledPlan Compiled}). */
    public CompiledPlan compiled() { return compiled; }

    public PipelineExecutor createExecutor() { return new PipelineExecutor(this); }
}
