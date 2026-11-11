package io.streamsforge.engine;

import io.streamsforge.engine.planning.CompiledPlan;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ops.IPipelineJoinStage;
import io.streamsforge.engine.runtime.ops.IPipelineOpChain;
import io.streamsforge.engine.runtime.ops.IPipelineSnapshotJoinStage;
import io.streamsforge.engine.runtime.ops.PipelineFilterProjectOp;
import io.streamsforge.engine.runtime.ops.PipelineJoinOp;
import io.streamsforge.engine.runtime.ops.PipelineSubqueryOp;
import io.streamsforge.engine.runtime.ops.PipelineUnnestOp;
import io.streamsforge.engine.runtime.ops.PipelineWindowOp;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-pipeline runtime. Single-threaded (called from one grain). Feed events via {@link #onEvent}; advance the
 * watermark periodically via {@link #advanceWatermark} (both may emit rows: matches, EMIT CHANGES updates, or
 * closed windows).
 *
 * <p>Façade over the pipeline-mode operator chain: a {@link PipelineJoinOp} per JOIN (left-to-right), a
 * {@link PipelineFilterProjectOp} (WHERE, +terminal projection when unwindowed) and — when the plan windows — a
 * {@link PipelineWindowOp}. Implements {@link IPipelineOpChain} so a whole compiled pipeline's executor can be
 * embedded as a node feeding another chain (plan 004 N1's derived-table seam).
 */
public final class PipelineExecutor implements IPipelineOpChain {
    private static final long ALLOWED_LATENESS_MS = 1000;

    /** One role a real leaf source name plays: feeds a plain FROM/JOIN alias directly, or first passes through a
     *  derived table/CTE's own nested executor whose emissions THEN become this alias's input events. */
    private static final class RoleEntry {
        boolean isFrom;
        int stageIndex; // -1 = FROM
        String alias;
        PipelineExecutor derived;
        /** true when stageIndex targets a Semi/Anti/Scalar join stage — nested emissions are delivered as a
         *  WHOLE BATCH to that stage's onRightBatch. */
        boolean isSnapshotJoin;
    }

    private final PipelinePlan plan;
    private long watermark;
    private long lateEvents;

    private boolean initialized;
    private final List<IPipelineJoinStage> joins = new ArrayList<>();
    private PipelineFilterProjectOp filterProject;
    private PipelineWindowOp window;

    // real leaf source name -> every role that name plays in the plan
    private final Map<String, List<RoleEntry>> roles = new HashMap<>();

    // Distinct nested executors for this plan's derived FROM/JOIN sources, in registration order.
    private final List<RoleEntry> derivedNodes = new ArrayList<>();

    // Plan 008 W3: set only when the plan's unionBranches is non-null — a set-operation root.
    private List<PipelineExecutor> unionBranchExecutors;
    private Map<String, List<PipelineExecutor>> unionRoles;

    PipelineExecutor(PipelinePlan plan) { this.plan = plan; }

    public long getWatermark() { return watermark; }

    /** Late events dropped so far (event ts below the watermark). */
    public long getLateEvents() { return lateEvents; }

    /** Advance watermark to max(current, nowMs - allowed lateness); closes due windows, evicts join buffers. */
    @Override public List<EventRecord> advanceWatermark(long nowMs) { return advanceWatermarkCore(nowMs); }

    @Override public List<EventRecord> onEvent(String sourceName, EventRecord evt) { return onEventCore(sourceName, evt); }

    private void ensureInit() {
        if (initialized) return;
        initialized = true;

        CompiledPlan compiled = plan.compiled();

        if (compiled.unionBranches != null) {
            ensureInitUnion(compiled);
            return;
        }

        var accumulated = new ArrayList<WorkingRow.Side>();
        accumulated.add(new WorkingRow.Side(compiled.sources.get(0).alias, compiled.sources.get(0).schema));

        for (int i = 0; i < compiled.joins.size(); i++) {
            var j = compiled.joins.get(i);
            if (j.kind == JoinKind.SEMI || j.kind == JoinKind.ANTI || j.kind == JoinKind.SCALAR) {
                joins.add(new PipelineSubqueryOp(j.kind, j.leftKey, j.rightKey, j.residual, compiled.bindings, new WorkingRow.Side(j.alias, j.schema)));
            } else if (j.kind == JoinKind.UNNEST) {
                joins.add(new PipelineUnnestOp(j.unnestExpr, j.alias, compiled.bindings));
            } else {
                joins.add(new PipelineJoinOp(j.kind, j.within, j.leftKey, j.rightKey, j.residual, compiled.bindings,
                    new ArrayList<>(accumulated), new WorkingRow.Side(j.alias, j.schema)));
            }
            accumulated.add(new WorkingRow.Side(j.alias, j.schema));
        }

        filterProject = new PipelineFilterProjectOp(compiled);
        if (compiled.window != null) window = new PipelineWindowOp(compiled);

        addRole(compiled.sources.get(0).sourceName, true, -1, compiled.sources.get(0).alias, compiled.sources.get(0).derivedPlan, false);
        for (int i = 0; i < compiled.joins.size(); i++) {
            var j = compiled.joins.get(i);
            if (j.kind == JoinKind.UNNEST) continue; // no external driving source
            boolean isSnapshotJoin = j.kind == JoinKind.SEMI || j.kind == JoinKind.ANTI || j.kind == JoinKind.SCALAR;
            addRole(j.sourceName, false, i, j.alias, j.derivedPlan, isSnapshotJoin);
        }
    }

    private void addRole(String sourceName, boolean isFrom, int stageIndex, String alias, CompiledPlan derivedPlan, boolean isSnapshotJoin) {
        var entry = new RoleEntry();
        entry.isFrom = isFrom; entry.stageIndex = stageIndex; entry.alias = alias; entry.isSnapshotJoin = isSnapshotJoin;
        if (derivedPlan == null) {
            entry.derived = null;
            addRoleUnder(sourceName, entry);
            return;
        }
        // Derived table/CTE (also always true of a synthesized subquery stage): one nested PipelineExecutor per
        // derived source, registered under every real leaf source name it transitively depends on.
        entry.derived = new PipelineExecutor(new PipelinePlan(derivedPlan));
        derivedNodes.add(entry);
        for (String leaf : derivedPlan.sourceNames) addRoleUnder(leaf, entry);
    }

    private void addRoleUnder(String sourceName, RoleEntry entry) {
        roles.computeIfAbsent(sourceName, k -> new ArrayList<>()).add(entry);
    }

    /** A UNION ALL executor is structurally "N nested executors, no join stages, no filter/project, concatenate
     *  emissions". One nested executor per branch, registered under every leaf source name that branch names. */
    private void ensureInitUnion(CompiledPlan compiled) {
        unionBranchExecutors = new ArrayList<>();
        unionRoles = new HashMap<>();
        for (var branch : compiled.unionBranches) {
            var exec = new PipelineExecutor(new PipelinePlan(branch));
            unionBranchExecutors.add(exec);
            for (String leaf : branch.sourceNames) unionRoles.computeIfAbsent(leaf, k -> new ArrayList<>()).add(exec);
        }
    }

    private List<EventRecord> onEventCore(String sourceName, EventRecord evt) {
        ensureInit();

        if (unionRoles != null) return onEventUnion(sourceName, evt);

        if (evt.timestamp() < watermark) {
            lateEvents++;
            return new ArrayList<>();
        }

        var results = new ArrayList<EventRecord>();

        var roleList = roles.get(sourceName);
        if (roleList != null) {
            for (var role : roleList) {
                if (role.derived == null) {
                    processIncomingRow(role, WorkingRow.fromEvent(role.alias, evt), results);
                    continue;
                }

                // The derived source's own nested executor sees the raw event first; only what IT emits becomes
                // this alias's input event one level up.
                var emissions = role.derived.onEvent(sourceName, evt);

                if (role.isSnapshotJoin) {
                    var batch = new ArrayList<WorkingRow>();
                    for (var e : emissions) batch.add(WorkingRow.fromEvent(role.alias, e));
                    ((IPipelineSnapshotJoinStage) joins.get(role.stageIndex)).onRightBatch(batch);
                    for (var emission : emissions) bumpWatermark(emission.timestamp());
                    continue;
                }

                for (var emission : emissions) {
                    processIncomingRow(role, WorkingRow.fromEvent(role.alias, emission), results);
                    bumpWatermark(emission.timestamp());
                }
            }
        }

        bumpWatermark(evt.timestamp());
        return results;
    }

    private void processIncomingRow(RoleEntry role, WorkingRow initial, List<EventRecord> results) {
        List<WorkingRow> combinedRows;
        if (role.isFrom) {
            var seed = new ArrayList<WorkingRow>();
            seed.add(initial);
            combinedRows = propagateForward(0, seed);
        } else {
            combinedRows = propagateForward(role.stageIndex + 1, joins.get(role.stageIndex).onRight(initial));
        }
        processRows(combinedRows, results);
    }

    private void bumpWatermark(long eventTs) {
        long candidate = eventTs - ALLOWED_LATENESS_MS;
        if (candidate > watermark) watermark = candidate;
    }

    /** Union-root onEvent — fan the incoming event out to every branch executor that subscribes to the source and
     *  concatenate their emissions verbatim. */
    private List<EventRecord> onEventUnion(String sourceName, EventRecord evt) {
        if (evt.timestamp() < watermark) {
            lateEvents++;
            return new ArrayList<>();
        }

        var results = new ArrayList<EventRecord>();
        var branches = unionRoles.get(sourceName);
        if (branches != null) {
            for (var branchExecutor : branches) results.addAll(branchExecutor.onEvent(sourceName, evt));
        }
        bumpWatermark(evt.timestamp());
        return results;
    }

    private List<EventRecord> advanceWatermarkUnion(long nowMs) {
        var results = new ArrayList<EventRecord>();
        for (var branchExecutor : unionBranchExecutors) results.addAll(branchExecutor.advanceWatermark(nowMs));
        long candidate = nowMs - ALLOWED_LATENESS_MS;
        if (candidate > watermark) watermark = candidate;
        return results;
    }

    private List<EventRecord> advanceWatermarkCore(long nowMs) {
        ensureInit();

        if (unionRoles != null) return advanceWatermarkUnion(nowMs);

        var results = new ArrayList<EventRecord>();

        // Advance every derived source's nested executor first, routing its watermark-triggered emissions through
        // the outer chain exactly like onEventCore does for a live event.
        for (var role : derivedNodes) {
            var emissions = role.derived.advanceWatermark(nowMs);

            if (role.isSnapshotJoin) {
                var batch = new ArrayList<WorkingRow>();
                for (var e : emissions) batch.add(WorkingRow.fromEvent(role.alias, e));
                ((IPipelineSnapshotJoinStage) joins.get(role.stageIndex)).onRightBatch(batch);
                for (var emission : emissions) bumpWatermark(emission.timestamp());
                continue;
            }

            for (var emission : emissions) {
                processIncomingRow(role, WorkingRow.fromEvent(role.alias, emission), results);
                bumpWatermark(emission.timestamp());
            }
        }

        long candidate = nowMs - ALLOWED_LATENESS_MS;
        long newWatermark = Math.max(watermark, candidate);
        watermark = newWatermark;

        for (int i = 0; i < joins.size(); i++) {
            var evicted = joins.get(i).evict(newWatermark);
            var propagated = propagateForward(i + 1, evicted);
            processRows(propagated, results);
        }

        if (window != null) results.addAll(window.evict(newWatermark));

        return results;
    }

    private List<WorkingRow> propagateForward(int fromStageIndexInclusive, List<WorkingRow> rows) {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < joins.size(); s++) {
            var next = new ArrayList<WorkingRow>();
            for (var r : current) next.addAll(joins.get(s).onLeft(r));
            current = next;
        }
        return current;
    }

    private void processRows(List<WorkingRow> rows, List<EventRecord> results) {
        if (window != null) {
            var filtered = filterProject.onBatch(rows);
            for (var row : filtered) results.addAll(window.onRow(row));
        } else {
            results.addAll(filterProject.onBatchTerminal(rows));
        }
    }
}
