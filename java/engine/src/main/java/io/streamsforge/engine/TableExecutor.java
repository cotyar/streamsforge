package io.streamsforge.engine;

import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ConsolidationLedger;
import io.streamsforge.engine.runtime.IRetentionScope;
import io.streamsforge.engine.runtime.JsonText;
import io.streamsforge.engine.runtime.LedgerRetentionScope;
import io.streamsforge.engine.runtime.TableRetentionSupport;
import io.streamsforge.engine.runtime.ops.ITableJoinStage;
import io.streamsforge.engine.runtime.ops.TableDistinctOp;
import io.streamsforge.engine.runtime.ops.TableFilterProjectOp;
import io.streamsforge.engine.runtime.ops.TableIngestOp;
import io.streamsforge.engine.runtime.ops.TableJoinOp;
import io.streamsforge.engine.runtime.ops.TableLatestByOp;
import io.streamsforge.engine.runtime.ops.TableOuterJoinOp;
import io.streamsforge.engine.runtime.ops.TableReduceOp;
import io.streamsforge.engine.runtime.ops.TableRowDelta;
import io.streamsforge.engine.runtime.ops.TableSemiAntiOp;
import io.streamsforge.engine.runtime.ops.TableUnnestOp;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-table runtime: Z-set (DBSP-style) incremental view maintenance. Single-threaded (called from one grain); no
 * frontier/timestamp machinery — tables are order-insensitive for the commutative operators this dialect supports.
 * Feed stream events via {@link #onStreamEvent} and upstream table deltas via {@link #onTableDelta} (or
 * {@link #onTableDeltaBatch}); all return the deltas this table itself emits downstream. {@link #snapshot()} exposes
 * the current consolidated output (weight &gt; 0 rows only).
 *
 * <p>Façade over the table-mode operator chain: one {@link TableIngestOp} per input role, a chain of join stages (one
 * per JOIN, left-to-right), a {@link TableFilterProjectOp} (WHERE, +terminal projection when ungrouped) and — when the
 * plan groups/aggregates — a {@link TableReduceOp}, or a {@link TableLatestByOp} for LATEST BY. Every emitted delta
 * also folds into this table's own consolidated output ledger.
 *
 * <p>EPOCH: single-partition, in-process. Each call to onStreamEvent/onTableDelta/onTableDeltaBatch is stamped with
 * its own epoch from a monotonically advancing counter; every op invoked while servicing one call shares that
 * epoch. WISHLIST #15: every element of a batch shares ONE epoch, and {@code consolidateEpochOutput} nets that
 * epoch's raw output by canonical row key before it is returned, so a transient intermediate row (stale aggregate,
 * null-padded join row) never reaches a consumer.
 */
public final class TableExecutor {
    /** One role a real leaf stream/table name plays: feeds a plain FROM/JOIN alias directly, or first passes through
     *  a derived table/CTE's own nested TableExecutor whose emitted TableDeltas THEN become this alias's input deltas. */
    private static final class RoleEntry {
        boolean isFrom;
        int stageIndex; // -1 = FROM
        String alias;
        TableExecutor derived;
    }

    private final TablePlan plan;

    private boolean initialized;
    private final List<ITableJoinStage> joins = new ArrayList<>();
    private TableFilterProjectOp filterProject;
    private TableReduceOp reduce;
    private TableLatestByOp latestBy;
    private final Map<String, TableIngestOp> ingestOps = new HashMap<>();

    // real leaf stream/table name -> every role it plays in the plan
    private final Map<String, List<RoleEntry>> roles = new HashMap<>();

    // Consolidated output Z-set ledger. snapshot() returns ledger.visible() BY REFERENCE.
    private final ConsolidationLedger ledger = new ConsolidationLedger();

    private long epochCounter;

    private long lastEpoch = -1;

    // Plan 011 C2 — row retention; inert until configureRetention installs an ENABLED policy.
    private TableRetentionPolicy retention = TableRetentionPolicy.NONE;
    private IRetentionScope retentionScope;
    private LedgerRetentionScope ledgerScope;

    // Plan 008 W3: set only when the plan's unionBranches is non-null — a set-operation root.
    private List<TableExecutor> unionBranchExecutors;
    private Map<String, List<TableExecutor>> unionRoles;
    private TableDistinctOp distinct;

    TableExecutor(TablePlan plan) { this.plan = plan; }

    public List<TableDelta> onStreamEvent(String source, EventRecord evt) {
        ensureInit();
        return handleIncoming(source, List.of(new TableDelta(evt, 1)));
    }

    public List<TableDelta> onTableDelta(String table, TableDelta delta) {
        ensureInit();
        return handleIncoming(table, List.of(delta));
    }

    /**
     * Wishlist #15 — the batch sibling of {@link #onTableDelta}: every element of {@code deltas} is admitted,
     * processed and emitted under ONE epoch. Use this whenever the deltas are known to have come from a SINGLE
     * upstream epoch/batch so that an upstream change expressed as [retract(old), assert(new)] is applied and
     * republished atomically. The returned batch is already consolidated. {@code onTableDelta(t, d)} is exactly
     * {@code onTableDeltaBatch(t, [d])}.
     */
    public List<TableDelta> onTableDeltaBatch(String table, List<TableDelta> deltas) {
        ensureInit();
        return handleIncoming(table, deltas);
    }

    /** Current consolidated output (weight &gt; 0 rows only), keyed by canonical row key. Live view, not a copy. */
    public Map<String, RowWeight> snapshot() {
        ensureInit();
        return ledger.visible();
    }

    /** The canonical identity {@link #snapshot()} keys rows by (a deterministic serialization of the row's fields). */
    public String canonicalRowKey(EventRecord row) { return JsonText.serializeCanonicalRow(row); }

    /** Static twin of {@link #canonicalRowKey}, for callers that have no live executor. Byte-identical output. */
    public static String canonicalRowKeyOf(Map<String, ?> row) { return JsonText.serializeCanonicalRow(row); }

    /**
     * Wishlist #14 option (a) — REAL backfill on attach. The epoch most recently allocated by onStreamEvent/
     * onTableDelta/onTableDeltaBatch — i.e. the epoch that produced whatever {@link #snapshot()} currently reflects.
     * -1 before this executor has ever admitted anything. A caller reads snapshot() and lastEpoch() together with no
     * await in between (an atomically-consistent pair), so a brand-new downstream table can admit the snapshot and
     * then apply only batches stamped with an epoch STRICTLY GREATER than this — no gap, no double-count.
     */
    public long lastEpoch() { return lastEpoch; }

    /**
     * Plan 011 C2 — the eviction seam. Installing the policy once (rather than driving an "evictNow()") keeps
     * eviction indistinguishable from an ordinary retraction to every consumer: the evictions are appended to
     * whatever onStreamEvent/onTableDelta return, after that call's own deltas. Idempotent and re-callable. Throws
     * {@link IllegalStateException} (C# InvalidOperationException) for an ENABLED policy on a plan shape where
     * retention cannot reclaim the real state — see {@link TablePlan#supportsRetention()}.
     */
    public void configureRetention(TableRetentionPolicy policy) {
        ensureInit();

        if (!policy.isEnabled()) {
            retention = TableRetentionPolicy.NONE;
            return;
        }

        if (!TableRetentionSupport.isSupported(plan.compiled())) {
            throw new IllegalStateException(
                "Row retention is not supported for this table's plan shape (joins, set operations, derived sources and GROUP BY/aggregates are excluded) — see TablePlan.SupportsRetention.");
        }

        retention = policy;

        if (latestBy != null) {
            latestBy.enableRetention();
            retentionScope = latestBy;
        } else {
            if (ledgerScope == null) ledgerScope = new LedgerRetentionScope(ledger);
            // Seed the ordering index from whatever is already consolidated.
            for (String key : new ArrayList<>(ledger.visible().keySet())) ledgerScope.observe(key);
            retentionScope = ledgerScope;
        }
    }

    /** Test-only introspection: the debt side-table's size (see ConsolidationLedger). */
    public int debtCount() { return ledger.debtCount(); }

    /** Test-only introspection: entries held by the retention scope; -1 when no policy is configured. */
    public int retainedStateCount() { return retentionScope != null ? retentionScope.retainedCount() : -1; }

    /** Introspection: retractions this table's GROUP BY received for a group it had never asserted; -1 when the plan
     *  has no GROUP BY. */
    public long unmatchedRetractions() { return reduce != null ? reduce.unmatchedRetractions() : -1; }

    // ------------------------------------------------------------------

    private void ensureInit() {
        if (initialized) return;
        initialized = true;

        CompiledTablePlan compiled = plan.compiled();

        if (compiled.unionBranches != null) {
            ensureInitUnion(compiled);
            return;
        }

        var accumulated = new ArrayList<WorkingRow.Side>();
        accumulated.add(new WorkingRow.Side(compiled.sources.get(0).alias, compiled.sources.get(0).schema));

        for (int i = 0; i < compiled.joins.size(); i++) {
            var j = compiled.joins.get(i);
            ITableJoinStage stage;
            switch (j.kind) {
                case LEFT: case RIGHT: case FULL:
                    stage = new TableOuterJoinOp(j.kind, j.leftKeys, j.rightKeys, j.residual, compiled.bindings,
                        new ArrayList<>(accumulated), new WorkingRow.Side(j.alias, j.schema));
                    break;
                case SEMI: case ANTI:
                    stage = new TableSemiAntiOp(j.kind, j.leftKey, j.rightKey, compiled.bindings);
                    break;
                case UNNEST:
                    stage = new TableUnnestOp(j.unnestExpr, j.alias, compiled.bindings);
                    break;
                default:
                    stage = new TableJoinOp(j.leftKey, j.rightKey, j.residual, compiled.bindings);
                    break;
            }
            joins.add(stage);
            accumulated.add(new WorkingRow.Side(j.alias, j.schema));
        }

        filterProject = new TableFilterProjectOp(compiled);

        if (compiled.groupBy != null || compiled.hasAggregates) {
            reduce = new TableReduceOp(compiled);
        } else if (compiled.latestBy != null) {
            latestBy = new TableLatestByOp(compiled, compiled.latestBy);
        }

        addRole(compiled.sources.get(0).sourceName, true, -1, compiled.sources.get(0).alias, compiled.sources.get(0).derivedPlan);
        for (int i = 0; i < compiled.joins.size(); i++) {
            if (compiled.joins.get(i).kind == JoinKind.UNNEST) continue; // no external driving source
            addRole(compiled.joins.get(i).sourceName, false, i, compiled.joins.get(i).alias, compiled.joins.get(i).derivedPlan);
        }
    }

    private void addRole(String name, boolean isFrom, int stageIndex, String alias, CompiledTablePlan derivedPlan) {
        var entry = new RoleEntry();
        entry.isFrom = isFrom; entry.stageIndex = stageIndex; entry.alias = alias;
        if (derivedPlan == null) {
            entry.derived = null;
            addRoleUnder(name, entry);
        } else {
            // Derived table/CTE: one nested TableExecutor per derived source, registered under every real leaf
            // stream AND table input it transitively depends on.
            entry.derived = new TableExecutor(new TablePlan(derivedPlan));
            for (String leaf : derivedPlan.streamInputs) addRoleUnder(leaf, entry);
            for (String leaf : derivedPlan.tableInputs) addRoleUnder(leaf, entry);
        }

        if (!ingestOps.containsKey(alias)) ingestOps.put(alias, new TableIngestOp(alias));
    }

    private void addRoleUnder(String name, RoleEntry entry) {
        roles.computeIfAbsent(name, k -> new ArrayList<>()).add(entry);
    }

    /** Plan 008 W3: table-mode union root. One nested TableExecutor per branch, registered under every real leaf
     *  stream AND table input it depends on. UNION (distinct) additionally wires a TableDistinctOp downstream of the
     *  branch concatenation; UNION ALL needs nothing further (applyConsolidation sums weights per canonical key). */
    private void ensureInitUnion(CompiledTablePlan compiled) {
        unionBranchExecutors = new ArrayList<>();
        unionRoles = new HashMap<>();
        for (var branch : compiled.unionBranches) {
            var exec = new TableExecutor(new TablePlan(branch));
            unionBranchExecutors.add(exec);
            for (String leaf : branch.streamInputs) unionRoles.computeIfAbsent(leaf, k -> new ArrayList<>()).add(exec);
            for (String leaf : branch.tableInputs) unionRoles.computeIfAbsent(leaf, k -> new ArrayList<>()).add(exec);
        }
        if (!compiled.unionAll) distinct = new TableDistinctOp();
    }

    private List<TableDelta> handleIncomingUnionBatch(String name, List<TableDelta> deltas) {
        var output = new ArrayList<TableDelta>();
        if (deltas.isEmpty()) return output;

        // Wishlist #14 option (a): allocate (and record as lastEpoch) unconditionally.
        var epoch = new Epoch(epochCounter++);
        lastEpoch = epoch.value();

        var branches = unionRoles.get(name);
        if (branches != null) {
            for (var branchExecutor : branches) output.addAll(branchExecutor.onTableDeltaBatchCore(name, deltas));
        }

        List<TableDelta> out = output;
        if (distinct != null) out = distinct.onBatch(epoch, out);

        // Gated on the CALL's own admitted batch size, not on output size (see consolidateEpochOutput).
        if (deltas.size() > 1) out = consolidateEpochOutput(out);

        for (var delta : out) applyConsolidation(delta);
        return out;
    }

    private List<TableDelta> onTableDeltaBatchCore(String table, List<TableDelta> deltas) {
        ensureInit();
        return handleIncoming(table, deltas);
    }

    /** The one admission path every entry point funnels through. The whole of {@code deltas} shares ONE epoch. */
    private List<TableDelta> handleIncoming(String name, List<TableDelta> deltas) {
        if (unionRoles != null) return handleIncomingUnionBatch(name, deltas);

        var output = new ArrayList<TableDelta>();
        if (deltas.isEmpty()) return output;
        var roleList = roles.get(name);
        if (roleList == null) return output;

        var epoch = new Epoch(epochCounter++);
        lastEpoch = epoch.value();

        for (var role : roleList) {
            // A derived role's admission goes through the nested executor's batch entry point unconditionally,
            // reproducing whichever admission the outer caller actually used.
            List<TableDelta> admission = role.derived == null ? deltas : role.derived.onTableDeltaBatchCore(name, deltas);

            if (admission.isEmpty()) continue;

            var admitted = ingestOps.get(role.alias).onBatch(epoch, admission);

            List<TableRowDelta> afterJoins = role.isFrom
                ? propagateForward(0, epoch, admitted)
                : propagateForward(role.stageIndex + 1, epoch, joins.get(role.stageIndex).onRightBatch(epoch, admitted));

            if (reduce != null) {
                var filtered = filterProject.onBatch(epoch, afterJoins);
                output.addAll(reduce.onBatch(epoch, filtered));
            } else if (latestBy != null) {
                var filtered = filterProject.onBatch(epoch, afterJoins);
                output.addAll(latestBy.onBatch(epoch, filtered));
            } else {
                output.addAll(filterProject.onBatchTerminal(epoch, afterJoins));
            }
        }

        // Wishlist #15, second half: nets this epoch's raw output by canonical row key BEFORE it is returned or
        // folded into the ledger. Gated on deltas.size() (this CALL's own admitted batch size): a SINGLE admitted
        // delta can legitimately produce a same-content retract+assert pair (TableReduceOp always emits
        // retract(old)+assert(new)), and that pair is the intended shape for a one-element admission.
        List<TableDelta> out = output;
        if (deltas.size() > 1) out = consolidateEpochOutput(out);

        for (var delta : out) applyConsolidation(delta);

        // Plan 011 C2: eviction retractions ride out on this call's own return value, AFTER its real deltas.
        if (retention.isEnabled()) {
            if (!(out instanceof ArrayList)) out = new ArrayList<>(out);
            out.addAll(runRetention());
        }

        return out;
    }

    private List<TableDelta> runRetention() {
        var evicted = new ArrayList<TableDelta>();
        var scope = retentionScope;

        if (retention.ttlMs() > 0) evicted.addAll(scope.evictOlderThan(scope.maxObservedTs() - retention.ttlMs()));

        if (retention.maxRows() > 0 && scope.retainedCount() > retention.maxRows()) {
            evicted.addAll(scope.evictOldest(scope.retainedCount() - retention.maxRows()));
        }

        for (var delta : evicted) applyConsolidation(delta);
        return evicted;
    }

    /**
     * Wishlist #15 — Z-set consolidation of ONE epoch's raw op output: net every delta by canonical row key and drop
     * any key whose net weight is exactly zero, i.e. any row this epoch asserted and then retracted (or the reverse)
     * before it ever left this table. Order-preserving (first-occurrence position survives, later occurrences only
     * contribute their weight). Callers gate this on deltas.size() > 1 — load-bearing, see handleIncoming.
     */
    private static List<TableDelta> consolidateEpochOutput(List<TableDelta> raw) {
        if (raw.size() <= 1) return raw;

        var netWeight = new HashMap<String, Long>();
        var firstRow = new HashMap<String, EventRecord>();
        var order = new ArrayList<String>();

        for (var delta : raw) {
            String key = JsonText.serializeCanonicalRow(delta.row());
            Long existing = netWeight.get(key);
            if (existing != null) {
                netWeight.put(key, existing + delta.weight());
            } else {
                netWeight.put(key, delta.weight());
                firstRow.put(key, delta.row());
                order.add(key);
            }
        }

        var consolidated = new ArrayList<TableDelta>(order.size());
        for (String key : order) {
            long weight = netWeight.get(key);
            if (weight != 0) consolidated.add(new TableDelta(firstRow.get(key), weight));
        }
        return consolidated;
    }

    private List<TableRowDelta> propagateForward(int fromStageIndexInclusive, Epoch epoch, List<TableRowDelta> rows) {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < joins.size(); s++) current = joins.get(s).onLeftBatch(epoch, current);
        return current;
    }

    private void applyConsolidation(TableDelta delta) {
        String key = JsonText.serializeCanonicalRow(delta.row());
        ledger.apply(key, delta.row(), delta.weight());
        // null unless this plan's retention scope IS the ledger.
        if (ledgerScope != null) ledgerScope.observe(key);
    }
}
