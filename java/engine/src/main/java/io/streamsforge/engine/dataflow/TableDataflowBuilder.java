package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.planning.CompiledTableJoin;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.Identifier;
import io.streamsforge.engine.sql.Ast.JoinKind;
import io.streamsforge.engine.sql.Ast.QualifiedIdentifier;
import io.streamsforge.engine.dataflow.TableDataflowPlan.RoutingKeySpec;
import io.streamsforge.engine.dataflow.TableDataflowPlan.StageBuild;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Builds a {@link TableDataflowPlan}'s stage/edge graph from a {@link CompiledTablePlan}, and constructs the
 *  per-(stage,partition) {@link ITableStageExecutor} instances. See TableDataflowPlan's class doc for the
 *  supported-shapes scope. This mirrors TableExecutor.ensureInit's op-selection switch 1:1 (same JoinKind to
 *  op-type mapping) so the partitioned graph and the single-partition façade are built from the same rules. */
public final class TableDataflowBuilder {
    private TableDataflowBuilder() {}

    /** Result of {@link #build}. */
    public record Built(List<TableStageDescriptor> stages, List<TableEdgeDescriptor> edges,
                        Map<Integer, StageBuild> builds, Map<Integer, RoutingKeySpec> routingSpecs) {}

    private static final class Counter { int next; int take() { return next++; } }

    public static Built build(CompiledTablePlan compiled, int partitionCount) {
        if (compiled.unionBranches != null) {
            // Plan 008 W3: a set-operation root has no FROM/JOIN chain of its own, so this check must come BEFORE the
            // compiled.sources[0] dereference below.
            throw new UnsupportedOperationException(
                "Partitioned execution (Parallelism > 1) does not support a set operation (UNION/UNION ALL) at " +
                "the table root; set-operation tables run single-partition only in this wave. " +
                "Use Parallelism = 1 for this table.");
        }
        if (compiled.sources.get(0).derivedPlan != null) {
            throw new UnsupportedOperationException(
                "Partitioned execution (Parallelism > 1) does not support a derived table/CTE in FROM position " +
                "(source '" + compiled.sources.get(0).sourceName + "'); only scalar-subquery and semi/anti (IN/EXISTS) joins are " +
                "supported as broadcast edges in M2. Use Parallelism = 1 for this table.");
        }
        for (var j : compiled.joins) {
            if (j.kind != JoinKind.SCALAR && j.kind != JoinKind.SEMI && j.kind != JoinKind.ANTI && j.derivedPlan != null) {
                throw new UnsupportedOperationException(
                    "Partitioned execution (Parallelism > 1) does not support a derived table/CTE JOIN source " +
                    "(alias '" + j.alias + "'); only scalar-subquery and semi/anti (IN/EXISTS) joins are supported as " +
                    "broadcast edges in M2. Use Parallelism = 1 for this table.");
            }
            // CROSS JOIN's problem is purely one of parallelism: its synthesized constant join key would hash every row
            // to the SAME partition — correct but silently serial at partitionCount > 1, so only that case throws.
            if (j.kind == JoinKind.CROSS && partitionCount > 1) {
                throw new UnsupportedOperationException(
                    "Partitioned execution (Parallelism > 1) does not support CROSS JOIN " +
                    "(alias '" + j.alias + "'); its constant join key would hash every row to a single partition, " +
                    "which is correct but silently serial. Use Parallelism = 1 for this table.");
            }
        }

        var stages = new ArrayList<TableStageDescriptor>();
        var edges = new ArrayList<TableEdgeDescriptor>();
        var builds = new HashMap<Integer, StageBuild>();
        var routingSpecs = new HashMap<Integer, RoutingKeySpec>();
        var edgeCounter = new Counter();
        var stageCounter = new Counter();

        // --- Stage: FROM ingest ---
        int fromIngestStage = stageCounter.take();
        String fromAlias = compiled.sources.get(0).alias;
        var edgeFactory = new TableDataflowEdge(edgeCounter);
        var fromIngestEdge = edgeFactory.make(-1, fromIngestStage, "In", TableEdgeMode.LOCAL, List.of(compiled.sources.get(0).sourceName), null);
        edges.add(fromIngestEdge);
        var fromStageDesc = new TableStageDescriptor(fromIngestStage, TableStageKind.INGEST, fromAlias, List.of(fromIngestEdge));
        stages.add(fromStageDesc);
        var fromBuild = new StageBuild();
        fromBuild.kind = TableStageKind.INGEST;
        fromBuild.stage = fromStageDesc;
        fromBuild.outEdge = fromIngestEdge; // placeholder; overwritten below once the real forward edge is known
        fromBuild.compiled = compiled;
        builds.put(fromIngestStage, fromBuild);

        int chainStage = fromIngestStage;
        boolean chainIsWire = false; // FROM ingest's raw output is a single-alias EventRecord, not wire-encoded yet
        String chainAlias = fromAlias;

        // Plan 008: every (alias, schema) folded into the chain so far.
        var accumulated = new ArrayList<WorkingRow.Side>();
        accumulated.add(new WorkingRow.Side(compiled.sources.get(0).alias, compiled.sources.get(0).schema));

        for (int i = 0; i < compiled.joins.size(); i++) {
            CompiledTableJoin j = compiled.joins.get(i);
            int stageId = stageCounter.take();

            // The very first hop out of the FROM-ingest stage (partition count 1) can never be "Local" once it feeds a
            // P-partition stage. Every later hop (i>0) IS producer-partition-preserving-safe.
            boolean producerIsIngest = chainStage == fromIngestStage;

            if (j.kind == JoinKind.UNNEST) {
                var leftMode = producerIsIngest ? TableEdgeMode.HASH_PARTITION : TableEdgeMode.LOCAL;
                var leftEdge = edgeFactory.make(chainStage, stageId, "Left", leftMode, List.of(), null);
                edges.add(leftEdge);
                builds.get(chainStage).outEdge = leftEdge;
                if (producerIsIngest) {
                    var rs = new RoutingKeySpec();
                    rs.isWireEncoded = false; rs.useRowContentHash = true; rs.toStageId = stageId;
                    routingSpecs.put(leftEdge.edgeId().value(), rs);
                }

                var stage = new TableStageDescriptor(stageId, TableStageKind.UNNEST, j.alias, List.of(leftEdge));
                stages.add(stage);
                var b = new StageBuild();
                b.kind = TableStageKind.UNNEST;
                b.stage = stage;
                b.outEdge = leftEdge; // placeholder; overwritten below
                b.join = j;
                b.leftEdge = leftEdge.edgeId();
                b.leftIsWire = chainIsWire;
                b.leftAlias = chainAlias;
                b.compiled = compiled;
                builds.put(stageId, b);
            } else {
                boolean isDerived = j.derivedPlan != null; // Scalar / Semi / Anti (validated above)
                var leftMode = isDerived
                    ? (producerIsIngest ? TableEdgeMode.HASH_PARTITION : TableEdgeMode.LOCAL)
                    : TableEdgeMode.HASH_PARTITION;

                // Plan 003 M3 arrangeability check: the Left side of THIS hop is a candidate for a shared arrangement
                // only at i==0 (producerIsIngest), and only when EVERY LeftKeys component is a BARE reference to the
                // FROM alias's own field (no transform) — checked via compiled.bindings.
                List<String> leftArrangeFields = producerIsIngest && leftMode == TableEdgeMode.HASH_PARTITION
                    ? bareOwnFieldRefs(j.leftKeys, compiled, fromAlias)
                    : null;
                var leftEdge = edgeFactory.make(chainStage, stageId, "Left", leftMode, List.of(), leftArrangeFields);
                edges.add(leftEdge);
                builds.get(chainStage).outEdge = leftEdge;
                if (isDerived && producerIsIngest) {
                    var rs = new RoutingKeySpec();
                    rs.isWireEncoded = false; rs.useRowContentHash = true; rs.toStageId = stageId;
                    routingSpecs.put(leftEdge.edgeId().value(), rs);
                }

                TableEdgeDescriptor rightEdge;
                CompiledTablePlan rightDerivedPlan = null;

                if (isDerived) {
                    var scalarInputs = new LinkedHashSet<String>(j.derivedPlan.streamInputs);
                    scalarInputs.addAll(j.derivedPlan.tableInputs);
                    rightEdge = edgeFactory.make(-1, stageId, "Right", TableEdgeMode.BROADCAST, new ArrayList<>(scalarInputs), null);
                    rightDerivedPlan = j.derivedPlan;
                } else {
                    int ingestStageId = stageCounter.take();
                    var ingestInEdge = edgeFactory.make(-1, ingestStageId, "In", TableEdgeMode.LOCAL, List.of(j.sourceName), null);
                    edges.add(ingestInEdge);
                    var ingestStage = new TableStageDescriptor(ingestStageId, TableStageKind.INGEST, j.alias, List.of(ingestInEdge));
                    stages.add(ingestStage);
                    var ib = new StageBuild();
                    ib.kind = TableStageKind.INGEST; ib.stage = ingestStage; ib.outEdge = ingestInEdge; ib.compiled = compiled;
                    builds.put(ingestStageId, ib);

                    // Plan 003 M3: a non-derived join's Right side is ALWAYS fed by its own dedicated Ingest stage — so
                    // its own outbound edge is arrangeable whenever every RightKeys component is a bare reference to the
                    // join alias's own field.
                    List<String> rightArrangeFields = bareOwnFieldRefs(j.rightKeys, compiled, j.alias);
                    rightEdge = edgeFactory.make(ingestStageId, stageId, "Right", TableEdgeMode.HASH_PARTITION, List.of(), rightArrangeFields);
                    ib.outEdge = rightEdge;
                }
                edges.add(rightEdge);

                var kind = (j.kind == JoinKind.SEMI || j.kind == JoinKind.ANTI) ? TableStageKind.SEMI_ANTI : TableStageKind.JOIN;
                var stage = new TableStageDescriptor(stageId, kind, j.alias, List.of(leftEdge, rightEdge));
                stages.add(stage);
                var b = new StageBuild();
                b.kind = kind;
                b.stage = stage;
                b.outEdge = leftEdge; // placeholder; overwritten below
                b.join = j;
                b.leftEdge = leftEdge.edgeId();
                b.leftIsWire = chainIsWire;
                b.leftAlias = chainAlias;
                b.rightEdge = rightEdge.edgeId();
                b.rightAlias = j.alias;
                b.rightDerivedPlan = rightDerivedPlan;
                // Plan 008: threaded through for every join kind; only a Left/Right/Full executor actually reads them.
                b.leftAliasesSoFar = new ArrayList<>(accumulated);
                b.rightSide = new WorkingRow.Side(j.alias, j.schema);
                b.compiled = compiled;
                builds.put(stageId, b);

                if (leftMode == TableEdgeMode.HASH_PARTITION) {
                    // A non-derived join's Left edge is always a real key hash. Plan 008: keyExprs is the join's FULL
                    // key list (composite keys co-partition correctly this way).
                    var rs = new RoutingKeySpec();
                    rs.isWireEncoded = chainIsWire;
                    rs.useRowContentHash = false;
                    rs.alias = chainAlias;
                    if (j.leftKeys == null) throw new UnsupportedOperationException("Join alias '" + j.alias + "' has no left key; only equi-joins are supported in table mode.");
                    rs.keyExprs = new ArrayList<>(j.leftKeys);
                    rs.bindings = compiled.bindings;
                    rs.toStageId = stageId;
                    routingSpecs.put(leftEdge.edgeId().value(), rs);
                }
                if (rightEdge.mode() == TableEdgeMode.HASH_PARTITION) {
                    var rs = new RoutingKeySpec();
                    rs.isWireEncoded = false;
                    rs.useRowContentHash = false;
                    rs.alias = j.alias;
                    if (j.rightKeys == null) throw new UnsupportedOperationException("Join alias '" + j.alias + "' has no right key; only equi-joins are supported in table mode.");
                    rs.keyExprs = new ArrayList<>(j.rightKeys);
                    rs.bindings = compiled.bindings;
                    rs.toStageId = stageId;
                    routingSpecs.put(rightEdge.edgeId().value(), rs);
                }
            }

            chainStage = stageId;
            chainIsWire = true; // every join-chain stage's output is a (possibly multi-alias) WorkingRow, wire-encoded from here on
            chainAlias = null;
            accumulated.add(new WorkingRow.Side(j.alias, j.schema));
        }

        // --- Stage: FilterProject ---
        // If there were no JOINs at all, FilterProject is fed directly by the 1-partition FROM-ingest stage and needs a
        // real fan-out (content hash); otherwise it's fed by an already-P-partition join stage and Local is correct.
        boolean fpProducerIsIngest = chainStage == fromIngestStage;
        int fpStage = stageCounter.take();
        var fpInMode = fpProducerIsIngest ? TableEdgeMode.HASH_PARTITION : TableEdgeMode.LOCAL;
        var fpInEdge = edgeFactory.make(chainStage, fpStage, "In", fpInMode, List.of(), null);
        edges.add(fpInEdge);
        builds.get(chainStage).outEdge = fpInEdge;
        if (fpProducerIsIngest) {
            var rs = new RoutingKeySpec();
            rs.isWireEncoded = false; rs.useRowContentHash = true; rs.toStageId = fpStage;
            routingSpecs.put(fpInEdge.edgeId().value(), rs);
        }

        boolean grouped = compiled.groupBy != null || compiled.hasAggregates;
        boolean latestBy = !grouped && compiled.latestBy != null;
        boolean terminalHere = !grouped && !latestBy;

        var fpStageDesc = new TableStageDescriptor(fpStage, TableStageKind.FILTER_PROJECT, "", List.of(fpInEdge));
        stages.add(fpStageDesc);
        var fpBuild = new StageBuild();
        fpBuild.kind = TableStageKind.FILTER_PROJECT;
        fpBuild.stage = fpStageDesc;
        fpBuild.outEdge = fpInEdge; // placeholder; overwritten below
        fpBuild.inEdge = fpInEdge.edgeId();
        fpBuild.inIsWire = chainIsWire;
        fpBuild.inAlias = chainAlias;
        fpBuild.terminal = terminalHere;
        fpBuild.compiled = compiled;
        builds.put(fpStage, fpBuild);

        if (terminalHere) {
            var termEdge = edgeFactory.make(fpStage, -1, "Out", TableEdgeMode.GATHER, List.of(), null);
            edges.add(termEdge);
            fpBuild.outEdge = termEdge;
        } else {
            int aggStage = stageCounter.take();
            var aggInEdge = edgeFactory.make(fpStage, aggStage, "In", TableEdgeMode.HASH_PARTITION, List.of(), null);
            edges.add(aggInEdge);
            fpBuild.outEdge = aggInEdge;

            var kind = grouped ? TableStageKind.REDUCE : TableStageKind.LATEST_BY;
            List<Expr> aggKeys = grouped ? compiled.groupBy : compiled.latestBy;
            var aggStageDesc = new TableStageDescriptor(aggStage, kind, "", List.of(aggInEdge));
            stages.add(aggStageDesc);
            var aggBuild = new StageBuild();
            aggBuild.kind = kind;
            aggBuild.stage = aggStageDesc;
            aggBuild.outEdge = aggInEdge; // placeholder; overwritten below
            aggBuild.inEdge = aggInEdge.edgeId();
            aggBuild.inIsWire = true;
            aggBuild.compiled = compiled;
            aggBuild.reduceOrLatestKeys = aggKeys;
            builds.put(aggStage, aggBuild);

            var rs = new RoutingKeySpec();
            rs.isWireEncoded = true;
            rs.useRowContentHash = false;
            rs.alias = null;
            rs.keyExprs = aggKeys != null ? aggKeys : new ArrayList<>();
            rs.bindings = compiled.bindings;
            rs.toStageId = aggStage;
            routingSpecs.put(aggInEdge.edgeId().value(), rs);

            var termEdge = edgeFactory.make(aggStage, -1, "Out", TableEdgeMode.GATHER, List.of(), null);
            edges.add(termEdge);
            aggBuild.outEdge = termEdge;
        }

        return new Built(stages, edges, builds, routingSpecs);
    }

    public static ITableStageExecutor createExecutor(StageBuild build, int partition) {
        return switch (build.kind) {
            case INGEST -> new TableStageExecutors.IngestStageExecutor(build, partition);
            case JOIN, SEMI_ANTI, UNNEST -> new TableStageExecutors.JoinChainStageExecutor(build, partition);
            case FILTER_PROJECT -> new TableStageExecutors.FilterProjectStageExecutor(build, partition);
            case REDUCE -> new TableStageExecutors.ReduceStageExecutor(build, partition);
            case LATEST_BY -> new TableStageExecutors.LatestByStageExecutor(build, partition);
        };
    }

    /** Hands out sequential EdgeIds from a shared counter (C# local function {@code NewEdge()}). */
    private static final class TableDataflowEdge {
        private final Counter counter;
        TableDataflowEdge(Counter counter) { this.counter = counter; }

        TableEdgeDescriptor make(int from, int to, String role, TableEdgeMode mode, List<String> names, List<String> arrangeFields) {
            return new TableEdgeDescriptor(new EdgeId(counter.take()), from, to, role, mode, names, arrangeFields);
        }
    }

    /**
     * Plan 003 M3 arrangeability rule: every expression qualifies as "a bare reference to {@code alias}'s own raw field,
     * with no pre-join transform" iff it is a leaf identifier node (Identifier or QualifiedIdentifier) AND it is present
     * in compiled.bindings (populated ONLY for identifier leaves the validator resolves) resolving to exactly
     * {@code alias}. Plan 008: a composite key is arrangeable only when EVERY component independently qualifies.
     * Returns the raw field names in order, or null when not arrangeable.
     */
    private static List<String> bareOwnFieldRefs(List<Expr> exprs, CompiledTablePlan compiled, String alias) {
        if (exprs == null || exprs.isEmpty()) return null;
        var result = new ArrayList<String>(exprs.size());
        for (Expr expr : exprs) {
            if (!(expr instanceof Identifier || expr instanceof QualifiedIdentifier)) return null;
            Binding binding = compiled.bindings.get(expr);
            if (binding == null) return null;
            if (!alias.equals(binding.alias())) return null;
            result.add(binding.field());
        }
        return result;
    }
}
