package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.planning.CompiledTableJoin;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.JsonText;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The compiled table plan's partitioned dataflow graph (plan 003 M2's engine seam): stages + edges + routing, and a
 * factory for per-stage per-partition executors. Obtain via {@code TablePlan.createDataflow}. Immutable and
 * re-derivable from the same TablePlan any number of times.
 *
 * <p>SCOPE (M2, extended by plan 008): every table plan built from Sources[0] (FROM) directly and JOIN aliases that
 * are either (a) a plain real stream/table alias — INNER, CROSS, LEFT, RIGHT, or FULL equi-join — or (b) a
 * derived/residual subquery join (Scalar, Semi, Anti) is supported. A derived table/CTE named directly in FROM or
 * JOIN position, or a set-operation root, is NOT supported for Parallelism &gt; 1: the constructor throws
 * {@link UnsupportedOperationException} for those plans.
 */
public final class TableDataflowPlan {
    private final CompiledTablePlan compiled;
    private final int partitionCount;
    private final List<TableStageDescriptor> stages;
    private final List<TableEdgeDescriptor> edges;
    private final Map<Integer, StageBuild> stageBuilds;
    private final Map<Integer, RoutingKeySpec> routingSpecs; // by EdgeId.value, HashPartition edges only
    private final TableEdgeDescriptor terminalEdge;

    public TableDataflowPlan(CompiledTablePlan compiled, int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("Partition count must be >= 1. (Parameter 'partitionCount')");
        }
        this.compiled = compiled;
        this.partitionCount = partitionCount;
        var built = TableDataflowBuilder.build(compiled, partitionCount);
        this.stages = built.stages();
        this.edges = built.edges();
        this.stageBuilds = built.builds();
        this.routingSpecs = built.routingSpecs();
        TableEdgeDescriptor terminal = null;
        int count = 0;
        for (var e : edges) {
            if (e.toStageId() == -1) { terminal = e; count++; }
        }
        if (count != 1) throw new IllegalStateException("Sequence contains " + (count == 0 ? "no matching element" : "more than one matching element"));
        this.terminalEdge = terminal;
    }

    public int partitionCount() { return partitionCount; }

    public List<TableStageDescriptor> stages() { return Collections.unmodifiableList(stages); }

    /** Every edge in the graph, flattened — includes each stage's inbound edges plus the one terminal edge. */
    public List<TableEdgeDescriptor> edges() { return Collections.unmodifiableList(edges); }

    /** The single edge whose toStageId == -1 — this table's output. */
    public TableEdgeDescriptor terminalEdge() { return terminalEdge; }

    public List<String> streamInputs() { return compiled.streamInputs; }

    public List<String> tableInputs() { return compiled.tableInputs; }

    /** Every edge a real external input (stream source or upstream table) feeds — an input can feed more than one edge. */
    public List<TableEdgeDescriptor> edgesForExternalInput(String inputName) {
        var result = new ArrayList<TableEdgeDescriptor>();
        for (var e : edges) {
            if (e.fromStageId() == -1 && e.externalInputNames().contains(inputName)) result.add(e);
        }
        return result;
    }

    /** Every outbound edge of a stage (fromStageId == stageId) — exactly one per stage in M2's graph shape. */
    public TableEdgeDescriptor outEdgeOf(int stageId) {
        TableEdgeDescriptor found = null;
        int count = 0;
        for (var e : edges) {
            if (e.fromStageId() == stageId) { found = e; count++; }
        }
        if (count != 1) throw new IllegalStateException("Sequence contains " + (count == 0 ? "no matching element" : "more than one matching element"));
        return found;
    }

    /** Plan 003 M3: every edge TableDataflowBuilder marked arrangeable — by construction every such edge originates
     *  from an Ingest-kind stage; use {@link #externalInputNameOf} to recover the real source/table name. */
    public List<TableEdgeDescriptor> arrangeableExternalEdges() {
        var result = new ArrayList<TableEdgeDescriptor>();
        for (var e : edges) if (e.arrangeKeyFields() != null) result.add(e);
        return result;
    }

    /** The real external input name an arrangeable edge ultimately reads from. Throws if not arrangeable. */
    public String externalInputNameOf(TableEdgeDescriptor edge) {
        if (edge.arrangeKeyFields() == null) {
            throw new IllegalStateException("Edge " + edge.edgeId() + " is not arrangeable.");
        }
        for (var s : stages) {
            if (s.stageId() == edge.fromStageId()) return s.inEdges().get(0).externalInputNames().get(0);
        }
        throw new IllegalStateException("Sequence contains no matching element");
    }

    /** Canonical key-spec string for an arrangeable edge (see {@link ArrangementKeySpec}); bakes in the CONSUMING
     *  stage's partition count. Throws if not arrangeable. */
    public String keySpecOf(TableEdgeDescriptor edge) {
        if (edge.arrangeKeyFields() == null) {
            throw new IllegalStateException("Edge " + edge.edgeId() + " is not arrangeable; it has no ArrangeKeyFields.");
        }
        return ArrangementKeySpec.canonicalize(edge.arrangeKeyFields(), partitionCountOf(edge.toStageId()));
    }

    /** Partition count a given stage runs at: 1 for Ingest, {@link #partitionCount()} for everything else. */
    public int partitionCountOf(int stageId) {
        return stageBuilds.get(stageId).kind == TableStageKind.INGEST ? 1 : partitionCount;
    }

    /** Computes which partition of the edge's TARGET stage {@code row} routes to, for a HashPartition-mode edge. Throws
     *  {@link IllegalStateException} for a non-HashPartition edge. */
    public int partitionOf(EdgeId edgeId, EventRecord row) {
        RoutingKeySpec spec = routingSpecs.get(edgeId.value());
        if (spec == null) {
            throw new IllegalStateException("Edge " + edgeId + " is not hash-partitioned; only HashPartition-mode edges have a routing key.");
        }

        String canonical;
        if (spec.useRowContentHash) {
            // No natural join/group key applies at this hop — spread load across partitions by the row's own content.
            canonical = JsonText.serializeCanonicalRow(row);
        } else {
            WorkingRow wr = spec.isWireEncoded ? WorkingRowWireCodec.fromWire(row) : WorkingRow.fromEvent(spec.alias, row);
            var ctx = new EvalContext(wr, spec.bindings);
            var values = new Object[spec.keyExprs.size()];
            for (int i = 0; i < values.length; i++) values[i] = ExpressionEvaluator.eval(spec.keyExprs.get(i), ctx);
            canonical = TableKeyEncoding.encodeGroupKey(values);
        }
        return ExchangeRouter.partitionOf(canonical, partitionCountOf(spec.toStageId));
    }

    /** Builds a fresh per-(stage, partition) executor — its own private op state. */
    public ITableStageExecutor createStageExecutor(int stageId, int partition) {
        StageBuild build = stageBuilds.get(stageId);
        int pcount = partitionCountOf(stageId);
        if (partition < 0 || partition >= pcount) {
            throw new IllegalArgumentException("Stage " + stageId + " runs at partition count " + pcount + ". (Parameter 'partition')");
        }
        return TableDataflowBuilder.createExecutor(build, partition);
    }

    /** Engine-internal per-stage build info (C# {@code internal sealed class StageBuild}). */
    public static final class StageBuild {
        public TableStageKind kind;
        public TableStageDescriptor stage;
        public TableEdgeDescriptor outEdge;

        // Join / SemiAnti / Unnest
        public CompiledTableJoin join;
        public EdgeId leftEdge;
        public boolean leftIsWire;
        public String leftAlias;
        public EdgeId rightEdge;
        public String rightAlias;
        public CompiledTablePlan rightDerivedPlan; // set => broadcast + nested TableExecutor on the right

        // Join only, JoinKind Left/Right/Full (plan 008): every (alias, schema) accumulated on the left so far, and
        // this join's own (alias, schema) on the right.
        public List<WorkingRow.Side> leftAliasesSoFar;
        public WorkingRow.Side rightSide;

        // Ingest / FilterProject / Reduce / LatestBy (single input)
        public EdgeId inEdge;
        public boolean inIsWire;
        public String inAlias;
        public boolean terminal; // FilterProject only: true => onBatchTerminal path

        public CompiledTablePlan compiled;
        public List<Expr> reduceOrLatestKeys; // Reduce: null (GroupBy read straight off Compiled); LatestBy: the LATEST BY key list
    }

    /** Engine-internal routing description for a HashPartition edge (C# {@code internal sealed class RoutingKeySpec}). */
    public static final class RoutingKeySpec {
        public boolean isWireEncoded;
        public boolean useRowContentHash;
        public String alias;
        public List<Expr> keyExprs;
        public Map<Expr, Binding> bindings;
        public int toStageId;
    }
}
