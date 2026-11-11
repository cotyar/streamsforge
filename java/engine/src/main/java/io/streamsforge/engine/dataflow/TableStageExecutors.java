package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import io.streamsforge.engine.TablePlan;
import io.streamsforge.engine.dataflow.TableDataflowPlan.StageBuild;
import io.streamsforge.engine.planning.CompiledTableJoin;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ops.ITableJoinStage;
import io.streamsforge.engine.runtime.ops.TableFilterProjectOp;
import io.streamsforge.engine.runtime.ops.TableJoinOp;
import io.streamsforge.engine.runtime.ops.TableLatestByOp;
import io.streamsforge.engine.runtime.ops.TableOuterJoinOp;
import io.streamsforge.engine.runtime.ops.TableReduceOp;
import io.streamsforge.engine.runtime.ops.TableRowDelta;
import io.streamsforge.engine.runtime.ops.TableSemiAntiOp;
import io.streamsforge.engine.runtime.ops.TableUnnestOp;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.List;

/** The M2 per-(stage,partition) executors (C# {@code TableStageExecutors.cs}); each is created through
 *  {@link TableDataflowBuilder#createExecutor}. */
public final class TableStageExecutors {
    private TableStageExecutors() {}

    /** Shared plumbing for every M2 per-(stage,partition) executor: knows its stage id/partition and its one outbound
     *  edge (every stage in M2's graph shape has exactly one forward edge). */
    public abstract static class TableStageExecutorBase implements ITableStageExecutor {
        protected final StageBuild build;
        private final int partition;

        protected TableStageExecutorBase(StageBuild build, int partition) {
            this.build = build;
            this.partition = partition;
        }

        @Override public int stageId() { return build.stage.stageId(); }

        @Override public int partition() { return partition; }

        @Override public abstract List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas);

        /** Every table-mode op's onFrontier is a documented pass-through today — table mode has no epoch-driven eviction. */
        @Override public List<TableStageOutput> onFrontier(Epoch epoch) { return new ArrayList<>(); }

        protected List<TableStageOutput> single(List<TableDelta> deltas) {
            var result = new ArrayList<TableStageOutput>();
            if (!deltas.isEmpty()) result.add(new TableStageOutput(build.outEdge, deltas));
            return result;
        }
    }

    /** Ingest stage: routing-only, no state — purely "forward on my one outbound edge". */
    public static final class IngestStageExecutor extends TableStageExecutorBase {
        public IngestStageExecutor(StageBuild build, int partition) { super(build, partition); }

        @Override public List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas) {
            return single(deltas);
        }
    }

    /** Join / SemiAnti / Unnest stage: wraps the corresponding ITableJoinStage op. For a derived (Scalar/Semi/Anti)
     *  join, the Right edge is broadcast and its raw external deltas are first run through a private nested
     *  TableExecutor (one per partition) before admission. Unnest has no Right edge at all. */
    public static final class JoinChainStageExecutor extends TableStageExecutorBase {
        private final ITableJoinStage op;
        private final TableExecutor rightNested;

        public JoinChainStageExecutor(StageBuild build, int partition) {
            super(build, partition);
            CompiledTableJoin j = build.join;
            // Plan 008: a Join-kind stage covers Inner/Cross/Scalar AND Left/Right/Full — the outer-kind check is on
            // j.kind (the actual JoinKind) directly.
            if (build.kind == TableStageKind.SEMI_ANTI) {
                op = new TableSemiAntiOp(j.kind, j.leftKey, j.rightKey, build.compiled.bindings);
            } else if (build.kind == TableStageKind.UNNEST) {
                op = new TableUnnestOp(j.unnestExpr, j.alias, build.compiled.bindings);
            } else if (build.kind == TableStageKind.JOIN && (j.kind == JoinKind.LEFT || j.kind == JoinKind.RIGHT || j.kind == JoinKind.FULL)) {
                op = new TableOuterJoinOp(j.kind, j.leftKeys, j.rightKeys, j.residual, build.compiled.bindings,
                    build.leftAliasesSoFar, build.rightSide);
            } else {
                op = new TableJoinOp(j.leftKey, j.rightKey, j.residual, build.compiled.bindings);
            }
            rightNested = build.rightDerivedPlan != null ? new TablePlan(build.rightDerivedPlan).createExecutor() : null;
        }

        @Override public List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas) {
            List<TableRowDelta> outRows;

            if (build.leftEdge != null && inEdge.equals(build.leftEdge)) {
                var rows = toRowDeltas(deltas, build.leftIsWire, build.leftAlias);
                outRows = op.onLeftBatch(epoch, rows);
            } else if (build.rightEdge != null && inEdge.equals(build.rightEdge)) {
                List<TableRowDelta> rightRows;
                if (rightNested != null) {
                    var nestedOut = new ArrayList<TableDelta>();
                    for (var d : deltas) nestedOut.addAll(rightNested.onTableDelta(originName, d));
                    rightRows = toRowDeltas(nestedOut, false, build.rightAlias);
                } else {
                    rightRows = toRowDeltas(deltas, false, build.rightAlias);
                }
                outRows = op.onRightBatch(epoch, rightRows);
            } else {
                throw new IllegalStateException("Stage " + stageId() + " (partition " + partition() + ") received a batch on unrecognized edge " + inEdge + ".");
            }

            if (outRows.isEmpty()) return new ArrayList<>();
            var wire = new ArrayList<TableDelta>(outRows.size());
            for (var r : outRows) wire.add(new TableDelta(WorkingRowWireCodec.toWire(r.row()), r.weight()));
            return single(wire);
        }

        private static List<TableRowDelta> toRowDeltas(List<TableDelta> deltas, boolean isWire, String alias) {
            var results = new ArrayList<TableRowDelta>(deltas.size());
            for (var d : deltas) {
                WorkingRow wr = isWire ? WorkingRowWireCodec.fromWire(d.row()) : WorkingRow.fromEvent(alias, d.row());
                results.add(new TableRowDelta(wr, d.weight()));
            }
            return results;
        }
    }

    /** FilterProject stage: WHERE (+terminal projection when ungrouped). Stateless. */
    public static final class FilterProjectStageExecutor extends TableStageExecutorBase {
        private final TableFilterProjectOp op;

        public FilterProjectStageExecutor(StageBuild build, int partition) {
            super(build, partition);
            op = new TableFilterProjectOp(build.compiled);
        }

        @Override public List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas) {
            var rows = new ArrayList<TableRowDelta>(deltas.size());
            for (var d : deltas) {
                WorkingRow wr = build.inIsWire ? WorkingRowWireCodec.fromWire(d.row()) : WorkingRow.fromEvent(build.inAlias, d.row());
                rows.add(new TableRowDelta(wr, d.weight()));
            }

            if (build.terminal) {
                return single(op.onBatchTerminal(epoch, rows));
            }

            var outRows = op.onBatch(epoch, rows);
            if (outRows.isEmpty()) return new ArrayList<>();
            var wire = new ArrayList<TableDelta>(outRows.size());
            for (var r : outRows) wire.add(new TableDelta(WorkingRowWireCodec.toWire(r.row()), r.weight()));
            return single(wire);
        }
    }

    /** Reduce stage: running GROUP BY aggregates, partitioned on the group key. Terminal. */
    public static final class ReduceStageExecutor extends TableStageExecutorBase {
        private final TableReduceOp op;

        public ReduceStageExecutor(StageBuild build, int partition) {
            super(build, partition);
            op = new TableReduceOp(build.compiled);
        }

        @Override public List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas) {
            var rows = new ArrayList<TableRowDelta>(deltas.size());
            for (var d : deltas) rows.add(new TableRowDelta(WorkingRowWireCodec.fromWire(d.row()), d.weight()));
            return single(op.onBatch(epoch, rows));
        }
    }

    /** LatestBy stage: argmax-by-timestamp per key, partitioned on the latest-by key. Terminal. */
    public static final class LatestByStageExecutor extends TableStageExecutorBase {
        private final TableLatestByOp op;

        public LatestByStageExecutor(StageBuild build, int partition) {
            super(build, partition);
            op = new TableLatestByOp(build.compiled, build.reduceOrLatestKeys);
        }

        @Override public List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas) {
            var rows = new ArrayList<TableRowDelta>(deltas.size());
            for (var d : deltas) rows.add(new TableRowDelta(WorkingRowWireCodec.fromWire(d.row()), d.weight()));
            return single(op.onBatch(epoch, rows));
        }
    }
}
