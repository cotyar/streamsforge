package io.streamsforge.host;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.DeltaBatch;
import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.EpochBuffer;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.dataflow.FrontierTracker;
import io.streamsforge.engine.dataflow.ITableStageExecutor;
import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableEdgeMode;
import io.streamsforge.engine.dataflow.TableStageDescriptor;
import io.streamsforge.engine.dataflow.UpstreamId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Parallelism &gt;= 2 only. One activation per (table, non-Ingest stage, partition) — Orleans {@code TableStageGrain},
 * key {@code table:stageId:partition}. Holds the engine's {@link ITableStageExecutor} (private operator state) plus a
 * {@link FrontierTracker} and an {@link EpochBuffer}:
 *
 * <ol>
 *   <li>{@link #push} buffers the inbound (edge, fromPartition, epoch) batch — empty markers included — and records
 *       that upstream's progress;</li>
 *   <li>when the MIN-combined frontier over all registered upstreams advances, every buffered batch with
 *       {@code epoch <= frontier} is fed to the executor in the deterministic (epoch, edge, fromPartition) order the
 *       buffer returns, independent of arrival order;</li>
 *   <li>the emitted deltas are routed by the stage's single outbound edge (Local / HashPartition / Gather) and EVERY
 *       downstream partition (or the output actor) gets a batch stamped with the new frontier, empty or not.</li>
 * </ol>
 * Because the push awaits the downstream pushes, a completed ingest flush means the epoch reached the output actor.
 */
final class TableStageActor {
    private record BatchKey(EdgeId edge, int from, Epoch epoch) {}

    private final HostEnv env;
    private final Map<BatchKey, String> originByBatch = new HashMap<>();
    private String table = "";
    private int partition;
    private boolean running;
    private TableDataflowPlan dataflow;
    private TableStageDescriptor stage;
    private TableEdgeDescriptor outEdge;
    private ITableStageExecutor executor;
    private FrontierTracker frontier;
    private EpochBuffer buffer;

    TableStageActor(HostEnv env) { this.env = env; }

    void start(Specs.TableSpec spec, int stageId, int partition) {
        stop();
        this.table = spec.name();
        this.partition = partition;
        this.dataflow = spec.plan().createDataflow(spec.parallelism());
        this.stage = dataflow.stages().stream().filter(s -> s.stageId() == stageId).findFirst().orElseThrow();
        this.outEdge = dataflow.outEdgeOf(stageId);

        var upstreams = new ArrayList<UpstreamId>();
        for (var edge : stage.inEdges()) {
            // (No shared ArrangementGrain here, so even an "arrangeable" edge is an ordinary full fan-in.)
            int producers = edge.fromStageId() == -1
                ? (edge.mode() == TableEdgeMode.BROADCAST ? Math.max(1, edge.externalInputNames().size()) : 1)
                : dataflow.partitionCountOf(edge.fromStageId());
            for (int p = 0; p < producers; p++) upstreams.add(new UpstreamId(edge.edgeId(), p));
        }
        frontier = new FrontierTracker(upstreams);
        buffer = new EpochBuffer();
        executor = dataflow.createStageExecutor(stageId, partition);
        running = true;
    }

    void stop() {
        running = false;
        dataflow = null;
        stage = null;
        executor = null;
        frontier = null;
        buffer = null;
        originByBatch.clear();
    }

    CompletionStage<Void> push(int edgeIdValue, int fromPartition, long epochValue, String origin, List<TableDelta> deltas) {
        if (!running) return Async.DONE;
        var edgeId = new EdgeId(edgeIdValue);
        var epoch = new Epoch(epochValue);

        buffer.add(new DeltaBatch(edgeId, fromPartition, epoch, deltas));
        if (!deltas.isEmpty() && !origin.isEmpty()) originByBatch.put(new BatchKey(edgeId, fromPartition, epoch), origin);

        var observation = frontier.observe(new UpstreamId(edgeId, fromPartition), epoch);
        if (!observation.advanced()) return Async.DONE;

        var ready = buffer.onFrontier(observation.frontier());
        Map<Integer, List<TableDelta>> outByPartition = new HashMap<>();
        for (var batch : ready) {
            String o = originByBatch.remove(new BatchKey(batch.edgeId(), batch.fromPartition(), batch.epoch()));
            if (batch.deltas().isEmpty()) continue;
            var outputs = executor.onBatch(batch.edgeId(), o == null ? "" : o, batch.epoch(), batch.deltas());
            for (var output : outputs) {
                for (var d : output.deltas()) {
                    int target = targetPartitionOf(output.outEdge(), d);
                    outByPartition.computeIfAbsent(target, k -> new ArrayList<>()).add(d);
                }
            }
        }
        return routeDownstream(observation.frontier(), outByPartition);
    }

    private int targetPartitionOf(TableEdgeDescriptor edge, TableDelta delta) {
        return switch (edge.mode()) {
            case LOCAL -> partition;
            case HASH_PARTITION -> dataflow.partitionOf(edge.edgeId(), delta.row());
            case GATHER -> 0;
            case BROADCAST -> throw new IllegalStateException("A stage's own outbound edge cannot be Broadcast (edge " + edge.edgeId() + ").");
        };
    }

    private CompletionStage<Void> routeDownstream(Epoch frontierEpoch, Map<Integer, List<TableDelta>> outByPartition) {
        if (outEdge.toStageId() == -1) {
            // Always publish, even with zero rows: the output actor's own frontier needs a marker per advance.
            List<TableDelta> all;
            if (outByPartition.isEmpty()) {
                all = List.of();
            } else {
                all = new ArrayList<>();
                for (var l : outByPartition.values()) all.addAll(l);
            }
            int from = partition;
            long f = frontierEpoch.value();
            return env.<TableOutputActor>ref(Actors.OUTPUT, table).ask(c -> c.state().publish(from, f, all));
        }
        int targetCount = dataflow.partitionCountOf(outEdge.toStageId());
        var sends = new ArrayList<CompletionStage<?>>(targetCount);
        int edgeId = outEdge.edgeId().value();
        int toStage = outEdge.toStageId();
        int from = partition;
        long f = frontierEpoch.value();
        for (int p = 0; p < targetCount; p++) {
            List<TableDelta> deltas = outByPartition.getOrDefault(p, List.of());
            sends.add(env.<TableStageActor>ref(Actors.STAGE, Actors.stageId(table, toStage, p))
                .ask(c -> c.state().push(edgeId, from, f, "", deltas)));
        }
        return Async.allOf(sends);
    }
}
