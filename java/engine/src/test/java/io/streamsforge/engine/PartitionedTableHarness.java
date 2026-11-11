package io.streamsforge.engine;

import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.dataflow.ITableStageExecutor;
import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableEdgeMode;
import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan 003 M2 acceptance (5b/5c): "an in-process harness driving stage executors WITHOUT Orleans — the engine seam
 * makes this possible". Simulates the grain-level protocol (batched-per-(edge,epoch) delivery, hash/broadcast/local/
 * gather routing via {@link TableDataflowPlan}'s own public seam) entirely in-process: no grains, no Orleans streams,
 * no threads. Each top-level {@link #admit} call is one external admission (mirrors TableExecutor.onStreamEvent/
 * onTableDelta — one epoch per call) that gets fully drained to a fixed point (every cross-edge message produced
 * along the way processed exactly once) before returning, with the resulting terminal deltas folded into a
 * DBSP-style consolidated snapshot — the same consolidation TableExecutor's ledger performs, so its output is
 * directly comparable to {@link TableExecutor#snapshot()}'s.
 *
 * <p>The {@code lifo} knob on {@link #admit} processes one admission's internal work queue LIFO instead of FIFO — a
 * same-epoch intra-admission reordering knob, on top of the caller's own freedom to call {@link #admit} for a
 * sequence of external events in any order it likes.
 */
final class PartitionedTableHarness {
    private record Work(TableEdgeDescriptor edge, int partition, String origin, List<TableDelta> deltas) {}

    private record Seed(EdgeId edge, int partition, List<TableDelta> deltas) {}

    private final TableDataflowPlan plan;
    private final Map<List<Integer>, ITableStageExecutor> executors = new HashMap<>();
    // Mirrors the ledger's own _consolidated/_debtWeights split exactly: this harness is the M2 equivalence oracle, so
    // it must apply the identical ledger fix or the "classic vs partitioned" comparisons would silently diverge again
    // under reversed admission order.
    private final Map<String, RowWeight> consolidated = new LinkedHashMap<>();
    private final Map<String, Long> debtWeights = new HashMap<>();
    private long epochCounter;

    PartitionedTableHarness(TableDataflowPlan plan) { this.plan = plan; }

    Map<String, RowWeight> snapshot() { return consolidated; }

    /** Admits one external event/table-delta (already weight-normalized by the caller — weight=1 for a genuine stream
     *  event, the delta's own signed weight for an upstream table delta) and drains it to a fixed point before
     *  returning. */
    void admit(String originName, EventRecord row, long weight) { admit(originName, row, weight, false); }

    void admit(String originName, EventRecord row, long weight, boolean lifo) {
        var epoch = new Epoch(epochCounter++);
        var initialDelta = new TableDelta(row, weight);
        var seed = new ArrayList<Seed>();

        for (var edge : plan.edgesForExternalInput(originName)) {
            if (edge.mode() == TableEdgeMode.BROADCAST) {
                for (int p = 0; p < plan.partitionCountOf(edge.toStageId()); p++) {
                    seed.add(new Seed(edge.edgeId(), p, List.of(initialDelta)));
                }
            } else {
                int p = edge.mode() == TableEdgeMode.HASH_PARTITION ? plan.partitionOf(edge.edgeId(), row) : 0;
                seed.add(new Seed(edge.edgeId(), p, List.of(initialDelta)));
            }
        }

        drain(originName, seed, epoch, lifo);
    }

    private void drain(String originName, List<Seed> seed, Epoch epoch, boolean lifo) {
        var edgeById = new HashMap<EdgeId, TableEdgeDescriptor>();
        for (var e : plan.edges()) edgeById.put(e.edgeId(), e);
        var work = new ArrayList<Work>();
        for (var s : seed) {
            if (s.deltas().isEmpty()) continue;
            work.add(new Work(edgeById.get(s.edge()), s.partition(), originName, s.deltas()));
        }

        while (!work.isEmpty()) {
            int idx = lifo ? work.size() - 1 : 0;
            var item = work.remove(idx);
            var edge = item.edge();
            int partition = item.partition();
            String origin = item.origin();
            var deltas = item.deltas();

            if (edge.toStageId() == -1) {
                for (var d : deltas) applyConsolidation(d);
                continue;
            }

            var executor = executorFor(edge.toStageId(), partition);
            var outputs = executor.onBatch(edge.edgeId(), origin, epoch, deltas);
            for (var output : outputs) {
                var outEdge = output.outEdge();
                if (outEdge.toStageId() == -1) {
                    work.add(new Work(outEdge, 0, origin, output.deltas()));
                    continue;
                }
                switch (outEdge.mode()) {
                    case BROADCAST:
                        for (int p = 0; p < plan.partitionCountOf(outEdge.toStageId()); p++) {
                            work.add(new Work(outEdge, p, origin, output.deltas()));
                        }
                        break;
                    case LOCAL:
                        work.add(new Work(outEdge, partition, origin, output.deltas()));
                        break;
                    default: { // HashPartition: different deltas in the same output batch may route to different partitions
                        var byPartition = new LinkedHashMap<Integer, List<TableDelta>>();
                        for (var d : output.deltas()) {
                            byPartition.computeIfAbsent(plan.partitionOf(outEdge.edgeId(), d.row()), k -> new ArrayList<>()).add(d);
                        }
                        for (var g : byPartition.entrySet()) {
                            work.add(new Work(outEdge, g.getKey(), origin, g.getValue()));
                        }
                        break;
                    }
                }
            }
        }
    }

    private ITableStageExecutor executorFor(int stageId, int partition) {
        var key = List.of(stageId, partition);
        var executor = executors.get(key);
        if (executor == null) {
            executor = plan.createStageExecutor(stageId, partition);
            executors.put(key, executor);
        }
        return executor;
    }

    private void applyConsolidation(TableDelta delta) {
        String key = JsonText.serializeCanonicalRow(delta.row());

        var existing = consolidated.get(key);
        long currentWeight = existing != null ? existing.weight() : debtWeights.getOrDefault(key, 0L);

        long newWeight = currentWeight + delta.weight();

        if (newWeight > 0) {
            consolidated.put(key, new RowWeight(delta.row(), newWeight));
            debtWeights.remove(key);
        } else if (newWeight < 0) {
            consolidated.remove(key);
            debtWeights.put(key, newWeight);
        } else { // newWeight == 0: fully cancelled out — no residue in either structure.
            consolidated.remove(key);
            debtWeights.remove(key);
        }
    }
}
