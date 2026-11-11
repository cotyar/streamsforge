package io.streamsforge.host;

import io.streamsforge.actors.ActorContext;
import io.streamsforge.actors.Turns;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableEdgeMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Parallelism &gt;= 2 only. One activation per (table, external input) — Orleans {@code TableIngestGrain}. Buffers the
 * input's deltas (stream rows: weight +1; upstream table deltas: their own weight) and cuts an EPOCH every
 * {@code Options.flushPeriod} or once {@code Options.flushThreshold} deltas are buffered. A flush routes the epoch's
 * batch along every edge the input feeds — hash-partitioned by the consuming stage's key, or broadcast — and sends
 * EVERY downstream partition a batch, an empty marker included, so a quiet ingest never stalls a downstream
 * {@code FrontierTracker}. The flush turn awaits the whole downstream chain, so epoch {@code e} is completely
 * processed (to the output actor) before epoch {@code e+1} starts: the ingest is its own backpressure.
 *
 * <p>The Ingest-kind engine stage has partition count 1 and exists as a graph-shape concept; this actor performs its
 * routing directly, exactly like the Orleans grain.
 */
final class TableIngestActor {
    private static final String FLUSH_TIMER = "flush";

    private final HostEnv env;
    private final List<TableDelta> pending = new ArrayList<>();
    private final List<Supplier<CompletionStage<?>>> cleanups = new ArrayList<>();
    private Specs.TableSpec spec;
    private String table = "";
    private String input = "";
    private TableDataflowPlan dataflow;
    private List<TableEdgeDescriptor> externalEdges = List.of();
    private boolean running;
    private boolean tickQueued;
    private long epochCounter;
    private long cutoff = Long.MIN_VALUE; // table inputs: batches with epoch <= cutoff are inside the seed snapshot

    TableIngestActor(HostEnv env) { this.env = env; }

    CompletionStage<Void> start(ActorContext<TableIngestActor> ctx, Specs.TableSpec spec, String input) {
        if (running) return Async.DONE;
        this.spec = spec;
        this.table = spec.name();
        this.input = input;
        this.dataflow = spec.plan().createDataflow(spec.parallelism());
        this.externalEdges = dataflow.edgesForExternalInput(input);
        this.pending.clear();
        this.epochCounter = 0;
        this.cutoff = Long.MIN_VALUE;
        this.tickQueued = false;
        this.running = true;

        var period = env.opts.flushPeriod();
        // Started inside the turn; the tick only enqueues a flush message (never works in the tick itself).
        ctx.startTimer(FLUSH_TIMER, period, period, c -> {
            var st = c.state();
            if (st.tickQueued) return;
            st.tickQueued = true;
            env.send(c.self(), x -> x.state().flushTick());
        });

        var self = ctx.self();
        var info = spec.inputs().get(input);
        CompletionStage<Void> wiring;
        if (info.kind() == Specs.InputKind.TABLE) {
            wiring = InputWiring.attachTable(env, self, info, (t, b) -> t.onTableBatch(b), cleanups).thenAccept(pair -> {
                cutoff = pair.lastEpoch();
                pending.addAll(pair.rows());
            });
        } else {
            wiring = InputWiring.subscribeRows(env, self, info, (t, rows) -> t.onRows(rows), cleanups);
        }
        return wiring.whenComplete((v, e) -> {
            if (e != null) {
                running = false;
                runCleanups();
            }
        });
    }

    CompletionStage<Void> stop(ActorContext<TableIngestActor> ctx) {
        ctx.cancelTimer(FLUSH_TIMER);
        running = false;
        pending.clear();
        dataflow = null;
        return runCleanups();
    }

    private CompletionStage<Void> runCleanups() {
        var stages = new ArrayList<CompletionStage<?>>();
        for (var c : cleanups) stages.add(c.get());
        cleanups.clear();
        return Async.allOfQuiet(stages);
    }

    // ------------------------------------------------------------------ input

    CompletionStage<Void> onRows(List<EventRecord> rows) {
        if (!running) return Async.DONE;
        for (var r : rows) pending.add(new TableDelta(r, 1)); // a stream event always asserts
        return afterAdd();
    }

    CompletionStage<Void> onTableBatch(TableDeltaBatch batch) {
        if (!running) return Async.DONE;
        if (batch.epoch() <= cutoff) return Async.DONE; // already in the seed snapshot
        pending.addAll(batch.deltas());
        return afterAdd();
    }

    private CompletionStage<Void> afterAdd() {
        if (pending.size() < env.opts.flushThreshold()) return Async.DONE;
        return flush(-1).thenApply(n -> (Void) null);
    }

    CompletionStage<Void> flushTick() {
        tickQueued = false;
        return flush(-1).thenApply(n -> (Void) null);
    }

    long epochCounter() { return epochCounter; }

    // ------------------------------------------------------------------ flush

    /**
     * Cuts one epoch. {@code minEpoch} lets {@code StreamsHost.quiesce} align every ingest of a table on the same epoch
     * number (epochs must increase, not be dense). Returns the number of deltas the epoch carried.
     */
    CompletionStage<Integer> flush(long minEpoch) {
        if (!running || dataflow == null) return Turns.completed(0);
        long epoch = Math.max(epochCounter, minEpoch);
        epochCounter = epoch + 1;
        List<TableDelta> batch = pending.isEmpty() ? List.of() : new ArrayList<>(pending);
        pending.clear();

        var sends = new ArrayList<CompletionStage<?>>();
        for (var edge : externalEdges) {
            if (edge.mode() == TableEdgeMode.BROADCAST) {
                int fromPartition = Math.max(0, edge.externalInputNames().indexOf(input));
                route(edge, fromPartition, epoch, batch, sends);
            } else {
                // The edge targets an Ingest-kind stage; the real routing decision is that stage's one forward edge.
                route(dataflow.outEdgeOf(edge.toStageId()), 0, epoch, batch, sends);
            }
        }
        int count = batch.size();
        return Async.allOf(sends).thenApply(v -> count);
    }

    private void route(TableEdgeDescriptor edge, int fromPartition, long epoch, List<TableDelta> batch, List<CompletionStage<?>> sends) {
        int pcount = dataflow.partitionCountOf(edge.toStageId());
        if (edge.mode() == TableEdgeMode.BROADCAST) {
            for (int p = 0; p < pcount; p++) sends.add(send(edge, fromPartition, p, epoch, batch));
            return;
        }
        Map<Integer, List<TableDelta>> byPartition = new LinkedHashMap<>();
        for (var d : batch) {
            byPartition.computeIfAbsent(dataflow.partitionOf(edge.edgeId(), d.row()), k -> new ArrayList<>()).add(d);
        }
        for (int p = 0; p < pcount; p++) {
            sends.add(send(edge, fromPartition, p, epoch, byPartition.getOrDefault(p, List.of())));
        }
    }

    private CompletionStage<Void> send(TableEdgeDescriptor edge, int fromPartition, int targetPartition, long epoch, List<TableDelta> deltas) {
        int edgeId = edge.edgeId().value();
        String origin = input;
        return env.<TableStageActor>ref(Actors.STAGE, Actors.stageId(table, edge.toStageId(), targetPartition))
            .ask(c -> c.state().push(edgeId, fromPartition, epoch, origin, deltas));
    }
}
