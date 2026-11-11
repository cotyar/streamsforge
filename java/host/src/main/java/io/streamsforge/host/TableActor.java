package io.streamsforge.host;

import io.streamsforge.actors.ActorContext;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import io.streamsforge.engine.dataflow.TableStageKind;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One activation per table (Orleans {@code TableGrain}).
 *
 * <p><b>Parallelism 1 (classic mode):</b> wraps a {@link TableExecutor}; source/pipeline rows and upstream table
 * batches arrive as mailbox messages, the executor's output is published under {@code exec.lastEpoch()}. This actor
 * is also the table's {@link TableView}.
 *
 * <p><b>Parallelism &gt;= 2 (coordinator mode):</b> only the LIFECYCLE coordinator — starts/stops the
 * {@link TableOutputActor}, the {@link TableStageActor}s and the {@link TableIngestActor}s. It is deliberately NOT on
 * the data path (the Orleans TableGrain is, which is why its {@code OnOutputBatchAsync} needs {@code [MayInterleave]}:
 * a stop turn awaiting an ingest that awaits a flush chain that ends in this grain deadlocks otherwise). Here the
 * ledger / frontier / subscribers live in the output actor, so no data-path message ever targets this actor.
 */
final class TableActor implements TableView {
    private final HostEnv env;
    private final String name;
    private final Publisher<TableDeltaBatch> out;
    private final List<Supplier<CompletionStage<?>>> cleanups = new ArrayList<>();
    private final Map<String, Long> cutoff = new HashMap<>();
    private Specs.TableSpec spec;
    private TableExecutor exec;
    private boolean running;
    private boolean exists;
    private final List<String> childKeys = new ArrayList<>();

    TableActor(HostEnv env, String name) {
        this.env = env;
        this.name = name;
        this.out = new Publisher<>(env);
    }

    void init() { exists = true; }

    CompletionStage<Void> reset(int parallelism) {
        exists = false;
        out.clear();
        if (parallelism >= 2) return env.exec(env.<TableOutputActor>ref(Actors.OUTPUT, name), TableOutputActor::reset);
        return Async.DONE;
    }

    boolean isRunning() { return running; }

    // ------------------------------------------------------------------ lifecycle

    CompletionStage<Void> start(ActorContext<TableActor> ctx, Specs.TableSpec spec) {
        if (running) return Async.DONE;
        this.spec = spec;
        this.running = true;
        cutoff.clear();
        var started = spec.parallelism() == 1 ? startClassic(ctx) : startCoordinator();
        return started.whenComplete((v, e) -> {
            if (e != null) {
                running = false;
                exec = null;
                stopInternal();
            }
        });
    }

    CompletionStage<Void> stop() {
        running = false;
        exec = null;
        return stopInternal();
    }

    private CompletionStage<Void> stopInternal() {
        var stages = new ArrayList<CompletionStage<?>>();
        if (spec != null && spec.parallelism() >= 2) {
            // Ingest first (no more input), then stages, then the output actor. Every flush chain an ingest had in
            // flight completes before its stop turn runs (the chain is awaited inside the flush turn).
            var ingests = new ArrayList<CompletionStage<?>>();
            for (var in : spec.allInputs()) {
                var key = Actors.ingestId(name, in);
                ingests.add(env.<TableIngestActor>ref(Actors.INGEST, key).ask(c -> c.state().stop(c)));
            }
            var chain = Async.allOfQuiet(ingests).thenCompose(v -> {
                var st = new ArrayList<CompletionStage<?>>();
                for (var k : new ArrayList<>(childKeys)) {
                    st.add(env.exec(env.<TableStageActor>ref(Actors.STAGE, k), TableStageActor::stop));
                }
                return Async.allOfQuiet(st);
            }).thenCompose(v -> env.exec(env.<TableOutputActor>ref(Actors.OUTPUT, name), TableOutputActor::stop))
              .thenCompose(v -> {
                  // Release the (stage, partition) / ingest activations: their state is gone, memory too.
                  var gone = new ArrayList<CompletionStage<?>>();
                  for (var k : new ArrayList<>(childKeys)) gone.add(env.rt.deactivate(new io.streamsforge.actors.ActorKey(Actors.STAGE, k)));
                  for (var in : spec.allInputs()) {
                      gone.add(env.rt.deactivate(new io.streamsforge.actors.ActorKey(Actors.INGEST, Actors.ingestId(name, in))));
                  }
                  childKeys.clear();
                  return Async.allOfQuiet(gone);
              });
            stages.add(chain);
        }
        for (var c : cleanups) stages.add(c.get());
        cleanups.clear();
        return Async.allOfQuiet(stages);
    }

    // ------------------------------------------------------------------ parallelism 1

    private CompletionStage<Void> startClassic(ActorContext<TableActor> ctx) {
        exec = spec.plan().createExecutor();
        var self = ctx.self();
        var steps = new ArrayList<Supplier<CompletionStage<?>>>();
        for (var in : spec.streamInputs()) {
            var info = spec.inputs().get(in);
            steps.add(() -> InputWiring.subscribeRows(env, self, info, (t, rows) -> t.onRows(in, rows), cleanups));
        }
        for (var up : spec.tableInputs()) {
            var info = spec.inputs().get(up);
            steps.add(() -> InputWiring.attachTable(env, self, info, (t, b) -> t.onTableBatch(up, b), cleanups).thenAccept(pair -> {
                cutoff.put(up, pair.lastEpoch());
                if (!pair.rows().isEmpty()) publish(exec.onTableDeltaBatch(up, pair.rows()));
            }));
        }
        return Async.sequence(steps);
    }

    CompletionStage<Void> onRows(String input, List<EventRecord> rows) {
        if (!running || exec == null) return Async.DONE;
        var deltas = new ArrayList<TableDelta>(rows.size());
        for (var r : rows) deltas.add(new TableDelta(r, 1));
        publish(exec.onTableDeltaBatch(input, deltas));
        return Async.DONE;
    }

    CompletionStage<Void> onTableBatch(String upstream, TableDeltaBatch batch) {
        if (!running || exec == null) return Async.DONE;
        Long cut = cutoff.get(upstream);
        // Already contained in the snapshot this table was seeded from: skip (no double count).
        if (cut != null && batch.epoch() <= cut) return Async.DONE;
        publish(exec.onTableDeltaBatch(upstream, batch.deltas()));
        return Async.DONE;
    }

    private void publish(List<TableDelta> deltas) {
        if (deltas.isEmpty()) return;
        out.publish(new TableDeltaBatch(exec.lastEpoch(), Collections.unmodifiableList(deltas)));
    }

    // ------------------------------------------------------------------ parallelism >= 2

    private CompletionStage<Void> startCoordinator() {
        var dataflow = spec.plan().createDataflow(spec.parallelism());
        childKeys.clear();
        var stageStarts = new ArrayList<CompletionStage<?>>();
        for (var stage : dataflow.stages()) {
            if (stage.kind() == TableStageKind.INGEST) continue; // an Ingest-kind stage is performed by the ingest actor
            int count = dataflow.partitionCountOf(stage.stageId());
            for (int p = 0; p < count; p++) childKeys.add(Actors.stageId(name, stage.stageId(), p));
        }
        var keys = new ArrayList<>(childKeys);
        var output = env.<TableOutputActor>ref(Actors.OUTPUT, name);
        var s = spec;
        return env.exec(output, o -> o.start(s)).thenCompose(v -> {
            for (var k : keys) {
                var parts = k.split(":");
                int stageId = Integer.parseInt(parts[parts.length - 2]);
                int partition = Integer.parseInt(parts[parts.length - 1]);
                stageStarts.add(env.exec(env.<TableStageActor>ref(Actors.STAGE, k), st -> st.start(s, stageId, partition)));
            }
            return Async.allOf(stageStarts);
        }).thenCompose(v -> {
            var ingestStarts = new ArrayList<CompletionStage<?>>();
            for (var in : s.allInputs()) {
                ingestStarts.add(env.<TableIngestActor>ref(Actors.INGEST, Actors.ingestId(name, in)).ask(c -> c.state().start(c, s, in)));
            }
            return Async.allOf(ingestStarts);
        });
    }

    // ------------------------------------------------------------------ view (parallelism 1)

    @Override
    public long subscribe(Consumer<TableDeltaBatch> consumer) {
        if (!exists) throw new CatalogException("unknown table '" + name + "'");
        return out.subscribe(consumer);
    }

    @Override
    public boolean unsubscribe(long id) { return out.unsubscribe(id); }

    @Override
    public SnapshotPair snapshotPair() {
        if (exec == null) return new SnapshotPair(List.of(), -1);
        var rows = new ArrayList<TableDelta>();
        for (RowWeight rw : exec.snapshot().values()) rows.add(new TableDelta(rw.row(), rw.weight()));
        return new SnapshotPair(rows, exec.lastEpoch());
    }

    @Override
    public Map<String, RowWeight> snapshotMap() {
        return exec == null ? Map.of() : new LinkedHashMap<>(exec.snapshot());
    }
}
