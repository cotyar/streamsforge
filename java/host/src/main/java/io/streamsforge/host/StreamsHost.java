package io.streamsforge.host;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.ActorType;
import io.streamsforge.actors.Turns;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.SourceSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A miniature StreamsForge host written once against the {@link ActorRuntime} SPI. Sources feed pipelines and tables;
 * tables run either as one executor actor (parallelism 1) or as the Orleans "coordinator mode" dataflow of ingest /
 * stage / output actors (parallelism &gt;= 2). See README.md for the actor catalog and the epoch/frontier protocol.
 *
 * <p>All methods are non-blocking and return {@link CompletionStage}s except {@link #quiesce} / {@link #errors}, which
 * are test and benchmark hooks and must not be called from inside a turn. Catalog refusals complete the stage
 * exceptionally with a {@link CatalogException}.
 *
 * <p><b>Idle collection:</b> every actor type is registered with {@code idleTimeout == null} (never collected). An
 * entity's state IS its activation (nothing is persisted), so collecting a running table's executor or a stage's
 * frontier would silently lose data. State is released only by {@code stop} / {@code delete}.
 */
public final class StreamsHost {
    /**
     * @param flushPeriod      parallelism &gt;= 2 ingest epoch cadence (Orleans: 250 ms, {@code TABLES__FLUSHMS})
     * @param flushThreshold   an ingest also cuts an epoch once this many deltas are buffered (Orleans: 1000)
     * @param watermarkPeriod  pipeline watermark timer period; {@code null} = no timer, only {@link #advanceWatermark}
     * @param clock            "now" for the watermark timer, in epoch ms (tests inject a fake)
     */
    public record Options(Duration flushPeriod, int flushThreshold, Duration watermarkPeriod, LongSupplier clock) {
        public static Options defaults() {
            return new Options(Duration.ofMillis(250), 1000, Duration.ofSeconds(1), System::currentTimeMillis);
        }

        public Options withFlushPeriod(Duration d) { return new Options(d, flushThreshold, watermarkPeriod, clock); }

        public Options withFlushThreshold(int n) { return new Options(flushPeriod, n, watermarkPeriod, clock); }

        public Options withWatermarkPeriod(Duration d) { return new Options(flushPeriod, flushThreshold, d, clock); }

        public Options withClock(LongSupplier c) { return new Options(flushPeriod, flushThreshold, watermarkPeriod, c); }
    }

    /** One catalog row. */
    public record EntityInfo(String name, String kind, boolean running, int parallelism) {}

    private final HostEnv env;
    private final ActorRuntime rt;
    private final ConcurrentHashMap<String, Integer> tableParallelism = new ConcurrentHashMap<>();

    public StreamsHost(ActorRuntime runtime) { this(runtime, Options.defaults()); }

    public StreamsHost(ActorRuntime runtime, Options options) {
        this.rt = runtime;
        this.env = new HostEnv(runtime, options);
        // idleTimeout == null everywhere: see the class comment.
        runtime.register(ActorType.of(Actors.REGISTRY, k -> new RegistryActor(env)));
        runtime.register(ActorType.of(Actors.SOURCE, k -> new SourceActor(env, k.id())));
        runtime.register(ActorType.of(Actors.PIPELINE, k -> new PipelineActor(env, k.id())));
        runtime.register(ActorType.of(Actors.TABLE, k -> new TableActor(env, k.id())));
        runtime.register(ActorType.of(Actors.INGEST, k -> new TableIngestActor(env)));
        runtime.register(ActorType.of(Actors.STAGE, k -> new TableStageActor(env)));
        runtime.register(ActorType.of(Actors.OUTPUT, k -> new TableOutputActor(env, k.id())));
    }

    // ------------------------------------------------------------------ catalog

    private io.streamsforge.actors.ActorRef<RegistryActor> registry() { return env.ref(Actors.REGISTRY, Actors.REGISTRY_ID); }

    public CompletionStage<Void> createSource(String name, SourceSchema schema) {
        return registry().ask(c -> c.state().createSource(name, schema));
    }

    public CompletionStage<Void> createPipeline(String name, String sql) {
        return registry().ask(c -> c.state().createPipeline(name, sql));
    }

    public CompletionStage<Void> createTable(String name, String sql) { return createTable(name, sql, 1); }

    public CompletionStage<Void> createTable(String name, String sql, int parallelism) {
        return registry().ask(c -> c.state().createTable(name, sql, parallelism)).thenAccept(p -> tableParallelism.put(name, p));
    }

    public CompletionStage<Void> start(String name) { return registry().ask(c -> c.state().start(name)); }

    public CompletionStage<Void> stop(String name) { return registry().ask(c -> c.state().stop(name)); }

    public CompletionStage<Void> delete(String name) {
        return registry().ask(c -> c.state().delete(name)).thenAccept(v -> tableParallelism.remove(name));
    }

    public CompletionStage<List<EntityInfo>> list() { return registry().call(RegistryActor::list); }

    // ------------------------------------------------------------------ data in

    /** Publishes rows to a source. Completes when the source has accepted the batch and handed it to its subscribers'
     *  mailboxes (processing downstream is asynchronous; see {@link #quiesce}). */
    public CompletionStage<Void> publish(String source, List<EventRecord> rows) {
        List<EventRecord> copy = List.copyOf(rows);
        return env.exec(env.<SourceActor>ref(Actors.SOURCE, source), a -> a.publish(copy));
    }

    // ------------------------------------------------------------------ data out

    /** Subscribes to a pipeline's result batches. The consumer runs ON the pipeline's turn: do not block in it. */
    public CompletionStage<Subscription> subscribePipeline(String name, Consumer<List<EventRecord>> consumer) {
        io.streamsforge.actors.ActorRef<RowPublisher> pub = env.ref(Actors.PIPELINE, name);
        return pub.call(p -> p.subscribeRows(consumer)).thenApply(id -> () -> pub.call(p -> p.unsubscribeRows(id)).thenAccept(b -> { }));
    }

    /** Subscribes to a table's output batches from now on (no backfill — see {@link #attachTable}). The consumer runs
     *  on the owning actor's turn: do not block in it. */
    public CompletionStage<Subscription> subscribeTable(String name, Consumer<TableDeltaBatch> consumer) {
        return viewOf(name).thenCompose(view -> view.call(v -> v.subscribe(consumer)).thenApply(id ->
            (Subscription) () -> view.call(v -> v.unsubscribe(id)).thenAccept(b -> { })));
    }

    /**
     * Subscribe, THEN read the (snapshot, lastEpoch) pair — the same protocol a downstream table uses. A consumer that
     * seeds itself from {@code snapshot} and applies only batches with {@code epoch > lastEpoch} ends with exactly the
     * table's rows: nothing missed, nothing counted twice.
     */
    public CompletionStage<TableAttach> attachTable(String name, Consumer<TableDeltaBatch> consumer) {
        return viewOf(name).thenCompose(view -> view.call(v -> v.subscribe(consumer)).thenCompose(id ->
            view.call(v -> new Object[] {v.snapshotMap(), v.snapshotPair().lastEpoch()}).thenApply(pair -> {
                @SuppressWarnings("unchecked")
                var snap = (Map<String, RowWeight>) pair[0];
                return new TableAttach(snap, (Long) pair[1], () -> view.call(v -> v.unsubscribe(id)).thenAccept(b -> { }));
            })));
    }

    /** The table's current consolidated output, keyed by canonical row key (weight &gt; 0 rows). */
    public CompletionStage<Map<String, RowWeight>> snapshot(String table) {
        return viewOf(table).thenCompose(view -> view.call(TableView::snapshotMap));
    }

    /** Advance a pipeline's watermark to {@code nowMs - 1s} and publish whatever closes (the manual twin of the timer). */
    public CompletionStage<Void> advanceWatermark(String pipeline, long nowMs) {
        return registry().call(r -> { r.requirePipeline(pipeline); return null; }).thenCompose(v ->
            env.exec(env.<PipelineActor>ref(Actors.PIPELINE, pipeline), p -> p.advance(nowMs)));
    }

    private CompletionStage<io.streamsforge.actors.ActorRef<TableView>> viewOf(String table) {
        Integer p = tableParallelism.get(table);
        if (p != null) return Turns.completed(env.viewRef(table, p));
        return registry().ask(c -> Turns.completed(c.state().tableParallelism(table))).thenApply(par -> {
            tableParallelism.put(table, par);
            return env.viewRef(table, par);
        });
    }

    // ------------------------------------------------------------------ test / benchmark hooks

    /**
     * Blocks until everything published so far has been fully processed: every internal message delivered and run, and,
     * for parallelism &gt;= 2 tables, every ingest's buffered deltas flushed and drained through all stages to the output
     * actor (all of a table's ingests are flushed to ONE common epoch number so each stage's min-combined frontier
     * reaches it). Repeats until a pass finds nothing left to do (a table's output feeds downstream ingests). Tests use
     * this instead of sleeping; it never runs inside a turn.
     */
    public void quiesce(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (true) {
                awaitIdle(deadline);
                var tables = registry().call(RegistryActor::dataflowTables).toCompletableFuture().get(remaining(deadline), TimeUnit.NANOSECONDS);
                long flushed = 0;
                for (var t : tables.entrySet()) flushed += flushTable(t.getValue(), deadline);
                if (flushed == 0 && env.inflight.get() == 0) return;
                if (System.nanoTime() > deadline) throw new IllegalStateException("quiesce timed out");
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("quiesce failed: " + Async.unwrap(e), Async.unwrap(e));
        }
    }

    private long flushTable(List<String> ingestIds, long deadline) throws Exception {
        long max = -1;
        for (var id : ingestIds) {
            long c = env.<TableIngestActor>ref(Actors.INGEST, id).call(TableIngestActor::epochCounter)
                .toCompletableFuture().get(remaining(deadline), TimeUnit.NANOSECONDS);
            max = Math.max(max, c);
        }
        long epoch = max + 1;
        long flushed = 0;
        for (var id : ingestIds) {
            flushed += env.<TableIngestActor>ref(Actors.INGEST, id).ask(c -> c.state().flush(epoch))
                .toCompletableFuture().get(remaining(deadline), TimeUnit.NANOSECONDS);
        }
        return flushed;
    }

    private void awaitIdle(long deadline) throws TimeoutException {
        while (env.inflight.get() != 0) {
            if (System.nanoTime() > deadline) throw new TimeoutException("quiesce: " + env.inflight.get() + " messages still in flight");
            LockSupport.parkNanos(100_000);
        }
    }

    private static long remaining(long deadline) { return Math.max(1, deadline - System.nanoTime()); }

    /** Failures of internal fire-and-forget messages and of subscriber callbacks. Tests assert this is empty. */
    public List<Throwable> errors() { return new ArrayList<>(env.errors); }

    /** Number of internal messages issued but not yet completed. */
    public long inflight() { return env.inflight.get(); }
}
