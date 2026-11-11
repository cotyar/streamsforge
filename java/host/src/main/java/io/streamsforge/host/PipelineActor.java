package io.streamsforge.host;

import io.streamsforge.actors.ActorContext;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.PipelineExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One activation per pipeline (Orleans {@code PipelineGrain}). Wraps a {@link PipelineExecutor}; sources push rows
 * in as mailbox messages, results go out through a {@link Publisher}. The watermark advances on an actor timer
 * (period {@code Options.watermarkPeriod}, clock {@code Options.clock}) or through {@link #advance}, the manual
 * hook tests use.
 */
final class PipelineActor implements RowPublisher {
    private static final String WATERMARK_TIMER = "watermark";

    private final HostEnv env;
    private final String name;
    private final Publisher<List<EventRecord>> out;
    private final List<Supplier<CompletionStage<?>>> cleanups = new ArrayList<>();
    private Specs.PipelineSpec spec;
    private PipelineExecutor exec;
    private boolean running;
    private boolean exists;

    PipelineActor(HostEnv env, String name) {
        this.env = env;
        this.name = name;
        this.out = new Publisher<>(env);
    }

    void init() { exists = true; }

    void reset() {
        exists = false;
        out.clear();
    }

    CompletionStage<Void> start(ActorContext<PipelineActor> ctx, Specs.PipelineSpec spec) {
        if (running) return Async.DONE;
        this.spec = spec;
        this.exec = spec.plan().createExecutor();
        this.running = true;
        var self = ctx.self();
        var steps = new ArrayList<Supplier<CompletionStage<?>>>();
        for (var src : spec.sources()) {
            var in = new Specs.InputInfo(src, Specs.InputKind.SOURCE, 1);
            steps.add(() -> InputWiring.subscribeRows(env, self, in, (a, rows) -> a.onEvents(src, rows), cleanups));
        }
        var period = env.opts.watermarkPeriod();
        if (period != null) {
            // Started here, inside the turn (ctx is only valid on it); ticks queue behind this start turn.
            ctx.startTimer(WATERMARK_TIMER, period, period, c ->
                env.send(c.self(), x -> {
                    x.state().advance(env.opts.clock().getAsLong());
                    return io.streamsforge.actors.Turns.done();
                }));
        }
        return Async.sequence(steps).whenComplete((v, e) -> {
            if (e != null) {
                running = false;
                exec = null;
                runCleanups();
            }
        });
    }

    CompletionStage<Void> stop(ActorContext<PipelineActor> ctx) {
        ctx.cancelTimer(WATERMARK_TIMER);
        running = false;
        exec = null;
        return runCleanups();
    }

    private CompletionStage<Void> runCleanups() {
        var stages = new ArrayList<CompletionStage<?>>();
        for (var c : cleanups) stages.add(c.get());
        cleanups.clear();
        return Async.allOfQuiet(stages);
    }

    CompletionStage<Void> onEvents(String source, List<EventRecord> rows) {
        if (!running) return Async.DONE;
        List<EventRecord> results = null;
        for (var row : rows) {
            var r = exec.onEvent(source, row);
            if (!r.isEmpty()) {
                if (results == null) results = new ArrayList<>();
                results.addAll(r);
            }
        }
        if (results != null) out.publish(Collections.unmodifiableList(results));
        return Async.DONE;
    }

    /** Advance the watermark to {@code nowMs - lateness} and publish whatever closes. */
    void advance(long nowMs) {
        if (!running) return;
        var results = exec.advanceWatermark(nowMs);
        if (!results.isEmpty()) out.publish(Collections.unmodifiableList(new ArrayList<>(results)));
    }

    boolean exists() { return exists; }

    long lateEvents() { return exec == null ? 0 : exec.getLateEvents(); }

    @Override
    public long subscribeRows(Consumer<List<EventRecord>> consumer) {
        if (!exists) throw new CatalogException("unknown pipeline '" + name + "'");
        return out.subscribe(consumer);
    }

    @Override
    public boolean unsubscribeRows(long id) { return out.unsubscribe(id); }
}
