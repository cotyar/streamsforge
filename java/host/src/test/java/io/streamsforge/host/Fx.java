package io.streamsforge.host;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.futures.FuturesActorRuntime;
import io.streamsforge.actors.loom.LoomActorRuntime;
import io.streamsforge.actors.pekko.PekkoActorRuntime;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/** Test fixture: one runtime + one host, plus blocking helpers and an engine-only oracle. */
final class Fx implements AutoCloseable {
    /** A named runtime factory (the parameter of every scenario test). */
    record Rt(String name, Supplier<ActorRuntime> make) {
        @Override
        public String toString() { return name; }
    }

    static Stream<Arguments> runtimes() {
        return Stream.of(
            Arguments.of(new Rt("futures", FuturesActorRuntime::new)),
            Arguments.of(new Rt("loom", LoomActorRuntime::new)),
            Arguments.of(new Rt("pekko", PekkoActorRuntime::new)));
    }

    static final Duration DEADLINE = Duration.ofSeconds(60);
    static final SourceSchema TRADES = schema("trades", "symbol", FieldKind.STRING, "price", FieldKind.DOUBLE, "qty", FieldKind.LONG);
    static final SourceSchema QUOTES = schema("quotes", "symbol", FieldKind.STRING, "bid", FieldKind.DOUBLE);
    static final SourceSchema REF = schema("ref", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);

    final ActorRuntime rt;
    final StreamsHost host;

    Fx(Rt rt) { this(rt, StreamsHost.Options.defaults().withFlushPeriod(Duration.ofMillis(10)).withWatermarkPeriod(null)); }

    Fx(Rt rt, StreamsHost.Options options) {
        this.rt = rt.make().get();
        this.host = new StreamsHost(this.rt, options);
    }

    @Override
    public void close() { rt.close(); }

    // ------------------------------------------------------------------ blocking helpers

    static <T> T get(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(DEADLINE.toSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable c = Async.unwrap(e);
            if (c instanceof RuntimeException r) throw r;
            if (c instanceof Error err) throw err;
            throw new RuntimeException(c);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    void quiesce() { host.quiesce(DEADLINE); }

    void source(SourceSchema s) { get(host.createSource(s.name(), s)); }

    void pipeline(String name, String sql) {
        get(host.createPipeline(name, sql));
        get(host.start(name));
    }

    void table(String name, String sql, int parallelism) {
        get(host.createTable(name, sql, parallelism));
        get(host.start(name));
    }

    void publish(String source, List<EventRecord> rows) { get(host.publish(source, rows)); }

    /** Polls until true or the deadline (generous: this is a safety net, not a timing assertion). */
    static void await(String what, BooleanSupplier cond) {
        long end = System.nanoTime() + DEADLINE.toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > end) throw new AssertionError("timed out waiting for " + what);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // ------------------------------------------------------------------ data

    static SourceSchema schema(String name, Object... fieldAndKind) {
        var fields = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < fieldAndKind.length; i += 2) fields.put((String) fieldAndKind[i], (FieldKind) fieldAndKind[i + 1]);
        return new SourceSchema(name, fields);
    }

    static EventRecord ev(long ts, String source, Object... fieldAndValue) {
        var e = new EventRecord();
        e.put("_ts", ts);
        e.put("_source", source);
        for (int i = 0; i < fieldAndValue.length; i += 2) e.put((String) fieldAndValue[i], fieldAndValue[i + 1]);
        return e;
    }

    /** canonical row key -> weight, sorted: the form every snapshot comparison uses. */
    static Map<String, Long> canon(Map<String, RowWeight> snapshot) {
        var m = new TreeMap<String, Long>();
        for (var e : snapshot.entrySet()) m.put(TableExecutor.canonicalRowKeyOf(e.getValue().row()), e.getValue().weight());
        return m;
    }

    Map<String, Long> snap(String table) { return canon(get(host.snapshot(table))); }

    // ------------------------------------------------------------------ oracle (engine only, single thread)

    /** A plain engine TableExecutor over the given streams/tables, for expected values. */
    static TableExecutor oracle(String sql, SourceSchema[] streams, SourceSchema[] tables) {
        var st = new LinkedHashMap<String, SourceSchema>();
        for (var s : streams) st.put(s.name(), s);
        var tb = new LinkedHashMap<String, SourceSchema>();
        for (var s : tables) tb.put(s.name(), s);
        var cr = SqlCompiler.compileTable(sql, st, tb);
        if (!cr.ok()) throw new AssertionError("oracle SQL failed: " + cr.diagnostics());
        return cr.plan().createExecutor();
    }

    static TableExecutor oracle(String sql, SourceSchema... streams) { return oracle(sql, streams, new SourceSchema[0]); }

    /** Output schema of table SQL over the given streams (to build a downstream oracle's table schema). */
    static SourceSchema compileOut(String sql, SourceSchema... streams) {
        var st = new LinkedHashMap<String, SourceSchema>();
        for (var s : streams) st.put(s.name(), s);
        return SqlCompiler.compileTable(sql, st, Map.of()).outputSchema();
    }

    static Map<String, Long> canonOf(TableExecutor e) { return canon(e.snapshot()); }

    /** Applies a table's collected output batches to a map (what a delta consumer would hold). */
    static Map<String, Long> fold(List<TableDeltaBatch> batches) {
        var net = new TreeMap<String, Long>();
        for (var b : batches) {
            for (TableDelta d : b.deltas()) net.merge(TableExecutor.canonicalRowKeyOf(d.row()), d.weight(), Long::sum);
        }
        net.values().removeIf(w -> w == 0);
        return net;
    }

    static <T> List<T> sync(List<T> l) { return java.util.Collections.synchronizedList(l); }

    static List<EventRecord> copyOf(List<EventRecord> l) { return new ArrayList<>(l); }
}
