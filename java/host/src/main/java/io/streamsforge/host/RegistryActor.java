package io.streamsforge.host;

import io.streamsforge.engine.CompileResult;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableCompileResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * The catalog (Orleans {@code RegistryGrain}): ONE non-reentrant activation, key {@code registry/default}. Every
 * catalog operation is a turn that returns the stage of the entity-actor calls it makes, so two operations never
 * interleave — {@code createTable} sees a consistent set of names, {@code start} a consistent set of running
 * entities.
 *
 * <p>Hard rule 3 (orchestrator &lt;-&gt; worker call cycles deadlock without a {@code [MayInterleave]} allowlist) is
 * satisfied by construction instead of by allowlist: the registry hands every entity a complete immutable spec
 * ({@link Specs}) and NO entity actor ever calls the registry. There is no cycle to allowlist.
 *
 * <p>Name namespace: sources, pipelines and tables share one, so a SQL identifier means one thing. A pipeline reads
 * sources only (hard rule 6: this is what keeps the graph acyclic); a table reads sources, pipelines (as a stream
 * relation with the pipeline's output schema) and other tables.
 */
final class RegistryActor {
    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private record SourceRec(SourceSchema schema) {}

    private static final class PipelineRec {
        final String sql;
        final CompileResult compiled;
        boolean running;

        PipelineRec(String sql, CompileResult compiled) {
            this.sql = sql;
            this.compiled = compiled;
        }
    }

    private static final class TableRec {
        final String sql;
        final int parallelism;
        final TableCompileResult compiled;
        boolean running;

        TableRec(String sql, int parallelism, TableCompileResult compiled) {
            this.sql = sql;
            this.parallelism = parallelism;
            this.compiled = compiled;
        }
    }

    private final HostEnv env;
    private final Map<String, Object> entities = new LinkedHashMap<>();

    RegistryActor(HostEnv env) { this.env = env; }

    // ------------------------------------------------------------------ create

    CompletionStage<Void> createSource(String name, SourceSchema schema) {
        checkNewName(name);
        var s = new SourceSchema(name, new LinkedHashMap<>(schema.fields()));
        return env.exec(env.<SourceActor>ref(Actors.SOURCE, name), a -> a.init(s)).thenAccept(v -> entities.put(name, new SourceRec(s)));
    }

    CompletionStage<Void> createPipeline(String name, String sql) {
        checkNewName(name);
        var sources = new LinkedHashMap<String, SourceSchema>();
        for (var e : entities.entrySet()) if (e.getValue() instanceof SourceRec r) sources.put(e.getKey(), r.schema());
        CompileResult cr = SqlCompiler.compile(sql, sources);
        if (!cr.ok() || cr.plan() == null) throw new CatalogException("invalid pipeline SQL", cr.diagnostics());
        return env.exec(env.<PipelineActor>ref(Actors.PIPELINE, name), PipelineActor::init)
            .thenAccept(v -> entities.put(name, new PipelineRec(sql, cr)));
    }

    CompletionStage<Integer> createTable(String name, String sql, int parallelism) {
        checkNewName(name);
        if (parallelism < 1) throw new CatalogException("parallelism must be >= 1");
        var streams = new LinkedHashMap<String, SourceSchema>();
        var tables = new LinkedHashMap<String, SourceSchema>();
        for (var e : entities.entrySet()) {
            if (e.getValue() instanceof SourceRec r) streams.put(e.getKey(), r.schema());
            // A pipeline is a stream relation whose schema is its output schema (hard rule 6).
            else if (e.getValue() instanceof PipelineRec p) streams.put(e.getKey(), new SourceSchema(e.getKey(), p.compiled.outputSchema().fields()));
            else if (e.getValue() instanceof TableRec t) tables.put(e.getKey(), new SourceSchema(e.getKey(), t.compiled.outputSchema().fields()));
        }
        TableCompileResult cr = SqlCompiler.compileTable(sql, streams, tables);
        if (!cr.ok() || cr.plan() == null) throw new CatalogException("invalid table SQL", cr.diagnostics());
        if (parallelism >= 2) {
            try {
                cr.plan().createDataflow(parallelism);
            } catch (UnsupportedOperationException e) {
                throw new CatalogException("table cannot run with parallelism " + parallelism + ": " + e.getMessage());
            }
        }
        var rec = new TableRec(sql, parallelism, cr);
        CompletionStage<Void> init = env.exec(env.<TableActor>ref(Actors.TABLE, name), TableActor::init);
        if (parallelism >= 2) init = init.thenCompose(v -> env.exec(env.<TableOutputActor>ref(Actors.OUTPUT, name), TableOutputActor::init));
        return init.thenApply(v -> {
            entities.put(name, rec);
            return parallelism;
        });
    }

    // ------------------------------------------------------------------ lifecycle

    CompletionStage<Void> start(String name) {
        Object e = lookup(name);
        if (e instanceof SourceRec) return env.exec(env.<SourceActor>ref(Actors.SOURCE, name), a -> a.setAccepting(true));
        if (e instanceof PipelineRec p) {
            if (p.running) return Async.DONE;
            var spec = new Specs.PipelineSpec(name, p.compiled.plan(), p.compiled.sourceNames());
            return env.<PipelineActor>ref(Actors.PIPELINE, name).ask(c -> c.state().start(c, spec)).thenAccept(v -> p.running = true);
        }
        var t = (TableRec) e;
        if (t.running) return Async.DONE;
        for (var up : t.compiled.tableInputs()) {
            if (!(entities.get(up) instanceof TableRec ut) || !ut.running) {
                throw new CatalogException("cannot start table '" + name + "': upstream table '" + up + "' is not running");
            }
        }
        var inputs = new LinkedHashMap<String, Specs.InputInfo>();
        for (var in : t.compiled.streamInputs()) {
            inputs.put(in, new Specs.InputInfo(in, entities.get(in) instanceof PipelineRec ? Specs.InputKind.PIPELINE : Specs.InputKind.SOURCE, 1));
        }
        for (var in : t.compiled.tableInputs()) {
            inputs.put(in, new Specs.InputInfo(in, Specs.InputKind.TABLE, ((TableRec) entities.get(in)).parallelism));
        }
        var spec = new Specs.TableSpec(name, t.parallelism, t.compiled.plan(), List.copyOf(t.compiled.streamInputs()),
            List.copyOf(t.compiled.tableInputs()), inputs, t.compiled.outputSchema());
        return env.<TableActor>ref(Actors.TABLE, name).ask(c -> c.state().start(c, spec)).thenAccept(v -> t.running = true);
    }

    CompletionStage<Void> stop(String name) {
        Object e = lookup(name);
        if (e instanceof SourceRec) return env.exec(env.<SourceActor>ref(Actors.SOURCE, name), a -> a.setAccepting(false));
        if (e instanceof PipelineRec p) {
            if (!p.running) return Async.DONE;
            return env.<PipelineActor>ref(Actors.PIPELINE, name).ask(c -> c.state().stop(c)).thenAccept(v -> p.running = false);
        }
        var t = (TableRec) e;
        if (!t.running) return Async.DONE;
        // A downstream table is seeded from this one; restarting this one would re-emit every row into it. State is
        // not persisted and there is no "rebuilding" handshake, so the dependent must be stopped first.
        for (var o : entities.entrySet()) {
            if (o.getValue() instanceof TableRec d && d.running && d.compiled.tableInputs().contains(name)) {
                throw new CatalogException("cannot stop table '" + name + "': running table '" + o.getKey() + "' reads it");
            }
        }
        return env.<TableActor>ref(Actors.TABLE, name).ask(c -> c.state().stop()).thenAccept(v -> t.running = false);
    }

    CompletionStage<Void> delete(String name) {
        Object e = lookup(name);
        for (var o : entities.entrySet()) {
            if (o.getKey().equals(name)) continue;
            if (references(o.getValue(), name)) {
                throw new CatalogException("cannot delete '" + name + "': '" + o.getKey() + "' reads it");
            }
        }
        CompletionStage<Void> stopped = (e instanceof SourceRec) ? Async.DONE : stop(name);
        return stopped.thenCompose(v -> {
            if (e instanceof SourceRec) return env.exec(env.<SourceActor>ref(Actors.SOURCE, name), SourceActor::reset);
            if (e instanceof PipelineRec) return env.exec(env.<PipelineActor>ref(Actors.PIPELINE, name), PipelineActor::reset);
            int p = ((TableRec) e).parallelism;
            return env.<TableActor>ref(Actors.TABLE, name).ask(c -> c.state().reset(p));
        }).thenAccept(v -> entities.remove(name));
    }

    private static boolean references(Object entity, String name) {
        if (entity instanceof PipelineRec p) return p.compiled.sourceNames().contains(name);
        if (entity instanceof TableRec t) return t.compiled.streamInputs().contains(name) || t.compiled.tableInputs().contains(name);
        return false;
    }

    // ------------------------------------------------------------------ queries

    List<StreamsHost.EntityInfo> list() {
        var out = new ArrayList<StreamsHost.EntityInfo>();
        for (var e : entities.entrySet()) {
            Object v = e.getValue();
            if (v instanceof SourceRec) out.add(new StreamsHost.EntityInfo(e.getKey(), "source", true, 1));
            else if (v instanceof PipelineRec p) out.add(new StreamsHost.EntityInfo(e.getKey(), "pipeline", p.running, 1));
            else if (v instanceof TableRec t) out.add(new StreamsHost.EntityInfo(e.getKey(), "table", t.running, t.parallelism));
        }
        return out;
    }

    int tableParallelism(String name) {
        if (entities.get(name) instanceof TableRec t) return t.parallelism;
        throw new CatalogException("unknown table '" + name + "'");
    }

    void requirePipeline(String name) {
        if (!(entities.get(name) instanceof PipelineRec)) throw new CatalogException("unknown pipeline '" + name + "'");
    }

    /** Running parallelism &gt;= 2 tables and the ingest-actor ids of each (for {@code StreamsHost.quiesce}). */
    Map<String, List<String>> dataflowTables() {
        var out = new LinkedHashMap<String, List<String>>();
        for (var e : entities.entrySet()) {
            if (e.getValue() instanceof TableRec t && t.running && t.parallelism >= 2) {
                var ids = new ArrayList<String>();
                for (var in : t.compiled.streamInputs()) ids.add(Actors.ingestId(e.getKey(), in));
                for (var in : t.compiled.tableInputs()) if (!ids.contains(Actors.ingestId(e.getKey(), in))) ids.add(Actors.ingestId(e.getKey(), in));
                out.put(e.getKey(), ids);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ helpers

    private Object lookup(String name) {
        Object e = entities.get(name);
        if (e == null) throw new CatalogException("unknown entity '" + name + "'");
        return e;
    }

    private void checkNewName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new CatalogException("invalid name '" + name + "': must be a SQL identifier ([A-Za-z_][A-Za-z0-9_]*)");
        }
        if (entities.containsKey(name)) {
            throw new CatalogException("name '" + name + "' is already used by a " + kindOf(entities.get(name))
                + " (sources, pipelines and tables share one namespace)");
        }
    }

    private static String kindOf(Object e) {
        return e instanceof SourceRec ? "source" : e instanceof PipelineRec ? "pipeline" : "table";
    }
}
