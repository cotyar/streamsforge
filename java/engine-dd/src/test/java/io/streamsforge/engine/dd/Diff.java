package io.streamsforge.engine.dd;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * Differential-test harness: feeds one randomly generated stream of inserts and retractions, epoch by epoch, to the
 * engine's {@link TableExecutor} and to the dataflow-compiled query, and compares the accumulated output (canonical
 * row key -> weight) after every epoch.
 */
final class Diff {
    private Diff() {}

    // ---- fixture sources -------------------------------------------------------------------------------------

    static SourceSchema schema(String name, Object... fieldAndKind) {
        var f = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < fieldAndKind.length; i += 2) f.put((String) fieldAndKind[i], (FieldKind) fieldAndKind[i + 1]);
        return new SourceSchema(name, f);
    }

    static final SourceSchema TRADES = schema("trades", "sym", FieldKind.STRING, "px", FieldKind.DOUBLE, "qty", FieldKind.LONG, "ok", FieldKind.BOOL, "fee", FieldKind.DOUBLE);
    static final SourceSchema QUOTES = schema("quotes", "sym", FieldKind.STRING, "bid", FieldKind.DOUBLE, "n", FieldKind.LONG);
    static final SourceSchema REF = schema("ref", "sym", FieldKind.STRING, "tag", FieldKind.STRING);
    static final SourceSchema STRUCTS = schema("structs", "id", FieldKind.STRING, "legs", FieldKind.JSON);

    static Map<String, SourceSchema> streams(SourceSchema... s) {
        var m = new LinkedHashMap<String, SourceSchema>();
        for (var x : s) m.put(x.name(), x);
        return m;
    }

    static final Map<String, SourceSchema> ALL = streams(TRADES, QUOTES, REF, STRUCTS);

    private static final String[] SYMS = {"A", "B", "C", "D"};

    private static Object sym(Random r) { return r.nextInt(100) < 7 ? null : SYMS[r.nextInt(SYMS.length)]; }

    /** Row generators; value domains are small so joins, groups and duplicates collide a lot. Doubles are multiples of
     *  0.25 so sums are exact in any order. */
    static final Map<String, Function<Random, EventRecord>> GEN = Map.of(
        "trades", r -> row(r, "sym", sym(r), "px", r.nextInt(100) < 10 ? null : r.nextInt(13) * 0.25,
            "qty", r.nextInt(100) < 10 ? null : (Object) (long) r.nextInt(5), "ok", r.nextBoolean(), "fee", r.nextInt(9) * 0.25),
        "quotes", r -> row(r, "sym", sym(r), "bid", r.nextInt(100) < 10 ? null : r.nextInt(13) * 0.25, "n", (long) r.nextInt(4)),
        "ref", r -> row(r, "sym", sym(r), "tag", r.nextInt(100) < 30 ? null : (r.nextBoolean() ? "x" : "y")),
        "structs", r -> {
            List<Object> legs = null;
            if (r.nextInt(100) >= 15) {
                legs = new ArrayList<>();
                for (int i = r.nextInt(4); i > 0; i--) legs.add((long) r.nextInt(3));
            }
            return row(r, "id", "s" + r.nextInt(3), "legs", legs);
        });

    private static EventRecord row(Random r, Object... kv) {
        var e = new EventRecord();
        e.put("_ts", (long) r.nextInt(5));
        for (int i = 0; i < kv.length; i += 2) e.put((String) kv[i], kv[i + 1]);
        return e;
    }

    // ---- the harness -----------------------------------------------------------------------------------------

    /** What a run covered. */
    record Stats(int cases, int epochs, long events, long retractions, long outputRows) {
        Stats plus(Stats o) {
            return new Stats(cases + o.cases, epochs + o.epochs, events + o.events, retractions + o.retractions, outputRows + o.outputRows);
        }
    }

    /** Mode of feeding the engine: one call per event, or one batch call per (source, epoch). */
    enum Feed { SINGLE, BATCH, SHUFFLED }

    static DataflowTablePlan plan(String sql, Map<String, SourceSchema> streams, Map<String, SourceSchema> tables) {
        var o = DataflowTableCompiler.compile(sql, streams, tables);
        if (!o.ok()) throw new AssertionError("compile of [" + sql + "] -> " + o.status() + ": " + o.message() + " " + o.diagnostics());
        return o.plan();
    }

    static Stats run(String sql, long baseSeed, int cases, int epochs) {
        return run(sql, ALL, null, baseSeed, cases, epochs, 6, null);
    }

    /** {@code feeds}: which engine feeding modes to rotate through (null = SINGLE and BATCH). */
    static Stats run(String sql, Map<String, SourceSchema> streams, Map<String, SourceSchema> tables, long baseSeed, int cases,
                     int epochs, int maxEventsPerEpoch, Feed[] feeds) {
        if (feeds == null) feeds = new Feed[] {Feed.SINGLE, Feed.BATCH};
        var compiled = io.streamsforge.engine.SqlCompiler.compileTable(sql, streams, tables == null ? Map.of() : tables);
        if (!compiled.ok()) throw new AssertionError("engine rejects [" + sql + "]: " + compiled.diagnostics());
        var ddPlan = plan(sql, streams, tables);
        Stats total = new Stats(0, 0, 0, 0, 0);
        for (int c = 0; c < cases; c++) {
            long seed = baseSeed * 100_003L + c;
            total = total.plus(runCase(sql, compiled.plan(), ddPlan, seed, epochs, maxEventsPerEpoch, feeds[c % feeds.length]));
        }
        return total;
    }

    private static Stats runCase(String sql, io.streamsforge.engine.TablePlan enginePlan, DataflowTablePlan ddPlan, long seed, int epochs,
                                 int maxEvents, Feed feed) {
        var rnd = new Random(seed);
        var engine = enginePlan.createExecutor();
        var dd = ddPlan.newQuery();
        var sources = new ArrayList<>(new LinkedHashSet<>(dd.inputNames()));
        var model = new LinkedHashMap<String, List<EventRecord>>();
        for (var s : sources) model.put(s, new ArrayList<>());

        long events = 0, retractions = 0, outRows = 0;
        for (int ep = 0; ep < epochs; ep++) {
            var batches = new LinkedHashMap<String, List<TableDelta>>();
            var epochDeltas = new ArrayList<Map.Entry<String, TableDelta>>();
            int n = rnd.nextInt(maxEvents + 1);
            for (int i = 0; i < n; i++) {
                String src = sources.get(rnd.nextInt(sources.size()));
                var live = model.get(src);
                TableDelta d;
                if (!live.isEmpty() && rnd.nextInt(100) < 35) {
                    var victim = live.remove(rnd.nextInt(live.size()));
                    d = new TableDelta(victim, -1);
                    retractions++;
                } else {
                    EventRecord e;
                    if (!live.isEmpty() && rnd.nextInt(100) < 15) e = live.get(rnd.nextInt(live.size())); // a duplicate (weight 2)
                    else e = GEN.get(src).apply(rnd);
                    live.add(e);
                    d = new TableDelta(e, 1);
                }
                events++;
                dd.update(src, d.row(), d.weight());
                batches.computeIfAbsent(src, k -> new ArrayList<>()).add(d);
                epochDeltas.add(Map.entry(src, d));
                if (feed == Feed.SINGLE) engine.onTableDelta(src, d);
            }
            if (feed == Feed.SHUFFLED) {
                java.util.Collections.shuffle(epochDeltas, rnd);
                for (var e : epochDeltas) engine.onTableDelta(e.getKey(), e.getValue());
            }
            if (feed == Feed.BATCH) for (var e : batches.entrySet()) engine.onTableDeltaBatch(e.getKey(), e.getValue());
            dd.commit();

            compare(sql, seed, ep, feed, engine.snapshot(), dd);
            outRows += engine.snapshot().size();
        }
        return new Stats(1, epochs, events, retractions, outRows);
    }

    static void compare(String sql, long seed, int epoch, Feed feed, Map<String, RowWeight> expected, DataflowTableQuery dd) {
        var actual = dd.snapshot();
        var raw = dd.rawWeights();
        var problems = new ArrayList<String>();
        Set<String> keys = new LinkedHashSet<>(expected.keySet());
        keys.addAll(actual.keySet());
        for (String k : keys) {
            var e = expected.get(k);
            var a = actual.get(k);
            if (e == null) problems.add("only in dataflow (w=" + a.weight() + "): " + k);
            else if (a == null) problems.add("only in engine (w=" + e.weight() + "): " + k);
            else if (e.weight() != a.weight()) problems.add("weight engine=" + e.weight() + " dataflow=" + a.weight() + ": " + k);
        }
        for (var e : raw.entrySet()) if (e.getValue() < 0) problems.add("dataflow holds a NEGATIVE weight " + e.getValue() + ": " + e.getKey());
        if (!problems.isEmpty()) {
            throw new AssertionError("MISMATCH sql=[" + sql + "] seed=" + seed + " epoch=" + epoch + " feed=" + feed + "\n  "
                + String.join("\n  ", problems.subList(0, Math.min(8, problems.size()))) + (problems.size() > 8 ? "\n  ... " + problems.size() + " total" : ""));
        }
    }

    /** Canonical snapshot of an engine executor, for ad-hoc tests. */
    static Map<String, Long> weights(TableExecutor e) {
        var m = new LinkedHashMap<String, Long>();
        for (var x : e.snapshot().entrySet()) m.put(x.getKey(), x.getValue().weight());
        return m;
    }
}
