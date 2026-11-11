package io.streamsforge.engine.dd;

import io.streamsforge.dataflow.Collection;
import io.streamsforge.dataflow.InputHandle;
import io.streamsforge.dataflow.Keyed;
import io.streamsforge.dataflow.LongTime;
import io.streamsforge.dataflow.Pair;
import io.streamsforge.dataflow.Unit;
import io.streamsforge.dataflow.Weighted;
import io.streamsforge.dataflow.Worker;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.planning.CompiledTableJoin;
import io.streamsforge.engine.planning.CompiledTablePlan;
import io.streamsforge.engine.runtime.ExpressionEvaluator;
import io.streamsforge.engine.runtime.ExpressionEvaluator.EvalContext;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Translates one {@link CompiledTablePlan} (the engine's validated + planned table query) into a differential
 *  dataflow graph on a fresh {@link Worker}. Expression evaluation is the engine's own {@link ExpressionEvaluator}. */
final class GraphBuilder {
    private static final class LR {
        final Wr l, r;
        LR(Wr l, Wr r) { this.l = l; this.r = r; }
    }

    final Worker worker = new Worker();
    private final DataflowTableCompiler.Options options;

    GraphBuilder(DataflowTableCompiler.Options options) { this.options = options; }
    final Map<String, InputHandle<EventRecord, LongTime>> inputs = new LinkedHashMap<>();

    private Collection<EventRecord, LongTime> leaf(String name) {
        return inputs.computeIfAbsent(name, n -> worker.<EventRecord>newInput()).collection();
    }

    Collection<Out, LongTime> build(CompiledTablePlan p) {
        if (p.unionBranches != null) {
            Collection<Out, LongTime> acc = null;
            for (var b : p.unionBranches) {
                var c = build(b);
                acc = acc == null ? c : acc.concat(c);
            }
            return p.unionAll ? acc : acc.distinct();
        }

        var bindings = p.bindings;

        // FROM + JOIN chain.
        var s0 = p.sources.get(0);
        Collection<Wr, LongTime> acc = sourceWr(s0.alias, s0.sourceName, s0.derivedPlan);
        var sides = new ArrayList<WorkingRow.Side>();
        sides.add(new WorkingRow.Side(s0.alias, s0.schema));
        for (var j : p.joins) {
            acc = join(p, acc, j, sides);
            sides.add(new WorkingRow.Side(j.alias, j.schema));
        }

        if (p.where != null) {
            Expr where = p.where;
            acc = acc.filter(w -> ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(where, new EvalContext(w.row, bindings))));
        }

        if (p.groupBy == null && !p.hasAggregates) {
            return acc.map(w -> {
                var evt = new EventRecord();
                var ctx = new EvalContext(w.row, bindings);
                for (var item : p.output) evt.put(item.name, ExpressionEvaluator.eval(item.expression, ctx));
                evt.put(EventRecord.TIMESTAMP_FIELD, w.row.ts());
                evt.put(EventRecord.SOURCE_FIELD, p.sourceLabel);
                return new Out(evt);
            });
        }
        return reduce(p, acc);
    }

    // ---- GROUP BY / aggregates

    private Collection<Out, LongTime> reduce(CompiledTablePlan p, Collection<Wr, LongTime> rows) {
        var bindings = p.bindings;
        var groupBy = p.groupBy;
        var aggs = p.aggregateNodes;
        Collection<Pair<String, Aggs.Tup>, LongTime> kv = rows.map(w -> {
            var ctx = new EvalContext(w.row, bindings);
            var gv = new ArrayList<Object>();
            if (groupBy != null) for (var g : groupBy) gv.add(ExpressionEvaluator.eval(g, ctx));
            var args = new ArrayList<Object>(aggs.size());
            for (var a : aggs) args.add(a.isStar ? (Object) Boolean.TRUE : ExpressionEvaluator.eval(a.arg, ctx));
            return Pair.of(TableKeyEncoding.encodeGroupKey(gv.toArray()), new Aggs.Tup(gv, args));
        });
        return Keyed.of(kv).<Out>reduce((key, in, out) -> {
            long total = 0;
            for (Weighted<Aggs.Tup> w : in) total += w.weight();
            if (total <= 0) return; // a group with no rows has no output row (also for global aggregates)
            var results = new Object[aggs.size()];
            for (int i = 0; i < results.length; i++) results[i] = Aggs.compute(aggs.get(i), i, in, options.sqlStandardEmptySum());
            List<Object> groupValues = in.get(0).value().group();
            var evt = new EventRecord();
            var dummy = new WorkingRow(0, new ArrayList<>(), new LinkedHashMap<>());
            var ctx = new EvalContext(dummy, bindings, node -> results[p.aggregateIndex.get(node)]);
            for (var item : p.output) {
                evt.put(item.name, item.groupByIndex != null
                    ? groupValues.get(item.groupByIndex) : ExpressionEvaluator.eval(item.expression, ctx));
            }
            evt.put(EventRecord.TIMESTAMP_FIELD, 0L);
            evt.put(EventRecord.SOURCE_FIELD, p.sourceLabel);
            out.add(new Weighted<>(new Out(evt), 1));
        }).map(Pair::second);
    }

    // ---- sources / joins

    private Collection<Wr, LongTime> sourceWr(String alias, String sourceName, CompiledTablePlan derived) {
        if (derived == null) return leaf(sourceName).map(e -> new Wr(WorkingRow.fromEvent(alias, e)));
        return build(derived).map(o -> new Wr(WorkingRow.fromEvent(alias, o.row())));
    }

    private static String key1(Expr key, Wr w, Map<Expr, ExpressionEvaluator.Binding> bindings) {
        Object v = ExpressionEvaluator.eval(key, new EvalContext(w.row, bindings));
        return v == null ? null : TableKeyEncoding.encodeScalar(v);
    }

    private static String keyN(List<Expr> keys, Wr w, Map<Expr, ExpressionEvaluator.Binding> bindings) {
        var ctx = new EvalContext(w.row, bindings);
        var vals = new Object[keys.size()];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = ExpressionEvaluator.eval(keys.get(i), ctx);
            if (vals[i] == null) return null;
        }
        return TableKeyEncoding.encodeGroupKey(vals);
    }

    private static Keyed<String, Wr, LongTime> keyed(Collection<Wr, LongTime> c, java.util.function.Function<Wr, String> key) {
        return Keyed.of(c.map(w -> Pair.of(key.apply(w), w)).filter(pr -> pr.first() != null));
    }

    private Collection<Wr, LongTime> join(CompiledTablePlan p, Collection<Wr, LongTime> left, CompiledTableJoin j,
                                          List<WorkingRow.Side> sidesSoFar) {
        var bindings = p.bindings;
        switch (j.kind) {
            case UNNEST: {
                String key = j.alias + "_" + j.alias;
                return left.flatMap(w -> {
                    Object value = ExpressionEvaluator.eval(j.unnestExpr, new EvalContext(w.row, bindings));
                    var out = new ArrayList<Wr>();
                    if (value instanceof List<?> list) {
                        for (Object element : list) {
                            var fields = new LinkedHashMap<String, Object>(w.row.fields());
                            fields.put(key, element);
                            var aliases = new ArrayList<String>(w.row.aliases());
                            aliases.add(j.alias);
                            out.add(new Wr(new WorkingRow(w.row.ts(), aliases, fields)));
                        }
                    }
                    return out;
                });
            }
            case SEMI:
            case ANTI: {
                var right = sourceWr(j.alias, j.sourceName, j.derivedPlan);
                var kl = keyed(left, w -> key1(j.leftKey, w, bindings));
                Collection<String, LongTime> keys = right.map(w -> key1(j.rightKey, w, bindings)).filter(k -> k != null);
                var res = j.kind == JoinKind.SEMI ? kl.semijoin(keys.distinct()) : kl.antijoin(keys);
                return res.map(Pair::second);
            }
            case INNER:
            case CROSS: {
                var right = sourceWr(j.alias, j.sourceName, j.derivedPlan);
                var kl = keyed(left, w -> key1(j.leftKey, w, bindings));
                var kr = keyed(right, w -> key1(j.rightKey, w, bindings));
                Collection<Wr, LongTime> joined = kl.join(kr, (k, l, r) -> new Wr(WorkingRow.combine(l.row, r.row)));
                if (j.residual != null) {
                    Expr res = j.residual;
                    joined = joined.filter(w -> ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(res, new EvalContext(w.row, bindings))));
                }
                return joined;
            }
            case LEFT:
            case RIGHT:
            case FULL: {
                var right = sourceWr(j.alias, j.sourceName, j.derivedPlan);
                var kl = keyed(left, w -> keyN(j.leftKeys, w, bindings));
                var kr = keyed(right, w -> keyN(j.rightKeys, w, bindings));
                Collection<LR, LongTime> pairs = kl.join(kr, (k, l, r) -> new LR(l, r));
                if (j.residual != null) {
                    Expr res = j.residual;
                    pairs = pairs.filter(pr -> ExpressionEvaluator.isTrue(ExpressionEvaluator.eval(res,
                        new EvalContext(WorkingRow.combine(pr.l.row, pr.r.row), bindings))));
                }
                Collection<Wr, LongTime> out = pairs.map(pr -> new Wr(WorkingRow.combine(pr.l.row, pr.r.row)));
                if (j.kind == JoinKind.LEFT || j.kind == JoinKind.FULL) {
                    var nullRight = WorkingRow.nullSide(List.of(new WorkingRow.Side(j.alias, j.schema)));
                    var matched = pairs.map(pr -> pr.l).distinct();
                    var unmatched = Keyed.of(left.map(w -> Pair.of(w, Unit.U))).antijoin(matched);
                    out = out.concat(unmatched.map(pr -> new Wr(WorkingRow.combine(pr.first().row, nullRight))));
                }
                if (j.kind == JoinKind.RIGHT || j.kind == JoinKind.FULL) {
                    var nullLeft = WorkingRow.nullSide(new ArrayList<>(sidesSoFar));
                    var matched = pairs.map(pr -> pr.r).distinct();
                    var unmatched = Keyed.of(right.map(w -> Pair.of(w, Unit.U))).antijoin(matched);
                    out = out.concat(unmatched.map(pr -> new Wr(WorkingRow.combine(nullLeft, pr.first().row))));
                }
                return out;
            }
            default:
                throw new IllegalStateException("unsupported join kind " + j.kind);
        }
    }
}
