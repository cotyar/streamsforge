package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

/**
 * A stream of updates {@code (data, time, diff)}. Its contents at time {@code t} are the sum of diffs of updates
 * with {@code time <= t}. All operators return new collections wired into the same {@link Worker}'s graph.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class Collection<D, T extends Time<T>> {
    final Worker worker;
    final Output out;

    Collection(Worker worker, Output out) {
        this.worker = worker;
        this.out = out;
    }

    public Worker worker() { return worker; }

    private <R, T2 extends Time<T2>> Collection<R, T2> stateless(Function<List<Update>, List<Update>> fn, Function<Time, Time> summary) {
        StatelessOp op = new StatelessOp(worker, fn, summary);
        out.connect(op.addIn());
        return new Collection<>(worker, op.out);
    }

    // ---- stateless, per-update operators

    public <R> Collection<R, T> map(Function<? super D, ? extends R> f) {
        return stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) r.add(new Update(f.apply((D) u.data()), u.time(), u.diff()));
            return r;
        }, null);
    }

    public Collection<D, T> filter(Predicate<? super D> p) {
        return stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) if (p.test((D) u.data())) r.add(u);
            return r;
        }, null);
    }

    public <R> Collection<R, T> flatMap(Function<? super D, ? extends Iterable<? extends R>> f) {
        return stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) for (R x : f.apply((D) u.data())) r.add(new Update(x, u.time(), u.diff()));
            return r;
        }, null);
    }

    /** Flips the sign of every diff. */
    public Collection<D, T> negate() {
        return stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) r.add(new Update(u.data(), u.time(), -u.diff()));
            return r;
        }, null);
    }

    /** Union (sum) of two collections. */
    public Collection<D, T> concat(Collection<D, T> other) {
        StatelessOp op = new StatelessOp(worker, l -> l, null);
        out.connect(op.addIn());
        other.out.connect(op.addIn());
        return new Collection<>(worker, op.out);
    }

    /** Calls {@code f} on every update passing through, then forwards it unchanged. */
    public Collection<D, T> inspect(Consumer<Update<D, T>> f) {
        return stateless(l -> {
            for (Update u : l) f.accept(u);
            return l;
        }, null);
    }

    // ---- progress-aware

    /** Holds updates until their time is complete, then emits them summed per record. */
    public Collection<D, T> consolidate() {
        ConsolidateOp op = new ConsolidateOp(worker);
        out.connect(op.in);
        return new Collection<>(worker, op.out);
    }

    public Probe<T> probe() {
        SinkOp s = new SinkOp(worker, null);
        out.connect(s.in);
        return new Probe<>(worker, s.in);
    }

    public Capture<D, T> capture() { return new Capture<>(worker, out); }

    // ---- keyed / arranged

    public <K, V> Keyed<K, V, T> keyed(Function<? super D, ? extends K> key, Function<? super D, ? extends V> val) {
        return Keyed.of(this.<Pair<K, V>>map(d -> Pair.of(key.apply(d), val.apply(d))));
    }

    /** Multiplicity-thresholded set semantics: each record appears once if its count is positive. */
    public Collection<D, T> distinct() { return threshold(c -> c > 0 ? 1 : 0); }

    /** Replaces each record's multiplicity {@code c} with {@code f(c)} (records with {@code f(c) == 0} vanish). */
    public Collection<D, T> threshold(LongUnaryOperator f) {
        return this.<D, Unit>keyed(d -> d, d -> Unit.U)
                .<Unit>reduce((k, in, o) -> {
                    long c = 0;
                    for (Weighted<Unit> w : in) c += w.weight();
                    long r = f.applyAsLong(c);
                    if (r != 0) o.add(new Weighted<>(Unit.U, r));
                })
                .map(Pair::first);
    }

    /** Number of copies (sum of diffs) of each distinct record. */
    public Collection<Pair<D, Long>, T> count() {
        return this.<D, Unit>keyed(d -> d, d -> Unit.U).count();
    }

    // ---- scopes

    Collection<D, Product<T, LongTime>> enter() {
        return stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) r.add(new Update(u.data(), new Product((T) u.time(), LongTime.ZERO), u.diff()));
            return r;
        }, t -> new Product(t, LongTime.ZERO));
    }

    static <D, O extends Time<O>> Collection<D, O> leave(Collection<D, Product<O, LongTime>> c) {
        return c.stateless(l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) r.add(new Update(u.data(), ((Product) u.time()).outer(), u.diff()));
            return r;
        }, t -> ((Product) t).outer());
    }

    /**
     * Fixpoint iteration. The collection seeds a loop variable; on each iteration {@code body} maps the variable's
     * current value to its next one, until nothing changes. The result is the fixpoint, maintained incrementally as
     * this collection changes at later outer times (including retractions).
     *
     * <p>Inside the loop, times are {@code Product(outerTime, iteration)}. The variable at iteration {@code i+1}
     * equals {@code body(variable at iteration i)}: it is {@code seed + feedback(body(variable) - seed)} with the
     * feedback edge incrementing the iteration coordinate. {@code body} must converge (e.g. end with
     * {@link #distinct()}) or the worker never quiesces.
     */
    public Collection<D, T> iterate(IterateBody<D, T> body) {
        Collection<D, Product<T, LongTime>> source = enter();
        StatelessOp fb = new StatelessOp(worker, l -> {
            List<Update> r = new ArrayList<>(l.size());
            for (Update u : l) {
                Product p = (Product) u.time();
                r.add(new Update(u.data(), new Product(p.outer(), new LongTime(((LongTime) p.inner()).value() + 1)), u.diff()));
            }
            return r;
        }, t -> {
            Product p = (Product) t;
            return new Product(p.outer(), new LongTime(((LongTime) p.inner()).value() + 1));
        });
        Collection<D, Product<T, LongTime>> feedback = new Collection<>(worker, fb.out);
        Collection<D, Product<T, LongTime>> variable = source.concat(feedback);
        Collection<D, Product<T, LongTime>> result = body.apply(new IterScope<>(), variable);
        result.concat(source.negate()).out.connect(fb.addIn());
        return leave(result);
    }
}
