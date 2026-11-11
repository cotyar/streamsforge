package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;

/**
 * A keyed collection indexed into a shared {@link Trace}. Several joins and reduces may read one arrangement, each
 * through its own cursor, so the index is built and compacted once.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class Arranged<K, V, T extends Time<T>> {
    private final Worker worker;
    private final ArrangeOp<K, V, T> op;

    Arranged(Worker worker, Output source) {
        this.worker = worker;
        this.op = new ArrangeOp<>(worker);
        source.connect(op.in);
    }

    public Trace<K, V, T> trace() { return op.trace; }

    /** Current number of entries stored in the shared trace. */
    public int traceSize() { return op.trace.size(); }

    public Collection<Pair<K, V>, T> asCollection() {
        StatelessOp pass = new StatelessOp(worker, l -> l, null);
        op.out.connect(pass.addIn());
        return new Collection<>(worker, pass.out);
    }

    /** Bilinear join with another arrangement; the output time of a pair is the join of the two times. */
    public <V2, R> Collection<R, T> join(Arranged<K, V2, T> other, JoinFn<K, V, V2, R> fn) {
        JoinOp<K, V, V2, R, T> j = new JoinOp<>(worker, op.trace, other.op.trace, fn);
        op.out.connect(j.inA);
        other.op.out.connect(j.inB);
        return new Collection<>(worker, j.out);
    }

    /** Per-key reduction. Output records are {@code (key, r)}. */
    public <R> Collection<Pair<K, R>, T> reduce(ReduceLogic<K, V, R> logic) {
        ReduceOp<K, V, R, T> r = new ReduceOp<>(worker, op.trace, logic);
        op.out.connect(r.in);
        return new Collection<>(worker, r.out);
    }

    /** Sum of multiplicities per key. */
    public Collection<Pair<K, Long>, T> count() {
        return this.<Long>reduce((k, in, o) -> {
            long c = 0;
            for (Weighted<V> w : in) c += w.weight();
            if (c != 0) o.add(new Weighted<>(c, 1));
        });
    }
}
