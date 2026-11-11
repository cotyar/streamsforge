package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Bilinear join over two arrangements: delta(A join B) = deltaA join B + A' join deltaB. Each side's batch is joined
 * against the other side's trace <em>as consumed so far by this join</em>, then marked consumed, so each pair of
 * updates is produced exactly once (by whichever of the two the join processes second). The output time of a pair is
 * the lattice join of the two times.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
final class JoinOp<K, V1, V2, R, T extends Time<T>> extends Op {
    final InPort inA = addIn();
    final InPort inB = addIn();
    final Output out = addOut();
    private final Trace<K, V1, T> traceA;
    private final Trace<K, V2, T> traceB;
    private final Trace<K, V1, T>.Reader readerA;
    private final Trace<K, V2, T>.Reader readerB;
    private final JoinFn<K, V1, V2, R> fn;

    JoinOp(Worker w, Trace<K, V1, T> a, Trace<K, V2, T> b, JoinFn<K, V1, V2, R> fn) {
        super(w);
        this.traceA = a;
        this.traceB = b;
        this.readerA = a.newReader();
        this.readerB = b.newReader();
        this.fn = fn;
    }

    @Override boolean step() {
        // Trace A is probed by future B updates, which are all at or beyond B's input frontier, and vice versa.
        readerA.setSince(inB.frontier);
        readerB.setSince(inA.frontier);
        List<Update> res = new ArrayList<>();
        boolean did = false;
        Msg m;
        while ((m = inA.queue.poll()) != null) {
            did = true;
            for (Update u : m.updates) {
                Pair<K, V1> p = (Pair<K, V1>) u.data();
                T t = (T) u.time();
                traceB.lookup(readerB, p.first(), e ->
                        res.add(new Update(fn.apply(p.first(), p.second(), e.val()), t.join(e.time()), u.diff() * e.diff())));
            }
            if (m.seq > 0) readerA.consumed = m.seq;
        }
        while ((m = inB.queue.poll()) != null) {
            did = true;
            for (Update u : m.updates) {
                Pair<K, V2> p = (Pair<K, V2>) u.data();
                T t = (T) u.time();
                traceA.lookup(readerA, p.first(), e ->
                        res.add(new Update(fn.apply(p.first(), e.val(), p.second()), e.time().join(t), e.diff() * u.diff())));
            }
            if (m.seq > 0) readerB.consumed = m.seq;
        }
        out.send(res);
        if (did) {
            traceA.maintain();
            traceB.maintain();
        }
        return did;
    }
}
