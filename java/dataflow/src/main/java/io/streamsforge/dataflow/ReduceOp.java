package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Per-key group-by over an arrangement, correct for partially ordered times.
 *
 * <p>For each key the operator keeps {@code known}, the join-closed set of times at which the key's input
 * accumulation can change (all lattice joins of update times), and {@code pending}, the subset that must be
 * (re)evaluated because an update at or below them arrived since they were last evaluated. A pending time {@code t}
 * is evaluated once it is complete (no input frontier element is {@code <= t}): the input accumulated at {@code t}
 * goes through the user logic and the operator emits {@code desired - already emitted at or below t} at time
 * {@code t}. Pending times are processed in a linear extension of the order so that earlier emissions are visible to
 * later ones. Pending times are held as output capabilities so downstream frontiers cannot pass them.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
final class ReduceOp<K, V, R, T extends Time<T>> extends Op {
    final InPort in = addIn();
    final Output out = addOut();
    private final Trace<K, V, T> trace;
    private final Trace<K, V, T>.Reader reader;
    private final ReduceLogic<K, V, R> logic;
    private final Map<K, KeyState> states = new HashMap<>();
    private final Set<K> active = new LinkedHashSet<>();

    private final class KeyState {
        final Set<T> known = new HashSet<>();
        final TreeSet<T> pending = new TreeSet<>();
        List<Trace.Entry<R, T>> emitted = new ArrayList<>();
    }

    ReduceOp(Worker w, Trace<K, V, T> trace, ReduceLogic<K, V, R> logic) {
        super(w);
        this.trace = trace;
        this.reader = trace.newReader();
        this.logic = logic;
    }

    @Override boolean step() {
        Antichain<T> f = in.frontier;
        boolean did = false;
        Map<K, List<T>> fresh = new LinkedHashMap<>();
        Msg m;
        while ((m = in.queue.poll()) != null) {
            did = true;
            if (m.seq > 0) reader.consumed = m.seq;
            for (Update u : m.updates) {
                Pair<K, V> p = (Pair<K, V>) u.data();
                fresh.computeIfAbsent(p.first(), x -> new ArrayList<>()).add((T) u.time());
            }
        }
        reader.setSince(f);
        for (var e : fresh.entrySet()) {
            KeyState st = states.computeIfAbsent(e.getKey(), k -> new KeyState());
            addInteresting(st, e.getValue());
            active.add(e.getKey());
        }
        List<Update> res = new ArrayList<>();
        for (K k : new ArrayList<>(active)) {
            KeyState st = states.get(k);
            if (processKey(k, st, f, res)) did = true;
            if (st.pending.isEmpty()) active.remove(k);
        }
        out.send(res);
        if (did) trace.maintain();
        return did;
    }

    /** Adds the new update times, and every join of them with known times, to pending; keeps {@code known} join-closed. */
    private void addInteresting(KeyState st, List<T> newTimes) {
        java.util.ArrayDeque<T> queue = new java.util.ArrayDeque<>();
        for (T n : newTimes) {
            st.pending.add(n);
            st.known.add(n);
            queue.add(n); // always examined: it changes the accumulation at every known time at or above it
        }
        while (!queue.isEmpty()) {
            T n = queue.poll();
            for (Object o : st.known.toArray()) {
                T j = n.join((T) o);
                st.pending.add(j);
                if (st.known.add(j)) queue.add(j);
            }
        }
    }

    private boolean processKey(K k, KeyState st, Antichain<T> f, List<Update> res) {
        boolean did = false;
        List<T> todo = new ArrayList<>();
        for (T t : st.pending) if (!f.lessEqual(t)) todo.add(t);
        for (T t : todo) {
            did = true;
            st.pending.remove(t);
            Map<V, long[]> acc = new LinkedHashMap<>();
            trace.lookup(reader, k, e -> {
                if (e.time().lessEqual(t)) acc.computeIfAbsent(e.val(), x -> new long[1])[0] += e.diff();
            });
            List<Weighted<V>> input = new ArrayList<>(acc.size());
            for (var e : acc.entrySet()) if (e.getValue()[0] != 0) input.add(new Weighted<>(e.getKey(), e.getValue()[0]));
            Map<R, long[]> delta = new LinkedHashMap<>();
            if (!input.isEmpty()) {
                List<Weighted<R>> desired = new ArrayList<>();
                logic.apply(k, input, desired);
                for (Weighted<R> w : desired) delta.computeIfAbsent(w.value(), x -> new long[1])[0] += w.weight();
            }
            for (Trace.Entry<R, T> e : st.emitted) {
                if (e.time().lessEqual(t)) delta.computeIfAbsent(e.val(), x -> new long[1])[0] -= e.diff();
            }
            for (var e : delta.entrySet()) {
                long d = e.getValue()[0];
                if (d != 0) {
                    res.add(new Update(Pair.of(k, e.getKey()), t, d));
                    st.emitted.add(new Trace.Entry<>(e.getKey(), t, d));
                }
            }
        }
        if (did || st.known.size() > 8) compactState(k, st, f);
        return did;
    }

    private void compactState(K k, KeyState st, Antichain<T> f) {
        if (!f.isEmpty()) {
            Set<T> adv = new HashSet<>();
            for (T t : st.known) adv.add(t.advanceBy(f));
            st.known.clear();
            st.known.addAll(adv);
        }
        st.emitted = Trace.compact(st.emitted, f);
        if (st.pending.isEmpty() && st.emitted.isEmpty() && !trace.hasKey(reader, k)) {
            states.remove(k);
        }
    }

    @Override void capabilities(Consumer<Time> sink) {
        for (K k : active) for (T t : states.get(k).pending) sink.accept(t);
    }

    @Override long stateSize() {
        long n = 0;
        for (KeyState s : states.values()) n += s.known.size() + s.pending.size() + s.emitted.size();
        return n;
    }
}
