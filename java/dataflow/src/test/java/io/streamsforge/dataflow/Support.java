package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

/** Test helpers: a signed multiset, a random update generator and naive recomputation helpers. */
final class Support {
    private Support() {}

    static <D> void add(Map<D, Long> m, D d, long diff) {
        long v = m.merge(d, diff, Long::sum);
        if (v == 0) m.remove(d);
    }

    /** Generates per-epoch random updates, retracting only records that are currently present. */
    static final class Gen<D> {
        final Random rnd;
        final Supplier<D> fresh;
        final Map<D, Long> state = new HashMap<>();
        final double retractProb;

        Gen(Random rnd, Supplier<D> fresh, double retractProb) {
            this.rnd = rnd;
            this.fresh = fresh;
            this.retractProb = retractProb;
        }

        /** Returns (record, diff) pairs for one epoch and applies them to {@link #state}. */
        List<Map.Entry<D, Long>> epoch(int maxUpdates) {
            List<Map.Entry<D, Long>> out = new ArrayList<>();
            int n = rnd.nextInt(maxUpdates + 1);
            for (int i = 0; i < n; i++) {
                if (!state.isEmpty() && rnd.nextDouble() < retractProb) {
                    List<D> present = new ArrayList<>(state.keySet());
                    present.sort((a, b) -> a.toString().compareTo(b.toString()));
                    D d = present.get(rnd.nextInt(present.size()));
                    add(state, d, -1);
                    out.add(Map.entry(d, -1L));
                } else {
                    D d = fresh.get();
                    add(state, d, 1);
                    out.add(Map.entry(d, 1L));
                }
            }
            return out;
        }
    }

    static <D> void feed(InputHandle<D, LongTime> in, List<Map.Entry<D, Long>> ups) {
        for (var e : ups) in.update(e.getKey(), e.getValue());
    }

    /** Naive group-by: {@code f} maps one key's (value -> count) multiset to its output multiset. */
    static <K, V, R> Map<Pair<K, R>, Long> naiveReduce(Map<Pair<K, V>, Long> in, Function<Map<V, Long>, Map<R, Long>> f) {
        Map<K, Map<V, Long>> byKey = new HashMap<>();
        for (var e : in.entrySet()) byKey.computeIfAbsent(e.getKey().first(), k -> new HashMap<>()).merge(e.getKey().second(), e.getValue(), Long::sum);
        Map<Pair<K, R>, Long> out = new HashMap<>();
        for (var e : byKey.entrySet()) {
            Map<V, Long> m = new HashMap<>(e.getValue());
            m.values().removeIf(v -> v == 0);
            if (m.isEmpty()) continue;
            for (var o : f.apply(m).entrySet()) if (o.getValue() != 0) add(out, Pair.of(e.getKey(), o.getKey()), o.getValue());
        }
        return out;
    }

    static <K, V1, V2> Map<Pair<K, Pair<V1, V2>>, Long> naiveJoin(Map<Pair<K, V1>, Long> a, Map<Pair<K, V2>, Long> b) {
        Map<Pair<K, Pair<V1, V2>>, Long> out = new HashMap<>();
        for (var x : a.entrySet()) {
            for (var y : b.entrySet()) {
                if (x.getKey().first().equals(y.getKey().first())) {
                    add(out, Pair.of(x.getKey().first(), Pair.of(x.getKey().second(), y.getKey().second())), x.getValue() * y.getValue());
                }
            }
        }
        return out;
    }
}
