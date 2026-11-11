package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An indexed history {@code key -> [(val, time, diff)]} shared by every operator reading one arrangement.
 *
 * <p>Storage is a list of immutable-ish batches in arrival order, merged geometrically like an LSM spine. Merging
 * performs <em>logical compaction</em>: every time is advanced to the trace's {@code since} frontier (the lower
 * envelope of what its readers still need to distinguish) and entries that then coincide are summed, vanishing when
 * they cancel. Readers are never shown more than they have consumed (a batch becomes visible to a reader only after
 * that reader processed the batch's message), which is what keeps a bilinear join from double counting; batches are
 * only merged once every reader has consumed them.
 */
public final class Trace<K, V, T extends Time<T>> {
    record Entry<V, T>(V val, T time, long diff) {}

    private static final class Batch<K, V, T> {
        long seq;
        Map<K, List<Entry<V, T>>> byKey = new HashMap<>();
        int size;
    }

    /** A cursor: how many batches a reader has consumed and which times it still needs distinguished. */
    public final class Reader {
        long consumed;
        Antichain<T> since;

        void setSince(Antichain<T> f) { since = f.copy(); }
    }

    private final List<Batch<K, V, T>> batches = new ArrayList<>();
    private final List<Reader> readers = new ArrayList<>();
    private long nextSeq = 1;

    Reader newReader() {
        Reader r = new Reader();
        readers.add(r);
        return r;
    }

    /** Total stored entries - the size metric compaction keeps bounded. */
    public int size() {
        int n = 0;
        for (Batch<K, V, T> b : batches) n += b.size;
        return n;
    }

    public int batchCount() { return batches.size(); }

    /** Inserts a consolidated batch of updates and returns its sequence number. */
    long add(List<Update<Pair<K, V>, T>> updates) {
        Batch<K, V, T> b = new Batch<>();
        b.seq = nextSeq++;
        for (Update<Pair<K, V>, T> u : updates) {
            b.byKey.computeIfAbsent(u.data().first(), k -> new ArrayList<>(2)).add(new Entry<>(u.data().second(), u.time(), u.diff()));
            b.size++;
        }
        batches.add(b);
        return b.seq;
    }

    /** Visits the entries for {@code key} in the batches {@code r} has consumed. */
    void lookup(Reader r, K key, Consumer<Entry<V, T>> f) {
        for (int i = 0; i < batches.size(); i++) {
            Batch<K, V, T> b = batches.get(i);
            if (b.seq > r.consumed) break;
            List<Entry<V, T>> l = b.byKey.get(key);
            if (l != null) for (int j = 0; j < l.size(); j++) f.accept(l.get(j));
        }
    }

    boolean hasKey(Reader r, K key) {
        for (Batch<K, V, T> b : batches) {
            if (b.seq > r.consumed) break;
            if (b.byKey.containsKey(key)) return true;
        }
        return false;
    }

    /** Merges consumed batches (compacting them) when the geometric rule says so. */
    void maintain() {
        if (batches.size() < 2) return;
        long minConsumed = Long.MAX_VALUE;
        Antichain<T> since = new Antichain<>();
        for (Reader r : readers) {
            if (r.since == null) return; // a reader hasn't reported yet: stay conservative
            minConsumed = Math.min(minConsumed, r.consumed);
            for (T t : r.since.elements()) since.insert(t);
        }
        int n = 0;
        while (n < batches.size() && batches.get(n).seq <= minConsumed) n++;
        // cascade-merge the tail of the consumed prefix while sizes are comparable
        while (n >= 2 && batches.get(n - 1).size * 2 >= batches.get(n - 2).size) {
            Batch<K, V, T> merged = merge(batches.get(n - 2), batches.get(n - 1), since);
            batches.remove(n - 1);
            if (merged.size == 0) {
                batches.remove(n - 2);
                n -= 2;
            } else {
                batches.set(n - 2, merged);
                n--;
            }
        }
    }

    private Batch<K, V, T> merge(Batch<K, V, T> a, Batch<K, V, T> b, Antichain<T> since) {
        Batch<K, V, T> out = new Batch<>();
        out.seq = Math.max(a.seq, b.seq);
        Map<K, Map<Entry<V, T>, long[]>> acc = new HashMap<>();
        for (Batch<K, V, T> src : List.of(a, b)) {
            for (var e : src.byKey.entrySet()) {
                Map<Entry<V, T>, long[]> m = acc.computeIfAbsent(e.getKey(), k -> new HashMap<>());
                for (Entry<V, T> en : e.getValue()) {
                    T t = since.isEmpty() ? en.time() : en.time().advanceBy(since);
                    m.computeIfAbsent(new Entry<>(en.val(), t, 0), x -> new long[1])[0] += en.diff();
                }
            }
        }
        for (var e : acc.entrySet()) {
            List<Entry<V, T>> l = new ArrayList<>();
            for (var d : e.getValue().entrySet()) {
                if (d.getValue()[0] != 0) l.add(new Entry<>(d.getKey().val(), d.getKey().time(), d.getValue()[0]));
            }
            if (!l.isEmpty()) {
                out.byKey.put(e.getKey(), l);
                out.size += l.size();
            }
        }
        return out;
    }

    /** Advances and consolidates a free-standing history (used for operator-private output histories). */
    static <V, T extends Time<T>> List<Entry<V, T>> compact(List<Entry<V, T>> l, Antichain<T> since) {
        if (l.size() < 2 && since.isEmpty()) return l;
        Map<Entry<V, T>, long[]> m = new HashMap<>();
        for (Entry<V, T> en : l) {
            T t = since.isEmpty() ? en.time() : en.time().advanceBy(since);
            m.computeIfAbsent(new Entry<>(en.val(), t, 0), x -> new long[1])[0] += en.diff();
        }
        List<Entry<V, T>> out = new ArrayList<>(m.size());
        for (var d : m.entrySet()) if (d.getValue()[0] != 0) out.add(new Entry<>(d.getKey().val(), d.getKey().time(), d.getValue()[0]));
        return out;
    }
}
