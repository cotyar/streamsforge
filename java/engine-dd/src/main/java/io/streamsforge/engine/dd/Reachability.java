package io.streamsforge.engine.dd;

import io.streamsforge.dataflow.Capture;
import io.streamsforge.dataflow.Collection;
import io.streamsforge.dataflow.InputHandle;
import io.streamsforge.dataflow.LongTime;
import io.streamsforge.dataflow.Pair;
import io.streamsforge.dataflow.Time;
import io.streamsforge.dataflow.Unit;
import io.streamsforge.dataflow.Worker;
import io.streamsforge.engine.EventRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Recursive queries - the one thing table-mode SQL cannot express (the dialect has no {@code WITH RECURSIVE}; a table
 * query is a fixed-depth pipeline of operators) - written directly on the dataflow API with {@code iterate}, fed by the
 * same event model as everything else: an {@code edges} table whose rows are {@link EventRecord}s with Long
 * {@code src}/{@code dst}, inserted with weight +1 and retracted with -1.
 *
 * <pre>
 *   reachable(n) :- root(n).
 *   reachable(m) :- reachable(n), edge(n, m).          -- "WITH RECURSIVE reach AS (...)" in SQL
 *
 *   path(a, c)   :- edge(a, c).
 *   path(a, c)   :- path(a, b), edge(b, c).            -- transitive closure
 * </pre>
 *
 * Both are maintained incrementally: inserting an edge extends the result; retracting one removes exactly the nodes /
 * pairs that lose their last derivation (including around cycles), without recomputing from scratch.
 */
public final class Reachability {
    private Reachability() {}

    /** Nodes reachable from {@code roots} along {@code edges} (directed). Roots are reachable (zero-length path). */
    public static <T extends Time<T>> Collection<Long, T> reachable(Collection<Long, T> roots, Collection<Pair<Long, Long>, T> edges) {
        return roots.iterate((scope, reach) -> {
            var e = scope.enter(edges).<Long, Long>keyed(Pair::first, Pair::second);                  // (src, dst)
            return reach.<Long, Unit>keyed(n -> n, n -> Unit.U)
                .join(e, (src, u, dst) -> dst)                                                        // follow one edge
                .concat(scope.enter(roots))
                .distinct();                                                                          // a set, so the loop converges
        });
    }

    /** All pairs (a, c) such that a directed path of length >= 1 leads from a to c. */
    public static <T extends Time<T>> Collection<Pair<Long, Long>, T> transitiveClosure(Collection<Pair<Long, Long>, T> edges) {
        return edges.iterate((scope, paths) -> {
            var e = scope.enter(edges);
            var byEnd = paths.<Long, Long>keyed(Pair::second, Pair::first);                           // b -> a   for path(a, b)
            var out = e.<Long, Long>keyed(Pair::first, Pair::second);                                 // b -> c   for edge(b, c)
            return byEnd.join(out, (b, a, c) -> Pair.of(a, c)).concat(e).distinct();
        });
    }

    /** A running pair of recursive queries over one edge table and one root table, driven epoch by epoch. */
    public static final class Query {
        private final Worker worker = new Worker();
        private final InputHandle<EventRecord, LongTime> edgeIn = worker.newInput();
        private final InputHandle<EventRecord, LongTime> rootIn = worker.newInput();
        private final Capture<Long, LongTime> reachCap;
        private final Capture<Pair<Long, Long>, LongTime> closureCap;
        private long epoch;

        public Query() {
            Collection<Pair<Long, Long>, LongTime> edges = edgeIn.collection().map(r -> Pair.of((Long) r.get("src"), (Long) r.get("dst")));
            Collection<Long, LongTime> roots = rootIn.collection().map(r -> (Long) r.get("node"));
            reachCap = Reachability.reachable(roots, edges).capture();
            closureCap = Reachability.transitiveClosure(edges).capture();
        }

        public static EventRecord edge(long src, long dst) {
            var e = new EventRecord();
            e.put("src", src);
            e.put("dst", dst);
            return e;
        }

        public static EventRecord root(long node) {
            var e = new EventRecord();
            e.put("node", node);
            return e;
        }

        /** Adds (+1) or retracts (-1) an edge in the open epoch. */
        public void edge(long src, long dst, long weight) { edgeIn.update(edge(src, dst), weight); }

        public void root(long node, long weight) { rootIn.update(root(node), weight); }

        /** Closes the open epoch and runs the dataflow to quiescence. */
        public void commit() {
            epoch++;
            edgeIn.advanceTo(LongTime.of(epoch));
            rootIn.advanceTo(LongTime.of(epoch));
            worker.step();
        }

        /** Nodes reachable from the roots as of the last committed epoch. */
        public Set<Long> reachable() { return positive(reachCap.accumulate(LongTime.of(epoch - 1))); }

        /** Transitive closure as of the last committed epoch. */
        public Set<Pair<Long, Long>> closure() { return positive(closureCap.accumulate(LongTime.of(epoch - 1))); }

        public long stateSize() { return worker.stateSize(); }

        private static <X> Set<X> positive(Map<X, Long> m) {
            Set<X> s = new HashSet<>();
            for (var e : m.entrySet()) {
                if (e.getValue() < 0) throw new IllegalStateException("negative multiplicity for " + e.getKey());
                if (e.getValue() > 0) s.add(e.getKey());
            }
            return s;
        }

        /** Multiplicities, for tests that assert the result really is a set. */
        public Map<Long, Long> reachableWeights() { return new HashMap<>(reachCap.accumulate(LongTime.of(epoch - 1))); }
    }
}
