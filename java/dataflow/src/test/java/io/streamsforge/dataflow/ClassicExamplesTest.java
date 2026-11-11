package io.streamsforge.dataflow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ClassicExamplesTest {

    @Test
    void wordCountWithRetractionsOverEpochs() {
        Worker w = new Worker();
        InputHandle<String, LongTime> in = w.newInput();
        Capture<Pair<String, Long>, LongTime> out = in.collection().count().capture();
        Probe<LongTime> probe = in.collection().probe();

        for (String s : "the quick brown fox jumps over the lazy dog the end".split(" ")) in.insert(s);
        in.advanceTo(LongTime.of(1));
        w.step();
        assertTrue(out.done(LongTime.of(0)));
        assertEquals(Map.of(Pair.of("the", 3L), 1L, Pair.of("quick", 1L), 1L, Pair.of("brown", 1L), 1L,
                Pair.of("fox", 1L), 1L, Pair.of("jumps", 1L), 1L, Pair.of("over", 1L), 1L, Pair.of("lazy", 1L), 1L,
                Pair.of("dog", 1L), 1L, Pair.of("end", 1L), 1L), out.accumulate(LongTime.of(0)));

        // epoch 1: one more "the", retract "fox" and "end"
        in.insert("the");
        in.remove("fox");
        in.remove("end");
        in.advanceTo(LongTime.of(2));
        w.step();
        Map<Pair<String, Long>, Long> e1 = out.accumulate(LongTime.of(1));
        assertEquals(1L, e1.get(Pair.of("the", 4L)));
        assertNull(e1.get(Pair.of("the", 3L)));
        assertFalse(e1.keySet().stream().anyMatch(p -> p.first().equals("fox") || p.first().equals("end")));
        // the earlier epoch's view is unchanged (the collection is versioned)
        assertEquals(1L, out.accumulate(LongTime.of(0)).get(Pair.of("the", 3L)));

        // epoch 2: re-add fox twice
        in.insert("fox");
        in.insert("fox");
        in.advanceTo(LongTime.of(3));
        w.step();
        assertEquals(1L, out.accumulate(LongTime.of(2)).get(Pair.of("fox", 2L)));
        assertTrue(probe.done(LongTime.of(2)));
        assertFalse(probe.done(LongTime.of(3)));
    }

    // ---- reachability

    static Set<Integer> bfs(Set<Integer> roots, Map<Integer, Long> edgeCounts) {
        Map<Integer, List<Integer>> adj = new HashMap<>();
        for (var e : edgeCounts.entrySet()) if (e.getValue() > 0) {
            // keys are encoded src*1000+dst
            adj.computeIfAbsent(e.getKey() / 1000, k -> new java.util.ArrayList<>()).add(e.getKey() % 1000);
        }
        Set<Integer> seen = new HashSet<>(roots);
        ArrayDeque<Integer> q = new ArrayDeque<>(roots);
        while (!q.isEmpty()) for (int n : adj.getOrDefault(q.poll(), List.of())) if (seen.add(n)) q.add(n);
        return seen;
    }

    static Collection<Integer, LongTime> reach(Collection<Integer, LongTime> roots, Collection<Pair<Integer, Integer>, LongTime> edges) {
        return roots.iterate((scope, r) -> {
            Keyed<Integer, Integer, Product<LongTime, LongTime>> e = scope.enter(edges).keyed(Pair::first, Pair::second);
            return r.<Integer, Unit>keyed(n -> n, n -> Unit.U)
                    .join(e, (k, u, dst) -> dst)
                    .concat(scope.enter(roots))
                    .distinct();
        });
    }

    @Test
    void reachabilityWithEdgeRetractionDisconnects() {
        Worker w = new Worker();
        InputHandle<Integer, LongTime> roots = w.newInput();
        InputHandle<Pair<Integer, Integer>, LongTime> edges = w.newInput();
        Capture<Integer, LongTime> out = reach(roots.collection(), edges.collection()).capture();

        roots.insert(0);
        for (int[] e : new int[][] {{0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 5}, {5, 4}}) edges.insert(Pair.of(e[0], e[1]));
        roots.advanceTo(LongTime.of(1));
        edges.advanceTo(LongTime.of(1));
        w.step();
        assertEquals(Map.of(0, 1L, 1, 1L, 2, 1L, 3, 1L, 4, 1L, 5, 1L), out.accumulate(LongTime.of(0)));

        // cut 2->3: 3 becomes unreachable, 4 still reachable through 5
        edges.remove(Pair.of(2, 3));
        roots.advanceTo(LongTime.of(2));
        edges.advanceTo(LongTime.of(2));
        w.step();
        assertTrue(out.done(LongTime.of(1)));
        assertEquals(Map.of(0, 1L, 1, 1L, 2, 1L, 4, 1L, 5, 1L), out.accumulate(LongTime.of(1)));

        // cut 5->4: now 4 is unreachable as well
        edges.remove(Pair.of(5, 4));
        roots.advanceTo(LongTime.of(3));
        edges.advanceTo(LongTime.of(3));
        w.step();
        assertEquals(Map.of(0, 1L, 1, 1L, 2, 1L, 5, 1L), out.accumulate(LongTime.of(2)));

        // add a cycle and reconnect everything
        edges.insert(Pair.of(2, 3));
        edges.insert(Pair.of(3, 0));
        edges.insert(Pair.of(5, 4));
        roots.advanceTo(LongTime.of(4));
        edges.advanceTo(LongTime.of(4));
        w.step();
        assertEquals(Map.of(0, 1L, 1, 1L, 2, 1L, 3, 1L, 4, 1L, 5, 1L), out.accumulate(LongTime.of(3)));
    }

    // ---- connected components (min-label propagation)

    static Collection<Pair<Integer, Integer>, LongTime> components(Collection<Integer, LongTime> nodes, Collection<Pair<Integer, Integer>, LongTime> edges) {
        Collection<Pair<Integer, Integer>, LongTime> seed = nodes.map(n -> Pair.of(n, n));
        return seed.iterate((scope, labels) -> {
            Collection<Pair<Integer, Integer>, Product<LongTime, LongTime>> e = scope.enter(edges);
            Collection<Pair<Integer, Integer>, Product<LongTime, LongTime>> both = e.concat(e.map(p -> Pair.of(p.second(), p.first())));
            return Keyed.of(labels).join(both.keyed(Pair::first, Pair::second), (k, label, dst) -> Pair.of(dst, label))
                    .concat(scope.enter(seed))
                    .<Integer, Integer>keyed(Pair::first, Pair::second)
                    .<Integer>reduce((k, in, o) -> {
                        int min = Integer.MAX_VALUE;
                        for (Weighted<Integer> x : in) if (x.weight() > 0) min = Math.min(min, x.value());
                        o.add(new Weighted<>(min, 1));
                    })
                    .map(p -> Pair.of(p.first(), p.second()));
        });
    }

    static Map<Integer, Integer> unionFind(Set<Integer> nodes, Map<Pair<Integer, Integer>, Long> edges) {
        Map<Integer, Integer> parent = new HashMap<>();
        for (int n : nodes) parent.put(n, n);
        for (var e : edges.entrySet()) {
            if (e.getValue() <= 0) continue;
            int a = find(parent, e.getKey().first()), b = find(parent, e.getKey().second());
            if (a != b) parent.put(Math.max(a, b), Math.min(a, b));
        }
        Map<Integer, Integer> label = new HashMap<>();
        for (int n : nodes) label.put(n, find(parent, n));
        return label;
    }

    private static int find(Map<Integer, Integer> p, int x) {
        while (p.get(x) != x) x = p.get(x);
        return x;
    }

    @Test
    void connectedComponentsWithRetractions() {
        Worker w = new Worker();
        InputHandle<Integer, LongTime> nodes = w.newInput();
        InputHandle<Pair<Integer, Integer>, LongTime> edges = w.newInput();
        Capture<Pair<Integer, Integer>, LongTime> out = components(nodes.collection(), edges.collection()).capture();
        Set<Integer> ns = new HashSet<>();
        for (int i = 0; i < 8; i++) { nodes.insert(i); ns.add(i); }
        Map<Pair<Integer, Integer>, Long> es = new HashMap<>();
        int[][][] epochs = {
            {{1, 2}, {2, 3}, {4, 5}, {6, 7}, {5, 6}},   // {0} {1,2,3} {4,5,6,7}
            {{3, 4}},                                     // merge into {1..7}
            {{2, 3}, {5, 6}},                             // retract: splits {1,2} {3,4,5} {6,7}
            {{0, 7}, {1, 2}}                              // attach 0 to 7; duplicate 1-2
        };
        boolean[] retract = {false, false, true, false};
        for (int t = 0; t < epochs.length; t++) {
            for (int[] e : epochs[t]) {
                edges.update(Pair.of(e[0], e[1]), retract[t] ? -1 : 1);
                Support.add(es, Pair.of(e[0], e[1]), retract[t] ? -1 : 1);
            }
            nodes.advanceTo(LongTime.of(t + 1));
            edges.advanceTo(LongTime.of(t + 1));
            w.step();
            assertTrue(out.done(LongTime.of(t)));
            Map<Pair<Integer, Integer>, Long> expect = new HashMap<>();
            unionFind(ns, es).forEach((n, l) -> expect.put(Pair.of(n, l), 1L));
            assertEquals(expect, out.accumulate(LongTime.of(t)), "epoch " + t);
        }
    }
}
