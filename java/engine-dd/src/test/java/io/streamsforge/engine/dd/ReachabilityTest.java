package io.streamsforge.engine.dd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.dataflow.Pair;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.SqlCompiler;
import io.streamsforge.engine.TableDelta;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReachabilityTest {
    private static Set<Long> set(Long... xs) { return new HashSet<>(List.of(xs)); }

    @Test
    void worksThroughEdgeInsertionRetractionAndACycle() {
        var q = new Reachability.Query();
        q.root(1, 1);
        q.edge(1, 2, 1);
        q.edge(2, 3, 1);
        q.edge(4, 5, 1);
        q.commit();
        assertEquals(set(1L, 2L, 3L), q.reachable());

        q.edge(3, 1, 1);                       // closes a cycle 1 -> 2 -> 3 -> 1
        q.edge(3, 4, 1);                       // extends the frontier to 4 and 5
        q.commit();
        assertEquals(set(1L, 2L, 3L, 4L, 5L), q.reachable());

        q.edge(1, 2, -1);                      // cut 1 -> 2: everything but the root is disconnected, although 2,3 still form a path to 1
        q.commit();
        assertEquals(set(1L), q.reachable());

        q.edge(1, 2, 1);                       // reconnect
        q.commit();
        assertEquals(set(1L, 2L, 3L, 4L, 5L), q.reachable());
        q.reachableWeights().values().forEach(w -> assertEquals(1L, w, "result is a set"));
    }

    @Test
    void duplicateEdgesNeedBothCopiesRetracted() {
        var q = new Reachability.Query();
        q.root(1, 1);
        q.edge(1, 2, 2);                       // edge asserted twice
        q.commit();
        assertEquals(set(1L, 2L), q.reachable());
        q.edge(1, 2, -1);
        q.commit();
        assertEquals(set(1L, 2L), q.reachable());
        q.edge(1, 2, -1);
        q.commit();
        assertEquals(set(1L), q.reachable());
    }

    // ---- randomized, against naive recomputation -------------------------------------------------------------

    private static Set<Long> bfs(Set<Long> roots, Map<Pair<Long, Long>, Integer> edges) {
        var seen = new HashSet<>(roots);
        var queue = new ArrayDeque<>(roots);
        while (!queue.isEmpty()) {
            long n = queue.poll();
            for (var e : edges.keySet()) if (e.first() == n && seen.add(e.second())) queue.add(e.second());
        }
        return seen;
    }

    private static Set<Pair<Long, Long>> closure(Map<Pair<Long, Long>, Integer> edges) {
        Set<Pair<Long, Long>> paths = new HashSet<>(edges.keySet());
        boolean grew = true;
        while (grew) {
            grew = false;
            for (var p : new ArrayList<>(paths)) {
                for (var e : edges.keySet()) {
                    if (e.first().equals(p.second()) && paths.add(Pair.of(p.first(), e.second()))) grew = true;
                }
            }
        }
        return paths;
    }

    @Test
    void randomizedAgainstNaiveRecomputation() {
        int cases = Integer.getInteger("dd.cases", 300), epochs = 8;
        long events = 0, retractions = 0, maxClosure = 0;
        for (int c = 0; c < cases; c++) {
            var rnd = new Random(9000 + c);
            var q = new Reachability.Query();
            var edges = new LinkedHashMap<Pair<Long, Long>, Integer>();
            var roots = new HashSet<Long>();
            for (int ep = 0; ep < epochs; ep++) {
                for (int i = rnd.nextInt(7); i > 0; i--) {
                    events++;
                    if (rnd.nextInt(10) == 0) {                         // toggle a root
                        long n = rnd.nextInt(7);
                        if (roots.remove(n)) q.root(n, -1); else { roots.add(n); q.root(n, 1); }
                    } else if (!edges.isEmpty() && rnd.nextInt(100) < 40) {  // retract an existing edge copy
                        var keys = new ArrayList<>(edges.keySet());
                        var victim = keys.get(rnd.nextInt(keys.size()));
                        if (edges.merge(victim, -1, Integer::sum) == 0) edges.remove(victim);
                        q.edge(victim.first(), victim.second(), -1);
                        retractions++;
                    } else {                                            // insert (self loops and duplicates allowed)
                        var e = Pair.of((long) rnd.nextInt(7), (long) rnd.nextInt(7));
                        edges.merge(e, 1, Integer::sum);
                        q.edge(e.first(), e.second(), 1);
                    }
                }
                q.commit();
                assertEquals(bfs(roots, edges), q.reachable(), "reachable, case " + c + " epoch " + ep);
                assertEquals(closure(edges), q.closure(), "closure, case " + c + " epoch " + ep);
                maxClosure = Math.max(maxClosure, q.closure().size());
            }
        }
        System.out.printf("DIFFSTAT reachability+closure           cases=%d epochs=%d events=%d retractions=%d maxClosure=%d%n",
            cases, cases * epochs, events, retractions, maxClosure);
        assertTrue(retractions > 0);
    }

    // ---- what table mode CAN say about the same edge table: a fixed depth ---------------------------------------

    @Test
    void tableModeSeesOnlyAFixedDepthOfTheSamePaths() {
        var schema = Map.of("edges", new SourceSchema("edges", new LinkedHashMap<>(Map.of("src", FieldKind.LONG, "dst", FieldKind.LONG))));
        var r = SqlCompiler.compileTable("SELECT a.src AS src, b.dst AS dst FROM edges a JOIN edges b ON a.dst = b.src", schema, Map.of());
        assertTrue(r.ok(), r.diagnostics().toString());
        var twoHop = r.plan().createExecutor();
        var q = new Reachability.Query();
        long[][] es = {{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 1}};
        for (long[] e : es) {
            twoHop.onTableDelta("edges", new TableDelta(Reachability.Query.edge(e[0], e[1]), 1));
            q.edge(e[0], e[1], 1);
        }
        q.commit();
        // every 2-hop pair the SQL join finds is in the closure ...
        var closure = q.closure();
        for (var row : twoHop.snapshot().values()) {
            EventRecord rec = row.row();
            assertTrue(closure.contains(Pair.of((Long) rec.get("src"), (Long) rec.get("dst"))), rec.toString());
        }
        // ... but the closure of a 5-cycle has 25 pairs while any fixed-depth self-join chain stays at 5 per depth.
        assertEquals(25, closure.size());
        assertEquals(5, twoHop.snapshot().size());
    }
}
