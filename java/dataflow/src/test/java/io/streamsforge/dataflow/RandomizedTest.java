package io.streamsforge.dataflow;

import static io.streamsforge.dataflow.Support.add;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Randomized differential testing: for each operator, random update streams with retractions over many epochs; at
 * every completed epoch the accumulated output must equal a naive from-scratch evaluation of the accumulated input.
 */
class RandomizedTest {
    static final int CASES = 300;
    static final int EPOCHS = 8;

    static Pair<Integer, Integer> kv(Random r, int keys, int vals) {
        return Pair.of(r.nextInt(keys), r.nextInt(vals));
    }

    @Test
    void join() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(seed);
            Worker w = new Worker();
            InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput(), b = w.newInput();
            Capture<Pair<Integer, Pair<Integer, Integer>>, LongTime> out = Keyed.of(a.collection()).join(Keyed.of(b.collection())).capture();
            var ga = new Support.Gen<>(r, () -> kv(r, 4, 3), 0.4);
            var gb = new Support.Gen<>(r, () -> kv(r, 4, 3), 0.4);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(a, ga.epoch(5));
                Support.feed(b, gb.epoch(5));
                a.advanceTo(LongTime.of(e + 1));
                b.advanceTo(LongTime.of(e + 1));
                w.step();
                assertTrue(out.done(LongTime.of(e)), "seed " + seed);
                assertEquals(Support.naiveJoin(ga.state, gb.state), out.accumulate(LongTime.of(e)), "seed " + seed + " epoch " + e);
            }
        }
    }

    @Test
    void selfJoinSharedArrangement() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(1000 + seed);
            Worker w = new Worker();
            InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput();
            Arranged<Integer, Integer, LongTime> arr = Keyed.of(a.collection()).arrange();
            Capture<Pair<Integer, Pair<Integer, Integer>>, LongTime> out = arr.join(arr, (k, x, y) -> Pair.of(k, Pair.of(x, y))).capture();
            var ga = new Support.Gen<>(r, () -> kv(r, 3, 3), 0.4);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(a, ga.epoch(5));
                a.advanceTo(LongTime.of(e + 1));
                w.step();
                assertEquals(Support.naiveJoin(ga.state, ga.state), out.accumulate(LongTime.of(e)), "seed " + seed + " epoch " + e);
            }
        }
    }

    @Test
    void reduceCountDistinctMinSum() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(2000 + seed);
            Worker w = new Worker();
            InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput();
            Keyed<Integer, Integer, LongTime> k = Keyed.of(a.collection());
            Arranged<Integer, Integer, LongTime> arr = k.arrange(); // one arrangement, three readers
            Capture<Pair<Integer, Long>, LongTime> count = arr.count().capture();
            Capture<Pair<Integer, Integer>, LongTime> min = arr.<Integer>reduce((key, in, o) -> {
                int m = Integer.MAX_VALUE;
                boolean any = false;
                for (Weighted<Integer> x : in) if (x.weight() > 0) { m = Math.min(m, x.value()); any = true; }
                if (any) o.add(new Weighted<>(m, 1));
            }).capture();
            Capture<Pair<Integer, Long>, LongTime> sum = arr.<Long>reduce((key, in, o) -> {
                long s = 0;
                for (Weighted<Integer> x : in) s += x.value() * x.weight();
                o.add(new Weighted<>(s, 1));
            }).capture();
            Capture<Pair<Integer, Integer>, LongTime> distinct = a.collection().distinct().capture();
            var g = new Support.Gen<>(r, () -> kv(r, 4, 4), 0.45);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(a, g.epoch(6));
                a.advanceTo(LongTime.of(e + 1));
                w.step();
                LongTime t = LongTime.of(e);
                assertTrue(count.done(t) && min.done(t) && sum.done(t) && distinct.done(t));
                String msg = "seed " + seed + " epoch " + e;
                assertEquals(Support.naiveReduce(g.state, m -> {
                    long c = 0;
                    for (long v : m.values()) c += v;
                    return c == 0 ? Map.of() : Map.of(c, 1L);
                }), count.accumulate(t), msg + " count");
                assertEquals(Support.naiveReduce(g.state, m -> Map.of(m.keySet().stream().min(Integer::compare).get(), 1L)), min.accumulate(t), msg + " min");
                assertEquals(Support.naiveReduce(g.state, m -> {
                    long s = 0;
                    for (var x : m.entrySet()) s += x.getKey() * x.getValue();
                    return Map.of(s, 1L);
                }), sum.accumulate(t), msg + " sum");
                Map<Pair<Integer, Integer>, Long> dist = new HashMap<>();
                g.state.forEach((x, c) -> { if (c > 0) dist.put(x, 1L); });
                assertEquals(dist, distinct.accumulate(t), msg + " distinct");
            }
        }
    }

    @Test
    void antijoinAndSemijoin() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(3000 + seed);
            Worker w = new Worker();
            InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput();
            InputHandle<Integer, LongTime> b = w.newInput();
            Keyed<Integer, Integer, LongTime> k = Keyed.of(a.collection());
            Capture<Pair<Integer, Integer>, LongTime> anti = k.antijoin(b.collection()).capture();
            Capture<Pair<Integer, Integer>, LongTime> semi = k.semijoin(b.collection()).capture();
            var ga = new Support.Gen<>(r, () -> kv(r, 5, 3), 0.4);
            var gb = new Support.Gen<>(r, () -> r.nextInt(5), 0.5);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(a, ga.epoch(5));
                Support.feed(b, gb.epoch(3));
                a.advanceTo(LongTime.of(e + 1));
                b.advanceTo(LongTime.of(e + 1));
                w.step();
                LongTime t = LongTime.of(e);
                Map<Pair<Integer, Integer>, Long> expAnti = new HashMap<>(), expSemi = new HashMap<>();
                for (var x : ga.state.entrySet()) {
                    long mult = gb.state.getOrDefault(x.getKey().first(), 0L);
                    if (mult <= 0) add(expAnti, x.getKey(), x.getValue());
                    if (mult != 0) add(expSemi, x.getKey(), x.getValue() * mult);
                }
                assertEquals(expAnti, anti.accumulate(t), "anti seed " + seed + " epoch " + e);
                assertEquals(expSemi, semi.accumulate(t), "semi seed " + seed + " epoch " + e);
            }
        }
    }

    @Test
    void statelessChainAndConsolidate() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(4000 + seed);
            Worker w = new Worker();
            InputHandle<Integer, LongTime> a = w.newInput(), b = w.newInput();
            Collection<Integer, LongTime> c = a.collection().map(x -> x * 2).filter(x -> x % 3 != 0)
                    .concat(b.collection().flatMap(x -> java.util.List.of(x, x + 1)).negate());
            Capture<Integer, LongTime> raw = c.capture();
            Capture<Integer, LongTime> cons = c.consolidate().capture();
            var ga = new Support.Gen<>(r, () -> r.nextInt(8), 0.4);
            var gb = new Support.Gen<>(r, () -> r.nextInt(8), 0.4);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(a, ga.epoch(6));
                Support.feed(b, gb.epoch(6));
                a.advanceTo(LongTime.of(e + 1));
                b.advanceTo(LongTime.of(e + 1));
                w.step();
                Map<Integer, Long> exp = new HashMap<>();
                ga.state.forEach((x, n) -> { if ((x * 2) % 3 != 0) add(exp, x * 2, n); });
                gb.state.forEach((x, n) -> { add(exp, x, -n); add(exp, x + 1, -n); });
                assertEquals(exp, raw.accumulate(LongTime.of(e)), "seed " + seed);
                assertEquals(exp, cons.accumulate(LongTime.of(e)), "seed " + seed);
                assertTrue(cons.updates().stream().noneMatch(u -> u.diff() == 0));
            }
        }
    }

    // ---- iterate

    @Test
    void iterateReachability() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(5000 + seed);
            Worker w = new Worker();
            InputHandle<Integer, LongTime> roots = w.newInput();
            InputHandle<Pair<Integer, Integer>, LongTime> edges = w.newInput();
            Capture<Integer, LongTime> out = ClassicExamplesTest.reach(roots.collection(), edges.collection()).capture();
            var gr = new Support.Gen<>(r, () -> r.nextInt(7), 0.4);
            var ge = new Support.Gen<>(r, () -> Pair.of(r.nextInt(7), r.nextInt(7)), 0.45);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(roots, gr.epoch(2));
                Support.feed(edges, ge.epoch(4));
                roots.advanceTo(LongTime.of(e + 1));
                edges.advanceTo(LongTime.of(e + 1));
                w.step();
                assertTrue(out.done(LongTime.of(e)));
                Map<Integer, Long> enc = new HashMap<>();
                ge.state.forEach((p, n) -> enc.put(p.first() * 1000 + p.second(), n));
                Set<Integer> rootSet = new HashSet<>();
                gr.state.forEach((x, n) -> { if (n > 0) rootSet.add(x); });
                Map<Integer, Long> exp = new HashMap<>();
                for (int n : ClassicExamplesTest.bfs(rootSet, enc)) exp.put(n, 1L);
                assertEquals(exp, out.accumulate(LongTime.of(e)), "seed " + seed + " epoch " + e);
            }
        }
    }

    @Test
    void iterateConnectedComponents() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(6000 + seed);
            Worker w = new Worker();
            InputHandle<Integer, LongTime> nodes = w.newInput();
            InputHandle<Pair<Integer, Integer>, LongTime> edges = w.newInput();
            Capture<Pair<Integer, Integer>, LongTime> out = ClassicExamplesTest.components(nodes.collection(), edges.collection()).capture();
            Set<Integer> ns = new HashSet<>();
            for (int i = 0; i < 7; i++) { nodes.insert(i); ns.add(i); }
            var ge = new Support.Gen<>(r, () -> Pair.of(r.nextInt(7), r.nextInt(7)), 0.45);
            for (int e = 0; e < EPOCHS; e++) {
                Support.feed(edges, ge.epoch(3));
                nodes.advanceTo(LongTime.of(e + 1));
                edges.advanceTo(LongTime.of(e + 1));
                w.step();
                Map<Pair<Integer, Integer>, Long> exp = new HashMap<>();
                ClassicExamplesTest.unionFind(ns, ge.state).forEach((n, l) -> exp.put(Pair.of(n, l), 1L));
                assertEquals(exp, out.accumulate(LongTime.of(e)), "seed " + seed + " epoch " + e);
            }
        }
    }

    // ---- partially ordered (Product) times at the top level

    record PT(long round, long lane) {
        Product<LongTime, LongTime> t() { return new Product<>(LongTime.of(round), LongTime.of(lane)); }
    }

    static final int LANES = 4;

    /** Sum of the diffs of updates with time <= (round,lane) in the product order. */
    static <D> Map<D, Long> accumulateAt(Map<D, Map<PT, Long>> hist, long round, long lane) {
        Map<D, Long> m = new HashMap<>();
        hist.forEach((d, byTime) -> byTime.forEach((t, n) -> {
            if (t.round() <= round && t.lane() <= lane) add(m, d, n);
        }));
        return m;
    }

    @Test
    void productTimesJoinCountDistinct() {
        for (int seed = 0; seed < CASES; seed++) {
            Random r = new Random(7000 + seed);
            Worker w = new Worker();
            var origin = new Product<>(LongTime.ZERO, LongTime.ZERO);
            InputHandle<Pair<Integer, Integer>, Product<LongTime, LongTime>> a = w.newInput(origin), b = w.newInput(origin);
            var ka = Keyed.of(a.collection());
            var kb = Keyed.of(b.collection());
            Capture<Pair<Integer, Pair<Integer, Integer>>, Product<LongTime, LongTime>> join = ka.join(kb).capture();
            Capture<Pair<Integer, Long>, Product<LongTime, LongTime>> count = ka.count().capture();
            Capture<Pair<Integer, Integer>, Product<LongTime, LongTime>> distinct = a.collection().distinct().capture();
            Map<Pair<Integer, Integer>, Map<PT, Long>> ha = new HashMap<>(), hb = new HashMap<>();
            int rounds = 5;
            for (int round = 0; round < rounds; round++) {
                for (int i = 0, n = r.nextInt(7); i < n; i++) {
                    PT t = new PT(round, r.nextInt(LANES));
                    Pair<Integer, Integer> d = kv(r, 3, 3);
                    long diff = r.nextInt(3) == 0 ? -1 : 1; // retractions without regard to presence: negative counts happen
                    boolean toA = r.nextBoolean();
                    (toA ? a : b).update(d, t.t(), diff);
                    (toA ? ha : hb).computeIfAbsent(d, x -> new HashMap<>()).merge(t, diff, Long::sum);
                }
                a.advanceTo(new Product<>(LongTime.of(round + 1), LongTime.ZERO));
                b.advanceTo(new Product<>(LongTime.of(round + 1), LongTime.ZERO));
                w.step();
                for (long rd = 0; rd <= round; rd++) {
                    for (long lane = 0; lane < LANES; lane++) {
                        var t = new PT(rd, lane).t();
                        assertTrue(join.done(t) && count.done(t) && distinct.done(t), "seed " + seed + " not done at " + t);
                        Map<Pair<Integer, Integer>, Long> accA = accumulateAt(ha, rd, lane), accB = accumulateAt(hb, rd, lane);
                        String msg = "seed " + seed + " time " + t;
                        assertEquals(Support.naiveJoin(accA, accB), join.accumulate(t), msg + " join");
                        assertEquals(Support.naiveReduce(accA, m -> {
                            long c = 0;
                            for (long v : m.values()) c += v;
                            return c == 0 ? Map.of() : Map.of(c, 1L);
                        }), count.accumulate(t), msg + " count");
                        Map<Pair<Integer, Integer>, Long> dist = new HashMap<>();
                        accA.forEach((x, c) -> { if (c > 0) dist.put(x, 1L); });
                        assertEquals(dist, distinct.accumulate(t), msg + " distinct");
                    }
                }
            }
        }
    }
}
