package io.streamsforge.dataflow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class LatticeAndFrontierTest {
    private static final Random R = new Random(42);

    static <T extends Time<T>> void checkLaws(Function<Random, T> gen) {
        for (int i = 0; i < 3000; i++) {
            T a = gen.apply(R), b = gen.apply(R), c = gen.apply(R);
            // partial order: reflexive, antisymmetric, transitive
            assertTrue(a.lessEqual(a));
            if (a.lessEqual(b) && b.lessEqual(a)) assertEquals(a, b);
            if (a.lessEqual(b) && b.lessEqual(c)) assertTrue(a.lessEqual(c));
            // join is the least upper bound
            T j = a.join(b);
            assertTrue(a.lessEqual(j) && b.lessEqual(j));
            if (a.lessEqual(c) && b.lessEqual(c)) assertTrue(j.lessEqual(c));
            // meet is the greatest lower bound
            T m = a.meet(b);
            assertTrue(m.lessEqual(a) && m.lessEqual(b));
            if (c.lessEqual(a) && c.lessEqual(b)) assertTrue(c.lessEqual(m));
            // commutative, associative, idempotent, absorption
            assertEquals(a.join(b), b.join(a));
            assertEquals(a.meet(b), b.meet(a));
            assertEquals(a.join(b).join(c), a.join(b.join(c)));
            assertEquals(a.meet(b).meet(c), a.meet(b.meet(c)));
            assertEquals(a, a.join(a));
            assertEquals(a, a.meet(a));
            assertEquals(a, a.join(a.meet(b)));
            assertEquals(a, a.meet(a.join(b)));
            // order characterisations
            assertEquals(a.lessEqual(b), a.join(b).equals(b));
            assertEquals(a.lessEqual(b), a.meet(b).equals(a));
            // compareTo is a linear extension of the order
            if (a.lessEqual(b)) assertTrue(a.compareTo(b) <= 0);
            if (a.lessThan(b)) assertTrue(a.compareTo(b) < 0);
        }
    }

    @Test
    void longTimeLaws() { checkLaws(r -> LongTime.of(r.nextInt(6))); }

    @Test
    void productLaws() { checkLaws(r -> new Product<>(LongTime.of(r.nextInt(4)), LongTime.of(r.nextInt(4)))); }

    @Test
    void nestedProductLaws() {
        checkLaws(r -> new Product<>(new Product<>(LongTime.of(r.nextInt(3)), LongTime.of(r.nextInt(3))), LongTime.of(r.nextInt(3))));
    }

    @Test
    void productIsGenuinelyPartial() {
        var a = new Product<>(LongTime.of(1), LongTime.of(2));
        var b = new Product<>(LongTime.of(2), LongTime.of(1));
        assertFalse(a.lessEqual(b));
        assertFalse(b.lessEqual(a));
        assertEquals(new Product<>(LongTime.of(2), LongTime.of(2)), a.join(b));
        assertEquals(new Product<>(LongTime.of(1), LongTime.of(1)), a.meet(b));
    }

    @Test
    void antichainInsertAndDominance() {
        Antichain<Product<LongTime, LongTime>> ac = new Antichain<>();
        var p = (java.util.function.BiFunction<Integer, Integer, Product<LongTime, LongTime>>) (x, y) -> new Product<>(LongTime.of(x), LongTime.of(y));
        assertTrue(ac.insert(p.apply(2, 2)));
        assertFalse(ac.insert(p.apply(3, 3)), "dominated by (2,2)");
        assertFalse(ac.insert(p.apply(2, 2)), "duplicate");
        assertTrue(ac.insert(p.apply(1, 5)), "incomparable");
        assertEquals(2, ac.size());
        assertTrue(ac.insert(p.apply(0, 0)), "dominates both");
        assertEquals(1, ac.size());
        assertEquals(Antichain.of(p.apply(0, 0)), ac);
        // lessEqual / lessThan against an element
        Antichain<Product<LongTime, LongTime>> f = Antichain.of(p.apply(2, 2), p.apply(1, 5));
        assertTrue(f.lessEqual(p.apply(2, 2)));
        assertFalse(f.lessThan(p.apply(2, 2)));
        assertTrue(f.lessEqual(p.apply(9, 5)));
        assertFalse(f.lessEqual(p.apply(1, 4)));
        assertTrue(Antichain.<LongTime>of().isEmpty());
        assertFalse(Antichain.<LongTime>of().lessEqual(LongTime.of(100)), "the empty frontier admits nothing");
    }

    @Test
    void antichainRandomInvariants() {
        for (int i = 0; i < 500; i++) {
            Antichain<Product<LongTime, LongTime>> ac = new Antichain<>();
            List<Product<LongTime, LongTime>> all = new ArrayList<>();
            for (int k = 0; k < 8; k++) {
                var t = new Product<>(LongTime.of(R.nextInt(5)), LongTime.of(R.nextInt(5)));
                all.add(t);
                ac.insert(t);
            }
            // elements mutually incomparable
            for (var x : ac.elements()) for (var y : ac.elements()) if (x != y) assertFalse(x.lessEqual(y));
            // every inserted time is at or beyond some element, and every element was inserted
            for (var t : all) assertTrue(ac.lessEqual(t));
            for (var e : ac.elements()) assertTrue(all.contains(e));
        }
    }

    @Test
    void advanceByCompactsToFrontier() {
        var f = Antichain.of(new Product<>(LongTime.of(3), LongTime.of(0)));
        var t = new Product<>(LongTime.of(1), LongTime.of(2));
        assertEquals(new Product<>(LongTime.of(3), LongTime.of(2)), t.advanceBy(f));
        // advancing preserves the order relative to anything at or beyond the frontier
        for (int i = 0; i < 1000; i++) {
            var a = new Product<>(LongTime.of(R.nextInt(6)), LongTime.of(R.nextInt(6)));
            var b = new Product<>(LongTime.of(R.nextInt(6)), LongTime.of(R.nextInt(6)));
            var fr = Antichain.of(new Product<>(LongTime.of(R.nextInt(6)), LongTime.of(R.nextInt(6))), new Product<>(LongTime.of(R.nextInt(6)), LongTime.of(R.nextInt(6))));
            if (fr.lessEqual(b)) assertEquals(a.lessEqual(b), a.advanceBy(fr).lessEqual(b), a + " " + b + " " + fr);
        }
    }

    @Test
    void frontierNeverRegressesAndProbesReportCompletion() {
        Worker w = new Worker();
        InputHandle<Integer, LongTime> in = w.newInput();
        Collection<Integer, LongTime> c = in.collection();
        Collection<Pair<Integer, Long>, LongTime> counts = c.map(x -> x % 3).count();
        Probe<LongTime> probe = counts.probe();
        Antichain<LongTime> prev = probe.frontier();
        assertEquals(Antichain.of(LongTime.ZERO), prev);
        for (int e = 0; e < 50; e++) {
            for (int i = 0; i < 3; i++) in.insert(R.nextInt(10));
            if (R.nextBoolean()) in.advanceTo(LongTime.of(e + 1));
            else in.advanceTo(LongTime.of(e + 1)); // epoch boundary either way; also step between
            w.step();
            Antichain<LongTime> cur = probe.frontier();
            assertTrue(prev.lessEqual(cur), prev + " regressed to " + cur);
            assertFalse(cur.lessEqual(LongTime.of(e)), "epoch " + e + " should be complete");
            assertTrue(probe.done(LongTime.of(e)));
            assertFalse(probe.done(LongTime.of(e + 1)));
            prev = cur;
        }
        in.close();
        w.step();
        assertTrue(probe.frontier().isEmpty(), "a closed input empties the frontier");
        assertTrue(prev.lessEqual(probe.frontier()));
    }

    @Test
    void operatorsDoNotFinishATimeWhileInputMayStillWriteIt() {
        Worker w = new Worker();
        InputHandle<Integer, LongTime> in = w.newInput();
        Capture<Pair<Integer, Long>, LongTime> out = in.collection().count().capture();
        in.insert(7);
        w.step();
        assertFalse(out.done(LongTime.of(0)), "input is still at time 0");
        assertTrue(out.updates().isEmpty(), "reduce must not emit at an incomplete time");
        in.insert(7);
        in.advanceTo(LongTime.of(1));
        w.step();
        assertTrue(out.done(LongTime.of(0)));
        assertEquals(Map.of(Pair.of(7, 2L), 1L), out.accumulate(LongTime.of(0)));
        assertEquals(1, out.updates().size(), "a single, final emission");
    }

    @Test
    void inputCannotRegress() {
        Worker w = new Worker();
        InputHandle<Integer, LongTime> in = w.newInput();
        in.advanceTo(LongTime.of(5));
        assertThrows(IllegalArgumentException.class, () -> in.advanceTo(LongTime.of(3)));
        assertThrows(IllegalArgumentException.class, () -> in.update(1, LongTime.of(2), 1));
    }
}
