package io.streamsforge.dataflow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CompactionAndPerfTest {

    /** A long-running word count with retractions: retained state must track the live data, not the history. */
    @Test
    void compactionBoundsTraceSizeOnLongRunningInputWithRetractions() {
        Worker w = new Worker();
        InputHandle<Pair<Integer, Integer>, LongTime> in = w.newInput();
        Arranged<Integer, Integer, LongTime> arr = Keyed.of(in.collection()).arrange();
        Capture<Pair<Integer, Long>, LongTime> counts = arr.count().capture();
        Random r = new Random(7);
        Map<Integer, Long> live = new HashMap<>();
        int vocab = 40, epochs = 3000;
        long totalUpdates = 0;
        int maxTrace = 0;
        long maxState = 0;
        for (int e = 0; e < epochs; e++) {
            for (int i = 0; i < 10; i++) {
                int word = r.nextInt(vocab);
                boolean retract = live.getOrDefault(word, 0L) > 0 && r.nextBoolean();
                in.update(Pair.of(word, 0), retract ? -1 : 1);
                live.merge(word, retract ? -1L : 1L, Long::sum);
                totalUpdates++;
            }
            in.advanceTo(LongTime.of(e + 1));
            w.step();
            maxTrace = Math.max(maxTrace, arr.traceSize());
            maxState = Math.max(maxState, w.stateSize());
            if (e % 500 == 499) {
                Map<Pair<Integer, Long>, Long> exp = new HashMap<>();
                live.forEach((k, c) -> { if (c != 0) exp.put(Pair.of(k, c), 1L); });
                assertEquals(exp, counts.accumulate(LongTime.of(e)));
            }
        }
        System.out.printf("compaction: %d updates in, max trace size %d, max total state %d%n", totalUpdates, maxTrace, maxState);
        assertTrue(maxTrace <= 6 * vocab, "trace grew to " + maxTrace + " for a " + vocab + "-key vocabulary");
        assertTrue(maxState <= 20 * vocab, "state grew to " + maxState);
        assertTrue(totalUpdates > 20 * maxTrace, "the test must push far more updates than are retained");
    }

    /** Same for a join: both traces stay bounded by the live data. */
    @Test
    void compactionBoundsJoinTraces() {
        Worker w = new Worker();
        InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput(), b = w.newInput();
        Arranged<Integer, Integer, LongTime> aa = Keyed.of(a.collection()).arrange(), bb = Keyed.of(b.collection()).arrange();
        aa.join(bb, (k, x, y) -> Pair.of(x, y)).capture();
        Random r = new Random(11);
        Map<Pair<Integer, Integer>, Long> la = new HashMap<>(), lb = new HashMap<>();
        int maxA = 0, maxB = 0;
        for (int e = 0; e < 1500; e++) {
            for (int i = 0; i < 6; i++) {
                var da = Pair.of(r.nextInt(10), r.nextInt(5));
                boolean ra = la.getOrDefault(da, 0L) > 0 && r.nextBoolean();
                a.update(da, ra ? -1 : 1);
                la.merge(da, ra ? -1L : 1L, Long::sum);
                var db = Pair.of(r.nextInt(10), r.nextInt(5));
                boolean rb = lb.getOrDefault(db, 0L) > 0 && r.nextBoolean();
                b.update(db, rb ? -1 : 1);
                lb.merge(db, rb ? -1L : 1L, Long::sum);
            }
            a.advanceTo(LongTime.of(e + 1));
            b.advanceTo(LongTime.of(e + 1));
            w.step();
            maxA = Math.max(maxA, aa.traceSize());
            maxB = Math.max(maxB, bb.traceSize());
        }
        System.out.printf("join compaction: max trace sizes %d / %d (50 distinct records each)%n", maxA, maxB);
        assertTrue(maxA <= 300 && maxB <= 300, maxA + " / " + maxB);
    }

    /** Rough throughput numbers; asserts only that it finishes quickly. Printed, not guaranteed. */
    @Test
    void roughThroughput() {
        Random r = new Random(3);
        // word count
        {
            Worker w = new Worker();
            InputHandle<Integer, LongTime> in = w.newInput();
            in.collection().count().capture();
            int epochs = 200, per = 2000;
            long t0 = System.nanoTime();
            for (int e = 0; e < epochs; e++) {
                for (int i = 0; i < per; i++) in.insert(r.nextInt(1000));
                in.advanceTo(LongTime.of(e + 1));
                w.step();
            }
            double s = (System.nanoTime() - t0) / 1e9;
            System.out.printf("throughput(count): %d updates in %.2fs = %.0f updates/s%n", epochs * per, s, epochs * per / s);
        }
        // join of two arrangements
        {
            Worker w = new Worker();
            InputHandle<Pair<Integer, Integer>, LongTime> a = w.newInput(), b = w.newInput();
            Keyed.of(a.collection()).join(Keyed.of(b.collection()), (k, x, y) -> x + y).consolidate();
            int epochs = 100, per = 1000;
            long t0 = System.nanoTime();
            for (int e = 0; e < epochs; e++) {
                for (int i = 0; i < per; i++) {
                    a.insert(Pair.of(r.nextInt(5000), i));
                    b.insert(Pair.of(r.nextInt(5000), i));
                }
                a.advanceTo(LongTime.of(e + 1));
                b.advanceTo(LongTime.of(e + 1));
                w.step();
            }
            double s = (System.nanoTime() - t0) / 1e9;
            System.out.printf("throughput(join): %d input updates in %.2fs = %.0f updates/s%n", epochs * per * 2, s, epochs * per * 2 / s);
        }
        // incremental reachability on a 2000-node random graph
        {
            Worker w = new Worker();
            InputHandle<Integer, LongTime> roots = w.newInput();
            InputHandle<Pair<Integer, Integer>, LongTime> edges = w.newInput();
            ClassicExamplesTest.reach(roots.collection(), edges.collection()).capture();
            roots.insert(0);
            int n = 2000;
            for (int i = 0; i < 3 * n; i++) edges.insert(Pair.of(r.nextInt(n), r.nextInt(n)));
            roots.advanceTo(LongTime.of(1));
            edges.advanceTo(LongTime.of(1));
            long t0 = System.nanoTime();
            w.step();
            double load = (System.nanoTime() - t0) / 1e9;
            t0 = System.nanoTime();
            int updates = 50;
            for (int e = 1; e <= updates; e++) {
                edges.insert(Pair.of(r.nextInt(n), r.nextInt(n)));
                edges.remove(Pair.of(r.nextInt(n), r.nextInt(n)) );
                roots.advanceTo(LongTime.of(e + 1));
                edges.advanceTo(LongTime.of(e + 1));
                w.step();
            }
            double inc = (System.nanoTime() - t0) / 1e9;
            System.out.printf("throughput(reach): initial %d edges in %.2fs; %d single-edge epochs in %.2fs (%.1f ms/epoch)%n", 3 * n, load, updates, inc, 1000 * inc / updates);
        }
    }
}
