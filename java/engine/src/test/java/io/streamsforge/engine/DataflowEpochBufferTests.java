package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.DeltaBatch;
import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.dataflow.EpochBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/** EpochBuffer holds the core replay guarantee: the flush sequence for a fixed set of batches depends only on
 *  (Epoch, EdgeId, FromPartition), never on Add() order. These tests exercise out-of-order arrival,
 *  shuffled-permutation determinism, empty-epoch markers, and the bounded-memory / no-silent-drop guarantees. */
class DataflowEpochBufferTests {
    private static final EdgeId EDGE0 = new EdgeId(0);
    private static final EdgeId EDGE1 = new EdgeId(1);

    private static TableDelta delta(String id) {
        var row = new EventRecord();
        row.put("id", id);
        return new TableDelta(row, 1);
    }

    private static DeltaBatch batch(EdgeId edge, int partition, long epoch, String... rowIds) {
        return new DeltaBatch(edge, partition, new Epoch(epoch), Arrays.stream(rowIds).map(DataflowEpochBufferTests::delta).collect(Collectors.toList()));
    }

    /** {@code canonical.OrderBy(_ => rng.Next())}: one random key per element, then a STABLE sort by key. */
    private static List<DeltaBatch> shuffled(List<DeltaBatch> canonical, DataflowTestRandom rng) {
        var keyed = new ArrayList<Object[]>();
        for (var b : canonical) keyed.add(new Object[] {rng.next(), b});
        keyed.sort(Comparator.comparingInt(k -> (Integer) k[0])); // List.sort is stable
        var result = new ArrayList<DeltaBatch>();
        for (var k : keyed) result.add((DeltaBatch) k[1]);
        return result;
    }

    @Test
    void outOfOrderArrivalFlushesInEpochOrder() {
        var buffer = new EpochBuffer();
        buffer.add(batch(EDGE0, 0, 3, "e3"));
        buffer.add(batch(EDGE0, 0, 1, "e1"));
        buffer.add(batch(EDGE0, 0, 2, "e2"));

        var ready = buffer.onFrontier(new Epoch(3));

        assertEquals(3, ready.size());
        assertEquals(new Epoch(1), ready.get(0).epoch());
        assertEquals(new Epoch(2), ready.get(1).epoch());
        assertEquals(new Epoch(3), ready.get(2).epoch());
    }

    @Test
    void onlyBatchesAtOrBelowFrontierAreFlushed_RestStayBuffered() {
        var buffer = new EpochBuffer();
        buffer.add(batch(EDGE0, 0, 1, "a"));
        buffer.add(batch(EDGE0, 0, 5, "b"));

        var ready = buffer.onFrontier(new Epoch(2));

        assertEquals(1, ready.size());
        var only = ready.get(0);
        assertEquals(new Epoch(1), only.epoch());
        assertEquals(1, buffer.batchCount()); // epoch 5 batch still buffered
        assertEquals(1, buffer.deltaCount());
    }

    @Test
    void withinAnEpochOrderIsDeterministicByEdgeThenPartition() {
        var buffer = new EpochBuffer();
        buffer.add(batch(EDGE1, 1, 1, "z"));
        buffer.add(batch(EDGE0, 2, 1, "y"));
        buffer.add(batch(EDGE0, 0, 1, "x"));
        buffer.add(batch(EDGE1, 0, 1, "w"));

        var ready = buffer.onFrontier(new Epoch(1));

        assertEquals(4, ready.size());
        var expectedOrder = List.of(
            List.of(EDGE0, 0), List.of(EDGE0, 2), List.of(EDGE1, 0), List.of(EDGE1, 1));
        var actual = ready.stream().map(b -> List.<Object>of(b.edgeId(), b.fromPartition())).collect(Collectors.toList());
        assertEquals(expectedOrder, actual);
    }

    @Test
    void emptyEpochMarkerIsFlushedLikeAnyOtherBatch() {
        var buffer = new EpochBuffer();
        var marker = new DeltaBatch(EDGE0, 0, new Epoch(1), List.of());
        buffer.add(marker);

        var ready = buffer.onFrontier(new Epoch(1));

        assertEquals(1, ready.size());
        var only = ready.get(0);
        assertTrue(only.isMarker());
        assertEquals(new Epoch(1), only.epoch());
    }

    @Test
    void repeatedFlushesNeverReturnTheSameBatchTwice_AndNothingIsSilentlyDropped() {
        var buffer = new EpochBuffer();
        for (int e = 0; e < 10; e++) {
            buffer.add(batch(EDGE0, 0, e, "row" + e));
        }

        var firstFlush = buffer.onFrontier(new Epoch(4));
        assertEquals(5, firstFlush.size()); // epochs 0..4
        assertEquals(5, buffer.batchCount()); // epochs 5..9 remain

        var secondFlush = buffer.onFrontier(new Epoch(9));
        assertEquals(5, secondFlush.size()); // epochs 5..9
        assertEquals(0, buffer.batchCount());

        var allFlushed = new ArrayList<Long>();
        for (var b : firstFlush) allFlushed.add(b.epoch().value());
        for (var b : secondFlush) allFlushed.add(b.epoch().value());
        var expected = new ArrayList<Long>();
        for (long i = 0; i < 10; i++) expected.add(i);
        assertEquals(expected, allFlushed);
    }

    @Test
    void flushingWithNoReadyBatchesReturnsEmptyAndLeavesBufferIntact() {
        var buffer = new EpochBuffer();
        buffer.add(batch(EDGE0, 0, 5, "a"));

        var ready = buffer.onFrontier(new Epoch(1));

        assertTrue(ready.isEmpty());
        assertEquals(1, buffer.batchCount());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    void deterministicReplay_SameBatchesInAnyArrivalOrderProduceIdenticalFlushSequence(int seed) {
        var rng = new DataflowTestRandom(seed);
        var canonical = new ArrayList<DeltaBatch>();
        for (int e = 0; e < 12; e++) {
            for (int p = 0; p < 3; p++) {
                var edge = p % 2 == 0 ? EDGE0 : EDGE1;
                canonical.add(batch(edge, p, e, "e" + e + "-p" + p));
            }
        }

        // The reference flush sequence, added in canonical order.
        var reference = new EpochBuffer();
        for (var b : canonical) reference.add(b);
        var referenceFlush = reference.onFrontier(new Epoch(11));

        // Try several random shuffles (permutations) of arrival order — every one must produce the exact same flush
        // sequence. This is the property that makes replay debugging work.
        for (int trial = 0; trial < 20; trial++) {
            var shuffled = shuffled(canonical, rng);
            var buffer = new EpochBuffer();
            for (var b : shuffled) buffer.add(b);
            var flush = buffer.onFrontier(new Epoch(11));

            assertEquals(referenceFlush.size(), flush.size());
            for (int i = 0; i < referenceFlush.size(); i++) {
                assertEquals(referenceFlush.get(i).epoch(), flush.get(i).epoch());
                assertEquals(referenceFlush.get(i).edgeId(), flush.get(i).edgeId());
                assertEquals(referenceFlush.get(i).fromPartition(), flush.get(i).fromPartition());
                assertEquals(
                    referenceFlush.get(i).deltas().stream().map(d -> d.row().get("id")).collect(Collectors.toList()),
                    flush.get(i).deltas().stream().map(d -> d.row().get("id")).collect(Collectors.toList()));
            }
        }
    }

    @Test
    void deterministicReplay_IncrementalFrontierAdvancesInAnyArrivalOrderMatchReference() {
        // Same idea but flushing incrementally at several frontier checkpoints instead of once at the end — the
        // concatenation of incremental flushes must equal the single big flush.
        var rng = new DataflowTestRandom(999);
        var canonical = new ArrayList<DeltaBatch>();
        for (int e = 0; e < 20; e++) {
            canonical.add(batch(EDGE0, e % 4, e, "row" + e));
        }
        long[] checkpoints = {3, 3, 10, 15, 19};

        var reference = new EpochBuffer();
        for (var b : canonical) reference.add(b);
        var referenceFlush = new ArrayList<DeltaBatch>();
        for (long checkpoint : checkpoints) {
            referenceFlush.addAll(reference.onFrontier(new Epoch(checkpoint)));
        }

        for (int trial = 0; trial < 10; trial++) {
            var shuffled = shuffled(canonical, rng);
            var buffer = new EpochBuffer();
            for (var b : shuffled) buffer.add(b);

            var flush = new ArrayList<DeltaBatch>();
            for (long checkpoint : checkpoints) {
                flush.addAll(buffer.onFrontier(new Epoch(checkpoint)));
            }

            assertEquals(
                referenceFlush.stream().map(b -> b.epoch().value()).collect(Collectors.toList()),
                flush.stream().map(b -> b.epoch().value()).collect(Collectors.toList()));
            assertEquals(
                referenceFlush.stream().map(b -> List.<Object>of(b.edgeId(), b.fromPartition())).collect(Collectors.toList()),
                flush.stream().map(b -> List.<Object>of(b.edgeId(), b.fromPartition())).collect(Collectors.toList()));
        }
    }
}
