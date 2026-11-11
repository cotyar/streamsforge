package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.ExchangeRouter;
import io.streamsforge.engine.dataflow.ExchangeSpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

/** ExchangeRouter maps canonical key bytes (TableKeyEncoding output) to a stable partition index via FNV-1a. These
 *  tests pin: determinism across calls/processes, partitionCount=1 degeneracy, and a "chi-square-lite" uniformity
 *  bound so a bad hash choice (e.g. a low-entropy fold) would fail loudly. */
class DataflowExchangeRouterTests {
    @Test
    void samePartitionCountOfOneAlwaysReturnsZero() {
        var rng = new DataflowTestRandom(1);
        for (int i = 0; i < 100; i++) {
            var key = randomKey(rng);
            assertEquals(0, ExchangeRouter.partitionOf(key, 1));
        }
    }

    @Test
    void resultIsAlwaysInRange() {
        var rng = new DataflowTestRandom(2);
        for (int i = 0; i < 500; i++) {
            int partitionCount = rng.next(1, 64);
            var key = randomKey(rng);
            int p = ExchangeRouter.partitionOf(key, partitionCount);
            assertTrue(p >= 0 && p <= partitionCount - 1, "partition " + p + " not in [0," + (partitionCount - 1) + "]");
        }
    }

    @Test
    void isDeterministicAcrossRepeatedCalls_SameKeySamePartitionEveryTime() {
        final String key = "S:AAPL";
        int first = ExchangeRouter.partitionOf(key, 16);
        for (int i = 0; i < 50; i++) {
            assertEquals(first, ExchangeRouter.partitionOf(key, 16));
        }
    }

    @Test
    void stringOverloadMatchesUtf8ByteOverload() {
        final String key = "S:MSFT|L:42";
        int viaString = ExchangeRouter.partitionOf(key, 8);
        int viaBytes = ExchangeRouter.partitionOf(key.getBytes(StandardCharsets.UTF_8), 8);
        assertEquals(viaBytes, viaString);
    }

    @Test
    void differentKeysCanLandOnDifferentPartitions() {
        // Not a strict requirement of any single pair, but with 10k distinct keys over 16 partitions we should see more
        // than one partition used — a constant router would fail this immediately.
        var used = new HashSet<Integer>();
        for (int i = 0; i < 10_000; i++) {
            used.add(ExchangeRouter.partitionOf("S:key-" + i, 16));
        }
        assertTrue(used.size() > 1, "router mapped every key to a single partition");
    }

    @Test
    void zeroOrNegativePartitionCountThrows() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeRouter.partitionOf("k", 0));
        assertThrows(IllegalArgumentException.class, () -> ExchangeRouter.partitionOf("k", -1));
    }

    @Test
    void uniformDistributionOverTenThousandRandomKeys_ChiSquareLiteBound() {
        final int partitionCount = 16;
        final int sampleSize = 10_000;
        var counts = new int[partitionCount];
        var rng = new DataflowTestRandom(2026_07_18);

        for (int i = 0; i < sampleSize; i++) {
            var key = randomKey(rng);
            int p = ExchangeRouter.partitionOf(key, partitionCount);
            counts[p]++;
        }

        double expected = (double) sampleSize / partitionCount; // 625
        double chiSquare = 0;
        for (int c : counts) chiSquare += (c - expected) * (c - expected) / expected;

        // Chi-square-lite bound: with 15 degrees of freedom, the 99.9% critical value is ~37.7. A well-mixed hash should
        // sit far below that; a broken one (e.g. hashing only the first byte) would blow way past it. Generous margin
        // to keep this test non-flaky.
        assertTrue(chiSquare < 60.0, "chi-square statistic " + chiSquare + " suggests a poorly-mixed router (counts: " + Arrays.toString(counts) + ")");

        // No partition should be wildly over/under-represented either.
        for (int c : counts) {
            assertTrue(c >= expected * 0.7 && c <= expected * 1.3, "count " + c + " not in [" + expected * 0.7 + "," + expected * 1.3 + "]");
        }
    }

    @Test
    void exchangeSpecRejectsNonPositivePartitionCount() {
        assertThrows(IllegalArgumentException.class, () -> new ExchangeSpec(new EdgeId(0), 0));
    }

    private static byte[] randomKey(DataflowTestRandom rng) {
        var bytes = new byte[rng.next(4, 32)];
        rng.nextBytes(bytes);
        return bytes;
    }
}
