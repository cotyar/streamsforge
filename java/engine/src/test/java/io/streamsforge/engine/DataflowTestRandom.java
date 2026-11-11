package io.streamsforge.engine;

/**
 * Port of .NET's seeded {@code System.Random} (Knuth subtractive generator, the "Net5CompatSeedImpl" algorithm every
 * seeded {@code new Random(seed)} still uses), so a ported test that draws from {@code new Random(seed)} sees the
 * exact same sequence as the C# original.
 */
final class DataflowTestRandom {
    private static final int MBIG = Integer.MAX_VALUE;
    private static final int MSEED = 161803398;

    private final int[] seedArray = new int[56];
    private int inext;
    private int inextp;

    DataflowTestRandom(int seed) {
        int subtraction = (seed == Integer.MIN_VALUE) ? Integer.MAX_VALUE : Math.abs(seed);
        int mj = MSEED - subtraction;
        seedArray[55] = mj;
        int mk = 1;
        for (int i = 1; i < 55; i++) {
            int ii = (21 * i) % 55;
            seedArray[ii] = mk;
            mk = mj - mk;
            if (mk < 0) mk += MBIG;
            mj = seedArray[ii];
        }
        for (int k = 1; k < 5; k++) {
            for (int i = 1; i < 56; i++) {
                seedArray[i] -= seedArray[1 + (i + 30) % 55];
                if (seedArray[i] < 0) seedArray[i] += MBIG;
            }
        }
        inext = 0;
        inextp = 21;
    }

    private int internalSample() {
        int locINext = inext;
        int locINextp = inextp;
        if (++locINext >= 56) locINext = 1;
        if (++locINextp >= 56) locINextp = 1;
        int retVal = seedArray[locINext] - seedArray[locINextp];
        if (retVal == MBIG) retVal--;
        if (retVal < 0) retVal += MBIG;
        seedArray[locINext] = retVal;
        inext = locINext;
        inextp = locINextp;
        return retVal;
    }

    private double sample() { return internalSample() * (1.0 / MBIG); }

    /** {@code Random.Next()} — a non-negative int less than int.MaxValue. */
    int next() { return internalSample(); }

    /** {@code Random.Next(maxValue)}. */
    int next(int maxValue) { return (int) (sample() * maxValue); }

    /** {@code Random.Next(minValue, maxValue)} (range assumed to fit an int, as in every ported use). */
    int next(int minValue, int maxValue) {
        long range = (long) maxValue - minValue;
        if (range <= Integer.MAX_VALUE) return (int) (sample() * range) + minValue;
        throw new UnsupportedOperationException("large range not needed by the ported tests");
    }

    /** {@code Random.NextBytes(buffer)}. */
    void nextBytes(byte[] buffer) {
        for (int i = 0; i < buffer.length; i++) buffer[i] = (byte) internalSample();
    }
}
