package io.streamsforge.engine.dataflow;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Routes a canonical key (TableKeyEncoding output) to a stable partition index via FNV-1a. FNV-1a rather than a
 * runtime hash because the router must be deterministic ACROSS PROCESSES (and now across runtimes): the same key must
 * land on the same partition on every node and after every restart.
 */
public final class ExchangeRouter {
    private ExchangeRouter() {}

    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L; // 14695981039346656037UL
    private static final long FNV_PRIME = 1099511628211L;

    /** Routes pre-extracted canonical key bytes to a partition in [0, partitionCount). Deterministic. */
    public static int partitionOf(byte[] keyBytes, int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1. (Parameter 'partitionCount', actual value was " + partitionCount + ")");
        }
        if (partitionCount == 1) return 0;

        long hash = fnv1a32(keyBytes);
        return (int) (hash % (long) partitionCount); // hash is a non-negative 32-bit value held in a long
    }

    /** Convenience overload for a canonical key already produced as a string — encodes it to UTF-8 bytes first. */
    public static int partitionOf(String canonicalKey, int partitionCount) {
        return partitionOf(utf8(canonicalKey), partitionCount);
    }

    /** .NET {@code Encoding.UTF8.GetBytes}: lone surrogates become U+FFFD (EF BF BD) rather than Java's '?'. */
    static byte[] utf8(String s) {
        try {
            CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .replaceWith(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD});
            ByteBuffer bb = enc.encode(CharBuffer.wrap(s));
            byte[] out = new byte[bb.remaining()];
            bb.get(out);
            return out;
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 64-bit FNV-1a folded to 32 bits (xor-fold) for a well-mixed partition hash. Returns the uint32 in a long. */
    private static long fnv1a32(byte[] data) {
        long hash = FNV_OFFSET_BASIS;
        for (byte b : data) {
            hash ^= (b & 0xFFL);
            hash *= FNV_PRIME; // wraps mod 2^64, like the C# ulong
        }
        return (hash ^ (hash >>> 32)) & 0xFFFFFFFFL;
    }
}
