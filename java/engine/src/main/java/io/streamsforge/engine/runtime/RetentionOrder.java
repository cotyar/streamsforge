package io.streamsforge.engine.runtime;

import java.util.Comparator;

/** Ordering shared by every retention scope: event timestamp ascending, then the entry's own identity string
 *  (ordinal) as the tie-break. */
public final class RetentionOrder {
    private RetentionOrder() {}

    /** The C# {@code (long Ts, string Key)} tuple. */
    public record TsKey(long ts, String key) {}

    public static final Comparator<TsKey> COMPARATOR = (a, b) -> {
        int byTs = Long.compare(a.ts(), b.ts());
        return byTs != 0 ? byTs : a.key().compareTo(b.key());
    };
}
