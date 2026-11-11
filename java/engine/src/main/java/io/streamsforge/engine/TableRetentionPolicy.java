package io.streamsforge.engine;

/**
 * Plan 011 C2 — an opt-in, per-table ROW RETENTION policy. DEFAULT OFF ({@link #NONE}), and that default is
 * load-bearing: a table with retention is NOT the full relation its SQL describes, it is a BOUNDED VIEW of
 * it — rows the SQL says belong in the table are deliberately dropped, with a real retraction, once the bound
 * is exceeded. Enable it when an unbounded key space would otherwise grow the table forever.
 *
 * <p>The two bounds compose (both applied, TTL first): {@code maxRows} caps retained rows; {@code ttlMs} caps
 * how far back in EVENT TIME a retained row may be. Both are evaluated against the row's event timestamp,
 * never the wall clock.
 *
 * @param maxRows maximum retained rows; 0 = unbounded
 * @param ttlMs maximum age in event-time ms, measured back from the highest event timestamp admitted; 0 = unbounded
 */
public record TableRetentionPolicy(int maxRows, long ttlMs) {
    /** Retention off — the default for every table, and byte-for-byte the pre-011 behavior. */
    public static final TableRetentionPolicy NONE = new TableRetentionPolicy(0, 0);

    /** True when at least one bound is set. */
    public boolean isEnabled() { return maxRows > 0 || ttlMs > 0; }
}
