package io.streamsforge.engine.dd;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableExecutor;

/** An output row of a dataflow-compiled table. Identity is the engine's canonical row key, so a collection of
 *  {@code Out} is a Z-set under exactly the row identity {@link TableExecutor#snapshot()} uses. */
public final class Out {
    private final String key;
    private final EventRecord row;

    public Out(EventRecord row) {
        this.row = row;
        this.key = TableExecutor.canonicalRowKeyOf(row);
    }

    public String key() { return key; }
    public EventRecord row() { return row; }

    @Override public boolean equals(Object o) { return o instanceof Out other && key.equals(other.key); }
    @Override public int hashCode() { return key.hashCode(); }
    @Override public String toString() { return key; }
}
