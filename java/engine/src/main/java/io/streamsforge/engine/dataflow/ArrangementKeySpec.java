package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import java.util.List;

/**
 * Plan 003 M3 — canonical key-spec encoding for shared arrangements. An arrangement is identified by
 * (inputName, keySpec, partition): two tables that each join the SAME raw input on the SAME raw field(s) derive the
 * IDENTICAL keySpec string. The canonical form does NOT include the join alias or table name — only the raw field
 * name(s), in order, plus the partition count.
 */
public final class ArrangementKeySpec {
    private ArrangementKeySpec() {}

    /** Canonical, human-readable key-spec string for a raw-field key list + partition count. Length-prefixes each
     *  field so no delimiter collision is possible. */
    public static String canonicalize(List<String> fields, int partitionCount) {
        var sb = new StringBuilder();
        sb.append("P=").append(partitionCount).append(';');
        for (String f : fields) sb.append(f.length()).append(':').append(f).append(';');
        return sb.toString();
    }

    /** Short deterministic FNV-1a hex identity (16 lower-case hex digits) for a canonical key-spec string. */
    public static String hashOf(String canonicalKeySpec) {
        byte[] bytes = ExchangeRouter.utf8(canonicalKeySpec);
        long hash = 0xcbf29ce484222325L;
        final long prime = 1099511628211L;
        for (byte b : bytes) {
            hash ^= (b & 0xFFL);
            hash *= prime;
        }
        return String.format("%016x", hash);
    }

    /** Partition a raw EventRecord the SAME way TableDataflowPlan.partitionOf would for a HashPartition join edge
     *  keyed on these raw field(s). */
    public static int partitionOfRow(List<String> fields, EventRecord row, int partitionCount) {
        var values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) values[i] = row.get(fields.get(i)); // absent => null
        String canonical = TableKeyEncoding.encodeGroupKey(values);
        return ExchangeRouter.partitionOf(canonical, partitionCount);
    }
}
