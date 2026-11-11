package io.streamsforge.engine.runtime;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.SourceSchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The runtime row flowing through joins/filter/window/select. Columns are keyed
 *  "{alias}_{field}" (so reserved "_ts"/"_source" become "{alias}__ts"/"{alias}__source"),
 *  which lets a single lookup formula serve both plain and reserved fields uniformly.
 *  Reference identity, like the C# class. */
public final class WorkingRow {
    private final long ts;
    private final List<String> aliases;
    private final Map<String, Object> fields;

    public WorkingRow(long ts, List<String> aliases, Map<String, Object> fields) {
        this.ts = ts; this.aliases = aliases; this.fields = fields;
    }

    public long ts() { return ts; }
    public List<String> aliases() { return aliases; }
    public Map<String, Object> fields() { return fields; }

    public static WorkingRow fromEvent(String alias, EventRecord evt) {
        var fields = new LinkedHashMap<String, Object>();
        for (var kv : evt.entrySet()) fields.put(alias + "_" + kv.getKey(), kv.getValue());
        var aliases = new ArrayList<String>();
        aliases.add(alias);
        return new WorkingRow(evt.timestamp(), aliases, fields);
    }

    public static WorkingRow combine(WorkingRow left, WorkingRow right) {
        var fields = new LinkedHashMap<String, Object>(left.fields);
        fields.putAll(right.fields);
        var aliases = new ArrayList<String>(left.aliases);
        aliases.addAll(right.aliases);
        return new WorkingRow(Math.max(left.ts, right.ts), aliases, fields);
    }

    /** A (alias, schema) pair for {@link #nullSide}. */
    public record Side(String alias, SourceSchema schema) {}

    /** Synthesizes an all-null row for one or more sides, used to null-pad outer-join misses. */
    public static WorkingRow nullSide(Iterable<Side> sides) {
        var fields = new LinkedHashMap<String, Object>();
        var aliases = new ArrayList<String>();
        for (var s : sides) {
            aliases.add(s.alias());
            fields.put(s.alias() + "_" + EventRecord.TIMESTAMP_FIELD, null);
            fields.put(s.alias() + "_" + EventRecord.SOURCE_FIELD, null);
            for (String field : s.schema().fields().keySet()) fields.put(s.alias() + "_" + field, null);
        }
        return new WorkingRow(0, aliases, fields);
    }
}
