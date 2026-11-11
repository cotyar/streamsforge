package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.runtime.WorkingRow;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan 003 M2: the "wire encoding" used for TableDelta batches that cross an INTERNAL dataflow edge where the row in
 * flight is really a multi-alias {@link WorkingRow}. WorkingRow round-trips through EventRecord using two reserved
 * keys no SQL identifier can collide with (NUL-prefixed: the tokenizer never produces an identifier containing NUL).
 */
public final class WorkingRowWireCodec {
    private WorkingRowWireCodec() {}

    // The C# literals are "<NUL>wr_ts" / "<NUL>wr_aliases" (a real U+0000 as the first char).
    private static final String TS_KEY = "\u0000wr_ts";
    private static final String ALIASES_KEY = "\u0000wr_aliases";

    public static EventRecord toWire(WorkingRow row) {
        var evt = new EventRecord();
        for (var kv : row.fields().entrySet()) evt.put(kv.getKey(), kv.getValue());
        evt.put(TS_KEY, row.ts());
        evt.put(ALIASES_KEY, new ArrayList<Object>(row.aliases()));
        return evt;
    }

    public static WorkingRow fromWire(EventRecord evt) {
        long ts = evt.get(TS_KEY) instanceof Long l ? l : 0L;
        List<String> aliases = new ArrayList<>();
        if (evt.get(ALIASES_KEY) instanceof List<?> list) {
            for (Object x : list) aliases.add((String) x);
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        for (var kv : evt.entrySet()) {
            if (kv.getKey().equals(TS_KEY) || kv.getKey().equals(ALIASES_KEY)) continue;
            fields.put(kv.getKey(), kv.getValue());
        }
        return new WorkingRow(ts, aliases, fields);
    }
}
