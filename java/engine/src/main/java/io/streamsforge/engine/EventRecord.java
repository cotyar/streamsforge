package io.streamsforge.engine;

import java.util.LinkedHashMap;
import java.util.Map;

/** A stream event / result row. Reserved keys: "_ts" (epoch ms Long), "_source" (String).
 *  Value model: Long, Double, Boolean, String, null, Map&lt;String,Object&gt; and List&lt;Object&gt; (JSON). */
public final class EventRecord extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;
    public static final String TIMESTAMP_FIELD = "_ts";
    public static final String SOURCE_FIELD = "_source";

    public EventRecord() {}

    public EventRecord(Map<String, ? extends Object> other) { super(other); }

    public long timestamp() { return get(TIMESTAMP_FIELD) instanceof Long l ? l : 0L; }

    public String source() { return get(SOURCE_FIELD) instanceof String s ? s : ""; }
}
