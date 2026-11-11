package io.streamsforge.engine.runtime;

import java.util.List;
import java.util.Map;

/** Encodes a scalar value (join key, GROUP BY key component) into a comparable string —
 *  mirrors WindowOperator's group-key encoding so equal SQL values always collide to the same bucket. */
public final class TableKeyEncoding {
    private TableKeyEncoding() {}

    public static String encodeScalar(Object value) {
        if (value == null) return "N";
        if (value instanceof Long l) return "L:" + l.longValue();
        if (value instanceof Double d) return "D:" + Invariant.toString(d);
        if (value instanceof String s) return "S:" + s;
        if (value instanceof Boolean b) return b ? "B:True" : "B:False";
        if (value instanceof Map<?, ?> || value instanceof List<?>) return "J:" + JsonText.serialize(value);
        return "?";
    }

    public static String encodeGroupKey(Object[] values) {
        if (values.length == 0) return "∅";
        var sb = new StringBuilder();
        for (Object v : values) sb.append(encodeScalar(v));
        return sb.toString();
    }
}
