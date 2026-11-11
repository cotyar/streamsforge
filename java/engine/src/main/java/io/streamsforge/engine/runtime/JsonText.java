package io.streamsforge.engine.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Compact JSON text rendering for the dialect's JSON value domain (Map&lt;String,Object&gt;, List&lt;Object&gt;,
 *  and primitive leaves). Used by {@code ->>} when the accessed node is itself a dict/list. */
public final class JsonText {
    private JsonText() {}

    public static String serialize(Object value) {
        var sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    /** Canonical (key-sorted, ordinal) JSON object text for a row's fields — the table (Z-set) runtime's
     *  multiset key: two rows with identical field values serialize identically regardless of insertion order.
     *  Output must stay byte-identical to the C# engine; row identity depends on it. */
    public static String serializeCanonicalRow(Map<String, ?> fields) {
        var sb = new StringBuilder();
        sb.append('{');
        String[] keys = fields.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        boolean first = true;
        for (String k : keys) {
            if (!first) sb.append(',');
            first = false;
            writeString(sb, k);
            sb.append(':');
            write(sb, fields.get(k));
        }
        sb.append('}');
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            writeString(sb, s);
        } else if (value instanceof Boolean b) {
            sb.append(b ? "true" : "false");
        } else if (value instanceof Long l) {
            sb.append(l.longValue());
        } else if (value instanceof Double d) {
            sb.append(Invariant.toString(d));
        } else if (value instanceof Map<?, ?> dict) {
            sb.append('{');
            boolean firstEntry = true;
            for (var kv : dict.entrySet()) {
                if (!firstEntry) sb.append(',');
                firstEntry = false;
                writeString(sb, (String) kv.getKey());
                sb.append(':');
                write(sb, kv.getValue());
            }
            sb.append('}');
        } else if (value instanceof List<?> list) {
            sb.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                write(sb, list.get(i));
            }
            sb.append(']');
        } else {
            // Unreachable for well-formed JSON values; fail soft.
            sb.append("null");
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append("\\u");
                        String hex = Integer.toHexString(c);
                        for (int p = hex.length(); p < 4; p++) sb.append('0');
                        sb.append(hex);
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        sb.append('"');
    }

    /** Convenience for tests / callers: a fresh mutable list. */
    public static List<Object> listOf(Object... items) { return new ArrayList<>(Arrays.asList(items)); }
}
