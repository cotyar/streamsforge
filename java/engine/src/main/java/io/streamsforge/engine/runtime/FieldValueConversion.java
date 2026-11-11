package io.streamsforge.engine.runtime;

import io.streamsforge.engine.FieldKind;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

/**
 * Canonical, non-throwing scalar coercion by {@link FieldKind} — the semantic core behind the dialect's
 * {@code TO_LONG}/{@code TO_DOUBLE}/{@code TO_BOOL}/{@code TO_TIMESTAMP}/{@code TO_STRING} SQL functions and
 * the {@code CAST(expr AS type)} sugar.
 *
 * <p>Rules pinned to the C# engine, including two deliberately surprising spots:
 * <ul>
 * <li>Bool-from-string is permissive: "true"/"false" parse case-insensitively and every OTHER non-empty,
 *     non-"0" string (garbage included) coerces to {@code true}.</li>
 * <li>Bool-from-number is nonzero-is-true.</li>
 * <li>Double-to-long uses an UNCHECKED narrowing cast ({@code (long)d}): a double outside the long range does
 *     NOT come back NULL. A numeric-STRING overflow does come back NULL.</li>
 * </ul>
 * <p>CLR date/time mapping (deviation, see README): {@code DateTimeOffset} ↔ {@link OffsetDateTime}/
 * {@link ZonedDateTime}/{@link Instant}; {@code DateTime} ↔ {@link LocalDateTime} (read as UTC, like
 * {@code DateTimeKind.Unspecified}). CLR numerics map to {@link Number} subclasses (Integer, Short, Byte, Float,
 * BigDecimal for decimal, BigInteger for ulong).
 */
public final class FieldValueConversion {
    private FieldValueConversion() {}

    /** Result of a {@code Try*} conversion: C#'s {@code bool} return + {@code out} value. */
    public record Coercion(boolean ok, Object value) {
        static final Coercion FAIL = new Coercion(false, null);
        static Coercion of(Object v) { return new Coercion(true, v); }
    }

    /** Coerces one already-normalized scalar leaf value to the shape {@code kind} expects. JSON is structural,
     *  so it always succeeds by passing the value through unchanged. */
    public static Coercion tryCoerce(FieldKind kind, Object value) {
        switch (kind) {
            case STRING: return Coercion.of(toDisplayString(value));
            case DOUBLE: return tryToDouble(value);
            case LONG: return tryToLong(value);
            case TIMESTAMP: return tryToTimestampMillis(value);
            case BOOL: return tryToBool(value);
            case JSON: return Coercion.of(value);
            default: return Coercion.FAIL;
        }
    }

    /** The TO_TIMESTAMP SQL function's own, wider rule: epoch-ms as a number OR a numeric string, or ISO-8601 text. */
    public static Coercion tryToTimestamp(Object value) {
        if (value instanceof Long l) return Coercion.of(l);
        if (value instanceof Double d) return Coercion.of((long) d.doubleValue());
        Long dt = dateTimeMillis(value);
        if (dt != null) return Coercion.of(dt);
        if (value instanceof String s) {
            Long epoch = parseLong(s);
            if (epoch != null) return Coercion.of(epoch);
            Long parsed = parseDateTimeOffsetMillis(s);
            if (parsed != null) return Coercion.of(parsed);
            return Coercion.FAIL;
        }
        return Coercion.FAIL;
    }

    /** Behavioral port of RowTimestamp.Resolve: a number is epoch-ms, a string is parsed as ISO-8601 (UTC);
     *  anything else falls back to {@code fallbackMs}. */
    public static long resolveTimestamp(Object value, long fallbackMs) {
        if (value instanceof Long l) return l;
        if (value instanceof Double d) return (long) d.doubleValue();
        Long dt = dateTimeMillis(value);
        if (dt != null) return dt;
        if (value instanceof String s) {
            Long parsed = parseDateTimeOffsetMillis(s);
            if (parsed != null) return parsed;
        }
        return fallbackMs;
    }

    private static String toDisplayString(Object value) {
        if (value instanceof String s) return s;
        if (value instanceof Boolean b) return b ? "true" : "false";
        if (dateTimeMillis(value) != null) return formatIso8601(value);
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) return value.toString();
        if (value instanceof Double d) return Invariant.toString(d);
        if (value instanceof Float f) return Invariant.toString(Double.parseDouble(Float.toString(f)));
        if (value instanceof BigDecimal bd) return bd.toPlainString();
        if (value instanceof BigInteger bi) return bi.toString();
        return value.toString();
    }

    /** ISO-8601 UTC, milliseconds, always Z — the single textual shape for an instant in this dialect. */
    public static String formatIso8601(Object value) {
        Long ms = dateTimeMillis(value);
        if (ms != null) return formatEpochMsIso8601(ms);
        if (value instanceof Long l) return formatEpochMsIso8601(l);
        return formatEpochMsIso8601(0);
    }

    private static final DateTimeFormatter ISO_MS = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'");

    /** The one formatter for epoch millis → ISO-8601 text, shared with TO_STRING(TO_TIMESTAMP(x)). */
    public static String formatEpochMsIso8601(long epochMs) {
        return ISO_MS.format(Instant.ofEpochMilli(epochMs).atOffset(ZoneOffset.UTC));
    }

    /** Epoch millis of a CLR date/time analogue; null when {@code value} is not one. */
    private static Long dateTimeMillis(Object value) {
        if (value instanceof Instant i) return i.toEpochMilli();
        if (value instanceof OffsetDateTime o) return o.toInstant().toEpochMilli();
        if (value instanceof ZonedDateTime z) return z.toInstant().toEpochMilli();
        if (value instanceof LocalDateTime l) return l.toInstant(ZoneOffset.UTC).toEpochMilli(); // Unspecified -> UTC
        return null;
    }

    private static Coercion tryToTimestampMillis(Object value) {
        Long dt = dateTimeMillis(value);
        if (dt != null) return Coercion.of(dt);
        return tryToLong(value);
    }

    private static Coercion tryToDouble(Object value) {
        if (value instanceof Double d) return Coercion.of(d);
        if (value instanceof Float f) return Coercion.of((double) f);
        if (value instanceof Long l) return Coercion.of((double) l);
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) return Coercion.of(((Number) value).doubleValue());
        if (value instanceof BigDecimal m) return Coercion.of(m.doubleValue());
        if (value instanceof BigInteger u) return Coercion.of(u.doubleValue());
        if (value instanceof Boolean b) return Coercion.of(b ? 1d : 0d);
        if (value instanceof String s) {
            Double parsed = parseDoubleFloatThousands(s);
            if (parsed != null) return Coercion.of(parsed);
        }
        return Coercion.FAIL;
    }

    private static Coercion tryToLong(Object value) {
        if (value instanceof Long l) return Coercion.of(l);
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) return Coercion.of(((Number) value).longValue());
        if (value instanceof Double d) return Coercion.of((long) d.doubleValue()); // unchecked, see class doc
        if (value instanceof Float f) return Coercion.of((long) f.floatValue());
        if (value instanceof BigDecimal m) return Coercion.of(m.longValue()); // truncates toward zero
        if (value instanceof BigInteger u) return Coercion.of(u.longValue());
        if (value instanceof Boolean b) return Coercion.of(b ? 1L : 0L);
        if (value instanceof String s) {
            Long parsed = parseLong(s);
            if (parsed != null) return Coercion.of(parsed);
        }
        return Coercion.FAIL;
    }

    private static Coercion tryToBool(Object value) {
        if (value instanceof Boolean b) return Coercion.of(b);
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) return Coercion.of(((Number) value).longValue() != 0);
        if (value instanceof Double d) return Coercion.of(d != 0);
        if (value instanceof Float f) return Coercion.of(f != 0);
        if (value instanceof BigDecimal m) return Coercion.of(m.signum() != 0);
        if (value instanceof BigInteger u) return Coercion.of(u.signum() != 0);
        if (value instanceof String s) {
            String t = trimWhite(s);
            if (t.equalsIgnoreCase("true")) return Coercion.of(true);
            if (t.equalsIgnoreCase("false")) return Coercion.of(false);
            return Coercion.of(!(s.isEmpty() || s.equals("0")));
        }
        return Coercion.FAIL;
    }

    // ------------------------------------------------------------------
    // .NET-compatible parsing helpers (culture-invariant)
    // ------------------------------------------------------------------

    private static boolean isWhite(char c) { return (c >= '\t' && c <= '\r') || c == ' '; }

    private static String trimWhite(String s) {
        int a = 0, b = s.length();
        while (a < b && isWhite(s.charAt(a))) a++;
        while (b > a && isWhite(s.charAt(b - 1))) b--;
        return s.substring(a, b);
    }

    private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");

    /** {@code long.TryParse(s, NumberStyles.Integer, Invariant)}: optional surrounding whitespace and sign, ASCII digits. */
    public static Long parseLong(String s) {
        String t = trimWhite(s);
        if (!INTEGER.matcher(t).matches()) return null;
        try { return Long.parseLong(t.startsWith("+") ? t.substring(1) : t); } catch (NumberFormatException e) { return null; }
    }

    private static final Pattern FLOAT = Pattern.compile("[+-]?(?:[0-9][0-9,]*)?(?:\\.[0-9]*)?(?:[eE][+-]?[0-9]+)?");

    /** {@code double.TryParse(s, Float | AllowThousands, Invariant)}. */
    private static Double parseDoubleFloatThousands(String s) {
        String t = trimWhite(s);
        if (t.isEmpty()) return null;
        String u = t.startsWith("+") || t.startsWith("-") ? t.substring(1) : t;
        if (u.equalsIgnoreCase("NaN")) return Double.NaN;
        if (u.equalsIgnoreCase("Infinity")) return t.startsWith("-") ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        if (!FLOAT.matcher(t).matches()) return null;
        // Need at least one digit in the mantissa.
        int e = Math.max(t.indexOf('e'), t.indexOf('E'));
        String mant = e >= 0 ? t.substring(0, e) : t;
        boolean hasDigit = false;
        for (int i = 0; i < mant.length(); i++) if (mant.charAt(i) >= '0' && mant.charAt(i) <= '9') { hasDigit = true; break; }
        if (!hasDigit) return null;
        try { return Double.parseDouble(t.replace(",", "")); } catch (NumberFormatException ex) { return null; }
    }

    private static final DateTimeFormatter[] US_PATTERNS = {
        DateTimeFormatter.ofPattern("M/d/uuuu H:mm:ss"),
        DateTimeFormatter.ofPattern("M/d/uuuu H:mm"),
    };

    /** Broad {@code DateTimeOffset.TryParse(AssumeUniversal | AdjustToUniversal)} stand-in: ISO-8601 with or
     *  without offset, space-separated date/time, date-only, and the invariant-culture US form. */
    private static Long parseDateTimeOffsetMillis(String s) {
        String t = trimWhite(s);
        if (t.isEmpty()) return null;
        try { return OffsetDateTime.parse(t).toInstant().toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
        try { return LocalDateTime.parse(t).toInstant(ZoneOffset.UTC).toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
        int sp = t.indexOf(' ');
        if (sp > 0) {
            String iso = t.substring(0, sp) + "T" + t.substring(sp + 1).trim();
            try { return OffsetDateTime.parse(iso).toInstant().toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
            try { return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC).toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
        }
        try { return LocalDate.parse(t).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
        for (var p : US_PATTERNS) {
            try { return LocalDateTime.parse(t, p).toInstant(ZoneOffset.UTC).toEpochMilli(); } catch (DateTimeParseException ignored) { /* next */ }
        }
        try { return LocalDate.parse(t, DateTimeFormatter.ofPattern("M/d/uuuu")).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(); }
        catch (DateTimeParseException ignored) { /* none */ }
        return null;
    }
}
