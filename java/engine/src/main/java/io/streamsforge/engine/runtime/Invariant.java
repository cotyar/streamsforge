package io.streamsforge.engine.runtime;

import java.math.BigDecimal;

/**
 * Culture-invariant text rendering that is byte-identical to .NET's {@code double.ToString(InvariantCulture)}
 * (shortest round-trippable digits; fixed notation for decimal exponents in [-4, 15), otherwise
 * {@code d.dddE+XX}). Java's {@code Double.toString} differs ("1.0E20", "100.0"), and row identity
 * ({@link JsonText#serializeCanonicalRow}) depends on this exact text.
 */
public final class Invariant {
    private Invariant() {}

    public static String toString(double d) {
        if (Double.isNaN(d)) return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
        if (d == 0) return (1 / d < 0) ? "-0" : "0";
        // JDK 19+ Double.toString yields the shortest digits that round-trip (same set .NET Core 3.0+ produces).
        var bd = new BigDecimal(Double.toString(Math.abs(d))).stripTrailingZeros();
        String digits = bd.unscaledValue().toString();
        int exp10 = digits.length() - bd.scale() - 1; // scientific exponent: value = d.ddd x 10^exp10
        var sb = new StringBuilder();
        if (d < 0) sb.append('-');
        if (exp10 >= -4 && exp10 < 15) {
            if (exp10 < 0) {
                sb.append("0.");
                for (int i = 0; i < -exp10 - 1; i++) sb.append('0');
                sb.append(digits);
            } else if (digits.length() <= exp10 + 1) {
                sb.append(digits);
                for (int i = digits.length(); i < exp10 + 1; i++) sb.append('0');
            } else {
                sb.append(digits, 0, exp10 + 1).append('.').append(digits, exp10 + 1, digits.length());
            }
        } else {
            sb.append(digits.charAt(0));
            if (digits.length() > 1) sb.append('.').append(digits, 1, digits.length());
            sb.append('E').append(exp10 < 0 ? '-' : '+');
            int ae = Math.abs(exp10);
            if (ae < 10) sb.append('0');
            sb.append(ae);
        }
        return sb.toString();
    }
}
