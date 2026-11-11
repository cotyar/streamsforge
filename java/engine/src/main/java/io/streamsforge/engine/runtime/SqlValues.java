package io.streamsforge.engine.runtime;

/** Shared numeric/ordering helpers implementing the dialect's value semantics:
 *  long/double promotion on mixed arithmetic, ordinal string comparison. */
public final class SqlValues {
    private SqlValues() {}

    public static boolean isNumber(Object v) { return v instanceof Long || v instanceof Double; }

    public static double toDouble(Object v) {
        if (v instanceof Double d) return d;
        if (v instanceof Long l) return (double) l;
        return 0d;
    }

    /** .NET {@code double.CompareTo}: NaN sorts below everything and equals NaN; -0.0 equals 0.0
     *  (unlike {@link Double#compare}). */
    public static int compareDouble(double a, double b) {
        if (a < b) return -1;
        if (a > b) return 1;
        if (a == b) return 0;
        boolean an = Double.isNaN(a), bn = Double.isNaN(b);
        if (an) return bn ? 0 : -1;
        return 1;
    }

    /** Compares two same-domain non-null values (numbers promoted, strings ordinal, bools by value). */
    public static int compare(Object a, Object b) {
        if (a instanceof String sa && b instanceof String sb) return sa.compareTo(sb);
        if (a instanceof Boolean ba && b instanceof Boolean bb) return Boolean.compare(ba, bb);
        if (isNumber(a) && isNumber(b)) return compareDouble(toDouble(a), toDouble(b));
        return 0;
    }
}
