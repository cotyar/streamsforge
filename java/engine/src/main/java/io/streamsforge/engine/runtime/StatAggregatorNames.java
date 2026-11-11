package io.streamsforge.engine.runtime;

import io.streamsforge.engine.sql.Ast;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The statistical aggregates: VAR_SAMP/VAR_POP/STDDEV_SAMP/STDDEV_POP (with the bare STDDEV/VARIANCE
 * spellings aliased to the _SAMP forms, which is what SQL means by them) and MEDIAN, plus PERCENTILE_CONT and
 * COUNT(DISTINCT). Both families are built to be <b>subtractable</b>, because the table path maintains a
 * Z-set: a row superseded by LATEST BY comes back with weight −1. An aggregate that only knows how to add
 * would keep a retracted row's contribution forever, and the error is silent — the number just drifts.
 */
public final class StatAggregatorNames {
    private StatAggregatorNames() {}

    public static final String VAR_SAMP = "VAR_SAMP";
    public static final String VAR_POP = "VAR_POP";
    public static final String STDDEV_SAMP = "STDDEV_SAMP";
    public static final String STDDEV_POP = "STDDEV_POP";
    public static final String MEDIAN = "MEDIAN";
    public static final String PERCENTILE_CONT = "PERCENTILE_CONT";

    /** The bare spellings, mapped to what SQL means by them (the sample forms). Null when not a stat name. */
    public static String canonical(String upperName) {
        switch (upperName) {
            case "VARIANCE": case "VAR": return VAR_SAMP;
            case "STDDEV": case "STDEV": return STDDEV_SAMP;
            case VAR_SAMP: case VAR_POP: case STDDEV_SAMP: case STDDEV_POP: case MEDIAN: case PERCENTILE_CONT: return upperName;
            default: return null;
        }
    }

    private static final List<String> ALL = List.of(
        VAR_SAMP, VAR_POP, STDDEV_SAMP, STDDEV_POP, "VARIANCE", "VAR", "STDDEV", "STDEV", MEDIAN, PERCENTILE_CONT);

    public static List<String> all() { return ALL; }

    /** Aggregates written {@code NAME(parameter, value)}. */
    public static boolean takesParameter(String name) { return PERCENTILE_CONT.equalsIgnoreCase(name); }
}

/**
 * Running moments in the subtractable (n, Σy, Σy²) form, where y = x − K for a fixed offset K taken from the
 * first value this accumulator ever sees (avoids catastrophic cancellation; Welford cannot subtract).
 * K never changes once set, including across retractions.
 */
final class Moments {
    private double offset;
    private boolean haveOffset;
    private double sum;
    private double sumSquares;
    private long count;

    void apply(Object value, long weight) {
        if (!SqlValues.isNumber(value)) return;
        double x = SqlValues.toDouble(value);
        if (!haveOffset) { offset = x; haveOffset = true; }
        double y = x - offset;
        count += weight;
        sum += y * weight;
        sumSquares += y * y * weight;
    }

    long count() { return count; }

    /** Population variance, or null when there is nothing to describe. Clamped at zero. */
    Double populationVariance() {
        if (count <= 0) return null;
        double mean = sum / count;
        return Math.max(0d, sumSquares / count - mean * mean);
    }

    /** Sample variance — null below two observations. */
    Double sampleVariance() {
        if (count < 2) return null;
        double mean = sum / count;
        return Math.max(0d, (sumSquares - sum * mean) / (count - 1));
    }

    Object result(boolean sample, boolean stdDev) {
        Double variance = sample ? sampleVariance() : populationVariance();
        if (variance == null) return null;
        return stdDev ? (Object) Math.sqrt(variance) : (Object) variance;
    }
}

final class VarianceAggregator extends Aggregator {
    private final Moments moments = new Moments();
    private final boolean sample, stdDev;
    VarianceAggregator(boolean sample, boolean stdDev) { this.sample = sample; this.stdDev = stdDev; }
    @Override public void add(Object value) { moments.apply(value, 1); }
    @Override public Object result() { return moments.result(sample, stdDev); }
}

final class VarianceZAggregator implements IZAggregator {
    private final Moments moments = new Moments();
    private final boolean sample, stdDev;
    VarianceZAggregator(boolean sample, boolean stdDev) { this.sample = sample; this.stdDev = stdDev; }
    @Override public void apply(Object value, long weight) { moments.apply(value, weight); }
    @Override public Object result() { return moments.result(sample, stdDev); }
}

/**
 * A weight-aware ordered multiset of numeric values — the shape a quantile needs and a running sum cannot
 * provide. Exact, over an ordered map — O(n) to read a quantile (the upgrade path is a t-digest behind this
 * same class).
 */
final class NumericMultiset {
    private final TreeMap<Double, Long> counts = new TreeMap<>(SqlValues::compareDouble);
    private long total;

    void apply(Object value, long weight) {
        if (!SqlValues.isNumber(value)) return;
        double x = SqlValues.toDouble(value);
        Long existing = counts.get(x);
        long next = existing != null ? existing + weight : weight;
        if (next <= 0) counts.remove(x);
        else counts.put(x, next);
        total += weight;
        if (total < 0) total = 0;
    }

    /** Continuous quantile with linear interpolation (PERCENTILE_CONT; MEDIAN is p = 0.5). Null when empty. */
    Object quantile(double p) {
        if (counts.isEmpty() || total <= 0) return null;
        if (p <= 0) return counts.firstKey();
        if (p >= 1) return counts.lastKey();

        double rank = p * (total - 1);
        long lowerIndex = (long) Math.floor(rank);
        double fraction = rank - lowerIndex;

        double lower = 0, upper;
        long seen = 0;
        boolean haveLower = false;
        for (var e : counts.entrySet()) {
            double value = e.getKey();
            seen += e.getValue();
            if (!haveLower && seen > lowerIndex) {
                lower = value;
                haveLower = true;
                if (seen > lowerIndex + 1) { upper = value; return interpolate(lower, upper, fraction); }
                continue;
            }
            if (haveLower) { upper = value; return interpolate(lower, upper, fraction); }
        }
        return haveLower ? (Object) lower : null;
    }

    private static Object interpolate(double lower, double upper, double fraction) {
        return fraction <= 0 ? lower : lower + (upper - lower) * fraction;
    }
}

final class PercentileAggregator extends Aggregator {
    private final NumericMultiset values = new NumericMultiset();
    private final double p;
    PercentileAggregator(double p) { this.p = p; }
    @Override public void add(Object value) { values.apply(value, 1); }
    @Override public Object result() { return values.quantile(p); }
}

final class PercentileZAggregator implements IZAggregator {
    private final NumericMultiset values = new NumericMultiset();
    private final double p;
    PercentileZAggregator(double p) { this.p = p; }
    @Override public void apply(Object value, long weight) { values.apply(value, weight); }
    @Override public Object result() { return values.quantile(p); }
}

/**
 * COUNT(DISTINCT x): a multiset of the values seen, whose result is how many distinct ones still have a
 * positive count. Retraction decrements and prunes at zero.
 */
final class DistinctCountAggregator extends Aggregator implements IZAggregator {
    /** Equality that agrees with {@link SqlValues#compare} — the long 1 and the double 1.0 are one value. */
    private static final class Key {
        final Object v;
        Key(Object v) { this.v = v; }
        @Override public boolean equals(Object o) { return o instanceof Key k && SqlValues.compare(v, k.v) == 0; }
        @Override public int hashCode() {
            if (SqlValues.isNumber(v)) { double d = SqlValues.toDouble(v); return d == 0 ? 0 : Double.hashCode(d); }
            return v.hashCode();
        }
    }

    private final Map<Key, Long> counts = new HashMap<>();

    @Override public void add(Object value) { apply(value, 1); }

    @Override public void apply(Object value, long weight) {
        if (value == null) return;
        var k = new Key(value);
        Long existing = counts.get(k);
        long next = existing != null ? existing + weight : weight;
        if (next <= 0) counts.remove(k);
        else counts.put(k, next);
    }

    @Override public Object result() { return (long) counts.size(); }
}

final class MedianAggregator extends Aggregator {
    private final NumericMultiset values = new NumericMultiset();
    @Override public void add(Object value) { values.apply(value, 1); }
    @Override public Object result() { return values.quantile(0.5); }
}

final class MedianZAggregator implements IZAggregator {
    private final NumericMultiset values = new NumericMultiset();
    @Override public void apply(Object value, long weight) { values.apply(value, weight); }
    @Override public Object result() { return values.quantile(0.5); }
}
