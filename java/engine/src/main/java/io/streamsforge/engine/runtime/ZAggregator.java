package io.streamsforge.engine.runtime;

import io.streamsforge.engine.sql.Ast.AggregateCallExpr;
import io.streamsforge.engine.sql.SqlFunctions;
import java.util.TreeMap;

/** Factory for the subtractable accumulators (see {@link IZAggregator}). */
public final class ZAggregator {
    private ZAggregator() {}

    public static IZAggregator create(AggregateCallExpr node) {
        String name = StatAggregatorNames.canonical(node.name);
        if (name == null) name = node.name;
        if (node.isDistinct) return new DistinctCountAggregator();
        if (name.equals(StatAggregatorNames.PERCENTILE_CONT)) return new PercentileZAggregator(AggregateParameters.probability(node));
        return createCanonical(name, node.isStar);
    }

    private static IZAggregator createCanonical(String name, boolean isStar) {
        switch (name) {
            case "COUNT": return new CountZAggregator(isStar);
            case "SUM": return new SumZAggregator();
            case "AVG": return new AvgZAggregator();
            case "MIN": return new MinMaxZAggregator(true);
            case "MAX": return new MinMaxZAggregator(false);
            case StatAggregatorNames.VAR_SAMP: return new VarianceZAggregator(true, false);
            case StatAggregatorNames.VAR_POP: return new VarianceZAggregator(false, false);
            case StatAggregatorNames.STDDEV_SAMP: return new VarianceZAggregator(true, true);
            case StatAggregatorNames.STDDEV_POP: return new VarianceZAggregator(false, true);
            case StatAggregatorNames.MEDIAN: return new MedianZAggregator();
            default: {
                var f = SqlFunctions.findAggregate(name);
                if (f == null) throw new IllegalArgumentException("Unknown aggregate '" + name + "'");
                return f.createZ();
            }
        }
    }
}

final class CountZAggregator implements IZAggregator {
    private final boolean isStar;
    private long count;
    CountZAggregator(boolean isStar) { this.isStar = isStar; }
    @Override public void apply(Object value, long weight) { if (isStar || value != null) count += weight; }
    @Override public Object result() { return count; }
    @Override public boolean isEmpty() { return count == 0; }
}

final class SumZAggregator implements IZAggregator {
    private double sum;
    private long longSum;
    private boolean sawDouble;
    private long count; // weighted count of non-NULL contributions

    @Override public void apply(Object value, long weight) {
        if (value instanceof Double d) { sum += d * weight; sawDouble = true; count += weight; }
        else if (value instanceof Long l) { sum += (double) (l * weight); longSum += l * weight; count += weight; }
    }

    // PostgreSQL: SUM over no non-NULL value (an all-NULL group) is NULL, not 0 — like AVG/MIN/MAX.
    @Override public Object result() { return count == 0 ? null : sawDouble ? (Object) sum : (Object) longSum; }

    // Linear: zero sums and count answer like a fresh accumulator for every future contribution.
    @Override public boolean isEmpty() { return sum == 0 && longSum == 0 && count == 0; }
}

final class AvgZAggregator implements IZAggregator {
    private double sum;
    private long count;

    @Override public void apply(Object value, long weight) {
        if (value instanceof Double d) { sum += d * weight; count += weight; }
        else if (value instanceof Long l) { sum += (double) (l * weight); count += weight; }
    }

    @Override public Object result() { return count == 0 ? null : (Object) (sum / count); }
    @Override public boolean isEmpty() { return count == 0 && sum == 0; }
}

/** Per-group SIGNED multiset of contributing values, ordered by SqlValues.compare — MIN/MAX read the first/last
 *  value whose count is positive. An entry is pruned only at exactly zero; a value retracted BEFORE it was
 *  asserted keeps a negative count (it used to be ignored, so the later assertion resurrected it: rows {3, 4},
 *  then -5, then +5 reported MAX 5 instead of 4) and stays invisible until its assertion nets it out. */
final class MinMaxZAggregator implements IZAggregator {
    private final boolean isMin;
    private final TreeMap<Object, Long> multiset = new TreeMap<>(SqlValues::compare);
    MinMaxZAggregator(boolean isMin) { this.isMin = isMin; }

    private int negativeEntries;

    @Override public void apply(Object value, long weight) {
        if (value == null || weight == 0) return;
        long existing = multiset.getOrDefault(value, 0L);
        long newWeight = existing + weight;
        if (existing < 0) negativeEntries--;
        if (newWeight < 0) negativeEntries++;
        if (newWeight == 0) multiset.remove(value);
        else multiset.put(value, newWeight);
    }

    @Override public Object result() {
        if (multiset.isEmpty()) return null;
        if (negativeEntries == 0) return isMin ? multiset.firstKey() : multiset.lastKey();
        for (var e : (isMin ? multiset : multiset.descendingMap()).entrySet()) {
            if (e.getValue() > 0) return e.getKey();
        }
        return null;
    }

    @Override public boolean isEmpty() { return multiset.isEmpty(); }
}
