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
}

final class SumZAggregator implements IZAggregator {
    private double sum;
    private long longSum;
    private boolean sawDouble;

    @Override public void apply(Object value, long weight) {
        if (value instanceof Double d) { sum += d * weight; sawDouble = true; }
        else if (value instanceof Long l) { sum += (double) (l * weight); longSum += l * weight; }
    }

    @Override public Object result() { return sawDouble ? (Object) sum : (Object) longSum; }
}

final class AvgZAggregator implements IZAggregator {
    private double sum;
    private long count;

    @Override public void apply(Object value, long weight) {
        if (value instanceof Double d) { sum += d * weight; count += weight; }
        else if (value instanceof Long l) { sum += (double) (l * weight); count += weight; }
    }

    @Override public Object result() { return count == 0 ? null : (Object) (sum / count); }
}

/** Per-group multiset of contributing values, ordered by SqlValues.compare — MIN/MAX read the first/last key.
 *  Retraction decrements the count; a value's entry is pruned once its count reaches zero. */
final class MinMaxZAggregator implements IZAggregator {
    private final boolean isMin;
    private final TreeMap<Object, Long> multiset = new TreeMap<>(SqlValues::compare);
    MinMaxZAggregator(boolean isMin) { this.isMin = isMin; }

    @Override public void apply(Object value, long weight) {
        if (value == null) return;
        Long existing = multiset.get(value);
        if (existing != null) {
            long newWeight = existing + weight;
            if (newWeight <= 0) multiset.remove(value);
            else multiset.put(value, newWeight);
        } else if (weight > 0) {
            multiset.put(value, weight);
        }
    }

    @Override public Object result() {
        return multiset.isEmpty() ? null : isMin ? multiset.firstKey() : multiset.lastKey();
    }
}
