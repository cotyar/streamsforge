package io.streamsforge.engine.runtime;

import io.streamsforge.engine.sql.Ast.AggregateCallExpr;
import io.streamsforge.engine.sql.SqlFunctions;

/** Incremental (streaming) aggregate accumulator. NULL inputs are skipped (except COUNT(*), which
 *  counts rows regardless of value). */
public abstract class Aggregator {
    public abstract void add(Object value);

    public abstract Object result();

    /** The extension seam is add/result (what a registered aggregate implements), not this factory. */
    public static Aggregator create(AggregateCallExpr node) {
        String name = StatAggregatorNames.canonical(node.name);
        if (name == null) name = node.name;
        if (node.isDistinct) return new DistinctCountAggregator();
        if (name.equals(StatAggregatorNames.PERCENTILE_CONT)) return new PercentileAggregator(AggregateParameters.probability(node));
        return createCanonical(name, node.isStar);
    }

    private static Aggregator createCanonical(String name, boolean isStar) {
        switch (name) {
            case "COUNT": return new CountAggregator(isStar);
            case "SUM": return new SumAggregator();
            case "AVG": return new AvgAggregator();
            case "MIN": return new MinMaxAggregator(true);
            case "MAX": return new MinMaxAggregator(false);
            case StatAggregatorNames.VAR_SAMP: return new VarianceAggregator(true, false);
            case StatAggregatorNames.VAR_POP: return new VarianceAggregator(false, false);
            case StatAggregatorNames.STDDEV_SAMP: return new VarianceAggregator(true, true);
            case StatAggregatorNames.STDDEV_POP: return new VarianceAggregator(false, true);
            case StatAggregatorNames.MEDIAN: return new MedianAggregator();
            default: {
                // Registered aggregates are consulted only after the built-ins.
                var f = SqlFunctions.findAggregate(name);
                if (f == null) throw new IllegalArgumentException("Unknown aggregate '" + name + "'");
                return f.createStream();
            }
        }
    }
}

final class CountAggregator extends Aggregator {
    private final boolean isStar;
    private long count;
    CountAggregator(boolean isStar) { this.isStar = isStar; }
    @Override public void add(Object value) { if (isStar || value != null) count++; }
    @Override public Object result() { return count; }
}

final class SumAggregator extends Aggregator {
    private double sum;
    private long longSum;
    private boolean sawDouble;
    private boolean any;

    @Override public void add(Object value) {
        if (value instanceof Double d) { sum += d; sawDouble = true; any = true; }
        else if (value instanceof Long l) { sum += l; longSum += l; any = true; }
    }

    // PostgreSQL: SUM over no non-NULL value is NULL, not 0.
    @Override public Object result() { return !any ? null : sawDouble ? (Object) sum : (Object) longSum; }
}

final class AvgAggregator extends Aggregator {
    private double sum;
    private long count;

    @Override public void add(Object value) {
        if (value instanceof Double d) { sum += d; count++; }
        else if (value instanceof Long l) { sum += l; count++; }
    }

    @Override public Object result() { return count == 0 ? null : (Object) (sum / count); }
}

final class MinMaxAggregator extends Aggregator {
    private final boolean isMin;
    private Object best;
    private boolean any;
    MinMaxAggregator(boolean isMin) { this.isMin = isMin; }

    @Override public void add(Object value) {
        if (value == null) return;
        if (!any) { best = value; any = true; return; }
        int cmp = SqlValues.compare(value, best);
        if ((isMin && cmp < 0) || (!isMin && cmp > 0)) best = value;
    }

    @Override public Object result() { return best; }
}
