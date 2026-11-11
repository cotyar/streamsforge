package io.streamsforge.engine.runtime;

/** Java-only test bridge: {@code DistinctCountAggregator}/{@code MedianZAggregator} are package-private in main
 *  (C# {@code internal}, reachable from the test assembly via InternalsVisibleTo). This lives in the same package
 *  on the test classpath so the ported StatAggregateTests can drive them directly without touching main. */
public final class StatAggregateBridge {
    private StatAggregateBridge() {}

    public static final class DistinctCounter {
        private final DistinctCountAggregator agg = new DistinctCountAggregator();
        public void add(Object v) { agg.add(v); }
        public void apply(Object v, long weight) { agg.apply(v, weight); }
        public Object result() { return agg.result(); }
    }

    public static final class MedianZ {
        private final MedianZAggregator agg = new MedianZAggregator();
        public void apply(Object v, long weight) { agg.apply(v, weight); }
        public Object result() { return agg.result(); }
    }
}
