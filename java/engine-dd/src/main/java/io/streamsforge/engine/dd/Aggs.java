package io.streamsforge.engine.dd;

import io.streamsforge.dataflow.Weighted;
import io.streamsforge.engine.runtime.SqlValues;
import io.streamsforge.engine.sql.Ast.AggregateCallExpr;
import java.util.List;
import java.util.TreeMap;

/**
 * Aggregate functions computed natively over a group's multiset of argument values (value, net weight).
 * Deliberately NOT built on the engine's {@code IZAggregator}: the differential tests are only worth something when
 * the two sides do not share the aggregate implementation.
 */
final class Aggs {
    private Aggs() {}

    /** One reduce input element: the GROUP BY values of the row and, per aggregate call, the evaluated argument. */
    record Tup(List<Object> group, List<Object> args) {}

    /** Names this compiler can aggregate. */
    static boolean supported(AggregateCallExpr n) {
        return switch (n.name) {
            case "COUNT" -> true;
            case "SUM", "AVG", "MIN", "MAX" -> !n.isDistinct;
            default -> false;
        };
    }

    static Object compute(AggregateCallExpr n, int idx, List<Weighted<Tup>> in, boolean sqlStandardEmptySum) {
        switch (n.name) {
            case "COUNT": {
                if (n.isDistinct) {
                    var m = new TreeMap<Object, Long>(SqlValues::compare);
                    for (var w : in) {
                        Object v = w.value().args().get(idx);
                        if (v != null) m.merge(v, w.weight(), Long::sum);
                    }
                    long c = 0;
                    for (long x : m.values()) if (x > 0) c++;
                    return c;
                }
                long c = 0;
                for (var w : in) if (n.isStar || w.value().args().get(idx) != null) c += w.weight();
                return c;
            }
            case "SUM": {
                double ds = 0;
                long ls = 0;
                boolean sawDouble = false, any = false;
                for (var w : in) {
                    Object v = w.value().args().get(idx);
                    if (v instanceof Long l) { ls += l * w.weight(); ds += (double) (l * w.weight()); any = true; }
                    else if (v instanceof Double d) { ds += d * w.weight(); sawDouble = true; any = true; }
                }
                if (!any) return sqlStandardEmptySum ? null : (Object) 0L; // ANSI SQL: NULL; the engine's dialect: 0
                return sawDouble ? (Object) ds : (Object) ls;
            }
            case "AVG": {
                double s = 0;
                long c = 0;
                for (var w : in) {
                    Object v = w.value().args().get(idx);
                    if (v instanceof Long l) { s += (double) (l * w.weight()); c += w.weight(); }
                    else if (v instanceof Double d) { s += d * w.weight(); c += w.weight(); }
                }
                return c == 0 ? null : (Object) (s / c);
            }
            case "MIN":
            case "MAX": {
                var m = new TreeMap<Object, Long>(SqlValues::compare);
                for (var w : in) {
                    Object v = w.value().args().get(idx);
                    if (v != null) m.merge(v, w.weight(), Long::sum);
                }
                Object best = null;
                boolean min = n.name.equals("MIN");
                for (var e : (min ? m : m.descendingMap()).entrySet()) {
                    if (e.getValue() > 0) { best = e.getKey(); break; }
                }
                return best;
            }
            default:
                throw new IllegalArgumentException("unsupported aggregate " + n.name);
        }
    }
}
