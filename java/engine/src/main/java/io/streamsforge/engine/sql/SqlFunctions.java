package io.streamsforge.engine.sql;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The registry the Validator, Parser and evaluator consult <b>after</b> their built-in switches, so an assembly
 * outside the Engine can add SQL functions without editing it. A static list (not DI discovery), registered from
 * host startup before anything compiles SQL; registration is idempotent by name.
 *
 * <p><b>Built-ins always win.</b> A registration whose name collides with a built-in function or aggregate is
 * rejected loudly at registration time rather than silently shadowing it — a third party redefining SUM would
 * change the meaning of SQL already deployed, and the failure would surface as wrong numbers.
 */
public final class SqlFunctions {
    private SqlFunctions() {}

    private static final Object GATE = new Object();
    private static final List<IScalarFunction> SCALARS = new ArrayList<>();
    private static final List<IAggregateFunction> AGGREGATES = new ArrayList<>();

    private static final List<String> BUILT_IN_SCALARS = List.of(
        "ABS", "ROUND", "UPPER", "LOWER", "COALESCE",
        "TO_LONG", "TO_DOUBLE", "TO_BOOL", "TO_TIMESTAMP", "TO_STRING",
        "IF");

    private static final List<String> BUILT_IN_AGGREGATES = List.of("COUNT", "SUM", "AVG", "MIN", "MAX");

    public static List<String> builtInScalarNames() { return BUILT_IN_SCALARS; }

    public static List<String> builtInAggregateNames() { return BUILT_IN_AGGREGATES; }

    /** Registers a scalar function. Re-registering the same name replaces the previous entry; colliding with a
     *  built-in throws. */
    public static void register(IScalarFunction function) {
        if (function == null) throw new NullPointerException("function");
        rejectReservedName(function.name());
        synchronized (GATE) {
            SCALARS.removeIf(f -> same(f.name(), function.name()));
            SCALARS.add(function);
        }
    }

    public static void register(IAggregateFunction aggregate) {
        if (aggregate == null) throw new NullPointerException("aggregate");
        rejectReservedName(aggregate.name());
        synchronized (GATE) {
            AGGREGATES.removeIf(a -> same(a.name(), aggregate.name()));
            AGGREGATES.add(aggregate);
        }
    }

    public static IScalarFunction findScalar(String name) {
        if (name == null || name.isEmpty()) return null;
        synchronized (GATE) {
            for (var f : SCALARS) if (same(f.name(), name)) return f;
            return null;
        }
    }

    public static IAggregateFunction findAggregate(String name) {
        if (name == null || name.isEmpty()) return null;
        synchronized (GATE) {
            for (var a : AGGREGATES) if (same(a.name(), name)) return a;
            return null;
        }
    }

    /** Every registered scalar name, sorted (case-insensitive ordinal). */
    public static List<String> registeredScalarNames() {
        synchronized (GATE) {
            var names = new ArrayList<String>();
            for (var f : SCALARS) names.add(f.name());
            names.sort(Comparator.comparing(String::toLowerCase));
            return names;
        }
    }

    public static List<String> registeredAggregateNames() {
        synchronized (GATE) {
            var names = new ArrayList<String>();
            for (var a : AGGREGATES) names.add(a.name());
            names.sort(Comparator.comparing(String::toLowerCase));
            return names;
        }
    }

    /** Test seam only — the registry is process-global. */
    public static void clear() {
        synchronized (GATE) {
            SCALARS.clear();
            AGGREGATES.clear();
        }
    }

    private static void rejectReservedName(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("A SQL function name may not be empty.");
        for (String b : BUILT_IN_SCALARS) if (same(b, name)) throw reserved(name);
        for (String b : BUILT_IN_AGGREGATES) if (same(b, name)) throw reserved(name);
    }

    private static IllegalArgumentException reserved(String name) {
        return new IllegalArgumentException("'" + name + "' is a built-in function and cannot be replaced — a registration that shadowed it would "
            + "change the meaning of SQL already written against the built-in, and the damage would show up as "
            + "wrong values rather than as an error. (Parameter name: name)");
    }

    private static boolean same(String a, String b) { return a.equalsIgnoreCase(b); }
}
