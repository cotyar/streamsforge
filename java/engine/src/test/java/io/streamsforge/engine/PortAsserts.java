package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Java-only xUnit-style assertion helpers for the ported engine tests ({@code Assert.Single}, {@code Assert.Contains},
 *  {@code Assert.IsType<double>}, {@code string.Join(";", diagnostics)}). */
public final class PortAsserts {
    private PortAsserts() {}

    /** xUnit {@code Assert.Single(collection)}. */
    public static <T> T single(Collection<T> c) {
        assertEquals(1, c.size(), "Assert.Single: expected exactly one element but got " + c);
        return c.iterator().next();
    }

    /** xUnit {@code Assert.Single(collection, predicate)}. */
    public static <T> T single(Collection<T> c, Predicate<T> p) {
        var matches = c.stream().filter(p).collect(Collectors.toList());
        assertEquals(1, matches.size(), "Assert.Single(predicate): expected exactly one match but got " + matches + " in " + c);
        return matches.get(0);
    }

    /** xUnit {@code Assert.Contains(collection, predicate)}. */
    public static <T> void containsMatch(Collection<T> c, Predicate<T> p) {
        assertTrue(c.stream().anyMatch(p), "Assert.Contains: no element matched in " + c);
    }

    /** xUnit {@code Assert.IsType<double>(x)}: exact boxed-type check, returns the primitive. */
    public static double isDouble(Object x) {
        assertTrue(x != null && x.getClass() == Double.class, "Expected Double but got " + (x == null ? "null" : x.getClass().getName() + " " + x));
        return (Double) x;
    }

    /** {@code string.Join(";", diagnostics)}. */
    public static String diag(List<SqlDiagnostic> diagnostics) {
        return diagnostics.stream().map(d -> d.line() + ":" + d.column() + " " + d.message()).collect(Collectors.joining(";"));
    }
}
