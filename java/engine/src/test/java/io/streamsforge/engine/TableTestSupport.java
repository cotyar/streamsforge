package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Java-only helpers shared by the table-mode test ports (xunit's Assert.Single / index-from-end have no JUnit twin). */
public final class TableTestSupport {
    private TableTestSupport() {}

    /** xunit {@code Assert.Single(collection)}. */
    public static <T> T single(List<T> list) {
        assertEquals(1, list.size(), "Expected exactly one element");
        return list.get(0);
    }

    /** xunit {@code Assert.Single(dictionary)} (a single key/value pair). */
    public static <K, V> Map.Entry<K, V> single(Map<K, V> map) {
        assertEquals(1, map.size(), "Expected exactly one entry");
        return map.entrySet().iterator().next();
    }

    /** xunit {@code Assert.Single(collection, predicate)}: exactly one element matches. */
    public static <T> T single(Collection<T> items, Predicate<T> predicate) {
        var matches = items.stream().filter(predicate).collect(Collectors.toList());
        assertEquals(1, matches.size(), "Expected exactly one matching element");
        return matches.get(0);
    }

    /** C# {@code list[^1]}. */
    public static <T> T last(List<T> list) { return list.get(list.size() - 1); }
}
