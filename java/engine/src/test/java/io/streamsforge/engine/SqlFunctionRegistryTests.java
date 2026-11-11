package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.Aggregator;
import io.streamsforge.engine.runtime.IZAggregator;
import io.streamsforge.engine.sql.IAggregateFunction;
import io.streamsforge.engine.sql.IScalarFunction;
import io.streamsforge.engine.sql.SqlFunctions;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The extension seam: an assembly outside the Engine adds a scalar function or an aggregate without editing it.
 * These tests ARE the seam's consumer proof — they register through the same public API and then drive the
 * result through real SQL, both ways the dialect runs it (a pipeline folding forward over a stream, and a table
 * maintaining a Z-set where a superseded row comes back with weight -1).
 */
class SqlFunctionRegistryTests {
    /** Deliberately not a plausible future built-in — the registry is process-global. */
    private static final String SCALAR_NAME = "TEST_HYPOT";
    private static final String AGGREGATE_NAME = "TEST_RANGE";

    @BeforeEach
    void setUp() {
        SqlFunctions.register(new HypotFunction(null));
        SqlFunctions.register(new RangeAggregate());
    }

    @AfterEach
    void dispose() {
        SqlFunctions.clear();
    }

    private static final SourceSchema MIXED = schema(
        "mixed", "s", FieldKind.STRING, "l", FieldKind.LONG, "d", FieldKind.DOUBLE, "e", FieldKind.DOUBLE);

    // ------------------------------------------------------------------
    // Scalars
    // ------------------------------------------------------------------

    @Test
    void a_registered_scalar_compiles_types_and_evaluates() {
        var r = compile("SELECT " + SCALAR_NAME + "(d, e) AS x FROM mixed", MIXED);
        assertTrue(r.ok(), r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining(";")));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("x"));

        var exec = compileAndCreate("SELECT " + SCALAR_NAME + "(d, e) AS x FROM mixed", MIXED);
        var results = exec.onEvent("mixed", evt(1000, "mixed", "s", "a", "l", 1L, "d", 3.0, "e", 4.0));
        assertEquals(1, results.size());
        assertEquals(5.0, results.get(0).get("x"));
    }

    @Test
    void a_registered_scalar_is_arity_checked_like_a_built_in() {
        var r = compile("SELECT " + SCALAR_NAME + "(d) AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("wrong number of arguments")));
    }

    @Test
    void an_unregistered_name_is_still_an_unknown_function() {
        var r = compile("SELECT NO_SUCH_FN(d) AS x FROM mixed", MIXED);
        assertFalse(r.ok());
        assertTrue(r.diagnostics().stream().anyMatch(d -> d.message().contains("Unknown function")));
    }

    /** The registry is consulted only after the built-in switches, and registration refuses a built-in's name
     *  outright — so a third party cannot change what SUM or ABS means in SQL that is already deployed. */
    @ParameterizedTest
    @ValueSource(strings = {"SUM", "ABS", "if"})
    void a_registration_may_not_shadow_a_built_in(String name) {
        assertThrows(IllegalArgumentException.class, () -> SqlFunctions.register(new HypotFunction(name)));
    }

    @Test
    void registering_the_same_name_twice_replaces_rather_than_duplicates() {
        SqlFunctions.register(new HypotFunction(null));
        assertEquals(1, SqlFunctions.registeredScalarNames().stream().filter(n -> n.equals(SCALAR_NAME)).count());
    }

    // ------------------------------------------------------------------
    // Aggregates — both accumulators, because the dialect runs SQL both ways
    // ------------------------------------------------------------------

    @Test
    void a_registered_aggregate_folds_forward_in_a_pipeline() {
        // A pipeline aggregate needs a WINDOW in this dialect; all three events fall in the same one.
        var exec = compileAndCreate(
            "SELECT s, " + AGGREGATE_NAME + "(d) AS spread FROM mixed GROUP BY s WINDOW TUMBLING(SIZE 1 SECONDS) EMIT CHANGES", MIXED);
        exec.onEvent("mixed", evt(1000, "mixed", "s", "A", "l", 1L, "d", 2.0, "e", 0.0));
        exec.onEvent("mixed", evt(1001, "mixed", "s", "A", "l", 1L, "d", 9.0, "e", 0.0));
        var last = exec.onEvent("mixed", evt(1002, "mixed", "s", "A", "l", 1L, "d", 4.0, "e", 0.0));
        assertEquals(1, last.size());
        assertEquals(7.0, last.get(0).get("spread")); // 9 - 2
    }

    /** The half that actually matters: in a table the aggregate must survive retraction. The LATEST BY below
     *  supersedes a key, which reaches the aggregate as weight -1. */
    @Test
    void a_registered_aggregate_survives_retraction_in_a_table() {
        var latest = compileTable("SELECT s, d FROM mixed LATEST BY (s)", MIXED);
        assertTrue(latest.ok(), latest.diagnostics().stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";")));
        var latestExec = latest.plan().createExecutor();

        var rolled = compileTable(
            "SELECT " + AGGREGATE_NAME + "(d) AS spread FROM latest_d",
            new SourceSchema[0],
            new SourceSchema[] {new SourceSchema("latest_d", latest.outputSchema().fields())});
        assertTrue(rolled.ok(), rolled.diagnostics().stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";")));
        var rolledExec = rolled.plan().createExecutor();

        push(latestExec, rolledExec, 1000, "A", 2.0);
        push(latestExec, rolledExec, 1001, "B", 9.0);
        var snap1 = rolledExec.snapshot();
        assertEquals(1, snap1.size());
        assertEquals(7.0, snap1.values().iterator().next().row().get("spread"));

        // B is superseded: 9.0 is retracted and 3.0 asserted, so the range must shrink to 3 - 2.
        push(latestExec, rolledExec, 1002, "B", 3.0);
        var snap2 = rolledExec.snapshot();
        assertEquals(1, snap2.size());
        assertEquals(1.0, snap2.values().iterator().next().row().get("spread"));
    }

    private static void push(TableExecutor latestExec, TableExecutor rolledExec, long ts, String key, double value) {
        for (var delta : latestExec.onStreamEvent("mixed", evt(ts, "mixed", "s", key, "l", 1L, "d", value, "e", 0.0))) {
            rolledExec.onTableDelta("latest_d", delta);
        }
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static final class HypotFunction implements IScalarFunction {
        private final String name;

        HypotFunction(String name) { this.name = name != null ? name : SCALAR_NAME; }

        @Override public String name() { return name; }
        @Override public boolean isValidArity(int argCount) { return argCount == 2; }
        @Override public FieldKind resultKind(List<FieldKind> argKinds) { return FieldKind.DOUBLE; }

        @Override public Object eval(List<Object> args) {
            if (args.size() != 2 || !(args.get(0) instanceof Double a) || !(args.get(1) instanceof Double b)) return null;
            return Math.sqrt(a * a + b * b);
        }
    }

    /** max - min. Chosen because a naive implementation cannot subtract: it forces the Z half to keep a real
     *  multiset, which is exactly what the retraction test above exercises. */
    private static final class RangeAggregate implements IAggregateFunction {
        @Override public String name() { return AGGREGATE_NAME; }
        @Override public FieldKind resultKind(FieldKind argKind) { return FieldKind.DOUBLE; }
        @Override public Aggregator createStream() { return new RangeStream(); }
        @Override public IZAggregator createZ() { return new RangeZ(); }
    }

    private static final class RangeStream extends Aggregator {
        private double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        private boolean any;

        @Override public void add(Object value) {
            if (!(value instanceof Double d)) return;
            any = true;
            min = Math.min(min, d);
            max = Math.max(max, d);
        }

        @Override public Object result() { return any ? (Object) (max - min) : null; }
    }

    private static final class RangeZ implements IZAggregator {
        private final TreeMap<Double, Long> counts = new TreeMap<>();

        @Override public void apply(Object value, long weight) {
            if (!(value instanceof Double d)) return;
            Long existing = counts.get(d);
            long next = existing != null ? existing + weight : weight;
            if (next <= 0) counts.remove(d);
            else counts.put(d, next);
        }

        @Override public Object result() { return counts.isEmpty() ? null : (Object) (counts.lastKey() - counts.firstKey()); }
    }
}
