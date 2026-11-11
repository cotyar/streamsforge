package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.containsMatch;
import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.isDouble;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.StatAggregateBridge;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * VAR_SAMP / VAR_POP / STDDEV_SAMP / STDDEV_POP / MEDIAN. Every one of them is built to subtract,
 * because the table path maintains a Z-set — a row superseded by LATEST BY comes back with weight −1 —
 * and an aggregate that only adds drifts silently rather than failing.
 */
class StatAggregateTests {
    private static final SourceSchema SAMPLES = schema(
        "samples", "g", FieldKind.STRING, "x", FieldKind.DOUBLE, "n", FieldKind.LONG);

    /** Folds a set of values through a windowed pipeline aggregate and returns the last result. */
    private static Object fold(String expr, double... values) {
        var exec = compileAndCreate(
            "SELECT g, " + expr + " AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS) EMIT CHANGES", SAMPLES);
        Object last = null;
        long ts = 1000;
        for (double v : values) {
            var rows = exec.onEvent("samples", evt(ts++, "samples", "g", "A", "x", v, "n", 1L));
            if (rows.size() > 0) last = rows.get(rows.size() - 1).get("y");
        }
        return last;
    }

    // The textbook sample: mean 5, population variance 4, sample variance 32/7.
    private static final double[] TEXTBOOK = {2, 4, 4, 4, 5, 5, 7, 9};

    @Test
    void variance_and_stddev_match_their_textbook_definitions() {
        assertEquals(4.0, isDouble(fold("VAR_POP(x)", TEXTBOOK)), 1e-10);
        assertEquals(2.0, isDouble(fold("STDDEV_POP(x)", TEXTBOOK)), 1e-10);
        assertEquals(32.0 / 7.0, isDouble(fold("VAR_SAMP(x)", TEXTBOOK)), 1e-10);
        assertEquals(Math.sqrt(32.0 / 7.0), isDouble(fold("STDDEV_SAMP(x)", TEXTBOOK)), 1e-10);
    }

    /** The bare spellings mean the sample forms, which is what SQL means by them. */
    @Test
    void sTDDEV_and_VARIANCE_alias_the_sample_forms() {
        assertEquals(32.0 / 7.0, isDouble(fold("VARIANCE(x)", TEXTBOOK)), 1e-10);
        assertEquals(Math.sqrt(32.0 / 7.0), isDouble(fold("STDDEV(x)", TEXTBOOK)), 1e-10);
    }

    /** A sample variance of one observation is undefined, not zero. */
    @Test
    void sample_variance_of_a_single_observation_is_null_while_the_population_form_is_zero() {
        assertNull(fold("VAR_SAMP(x)", 7.0));
        assertEquals(0.0, isDouble(fold("VAR_POP(x)", 7.0)), 1e-10);
    }

    /** The reason the moments carry an offset. Σx² over values near 1e8 throws away most of a
     *  double's mantissa before the subtraction, so the naive form returns garbage (often a negative)
     *  for a spread this small. */
    @Test
    void variance_survives_large_values_with_a_small_spread() {
        assertEquals(2.0 / 3.0, isDouble(fold("VAR_POP(x)", 1e8, 1e8 + 1, 1e8 + 2)), 1e-9);
        assertEquals(1.0, isDouble(fold("VAR_SAMP(x)", 1e8, 1e8 + 1, 1e8 + 2)), 1e-9);
        // Identical values must give exactly zero, never a rounding-negative that NaNs the square root.
        assertEquals(0.0, isDouble(fold("STDDEV_POP(x)", 1e8, 1e8, 1e8)), 1e-12);
    }

    @Test
    void median_interpolates_between_the_two_middle_observations() {
        assertEquals(2.0, isDouble(fold("MEDIAN(x)", 1, 2, 3)), 1e-10);
        assertEquals(2.5, isDouble(fold("MEDIAN(x)", 1, 2, 3, 4)), 1e-10);
        assertEquals(5.0, isDouble(fold("MEDIAN(x)", 9, 1, 5)), 1e-10); // order of arrival is irrelevant
        assertEquals(4.0, isDouble(fold("MEDIAN(x)", 4, 4, 4)), 1e-10); // a run wider than the rank
    }

    @Test
    void a_statistical_aggregate_is_a_double_even_over_integer_input() {
        var r = compile("SELECT g, STDDEV_SAMP(n) AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", SAMPLES);
        assertTrue(r.ok(), () -> r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining(";")));
        assertEquals(FieldKind.DOUBLE, r.outputSchema().fields().get("y"));
    }

    // ------------------------------------------------------------------
    // PERCENTILE_CONT(p, x) and COUNT(DISTINCT x)
    // ------------------------------------------------------------------

    @Test
    void percentile_agrees_with_median_at_the_middle_and_with_min_max_at_the_ends() {
        double[] values = {1, 2, 3, 4};
        assertEquals(2.5, isDouble(fold("PERCENTILE_CONT(0.5, x)", values)), 1e-10);
        assertEquals(isDouble(fold("MEDIAN(x)", values)), isDouble(fold("PERCENTILE_CONT(0.5, x)", values)), 1e-10);
        assertEquals(1.0, isDouble(fold("PERCENTILE_CONT(0, x)", values)), 1e-10);
        assertEquals(4.0, isDouble(fold("PERCENTILE_CONT(1, x)", values)), 1e-10);
    }

    /** The motivating use: a 5% VaR cut over a hundred simulated P&amp;Ls. Rank 0.05*(100-1) =
     *  4.95 sits between the 5th and 6th observations, so the answer interpolates to 5.95 — the
     *  PERCENTILE_CONT definition, not a nearest-rank pick. */
    @Test
    void percentile_interpolates_at_a_fractional_rank() {
        var values = IntStream.rangeClosed(1, 100).mapToDouble(i -> (double) i).toArray();
        assertEquals(5.95, isDouble(fold("PERCENTILE_CONT(0.05, x)", values)), 1e-9);
    }

    // A per-row probability has no meaning for an aggregate — it would silently take whichever row
    // arrived first — so the parameter must be a literal.
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "PERCENTILE_CONT(x, x)|constant number",
        "PERCENTILE_CONT(1.5, x)|constant number",
        "PERCENTILE_CONT(x)|exactly two arguments",
        "PERCENTILE_CONT(0.5, x, x)|exactly two arguments"})
    void a_bad_percentile_parameter_is_rejected(String expr, String expected) {
        var r = compile("SELECT g, " + expr + " AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", SAMPLES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains(expected));
    }

    /** The parser used to keep only the first argument of an aggregate and drop the rest, so
     *  SUM(a, b) compiled as SUM(a). Harmless until a parameterised aggregate made the count meaningful. */
    @Test
    void an_ordinary_aggregate_still_takes_exactly_one_argument() {
        var r = compile("SELECT g, SUM(x, x) AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", SAMPLES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains("wrong number of arguments"));
    }

    @Test
    void count_distinct_counts_values_not_rows() {
        assertEquals(3L, fold("COUNT(DISTINCT x)", 1, 1, 2, 3, 3, 3));
        assertEquals(1L, fold("COUNT(DISTINCT x)", 7, 7, 7));

        var r = compile("SELECT g, COUNT(DISTINCT x) AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", SAMPLES);
        assertTrue(r.ok(), () -> r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining(";")));
        assertEquals(FieldKind.LONG, r.outputSchema().fields().get("y"));
    }

    /** DISTINCT keys compare the way SQL compares values, so the long 1 and the double 1.0 are one
     *  distinct number. Plain object equality would call them two. */
    @Test
    void distinct_keys_use_sql_value_equality_not_object_equality() {
        var counter = new StatAggregateBridge.DistinctCounter();
        counter.add(1L);
        counter.add(1.0);
        assertEquals(1L, counter.result());

        counter.add(2L);
        assertEquals(2L, counter.result());

        // Retracting one of two identical contributions leaves the value counted; retracting both drops it.
        counter.apply(2L, -1);
        assertEquals(1L, counter.result());
    }

    @Test
    void a_column_named_distinct_still_parses_as_a_column() {
        var schema = schema("t", "distinct", FieldKind.LONG);
        var r = compile("SELECT distinct AS y FROM t", schema);
        assertTrue(r.ok(), () -> r.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining(";")));
    }

    // `COUNT(DISTINCT *)` is caught earlier, by the grammar — '*' is not an expression there.
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "SUM(DISTINCT x)|only supported on COUNT",
        "COUNT(DISTINCT)|needs an expression",
        "COUNT(DISTINCT *)|Expected an expression"})
    void dISTINCT_is_rejected_where_it_has_no_meaning(String expr, String expected) {
        var r = compile("SELECT g, " + expr + " AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", SAMPLES);
        assertFalse(r.ok());
        containsMatch(r.diagnostics(), d -> d.message().contains(expected));
    }

    // ------------------------------------------------------------------
    // The half that actually matters: retraction
    // ------------------------------------------------------------------

    /** Drives values through a LATEST BY table into a rolling aggregate, so a repeated key
     *  retracts its previous value with weight −1 before asserting the new one. */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "VAR_POP(x)",
        "STDDEV_SAMP(x)",
        "MEDIAN(x)",
        "PERCENTILE_CONT(0.25, x)"})
    void a_superseded_row_is_subtracted_back_out(String expr) {
        var latest = compileTable("SELECT g, x FROM samples LATEST BY (g)", SAMPLES);
        assertTrue(latest.ok(), () -> diag(latest.diagnostics()));
        var latestExec = latest.plan().createExecutor();

        var rolled = compileTable("SELECT " + expr + " AS y FROM latest_x", new SourceSchema[0],
            new SourceSchema[] {new SourceSchema("latest_x", latest.outputSchema().fields())});
        assertTrue(rolled.ok(), () -> diag(rolled.diagnostics()));
        var rolledExec = rolled.plan().createExecutor();

        long[] ts = {1000};
        java.util.function.BiConsumer<String, Double> push = (key, value) -> {
            for (var delta : latestExec.onStreamEvent("samples", evt(ts[0]++, "samples", "g", key, "x", value, "n", 1L))) {
                rolledExec.onTableDelta("latest_x", delta);
            }
        };

        // Three keys carrying a wrong value first, then corrected to 1/2/3 one at a time.
        push.accept("a", 100.0); push.accept("b", 200.0); push.accept("c", 300.0);
        push.accept("a", 1.0); push.accept("b", 2.0); push.accept("c", 3.0);

        var actual = single(rolledExec.snapshot().entrySet()).getValue().row().get("y");

        // What a from-scratch aggregate over exactly {1,2,3} produces — if any retraction leaked, the
        // 100/200/300 contributions would still be in here and this would be wildly off.
        var expected = fold(expr, 1, 2, 3);
        assertEquals(isDouble(expected), isDouble(actual), 1e-9);
    }

    /** Retracting everything must leave the aggregate empty rather than at a stale last value —
     *  the multiset has to prune entries at zero, not merely stop counting them. */
    @Test
    void retracting_every_contribution_empties_the_aggregate() {
        var median = new StatAggregateBridge.MedianZ();
        median.apply(1.0, 1);
        median.apply(3.0, 1);
        assertEquals(2.0, isDouble(median.result()), 1e-10);
        median.apply(1.0, -1);
        assertEquals(3.0, isDouble(median.result()), 1e-10);
        median.apply(3.0, -1);
        assertNull(median.result());
    }
}
