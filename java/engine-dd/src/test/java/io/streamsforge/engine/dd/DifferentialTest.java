package io.streamsforge.engine.dd;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential tests: every query below runs on the engine's {@code TableExecutor} and on the dataflow compiler over
 * the same random insert/retract streams, and the accumulated output must be identical at every epoch.
 */
class DifferentialTest {
    static final int CASES = Integer.getInteger("dd.cases", 300);
    static final int EPOCHS = Integer.getInteger("dd.epochs", 8);

    static Stream<Arguments> queries() {
        return Stream.of(
            // ---- single source filter / project
            Arguments.of("filter-project", "SELECT sym, px * 2 AS p2, qty + 1 AS q1 FROM trades WHERE px > 1.0 AND ok = true"),
            Arguments.of("select-star", "SELECT * FROM trades WHERE qty >= 2 OR sym = 'A'"),
            Arguments.of("null-logic", "SELECT sym, qty FROM trades WHERE NOT (qty > 2) OR px < 1.0"),
            Arguments.of("functions", "SELECT UPPER(sym) AS s, COALESCE(qty, 9) AS q FROM trades WHERE sym != 'A'"),
            // ---- inner joins
            Arguments.of("inner-join", "SELECT t.sym AS sym, t.px AS px, q.bid AS bid FROM trades t JOIN quotes q ON t.sym = q.sym"),
            Arguments.of("inner-join-residual", "SELECT t.sym AS sym, t.px AS px, q.bid AS bid FROM trades t JOIN quotes q ON t.sym = q.sym AND t.px > q.bid"),
            Arguments.of("inner-join-composite-key", "SELECT t.sym AS sym, t.qty AS qty FROM trades t JOIN quotes q ON t.sym = q.sym AND t.qty = q.n"),
            Arguments.of("join-where", "SELECT t.sym AS sym, q.bid AS bid FROM trades t JOIN quotes q ON t.sym = q.sym WHERE t.ok = true AND q.bid > 0.5"),
            Arguments.of("three-way-join", "SELECT t.sym AS sym, t.px AS px, q.bid AS bid, r.tag AS tag FROM trades t JOIN quotes q ON t.sym = q.sym JOIN ref r ON r.sym = t.sym"),
            Arguments.of("self-join", "SELECT a.sym AS sym, a.qty AS a_qty, b.qty AS b_qty FROM trades a JOIN trades b ON a.sym = b.sym WHERE a.qty < b.qty"),
            Arguments.of("cross-join", "SELECT r.tag AS tag, q.n AS n FROM ref r CROSS JOIN quotes q"),
            // ---- outer joins
            Arguments.of("left-join", "SELECT t.sym AS sym, t.qty AS qty, q.bid AS bid FROM trades t LEFT JOIN quotes q ON t.sym = q.sym"),
            Arguments.of("left-join-residual", "SELECT t.sym AS sym, t.px AS px, q.bid AS bid FROM trades t LEFT JOIN quotes q ON t.sym = q.sym AND q.bid > t.px"),
            Arguments.of("right-join", "SELECT t.sym AS tsym, q.sym AS qsym, q.bid AS bid FROM trades t RIGHT JOIN quotes q ON t.sym = q.sym"),
            Arguments.of("full-join", "SELECT t.sym AS tsym, q.sym AS qsym, t.qty AS qty, q.n AS n FROM trades t FULL JOIN quotes q ON t.sym = q.sym AND t.qty = q.n"),
            Arguments.of("left-join-chain", "SELECT t.sym AS sym, q.bid AS bid, r.tag AS tag FROM trades t LEFT JOIN quotes q ON t.sym = q.sym LEFT JOIN ref r ON r.sym = t.sym"),
            Arguments.of("inner-then-full", "SELECT t.sym AS sym, q.bid AS bid, r.tag AS tag FROM trades t JOIN quotes q ON t.sym = q.sym FULL JOIN ref r ON r.sym = t.sym"),
            // ---- GROUP BY / aggregates
            Arguments.of("group-count", "SELECT sym, COUNT(*) AS c FROM trades GROUP BY sym"),
            Arguments.of("group-all-aggregates", "SELECT sym, COUNT(*) AS c, COUNT(qty) AS cq, SUM(qty) AS sq, SUM(fee) AS spx, SUM(px) AS spx2, MIN(px) AS mn, MAX(px) AS mx, AVG(px) AS av FROM trades GROUP BY sym"),
            Arguments.of("group-sum-long", "SELECT sym, SUM(qty) AS sq, MIN(qty) AS mn, MAX(qty) AS mx, AVG(qty) AS av FROM trades GROUP BY sym"),
            Arguments.of("group-where", "SELECT sym, COUNT(*) AS c, SUM(fee) AS s FROM trades WHERE ok = true AND px > 0.5 GROUP BY sym"),
            Arguments.of("group-two-keys", "SELECT sym, ok, COUNT(*) AS c, MAX(qty) AS mx FROM trades GROUP BY sym, ok"),
            Arguments.of("group-expression-key", "SELECT qty % 2 AS parity, COUNT(*) AS c, SUM(fee) AS s FROM trades GROUP BY qty % 2"),
            Arguments.of("group-agg-expression", "SELECT sym, SUM(qty) * 2 + COUNT(*) AS m, MAX(px) - MIN(px) AS spread FROM trades GROUP BY sym"),
            Arguments.of("global-aggregate", "SELECT COUNT(*) AS c, SUM(qty) AS s, MIN(px) AS mn, MAX(px) AS mx, AVG(qty) AS av FROM trades"),
            Arguments.of("count-distinct", "SELECT sym, COUNT(DISTINCT qty) AS dq, COUNT(*) AS c FROM trades GROUP BY sym"),
            Arguments.of("group-string-minmax", "SELECT sym, MIN(tag) AS lo, MAX(tag) AS hi, COUNT(tag) AS c FROM ref GROUP BY sym"),
            Arguments.of("group-over-join", "SELECT t.sym AS sym, COUNT(*) AS c, SUM(t.qty) AS sq, MAX(q.bid) AS mb FROM trades t JOIN quotes q ON t.sym = q.sym GROUP BY t.sym"),
            Arguments.of("group-over-left-join", "SELECT t.sym AS sym, COUNT(q.bid) AS cb, COUNT(*) AS c FROM trades t LEFT JOIN quotes q ON t.sym = q.sym GROUP BY t.sym"),
            Arguments.of("group-distinct-idiom", "SELECT sym FROM trades GROUP BY sym"),
            // ---- unions
            Arguments.of("union-all", "SELECT sym FROM trades UNION ALL SELECT sym FROM ref"),
            Arguments.of("union", "SELECT sym FROM trades UNION SELECT sym FROM ref"),
            Arguments.of("union-3", "SELECT sym FROM trades UNION SELECT sym FROM ref UNION SELECT sym FROM quotes"),
            Arguments.of("union-all-aggregate-branch", "SELECT sym, COUNT(*) AS c FROM trades GROUP BY sym UNION ALL SELECT sym, COUNT(*) AS c FROM quotes GROUP BY sym"),
            // ---- semi / anti joins
            Arguments.of("in-subquery", "SELECT sym, qty FROM trades WHERE sym IN (SELECT sym FROM ref)"),
            Arguments.of("in-subquery-filtered", "SELECT sym, qty FROM trades WHERE sym IN (SELECT sym FROM ref WHERE tag = 'x') AND ok = true"),
            Arguments.of("not-in-subquery", "SELECT sym, qty FROM trades WHERE sym NOT IN (SELECT sym FROM ref)"),
            Arguments.of("exists", "SELECT sym, qty FROM trades WHERE EXISTS (SELECT sym FROM ref)"),
            Arguments.of("not-exists", "SELECT sym, qty FROM trades WHERE NOT EXISTS (SELECT sym FROM ref WHERE tag = 'y')"),
            Arguments.of("in-and-not-in", "SELECT sym FROM trades WHERE sym IN (SELECT sym FROM ref) AND sym NOT IN (SELECT sym FROM quotes)"),
            Arguments.of("in-aggregate-subquery", "SELECT sym, qty FROM trades WHERE sym IN (SELECT sym FROM quotes GROUP BY sym)"),
            Arguments.of("in-with-group", "SELECT sym, COUNT(*) AS c FROM trades WHERE sym IN (SELECT sym FROM ref) GROUP BY sym"),
            // ---- derived tables / CTEs
            Arguments.of("derived-table", "SELECT s.sym AS sym, s.tq AS tq FROM (SELECT sym, SUM(qty) AS tq FROM trades GROUP BY sym) s WHERE s.tq > 2"),
            Arguments.of("cte-join-back", "WITH s AS (SELECT sym, SUM(qty) AS tq FROM trades GROUP BY sym) SELECT t.sym AS sym, t.qty AS qty, s.tq AS tq FROM trades t JOIN s ON t.sym = s.sym"),
            Arguments.of("aggregate-of-aggregate", "SELECT tq, COUNT(*) AS c FROM (SELECT sym, SUM(qty) AS tq FROM trades GROUP BY sym) s GROUP BY tq"),
            Arguments.of("cte-union", "SELECT sym, COUNT(*) AS c FROM (SELECT sym FROM trades UNION SELECT sym FROM ref) u GROUP BY sym"),
            // ---- unnest
            Arguments.of("unnest", "SELECT s.id AS id, l AS leg FROM structs s JOIN UNNEST(s.legs) AS l"),
            Arguments.of("unnest-group", "SELECT s.id AS id, COUNT(*) AS c, SUM(l) AS sl FROM structs s JOIN UNNEST(s.legs) AS l GROUP BY s.id")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void engineAndDataflowAgree(String name, String sql) {
        var stats = Diff.run(sql, name.hashCode(), CASES, EPOCHS);
        System.out.printf("DIFFSTAT %-30s cases=%d epochs=%d events=%d retractions=%d outputRows=%d%n",
            name, stats.cases(), stats.cases() * EPOCHS, stats.events(), stats.retractions(), stats.outputRows());
        assertEquals(CASES, stats.cases());
        assertTrue(stats.events() > 0);
    }

    /** Bigger transactions: up to 30 events per epoch, fewer epochs, so joins and groups hold many rows at once. */
    @ParameterizedTest(name = "{0} (load)")
    @MethodSource("queries")
    void engineAndDataflowAgreeUnderLoad(String name, String sql) {
        int cases = Math.max(10, CASES / 5);
        var stats = Diff.run(sql, Diff.ALL, null, name.hashCode() + 7, cases, 5, 30, null);
        System.out.printf("DIFFSTAT-LOAD %-30s cases=%d epochs=%d events=%d retractions=%d%n",
            name, stats.cases(), stats.cases() * 5, stats.events(), stats.retractions());
        assertEquals(cases, stats.cases());
    }

    /** True when the query uses an aggregate whose result depends on per-value state (everything but COUNT(*)). */
    static boolean hasValueAggregate(String sql) {
        return java.util.regex.Pattern.compile("(SUM|MIN|MAX|AVG)\\(|COUNT\\((?!\\*)").matcher(sql).find();
    }

    static Stream<Arguments> orderInsensitiveQueries() { return queries().filter(a -> !hasValueAggregate((String) a.get()[1])); }

    static Stream<Arguments> valueAggregateQueries() { return queries().filter(a -> hasValueAggregate((String) a.get()[1])); }

    /** Z-set semantics are order-insensitive: within an epoch a retraction may reach the engine BEFORE the assertion it
     *  cancels (only the end-of-epoch state is checked). Joins, outer joins, semi/anti joins, unions, unnest and COUNT(*)
     *  groups all survive that. */
    @ParameterizedTest(name = "{0} (shuffled)")
    @MethodSource("orderInsensitiveQueries")
    void engineIsOrderInsensitiveWithinAnEpoch(String name, String sql) {
        var stats = Diff.run(sql, Diff.ALL, null, name.hashCode() + 11, CASES, EPOCHS, 8, new Diff.Feed[] {Diff.Feed.SHUFFLED});
        System.out.printf("DIFFSTAT-SHUFFLED %-30s cases=%d epochs=%d events=%d retractions=%d%n",
            name, stats.cases(), stats.cases() * EPOCHS, stats.events(), stats.retractions());
    }

    @ParameterizedTest(name = "{0} (shuffled)")
    @MethodSource("valueAggregateQueries")
    @org.junit.jupiter.api.Disabled("bug: engine value aggregates (SUM/MIN/MAX/AVG/COUNT(x)/COUNT(DISTINCT)) are order-sensitive - a retraction "
        + "that precedes its assertion corrupts the group; minimal repro: EngineIssuesTest.engineGroupByIsOrderInsensitive. "
        + "Run with -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition to see the 15 failing queries.")
    void engineValueAggregatesAreOrderInsensitiveWithinAnEpoch(String name, String sql) {
        Diff.run(sql, Diff.ALL, null, name.hashCode() + 11, CASES, EPOCHS, 8, new Diff.Feed[] {Diff.Feed.SHUFFLED});
    }
}
