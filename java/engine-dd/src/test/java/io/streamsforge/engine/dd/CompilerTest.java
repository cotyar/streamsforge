package io.streamsforge.engine.dd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.TableDelta;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CompilerTest {
    static final Map<String, io.streamsforge.engine.SourceSchema> STREAMS = Diff.ALL;

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT sym, STDDEV_SAMP(px) AS sd FROM trades GROUP BY sym",
        "SELECT sym, MEDIAN(px) AS m FROM trades GROUP BY sym",
        "SELECT sym, qty FROM trades WHERE px > (SELECT AVG(px) FROM trades)",
        "SELECT sym, px - (SELECT AVG(px) FROM trades) AS rel FROM trades",
    })
    void unsupportedConstructsAreRefusedWithAReason(String sql) {
        var o = DataflowTableCompiler.compile(sql, STREAMS, Map.of());
        assertEquals(DataflowTableCompiler.Status.UNSUPPORTED, o.status(), o.message());
        assertNull(o.plan());
        assertTrue(o.message().startsWith("unsupported by the dataflow compiler:"), o.message());
    }

    @Test
    void latestByIsRefused() {
        var schemas = Map.of("order_events", Diff.schema("order_events", "order_id", FieldKind.STRING, "stage", FieldKind.STRING));
        var o = DataflowTableCompiler.compile("SELECT order_id, stage FROM order_events LATEST BY (order_id)", schemas, Map.of());
        assertEquals(DataflowTableCompiler.Status.UNSUPPORTED, o.status());
        assertTrue(o.message().contains("LATEST BY"), o.message());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELEC sym FROM trades",
        "SELECT nope FROM trades",
        "SELECT sym FROM missing",
        "WITH RECURSIVE r AS (SELECT sym FROM trades) SELECT sym FROM r",
    })
    void queriesTheEngineRejectsAreInvalidNotUnsupported(String sql) {
        var o = DataflowTableCompiler.compile(sql, STREAMS, Map.of());
        assertEquals(DataflowTableCompiler.Status.INVALID, o.status());
        assertFalse(o.diagnostics().isEmpty());
    }

    /** One of the inputs is a materialized TABLE (its deltas rather than a stream's events): same plans, same answers. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT t.sym AS sym, q.bid AS bid FROM trades t JOIN quotes q ON t.sym = q.sym WHERE q.bid > 0.5",
        "SELECT t.sym AS sym, COUNT(*) AS c, MAX(q.bid) AS mb FROM trades t LEFT JOIN quotes q ON t.sym = q.sym GROUP BY t.sym",
        "SELECT sym, qty FROM trades WHERE sym IN (SELECT sym FROM quotes)",
    })
    void tableInputsBehaveLikeStreamInputs(String sql) {
        var streams = Diff.streams(Diff.TRADES, Diff.REF);
        var tables = Diff.streams(Diff.QUOTES);
        var stats = Diff.run(sql, streams, tables, sql.hashCode(), 200, 8, 6, null);
        System.out.printf("DIFFSTAT table-input                    cases=%d epochs=%d events=%d retractions=%d  %s%n",
            stats.cases(), stats.cases() * 8, stats.events(), stats.retractions(), sql);
    }

    @Test
    void epochsCanBeEmptyAndDeltasArePerEpoch() {
        var q = Diff.plan("SELECT sym, COUNT(*) AS c FROM trades GROUP BY sym", STREAMS, null).newQuery();
        var r1 = EngineIssuesTest.trade(1, "A", 1.0, 1L);
        var r2 = EngineIssuesTest.trade(2, "A", 2.0, 2L);
        q.update("trades", r1, 1);
        assertEquals(0, q.commit());
        assertEquals(1, q.deltasAt(0).size());
        assertEquals(1, q.commit());                 // an empty epoch changes nothing
        assertTrue(q.deltasAt(1).isEmpty());
        q.update("trades", r2, 1);
        q.commit();
        var d = q.deltasAt(2);                       // count 1 -> 2: retract the old group row, assert the new one
        assertEquals(2, d.size());
        assertEquals(List.of(-1L, 1L), d.stream().map(TableDelta::weight).sorted().toList());
        assertEquals(1, q.snapshot().size());
        assertNotNull(q.snapshot().values().iterator().next().row().get("c"));
        assertEquals(2L, q.snapshot().values().iterator().next().row().get("c"));
    }

    @Test
    void unknownSourcesAreIgnoredLikeTheEngineDoes() {
        var q = Diff.plan("SELECT sym FROM trades", STREAMS, null).newQuery();
        q.update("not_an_input", EngineIssuesTest.trade(1, "A", 1.0, 1L), 1);
        q.commit();
        assertTrue(q.snapshot().isEmpty());
    }
}
