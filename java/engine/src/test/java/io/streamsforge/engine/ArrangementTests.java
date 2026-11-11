package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.EVENTS;
import static io.streamsforge.engine.TestHelpers.QUOTES;
import static io.streamsforge.engine.TestHelpers.REF;
import static io.streamsforge.engine.TestHelpers.TRADES;
import static io.streamsforge.engine.TestHelpers.compileTable;
import static io.streamsforge.engine.TestHelpers.evt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.ArrangementKeySpec;
import io.streamsforge.engine.dataflow.TableDataflowPlan;
import io.streamsforge.engine.dataflow.TableEdgeDescriptor;
import io.streamsforge.engine.dataflow.TableStageDescriptor;
import io.streamsforge.engine.dataflow.TableStageKind;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 003 M3 acceptance: keySpec canonicalization (same spec for semantically-equal keys, different for different
 * keys/partition counts), TableDataflowBuilder's arrangeability rule (marks a plain equi-join's bare-column key
 * arrangeable; does NOT mark a transformed key or a chained (i&gt;0) join's Left side arrangeable), and a
 * harness-level proof that two DIFFERENT tables sharing the same raw input each converge to their own classic-executor
 * baseline when fed that input's event stream.
 */
class ArrangementTests {
    private record Ev(String origin, EventRecord row, long weight, boolean isTable) {}

    private static Ev s(String source, EventRecord row) { return new Ev(source, row, 1, false); }

    private static Ev t(String table, EventRecord row, long weight) { return new Ev(table, row, weight, true); }

    private static String canon(TableExecutor exec) {
        return exec.snapshot().entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(kv -> kv.getKey() + "=" + kv.getValue().weight()).collect(Collectors.joining("\n"));
    }

    private static String canon(PartitionedTableHarness harness) {
        return harness.snapshot().entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(kv -> kv.getKey() + "=" + kv.getValue().weight()).collect(Collectors.joining("\n"));
    }

    /** {@code dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == name)}. */
    private static TableEdgeDescriptor arrangeableEdgeFor(TableDataflowPlan dataflow, String name) {
        var matches = dataflow.arrangeableExternalEdges().stream()
            .filter(e -> dataflow.externalInputNameOf(e).equals(name)).collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0);
    }

    // ------------------------------------------------------------------------------------------------
    // ArrangementKeySpec: canonicalization + hashing, tested directly (no SQL involved).
    // ------------------------------------------------------------------------------------------------

    @Test
    void keySpec_SameFieldsAndPartitionCount_SameCanonicalFormAndHash() {
        var a = ArrangementKeySpec.canonicalize(List.of("symbol"), 4);
        var b = ArrangementKeySpec.canonicalize(List.of("symbol"), 4);
        assertEquals(a, b);
        assertEquals(ArrangementKeySpec.hashOf(a), ArrangementKeySpec.hashOf(b));
    }

    @Test
    void keySpec_DifferentField_DifferentCanonicalFormAndHash() {
        var a = ArrangementKeySpec.canonicalize(List.of("symbol"), 4);
        var b = ArrangementKeySpec.canonicalize(List.of("qty"), 4);
        assertNotEquals(a, b);
        assertNotEquals(ArrangementKeySpec.hashOf(a), ArrangementKeySpec.hashOf(b));
    }

    @Test
    void keySpec_DifferentPartitionCount_DifferentCanonicalFormAndHash() {
        // Different partition count is a genuinely different physical index — must NOT collide even though the key
        // fields are identical.
        var a = ArrangementKeySpec.canonicalize(List.of("symbol"), 2);
        var b = ArrangementKeySpec.canonicalize(List.of("symbol"), 4);
        assertNotEquals(a, b);
        assertNotEquals(ArrangementKeySpec.hashOf(a), ArrangementKeySpec.hashOf(b));
    }

    @Test
    void keySpec_FieldNameLengthPrefixed_NoDelimiterCollision() {
        // Two different (field-list) shapes that would collide under a naive "join with ';'" scheme don't collide
        // here because each field is length-prefixed.
        var a = ArrangementKeySpec.canonicalize(List.of("ab", "c"), 4);
        var b = ArrangementKeySpec.canonicalize(List.of("a", "bc"), 4);
        assertNotEquals(a, b);
    }

    // ------------------------------------------------------------------------------------------------
    // KeySpec canonicalization is ALIAS-independent.
    // ------------------------------------------------------------------------------------------------

    @Test
    void keySpec_SameForSemanticallyEqualJoin_DifferentAliasesAndSql() {
        var planA = compileTable(
            "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol",
            new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var planB = compileTable(
            "SELECT x.symbol, COUNT(*) AS cnt FROM trades x JOIN ref y ON x.symbol = y.symbol GROUP BY x.symbol",
            new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();

        var dfA = planA.createDataflow(4);
        var dfB = planB.createDataflow(4);

        var tradesEdgeA = arrangeableEdgeFor(dfA, "trades");
        var tradesEdgeB = arrangeableEdgeFor(dfB, "trades");

        assertEquals(dfA.keySpecOf(tradesEdgeA), dfB.keySpecOf(tradesEdgeB));
        assertEquals(
            ArrangementKeySpec.hashOf(dfA.keySpecOf(tradesEdgeA)),
            ArrangementKeySpec.hashOf(dfB.keySpecOf(tradesEdgeB)));

        // "ref" is arrangeable too (the join's own dedicated Right ingest, bare-field-keyed) — but on a DIFFERENT
        // field-spec identity than "trades" (different input name), obviously not colliding.
        var refEdgeA = arrangeableEdgeFor(dfA, "ref");
        assertEquals(List.of("symbol"), refEdgeA.arrangeKeyFields());
    }

    // ------------------------------------------------------------------------------------------------
    // Builder arrangeability rule.
    // ------------------------------------------------------------------------------------------------

    @Test
    void builder_PlainEquiJoin_BothSidesArrangeable_BareColumnKeys() {
        var plan = compileTable(
            "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol",
            new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertEquals(2, dataflow.arrangeableExternalEdges().size());

        var tradesEdge = arrangeableEdgeFor(dataflow, "trades");
        assertEquals(List.of("symbol"), tradesEdge.arrangeKeyFields());
        assertEquals("Left", tradesEdge.role());

        var refEdge = arrangeableEdgeFor(dataflow, "ref");
        assertEquals(List.of("symbol"), refEdge.arrangeKeyFields());
        assertEquals("Right", refEdge.role());
    }

    @Test
    void builder_NoJoin_NothingArrangeable() {
        var plan = compileTable("SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol", TRADES).plan();
        assertTrue(plan.createDataflow(4).arrangeableExternalEdges().isEmpty());
    }

    @Test
    void builder_TransformedJoinKey_ThatSideNotArrangeable_OtherSideStillIs() {
        // Left key is a JSON-path extraction (a transform) -> NOT a bare own-field reference -> not arrangeable. Right
        // key (r.symbol) is still a bare reference -> still arrangeable. Proves the check is per-side, not per-edge-pair.
        var plan = compileTable(
            "SELECT e.eventType, r.tag FROM events e JOIN ref r ON e.payload ->> 'symbol' = r.symbol",
            new SourceSchema[] {EVENTS}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        assertTrue(dataflow.arrangeableExternalEdges().stream().noneMatch(e -> dataflow.externalInputNameOf(e).equals("events")));
        assertEquals(1, dataflow.arrangeableExternalEdges().size());
        var refEdge = dataflow.arrangeableExternalEdges().get(0);
        assertEquals("ref", dataflow.externalInputNameOf(refEdge));
        assertEquals(List.of("symbol"), refEdge.arrangeKeyFields());
    }

    @Test
    void builder_ChainedSecondJoin_LeftSideNotArrangeable_RightSideStillIs() {
        // Two joins: trades JOIN ref (hop 0) JOIN quotes (hop 1), both on symbol. The SECOND join's Left side is fed by
        // the first join's already-computed multi-alias WorkingRow — not the raw FROM ingest — so it must NOT be
        // arrangeable, even though its own RightKey (quotes' dedicated ingest, bare column) still is.
        var plan = compileTable(
            "SELECT t.symbol, r.tag, q.bid FROM trades t JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON t.symbol = q.symbol",
            new SourceSchema[] {TRADES, QUOTES}, new SourceSchema[] {REF}).plan();
        var dataflow = plan.createDataflow(4);

        var joinStages = new ArrayList<TableStageDescriptor>(dataflow.stages().stream().filter(s -> s.kind() == TableStageKind.JOIN).collect(Collectors.toList()));
        joinStages.sort(Comparator.comparingInt(TableStageDescriptor::stageId));
        assertEquals(2, joinStages.size());
        var secondJoin = joinStages.get(1);
        var secondLeftEdges = secondJoin.inEdges().stream().filter(e -> e.role().equals("Left")).collect(Collectors.toList());
        assertEquals(1, secondLeftEdges.size());
        assertNull(secondLeftEdges.get(0).arrangeKeyFields()); // chained hop -> not arrangeable

        // The first join's Left (trades, hop 0) and Right (ref) ARE arrangeable, and quotes (second join's Right, its
        // own dedicated ingest regardless of chain position) is too.
        assertEquals(3, dataflow.arrangeableExternalEdges().size());
        Predicate<String> has = name -> dataflow.arrangeableExternalEdges().stream().anyMatch(e -> dataflow.externalInputNameOf(e).equals(name));
        assertTrue(has.test("trades"));
        assertTrue(has.test("ref"));
        assertTrue(has.test("quotes"));
    }

    @Test
    void builder_ScalarBroadcastEdge_NeverArrangeable() {
        // Broadcast (scalar-subquery residual) edges are never real hash-partitioned join-input indexes — so they must
        // never appear in ArrangeableExternalEdges regardless of key shape.
        var plan = compileTable("SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades", TRADES).plan();
        var dataflow = plan.createDataflow(4);
        assertTrue(dataflow.arrangeableExternalEdges().isEmpty());
    }

    // ------------------------------------------------------------------------------------------------
    // Harness-level acceptance: two DIFFERENT tables sharing the same raw input each converge to their OWN classic
    // single-partition baseline when independently fed that input's event stream.
    // ------------------------------------------------------------------------------------------------

    @Test
    void sharedArrangement_TwoDifferentTablesOverSameRawInput_BothConvergeToOwnClassicBaseline() {
        var sqlA = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
        var sqlB = "SELECT t.symbol, COUNT(*) AS legs, MAX(t.price) AS hi FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";

        var compileA = compileTable(sqlA, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        var compileB = compileTable(sqlB, new SourceSchema[] {TRADES}, new SourceSchema[] {REF});
        assertTrue(compileA.ok());
        assertTrue(compileB.ok());

        var dataflowA = compileA.plan().createDataflow(4);
        var dataflowB = compileB.plan().createDataflow(4);

        // Both tables' "trades" edge resolves to the identical keySpec (same arrangement identity) — the precondition
        // for TableGrain's coordinator to attach BOTH to the SAME ArrangementGrain set.
        var tradesEdgeA = arrangeableEdgeFor(dataflowA, "trades");
        var tradesEdgeB = arrangeableEdgeFor(dataflowB, "trades");
        assertEquals(dataflowA.keySpecOf(tradesEdgeA), dataflowB.keySpecOf(tradesEdgeB));

        // The SAME raw event stream (as either a private ingest or a shared arrangement would deliver it) feeds BOTH
        // tables' harnesses.
        var events = List.of(
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), 1),
            t("ref", evt(0, "ref", "symbol", "MSFT", "tag", "watch"), 1),
            s("trades", evt(1, "trades", "symbol", "AAPL", "price", 10.0, "qty", 5L, "active", true)),
            s("trades", evt(2, "trades", "symbol", "AAPL", "price", 12.0, "qty", 3L, "active", true)),
            s("trades", evt(3, "trades", "symbol", "MSFT", "price", 20.0, "qty", 7L, "active", true)),
            s("trades", evt(4, "trades", "symbol", "GOOG", "price", 30.0, "qty", 2L, "active", true)), // no matching ref row
            t("ref", evt(0, "ref", "symbol", "AAPL", "tag", "core"), -1)); // retraction cascades through both

        var classicA = compileA.plan().createExecutor();
        var classicB = compileB.plan().createExecutor();
        var harnessA = new PartitionedTableHarness(dataflowA);
        var harnessB = new PartitionedTableHarness(dataflowB);

        for (var e : events) {
            if (e.isTable()) {
                classicA.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
                classicB.onTableDelta(e.origin(), new TableDelta(e.row(), e.weight()));
            } else {
                classicA.onStreamEvent(e.origin(), e.row());
                classicB.onStreamEvent(e.origin(), e.row());
            }
            harnessA.admit(e.origin(), e.row(), e.weight());
            harnessB.admit(e.origin(), e.row(), e.weight());
        }

        assertEquals(canon(classicA), canon(harnessA));
        assertEquals(canon(classicB), canon(harnessB));
        // And the two tables' own outputs genuinely differ (different SQL) — sanity check that this isn't vacuously true
        // because both happened to compute the same thing.
        assertNotEquals(canon(harnessA), canon(harnessB));
    }
}
