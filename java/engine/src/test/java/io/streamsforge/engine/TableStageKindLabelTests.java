package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.ORDER_EVENTS;
import static io.streamsforge.engine.TestHelpers.REF;
import static io.streamsforge.engine.TestHelpers.STRUCTURES;
import static io.streamsforge.engine.TestHelpers.TRADES;
import static io.streamsforge.engine.TestHelpers.compileTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.TableStageDescriptor;
import io.streamsforge.engine.dataflow.TableStageKind;
import io.streamsforge.engine.dataflow.TableStageKindLabel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Plan 003 M4 acceptance: {@link TableStageKindLabel} is exhaustive over every {@link TableStageKind} value, AND every
 * stage kind the builder actually emits across a representative set of plan shapes (the same six shapes
 * PartitionedDataflowTests uses: aggregate-only, join+aggregate, unnest+aggregate, latest-by, semi-join, scalar
 * broadcast) round-trips through the mapping without throwing and with a sane, non-empty label.
 */
class TableStageKindLabelTests {
    private static final TableStageKind[] ALL_KINDS = TableStageKind.values();

    /** Every enum value maps to a distinct, non-empty label — a future TableStageKind addition that forgets to extend
     *  the mapping fails HERE, not silently in production. */
    @ParameterizedTest
    @EnumSource(TableStageKind.class)
    void of_NeverThrows_And_ReturnsNonEmptyLabel(TableStageKind kind) {
        var label = TableStageKindLabel.of(kind);
        assertFalse(label == null || label.isBlank());
    }

    @Test
    void of_LabelsAreDistinctAcrossAllKinds() {
        var labels = Arrays.stream(ALL_KINDS).map(TableStageKindLabel::of).collect(Collectors.toList());
        assertEquals(labels.size(), new HashSet<>(labels).size());
    }

    @Test
    void of_MatchesExpectedWording() {
        assertEquals("Ingest", TableStageKindLabel.of(TableStageKind.INGEST));
        assertEquals("Join", TableStageKindLabel.of(TableStageKind.JOIN));
        assertEquals("SemiAnti", TableStageKindLabel.of(TableStageKind.SEMI_ANTI));
        assertEquals("Unnest", TableStageKindLabel.of(TableStageKind.UNNEST));
        assertEquals("FilterProject", TableStageKindLabel.of(TableStageKind.FILTER_PROJECT));
        assertEquals("Reduce", TableStageKindLabel.of(TableStageKind.REDUCE));
        assertEquals("LatestBy", TableStageKindLabel.of(TableStageKind.LATEST_BY));
    }

    // ------------------------------------------------------------------------------------------------
    // Representative plan shapes (mirrors PartitionedDataflowTests's six shapes).
    // ------------------------------------------------------------------------------------------------

    private static final String AGG_SQL = "SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol";
    private static final String JOIN_AGG_SQL = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
    private static final String UNNEST_AGG_SQL = "SELECT s.trade_id, COUNT(*) AS legcount FROM structures s CROSS JOIN UNNEST(s.legs) AS l GROUP BY s.trade_id";
    private static final String LATEST_BY_SQL = "SELECT order_id, stage, filled_qty FROM order_events LATEST BY (order_id)";
    private static final String SEMI_SQL = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
    private static final String SCALAR_SQL = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";

    private static List<TableStageKind> stageKindsOf(TableCompileResult compileResult) {
        assertTrue(compileResult.ok(), compileResult.diagnostics().stream().map(SqlDiagnostic::message).collect(Collectors.joining("; ")));
        var dataflow = compileResult.plan().createDataflow(4);
        return dataflow.stages().stream().map(TableStageDescriptor::kind).collect(Collectors.toList());
    }

    private record Shape(String sql, TableCompileResult compile) {}

    @Test
    void everyStageKindTheBuilderEmitsAcrossRepresentativePlans_HasALabel_AndUnionCoversTheWholeEnum() {
        var shapes = List.of(
            new Shape(AGG_SQL, compileTable(AGG_SQL, TRADES)),
            new Shape(JOIN_AGG_SQL, compileTable(JOIN_AGG_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF})),
            new Shape(UNNEST_AGG_SQL, compileTable(UNNEST_AGG_SQL, STRUCTURES)),
            new Shape(LATEST_BY_SQL, compileTable(LATEST_BY_SQL, ORDER_EVENTS)),
            new Shape(SEMI_SQL, compileTable(SEMI_SQL, new SourceSchema[] {TRADES}, new SourceSchema[] {REF})),
            new Shape(SCALAR_SQL, compileTable(SCALAR_SQL, TRADES)));

        var union = new HashSet<TableStageKind>();
        for (var shape : shapes) {
            var kinds = stageKindsOf(shape.compile());
            assertFalse(kinds.isEmpty()); // every plan shape must have produced SOME stages
            for (var kind : kinds) {
                var label = TableStageKindLabel.of(kind); // must not throw for anything the builder actually emits
                assertFalse(label == null || label.isBlank(), shape.sql() + ": " + kind + " mapped to a blank label");
                union.add(kind);
            }
        }

        // The six representative shapes, together, exercise every TableStageKind.
        assertEquals(new TreeSet<>(Arrays.asList(ALL_KINDS)).stream().collect(Collectors.toList()),
            new TreeSet<>(union).stream().collect(Collectors.toList()));
    }
}
