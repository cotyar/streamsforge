package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.diag;
import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;

/**
 * What happens when a GROUP BY table is attached to an upstream that ALREADY HOLDS ROWS and those rows
 * are never replayed to it.
 *
 * <p>LATEST BY emits a bare assert on first sight of a key and retract(−1)+assert(+1) thereafter, so
 * a consumer that attached late receives a RETRACTION for a row it has never seen. That is not an
 * internal inconsistency — the deltas are individually well-formed — it is missing history, and no
 * arithmetic downstream can invent it. The only real fix is to replay the upstream's current contents
 * on attach; these tests pin what happens until that exists, which is the difference between an
 * obviously-empty answer and a convincingly wrong one.
 */
class AggregateOverWarmTableTests {
    private static final SourceSchema TICKS = schema("ticks", "g", FieldKind.STRING, "x", FieldKind.DOUBLE);

    private record Late(TableExecutor latest, TableExecutor rolled, SourceSchema rolledInput) {}

    /** Builds {@code rolled} over a LATEST BY table WITHOUT replaying what the LATEST BY already holds
     *  — exactly the state a table created mid-session lands in. */
    private static Late buildLate(String rolledSql) {
        var latest = compileTable("SELECT g, x FROM ticks LATEST BY (g)", TICKS);
        assertTrue(latest.ok(), () -> diag(latest.diagnostics()));
        var input = new SourceSchema("latest_x", latest.outputSchema().fields());

        var rolled = compileTable(rolledSql, new SourceSchema[0], new SourceSchema[] {input});
        assertTrue(rolled.ok(), () -> diag(rolled.diagnostics()));
        return new Late(latest.plan().createExecutor(), rolled.plan().createExecutor(), input);
    }

    /** The reported symptom: the aggregate used to report ONE contributing row per group, however many the
     *  upstream actually held, because the unmatched retraction destroyed the empty group and the assert that
     *  followed rebuilt it from that single row. It now reports nothing at all until the missing asserts are
     *  supplied — the same information, impossible to mistake for an answer. */
    @Test
    void an_aggregate_attached_to_a_populated_table_reports_nothing_rather_than_a_plausible_wrong_count() {
        var late = buildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var latest = late.latest();
        var rolled = late.rolled();

        long ts = 1000;
        // Three keys land in the LATEST BY while `rolled` is NOT yet listening.
        for (var key : new String[] {"a", "b", "c"}) {
            latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", key, "x", 1.0));
        }

        // `rolled` attaches here and now sees every subsequent delta — for an existing key that is
        // retract(old) then assert(new).
        for (var delta : latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 2.0))) {
            rolled.onTableDelta("latest_x", delta);
        }

        assertTrue(rolled.snapshot().isEmpty());
        assertEquals(1L, rolled.unmatchedRetractions());

        // And it stays stable rather than flapping between one row and none on every later change.
        for (int i = 0; i < 3; i++) {
            for (var delta : latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 3.0 + i))) {
                rolled.onTableDelta("latest_x", delta);
            }
            assertTrue(rolled.snapshot().isEmpty());
        }
        assertEquals(4L, rolled.unmatchedRetractions());
    }

    /** The property that makes a future backfill safe: the group's arithmetic is only PAUSED, not corrupted.
     *  Feeding the asserts the consumer missed brings it to the right answer, which it could not do if the
     *  unmatched retraction had thrown the group away. */
    @Test
    void supplying_the_missed_asserts_afterwards_converges_to_the_right_answer() {
        var late = buildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var latest = late.latest();
        var rolled = late.rolled();

        long ts = 1000;
        var missed = new ArrayList<TableDelta>();
        for (var key : new String[] {"a", "a", "a"}) {
            // Same group three times over: the LATEST BY collapses them to one key, but the aggregate
            // below groups on a constant, so what matters is that `rolled` never saw these asserts.
            missed.addAll(latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", key, "x", 1.0)));
        }

        for (var delta : latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", "a", "x", 9.0))) {
            rolled.onTableDelta("latest_x", delta);
        }
        assertTrue(rolled.snapshot().isEmpty());

        // The backfill that does not exist yet, done by hand: replay every delta the consumer missed,
        // retractions included. Replaying only the asserts would over-count — the three pushes above are
        // one assert followed by two retract+assert pairs, so the history nets to a single row.
        for (var delta : missed) {
            rolled.onTableDelta("latest_x", delta);
        }

        var row = single(rolled.snapshot().entrySet());
        assertEquals(1L, row.getValue().row().get("n"));
    }

    /** The counter must stay at zero for a table built the ordinary way, or it is noise rather than a signal. */
    @Test
    void a_table_attached_before_its_upstream_had_rows_reports_no_unmatched_retractions() {
        var late = buildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var latest = late.latest();
        var rolled = late.rolled();

        long ts = 1000;
        for (var key : new String[] {"a", "b", "a", "c", "b"}) {
            for (var delta : latest.onStreamEvent("ticks", evt(ts++, "ticks", "g", key, "x", 1.0))) {
                rolled.onTableDelta("latest_x", delta);
            }
        }

        assertEquals(0L, rolled.unmatchedRetractions());
        assertEquals(3, rolled.snapshot().size());
    }

    @Test
    void a_table_with_no_group_by_reports_minus_one_rather_than_a_misleading_zero() {
        var late = buildLate("SELECT g, x FROM latest_x");
        assertEquals(-1L, late.rolled().unmatchedRetractions());
    }
}
