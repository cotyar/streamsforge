package io.streamsforge.engine;

import static io.streamsforge.engine.TableTestSupport.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.ExpressionEvaluator.Binding;
import io.streamsforge.engine.runtime.TableKeyEncoding;
import io.streamsforge.engine.runtime.WorkingRow;
import io.streamsforge.engine.runtime.ops.TableIngestOp;
import io.streamsforge.engine.runtime.ops.TableOuterJoinOp;
import io.streamsforge.engine.runtime.ops.TableRowDelta;
import io.streamsforge.engine.sql.Ast.Expr;
import io.streamsforge.engine.sql.Ast.Identifier;
import io.streamsforge.engine.sql.Ast.JoinKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Op-level unit tests for TableOuterJoinOp (plan 008 wave 2a-B). Modeled on TableOpsUnitTests' TableJoinOp
 * section: the op is constructed directly (never through TableExecutor) and driven with explicit
 * deltas-in/deltas-out assertions. Every SQL-derived test compiles the equivalent INNER join and hands its
 * keys/bindings to TableOuterJoinOp's constructor with an explicit JoinKind — the compiled ON clause (an
 * equi-key + optional residual) is identical regardless of which JoinKind ultimately consumes it. Composite-key
 * tests build their Expr/bindings by hand instead — see createCompositeOp.
 */
class TableOuterJoinOpUnitTests {
    private static final Epoch E0 = new Epoch(0);
    private static final Epoch E1 = new Epoch(1);
    private static final Epoch E2 = new Epoch(2);

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static TableOuterJoinOp createJoinOp(JoinKind kind) { return createJoinOp(kind, null); }

    /** Compiles "trades t JOIN ref r ON t.symbol = r.symbol [AND extraOn]" as INNER and builds a TableOuterJoinOp
     *  of the requested kind from the compiled keys/residual/bindings. */
    private static TableOuterJoinOp createJoinOp(JoinKind kind, String extraOn) {
        var on = extraOn == null ? "t.symbol = r.symbol" : "t.symbol = r.symbol AND " + extraOn;
        var compiled = compileTable("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON " + on, new SourceSchema[] {TRADES}, new SourceSchema[] {REF}).plan().compiled();
        var j = compiled.joins.get(0);
        var leftSide = new WorkingRow.Side(compiled.sources.get(0).alias, compiled.sources.get(0).schema);
        var rightSide = new WorkingRow.Side(j.alias, j.schema);
        return new TableOuterJoinOp(kind, List.of(j.leftKey), List.of(j.rightKey), j.residual, compiled.bindings, List.of(leftSide), rightSide);
    }

    private static List<TableRowDelta> left(TableOuterJoinOp op, Epoch epoch, String symbol, long weight) {
        return new ArrayList<>(op.onLeftBatch(epoch, new TableIngestOp("t").onBatch(epoch, List.of(new TableDelta(evt(1000, "trades", "symbol", symbol), weight)))));
    }

    private static List<TableRowDelta> right(TableOuterJoinOp op, Epoch epoch, String symbol, String tag, long weight) {
        return new ArrayList<>(op.onRightBatch(epoch, new TableIngestOp("r").onBatch(epoch, List.of(new TableDelta(evt(0, "ref", "symbol", symbol, "tag", tag), weight)))));
    }

    private static boolean isPad(TableRowDelta d) { return d.row().fields().get("t_symbol") == null || d.row().fields().get("r_symbol") == null; }

    // ------------------------------------------------------------------
    // LEFT join — core T1/T2/T3 rules and the presence-flip machinery.
    // ------------------------------------------------------------------

    @Test
    void leftAssert_EmptyRightBucket_Pads_AndLeftIsIndexed() {
        var op = createJoinOp(JoinKind.LEFT);

        var outp = left(op, E0, "AAPL", 1);

        var d = single(outp);
        assertEquals(1L, d.weight());
        assertEquals("AAPL", d.row().fields().get("t_symbol"));
        assertNull(d.row().fields().get("r_symbol"));
        assertNull(d.row().fields().get("r_tag"));

        single(op.left.lookup(TableKeyEncoding.encodeScalar("AAPL")));
    }

    @Test
    void leftAssert_NonEmptyRightBucket_ProductsOnly_NoPad() {
        var op = createJoinOp(JoinKind.LEFT);
        right(op, E0, "AAPL", "watchlist", 2);

        var outp = left(op, E1, "AAPL", 1);

        var d = single(outp);
        assertEquals(2L, d.weight()); // 1 * 2
        assertEquals("AAPL", d.row().fields().get("t_symbol"));
        assertEquals("watchlist", d.row().fields().get("r_tag"));
        assertFalse(isPad(d));
    }

    @Test
    void rightAssert_FirstForKey_ProductsAndPadRetraction_TheFlip() {
        var op = createJoinOp(JoinKind.LEFT);
        left(op, E0, "AAPL", 1); // pads: right bucket is empty

        var outp = right(op, E1, "AAPL", "watchlist", 2);

        assertEquals(2, outp.size());
        var product = single(outp, d -> !isPad(d));
        assertEquals(2L, product.weight()); // 1 * 2
        var padRetraction = single(outp, TableOuterJoinOpUnitTests::isPad);
        assertEquals(-1L, padRetraction.weight()); // retracts the earlier left pad (weight 1)
        assertEquals("AAPL", padRetraction.row().fields().get("t_symbol"));
        assertNull(padRetraction.row().fields().get("r_symbol"));
    }

    @Test
    void rightAssert_SecondRowSameKey_ProductsOnly_NoPadTraffic() {
        // Load-bearing negative test: the flip is per-KEY presence (right.lookup(k).any()), not
        // per-delta — a second right row under an already-present key must not re-trigger it.
        var op = createJoinOp(JoinKind.LEFT);
        left(op, E0, "AAPL", 1);
        right(op, E1, "AAPL", "watchlist", 1); // first: flips, pad retracts

        var outp = right(op, E2, "AAPL", "core", 3);

        var d = single(outp);
        assertFalse(isPad(d));
        assertEquals(3L, d.weight()); // 1 (left) * 3 (right)
        assertEquals("core", d.row().fields().get("r_tag"));
    }

    @Test
    void rightRetract_NotLastRow_NoPadTraffic() {
        var op = createJoinOp(JoinKind.LEFT);
        left(op, E0, "AAPL", 1);
        right(op, E1, "AAPL", "watchlist", 1);
        right(op, E1, "AAPL", "core", 1); // two distinct right rows now present under AAPL

        var outp = right(op, E2, "AAPL", "watchlist", -1); // retract one; the other keeps the key present

        var d = single(outp);
        assertFalse(isPad(d));
        assertEquals(-1L, d.weight());
        assertEquals("watchlist", d.row().fields().get("r_tag"));
    }

    @Test
    void rightRetract_LastRow_PadReassertion_ForEveryLeftRow() {
        var op = createJoinOp(JoinKind.LEFT);
        left(op, E0, "AAPL", 1);
        right(op, E1, "AAPL", "watchlist", 1);

        var outp = right(op, E2, "AAPL", "watchlist", -1); // the only right row retracts

        assertEquals(2, outp.size());
        var productRetraction = single(outp, d -> !isPad(d));
        assertEquals(-1L, productRetraction.weight());
        var padReassertion = single(outp, TableOuterJoinOpUnitTests::isPad);
        assertEquals(1L, padReassertion.weight());
        assertEquals("AAPL", padReassertion.row().fields().get("t_symbol"));
    }

    @Test
    void leftRetract_WhileUnmatched_PadRetraction_AndIndexPruning() {
        var op = createJoinOp(JoinKind.LEFT);
        left(op, E0, "AAPL", 1); // pads (right empty)

        var outp = left(op, E1, "AAPL", -1);

        var d = single(outp);
        assertTrue(isPad(d));
        assertEquals(-1L, d.weight());
        assertTrue(op.left.lookup(TableKeyEncoding.encodeScalar("AAPL")).isEmpty());
    }

    @Test
    void leftRetract_WhileMatched_ProductRetractionOnly() {
        var op = createJoinOp(JoinKind.LEFT);
        right(op, E0, "AAPL", "watchlist", 1);
        left(op, E1, "AAPL", 1); // matched: product only, no pad

        var outp = left(op, E2, "AAPL", -1);

        var d = single(outp);
        assertFalse(isPad(d));
        assertEquals(-1L, d.weight());
        assertTrue(op.left.lookup(TableKeyEncoding.encodeScalar("AAPL")).isEmpty());
    }

    @Test
    void leftNullKey_PadEmitted_NotIndexed() {
        var op = createJoinOp(JoinKind.LEFT);

        var outp = op.onLeftBatch(E0, new TableIngestOp("t").onBatch(E0, List.of(new TableDelta(evt(1000, "trades", "symbol", null), 1))));

        var d = single(outp);
        assertTrue(isPad(d));
        assertEquals(1L, d.weight());
        assertTrue(op.left.lookup(TableKeyEncoding.encodeScalar(null)).isEmpty());
    }

    @Test
    void rightNullKey_NotIndexed_DoesNotMakeKeyPresent() {
        // Guards the worst bug in the spec: if a NULL-keyed right row were indexed under a shared
        // placeholder key, it would fake "presence" and silently suppress every left pad under that
        // key. Prove it has zero effect: the left pad survives, and a REAL right row for the same key
        // still triggers the normal flip afterwards.
        var op = createJoinOp(JoinKind.LEFT);
        var leftPad = left(op, E0, "AAPL", 1);
        single(leftPad); // sanity: padded, right bucket empty

        var nullOutp = right(op, E1, null, "whatever", 1);

        assertTrue(nullOutp.isEmpty());
        assertTrue(op.right.lookup(TableKeyEncoding.encodeScalar(null)).isEmpty());

        // A genuine right row for AAPL afterwards must still see the normal flip — proving the NULL
        // row above never made ANY key (least of all "AAPL") look present.
        var realOutp = right(op, E2, "AAPL", "watchlist", 1);
        assertEquals(2, realOutp.size());
        single(realOutp, d -> !isPad(d));
        var flip = single(realOutp, TableOuterJoinOpUnitTests::isPad);
        assertEquals(-1L, flip.weight());
    }

    @Test
    void outOfOrder_RetractThenAssert_SelfHeals() {
        // The exact trace from the op's doc comment: R(r,-1), L(l,+1), R(r,+1) must net to
        // products 0 and pad +1 — proof that presence is "!= 0", not "> 0".
        var op = createJoinOp(JoinKind.LEFT);

        var step1 = right(op, E0, "AAPL", "watchlist", -1);
        var step2 = left(op, E0, "AAPL", 1);
        var step3 = right(op, E0, "AAPL", "watchlist", 1);

        var all = new ArrayList<TableRowDelta>();
        all.addAll(step1); all.addAll(step2); all.addAll(step3);
        long productNet = all.stream().filter(d -> !isPad(d)).mapToLong(TableRowDelta::weight).sum();
        long padNet = all.stream().filter(TableOuterJoinOpUnitTests::isPad).mapToLong(TableRowDelta::weight).sum();

        assertEquals(0L, productNet);
        assertEquals(1L, padNet);
    }

    // ------------------------------------------------------------------
    // RIGHT mirrors LEFT; FULL runs both halves and never emits (null, null).
    // ------------------------------------------------------------------

    @Test
    void rightJoin_MirrorsLeft_OwnPadThenFlipOnOppositeArrival() {
        var op = createJoinOp(JoinKind.RIGHT);

        // A lone right row with no left match: pads itself (mirror of T2), and IS indexed.
        var rightAlone = right(op, E0, "AAPL", "watchlist", 1);
        var pad = single(rightAlone);
        assertTrue(isPad(pad));
        assertNull(pad.row().fields().get("t_symbol"));
        assertEquals("watchlist", pad.row().fields().get("r_tag"));

        // A matching left row arrives: product fires, and the right row's earlier pad retracts (the
        // mirrored flip, detected on the Left index per the class doc).
        var leftArrival = left(op, E1, "AAPL", 1);
        assertEquals(2, leftArrival.size());
        var product = single(leftArrival, d -> !isPad(d));
        assertEquals(1L, product.weight());
        var padRetraction = single(leftArrival, TableOuterJoinOpUnitTests::isPad);
        assertEquals(-1L, padRetraction.weight());
    }

    @Test
    void fullJoin_PadsBothSides_NeverEmitsNullNullRow() {
        var op = createJoinOp(JoinKind.FULL);
        var all = new ArrayList<TableRowDelta>();

        all.addAll(left(op, E0, "AAPL", 1));   // unmatched left -> pads itself
        all.addAll(right(op, E1, "MSFT", "other", 1)); // unmatched right (different key) -> pads itself
        all.addAll(right(op, E2, "AAPL", "watchlist", 1)); // matches the left row: product + left's pad retracts

        assertEquals(4, all.size()); // left pad, right pad, product, left-pad retraction
        for (var d : all) {
            boolean leftNull = d.row().fields().get("t_symbol") == null;
            boolean rightNull = d.row().fields().get("r_symbol") == null;
            assertFalse(leftNull && rightNull);
        }

        var leftPad = single(all, d -> d.row().fields().get("r_symbol") == null && d.weight() > 0);
        assertEquals(1L, leftPad.weight());
        var rightPad = single(all, d -> d.row().fields().get("t_symbol") == null);
        assertEquals(1L, rightPad.weight());
        var product = single(all, d -> d.row().fields().get("t_symbol") != null && d.row().fields().get("r_symbol") != null && d.weight() > 0);
        assertEquals(1L, product.weight());
        var retraction = single(all, d -> d.row().fields().get("r_symbol") == null && d.weight() < 0);
        assertEquals(-1L, retraction.weight());
    }

    // ------------------------------------------------------------------
    // Composite keys — hand-built Expr/bindings (validator doesn't produce key lists yet).
    // ------------------------------------------------------------------

    private static WorkingRow row(String alias, Object... fieldAndValue) {
        var dict = new LinkedHashMap<String, Object>();
        for (int i = 0; i < fieldAndValue.length; i += 2) dict.put(alias + "_" + fieldAndValue[i], fieldAndValue[i + 1]);
        return new WorkingRow(0, List.of(alias), dict);
    }

    private static TableOuterJoinOp createCompositeOp(JoinKind kind) {
        var la = new Identifier("l.a", 0, 0);
        var lb = new Identifier("l.b", 0, 0);
        var ra = new Identifier("r.a", 0, 0);
        var rb = new Identifier("r.b", 0, 0);
        Map<Expr, Binding> bindings = new HashMap<>();
        bindings.put(la, new Binding("l", "a"));
        bindings.put(lb, new Binding("l", "b"));
        bindings.put(ra, new Binding("r", "a"));
        bindings.put(rb, new Binding("r", "b"));

        var leftSchema = schema("lsrc", "a", FieldKind.STRING, "b", FieldKind.STRING);
        var rightSchema = schema("rsrc", "a", FieldKind.STRING, "b", FieldKind.STRING);
        return new TableOuterJoinOp(kind, List.of(la, lb), List.of(ra, rb), null, bindings,
            List.of(new WorkingRow.Side("l", leftSchema)), new WorkingRow.Side("r", rightSchema));
    }

    @Test
    void compositeKey_Matching_ProductsAcrossBothComponents() {
        var op = createCompositeOp(JoinKind.LEFT);
        op.onRightBatch(E0, List.of(new TableRowDelta(row("r", "a", "X", "b", "Y"), 1)));

        var outp = op.onLeftBatch(E1, List.of(new TableRowDelta(row("l", "a", "X", "b", "Y"), 2)));

        var d = single(outp);
        assertEquals(2L, d.weight());
        assertEquals("X", d.row().fields().get("l_a"));
        assertEquals("Y", d.row().fields().get("r_b"));
    }

    @Test
    void compositeKey_NonMatching_OneComponentDiffers_Pads() {
        var op = createCompositeOp(JoinKind.LEFT);
        op.onRightBatch(E0, List.of(new TableRowDelta(row("r", "a", "X", "b", "Z"), 1))); // b differs

        var outp = op.onLeftBatch(E1, List.of(new TableRowDelta(row("l", "a", "X", "b", "Y"), 1)));

        var d = single(outp);
        assertNull(d.row().fields().get("r_a"));
        assertEquals(1L, d.weight());
    }

    @Test
    void compositeKey_NullInOneComponent_PadsAndIsNotIndexed() {
        var op = createCompositeOp(JoinKind.LEFT);

        var outp = op.onLeftBatch(E0, List.of(new TableRowDelta(row("l", "a", "X", "b", null), 1)));

        var d = single(outp);
        assertEquals("X", d.row().fields().get("l_a"));
        assertNull(d.row().fields().get("r_a"));
        assertEquals(1L, d.weight());

        // Not indexed under any encoding that includes this row's non-null component.
        assertTrue(op.left.lookup(TableKeyEncoding.encodeGroupKey(new Object[] {"X", null})).isEmpty());
    }

    // ------------------------------------------------------------------
    // Residual predicate — breaks the cheap key-level presence test; must be evaluated per left row.
    // ------------------------------------------------------------------

    @Test
    void residual_NonEmptyBucketButNoPassingCandidate_StillPads_ThenUnpadsWhenOnePasses() {
        // "t.symbol = r.tag" as the residual: passes only when the left row's own symbol equals the
        // right row's tag — a plain "is the bucket non-empty" test would wrongly report present.
        var op = createJoinOp(JoinKind.LEFT, "t.symbol = r.tag");

        right(op, E0, "AAPL", "watchlist", 1); // bucket becomes non-empty, but "AAPL" != "watchlist"

        var leftOutp = left(op, E1, "AAPL", 1);
        var pad = single(leftOutp);
        assertTrue(isPad(pad));
        assertEquals(1L, pad.weight());

        var rightOutp = right(op, E2, "AAPL", "AAPL", 1); // this candidate DOES pass ("AAPL" == "AAPL")

        assertEquals(2, rightOutp.size());
        var product = single(rightOutp, d -> !isPad(d));
        assertEquals(1L, product.weight());
        assertEquals("AAPL", product.row().fields().get("r_tag"));
        var unpad = single(rightOutp, TableOuterJoinOpUnitTests::isPad);
        assertEquals(-1L, unpad.weight()); // retracts the earlier pad
    }

    // ------------------------------------------------------------------
    // OnFrontier
    // ------------------------------------------------------------------

    @Test
    void onFrontierIsADocumentedPassThrough() {
        var op = createJoinOp(JoinKind.FULL);
        assertTrue(op.onFrontier(E0).isEmpty());
        assertTrue(op.onFrontier(new Epoch(999)).isEmpty());
    }
}
