package io.streamsforge.engine;

import static io.streamsforge.engine.PortAsserts.single;
import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Proves plan 003 M1 Part B's composability requirement — "such that an entire op-chain can itself be
 * wrapped as a node inside another chain" — which is exactly what plan 004 N1 (derived tables / WITH,
 * windows-in-windows) needs: a HAND-BUILT two-level chain, built WITHOUT any parser/planner support for a real
 * nested-query AST node; the test code forwards every row the inner chain emits into the outer chain's onEvent.
 */
class PipelineComposabilityTests {
    @Test
    void selectOverTheOutputOfAnInnerWindowedSelectProducesExpectedRows() {
        // Inner: 10s tumbling window, EMIT FINAL, one row per symbol per window with its average price.
        var inner = compileAndCreate(
            "SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            TRADES);

        // Outer: filters the inner chain's emissions down to symbols whose windowed average exceeds 50.
        var hotSchema = schema("hot", "symbol", FieldKind.STRING, "avg_px", FieldKind.DOUBLE);
        var outer = compileAndCreate("SELECT symbol, avg_px FROM hot WHERE avg_px > 50", hotSchema);

        var outerResults = new ArrayList<EventRecord>();

        // This loop IS the embedding seam: the inner chain's emitted rows become the outer chain's
        // onEvent input events, exactly as plan 004 N1 describes for a derived-table/CTE node.
        java.util.function.Consumer<List<EventRecord>> forward = innerEmissions -> {
            for (var row : innerEmissions) outerResults.addAll(outer.onEvent("hot", row));
        };

        forward.accept(inner.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true)));
        forward.accept(inner.onEvent("trades", evt(1500, "trades", "symbol", "AAPL", "price", 200.0, "qty", 1L, "active", true)));
        forward.accept(inner.onEvent("trades", evt(2000, "trades", "symbol", "MSFT", "price", 10.0, "qty", 1L, "active", true)));
        forward.accept(inner.onEvent("trades", evt(2500, "trades", "symbol", "MSFT", "price", 20.0, "qty", 1L, "active", true)));

        // Advancing the inner chain's watermark closes the tumbling window — its emissions (the closed
        // window's final rows) are exactly "the child chain's emissions" that become the outer chain's
        // input events.
        forward.accept(inner.advanceWatermark(12000));

        // AAPL(avg=150) passes the outer WHERE; MSFT(avg=15) is filtered out.
        var aapl = single(outerResults);
        assertEquals("AAPL", aapl.get("symbol"));
        assertEquals(150.0, aapl.get("avg_px"));
    }

    @Test
    void innerChainCanFeedAJoinInTheOuterChain_NotJustAPlainFilter() {
        // A slightly richer embedding: the outer chain JOINs the inner chain's windowed emissions against
        // a second live stream — proving the seam composes with the outer chain's OWN op chain (join
        // stage), not just a bare filter/project.
        var inner = compileAndCreate(
            "SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            TRADES);

        var hotSchema = schema("hot", "symbol", FieldKind.STRING, "avg_px", FieldKind.DOUBLE);
        var outer = compileAndCreate(
            "SELECT h.symbol, h.avg_px, q.bid FROM hot h JOIN quotes q WITHIN 1 SECONDS ON h.symbol = q.symbol",
            hotSchema, QUOTES);

        var outerResults = new ArrayList<EventRecord>();
        java.util.function.Consumer<List<EventRecord>> forward = innerEmissions -> {
            for (var row : innerEmissions) outerResults.addAll(outer.onEvent("hot", row));
        };

        forward.accept(inner.onEvent("trades", evt(1000, "trades", "symbol", "AAPL", "price", 100.0, "qty", 1L, "active", true)));
        var closed = inner.advanceWatermark(12000); // closes window at t=10000, emits AAPL avg_px=100

        assertTrue(outer.onEvent("hot", closed.get(0)).isEmpty()); // no matching quote yet
        var matched = outer.onEvent("quotes", evt(closed.get(0).timestamp() + 200, "quotes", "symbol", "AAPL", "bid", 99.0, "ask", 101.0));

        var row = single(matched);
        assertEquals("AAPL", row.get("symbol"));
        assertEquals(100.0, row.get("avg_px"));
        assertEquals(99.0, row.get("bid"));
    }
}
