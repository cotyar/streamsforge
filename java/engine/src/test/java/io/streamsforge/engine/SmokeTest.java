package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Java-only smoke test (no C# counterpart): the end-to-end happy paths of both compilers. */
class SmokeTest {
    @Test
    void pipelineFilterProject() {
        var exec = compileAndCreate("SELECT symbol, price * 2 AS p2 FROM trades WHERE qty > 1", TRADES);
        var out = exec.onEvent("trades", evt(1000, "trades", "symbol", "A", "price", 1.5, "qty", 2L, "active", true));
        assertEquals(1, out.size());
        assertEquals(3.0, out.get(0).get("p2"));
    }

    @Test
    void tableGroupBy() {
        var exec = compileTableAndCreate("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", TRADES);
        var d1 = exec.onStreamEvent("trades", evt(1, "trades", "symbol", "A", "price", 1.0, "qty", 1L, "active", true));
        assertEquals(1, d1.size());
        var d2 = exec.onStreamEvent("trades", evt(2, "trades", "symbol", "A", "price", 1.0, "qty", 1L, "active", true));
        assertEquals(2, d2.size());
        assertEquals(2L, d2.get(1).row().get("cnt"));
        assertEquals(1, exec.snapshot().size());
    }

    @Test
    void extractReferences() {
        assertEquals(List.of("trades", "quotes"), SqlCompiler.extractReferences("SELECT * FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol"));
    }
}
