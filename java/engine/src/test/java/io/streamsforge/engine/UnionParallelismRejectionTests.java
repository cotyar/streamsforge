package io.streamsforge.engine;

import static io.streamsforge.engine.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Plan 008 W3 — set-operation tables are pinned to Parallelism = 1: the table dataflow builder rejects a
 * union-root plan unconditionally (mirroring the derived-table-in-FROM-position guard right next to it — the check
 * must come BEFORE the {@code compiled.Sources[0]} dereference, since a union root's Sources list is empty). C#
 * {@code NotSupportedException} maps to Java {@link UnsupportedOperationException}.
 */
class UnionParallelismRejectionTests {
    private static final SourceSchema LEFT = schema("left_src", "symbol", FieldKind.STRING);
    private static final SourceSchema RIGHT = schema("right_src", "symbol", FieldKind.STRING);
    private static final String SQL = "SELECT symbol FROM left_src UNION ALL SELECT symbol FROM right_src";

    private static final SourceSchema[] NONE = new SourceSchema[0];

    private static String join(TableCompileResult r) {
        return r.diagnostics().stream().map(SqlDiagnostic::toString).collect(Collectors.joining(";"));
    }

    @Test
    void unionAllTableCompiles() {
        var r = compileTable(SQL, NONE, new SourceSchema[] {LEFT, RIGHT});
        assertTrue(r.ok(), join(r));
    }

    @Test
    void createDataflowAtParallelism4ThrowsWithTheIntendedMessage() {
        var result = compileTable(SQL, NONE, new SourceSchema[] {LEFT, RIGHT});
        assertTrue(result.ok(), join(result));

        var ex = assertThrows(UnsupportedOperationException.class, () -> result.plan().createDataflow(4));

        assertTrue(ex.getMessage().contains("Use Parallelism = 1"));
        assertTrue(ex.getMessage().contains("UNION"));
    }

    @Test
    void createDataflowAtParallelism4ThrowsForUnionDistinctToo() {
        var distinctSql = "SELECT symbol FROM left_src UNION SELECT symbol FROM right_src";
        var result = compileTable(distinctSql, NONE, new SourceSchema[] {LEFT, RIGHT});
        assertTrue(result.ok(), join(result));

        var ex = assertThrows(UnsupportedOperationException.class, () -> result.plan().createDataflow(4));
        assertTrue(ex.getMessage().contains("Use Parallelism = 1"));
    }

    @Test
    void createDataflowThrowsBeforeEverDereferencingTheEmptySourcesList() {
        // A union-root CompiledTablePlan has an EMPTY Sources list — if the union guard were missing or ordered
        // after the Sources[0].DerivedPlan check, this would throw IndexOutOfBounds instead of the clear
        // unsupported-operation exception. Pinning the exception TYPE (not just message) makes that visible.
        var result = compileTable(SQL, NONE, new SourceSchema[] {LEFT, RIGHT});
        assertTrue(result.ok(), join(result));

        Throwable ex = null;
        try {
            result.plan().createDataflow(4);
        } catch (Throwable t) {
            ex = t;
        }
        assertNotNull(ex);
        assertEquals(UnsupportedOperationException.class, ex.getClass());
    }
}
