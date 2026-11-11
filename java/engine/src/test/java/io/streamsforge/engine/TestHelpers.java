package io.streamsforge.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.opentest4j.AssertionFailedError;

/** Port of the C# {@code TestHelpers}. Tuple params {@code (Field, Kind)} / {@code (Field, Value)} become
 *  alternating varargs: {@code schema("trades", "symbol", FieldKind.STRING, ...)}, {@code evt(1000, "trades", "price", 1.5)}. */
public final class TestHelpers {
    private TestHelpers() {}

    public static SourceSchema schema(String name, Object... fieldAndKind) {
        var fields = new LinkedHashMap<String, FieldKind>();
        for (int i = 0; i < fieldAndKind.length; i += 2) fields.put((String) fieldAndKind[i], (FieldKind) fieldAndKind[i + 1]);
        return new SourceSchema(name, fields);
    }

    public static EventRecord evt(long ts, String source, Object... fieldAndValue) {
        var e = new EventRecord();
        e.put("_ts", ts);
        e.put("_source", source);
        for (int i = 0; i < fieldAndValue.length; i += 2) e.put((String) fieldAndValue[i], fieldAndValue[i + 1]);
        return e;
    }

    public static Map<String, SourceSchema> schemas(SourceSchema... schemas) {
        var m = new LinkedHashMap<String, SourceSchema>();
        for (var s : schemas) m.put(s.name(), s);
        return m;
    }

    public static final SourceSchema TRADES = schema("trades", "symbol", FieldKind.STRING, "price", FieldKind.DOUBLE, "qty", FieldKind.LONG, "active", FieldKind.BOOL);
    public static final SourceSchema QUOTES = schema("quotes", "symbol", FieldKind.STRING, "bid", FieldKind.DOUBLE, "ask", FieldKind.DOUBLE);
    public static final SourceSchema REF = schema("ref", "symbol", FieldKind.STRING, "tag", FieldKind.STRING);
    public static final SourceSchema EVENTS = schema("events", "eventType", FieldKind.STRING, "payload", FieldKind.JSON);

    // Plan 002 L2/L3 fixtures — multileg instruments (UNNEST) and order lifecycle events (LATEST BY).
    public static final SourceSchema STRUCTURES = schema("structures", "trade_id", FieldKind.STRING, "legs", FieldKind.JSON, "payload", FieldKind.JSON);
    public static final SourceSchema ORDER_EVENTS = schema("order_events", "order_id", FieldKind.STRING, "stage", FieldKind.STRING, "filled_qty", FieldKind.LONG);

    public static CompileResult compile(String sql, SourceSchema... schemas) { return SqlCompiler.compile(sql, schemas(schemas)); }

    private static String describe(java.util.List<SqlDiagnostic> diagnostics) {
        return diagnostics.stream().map(d -> d.line() + ":" + d.column() + " " + d.message()).collect(Collectors.joining("; "));
    }

    public static PipelineExecutor compileAndCreate(String sql, SourceSchema... schemas) {
        var result = compile(sql, schemas);
        if (!result.ok() || result.plan() == null) {
            throw new AssertionFailedError("Expected successful compile but got: " + describe(result.diagnostics()));
        }
        return result.plan().createExecutor();
    }

    // ------------------------------------------------------------------
    // Table-mode helpers
    // ------------------------------------------------------------------

    /** {@code tableSchemas} may be null (none). */
    public static TableCompileResult compileTable(String sql, SourceSchema[] streamSchemas, SourceSchema[] tableSchemas) {
        return SqlCompiler.compileTable(sql, schemas(streamSchemas), schemas(tableSchemas == null ? new SourceSchema[0] : tableSchemas));
    }

    public static TableCompileResult compileTable(String sql, SourceSchema... streamSchemas) {
        return compileTable(sql, streamSchemas, null);
    }

    public static TableExecutor compileTableAndCreate(String sql, SourceSchema[] streamSchemas, SourceSchema[] tableSchemas) {
        var result = compileTable(sql, streamSchemas, tableSchemas);
        if (!result.ok() || result.plan() == null) {
            throw new AssertionFailedError("Expected successful table compile but got: " + describe(result.diagnostics()));
        }
        return result.plan().createExecutor();
    }

    public static TableExecutor compileTableAndCreate(String sql, SourceSchema... streamSchemas) {
        return compileTableAndCreate(sql, streamSchemas, null);
    }
}
