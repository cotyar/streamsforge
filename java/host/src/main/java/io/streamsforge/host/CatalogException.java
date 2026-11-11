package io.streamsforge.host;

import io.streamsforge.engine.SqlDiagnostic;
import java.util.List;
import java.util.stream.Collectors;

/** A catalog operation was refused: invalid SQL (with the engine's diagnostics), name collision, unknown name, ... */
public final class CatalogException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final transient List<SqlDiagnostic> diagnostics;

    public CatalogException(String message) {
        super(message);
        this.diagnostics = List.of();
    }

    public CatalogException(String message, List<SqlDiagnostic> diagnostics) {
        super(message + (diagnostics.isEmpty() ? "" : ": " + diagnostics.stream()
            .map(d -> d.line() + ":" + d.column() + " " + d.message()).collect(Collectors.joining("; "))));
        this.diagnostics = List.copyOf(diagnostics);
    }

    /** The engine's diagnostics when the refusal was a compile failure; empty otherwise. */
    public List<SqlDiagnostic> diagnostics() { return diagnostics; }
}
