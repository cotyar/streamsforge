package io.streamsforge.engine;

/** A positioned compile diagnostic (1-based line/column). */
public record SqlDiagnostic(String message, int line, int column, DiagnosticSeverity severity) {
    public SqlDiagnostic(String message, int line, int column) { this(message, line, column, DiagnosticSeverity.ERROR); }
}
