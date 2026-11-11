package io.streamsforge.engine.sql;

/** A lexical token with 1-based line/column position. (C# {@code internal}; public here because the
 *  tests and later waves live in other packages.) */
public final class Token {
    public enum Kind { IDENTIFIER, NUMBER, STRING, SYMBOL, END_OF_INPUT }

    public final Kind kind;
    /** Raw source text (identifier name, symbol, or the number's source text). */
    public final String text;
    public final int line;
    public final int column;
    /** Set when kind == NUMBER and the literal contains a decimal point (or overflowed a long). */
    public final Double doubleValue;
    /** Set when kind == NUMBER and the literal is a plain integer. */
    public final Long longValue;
    /** Set when kind == STRING: the unescaped string contents. */
    public final String stringValue;

    public Token(Kind kind, String text, int line, int column) { this(kind, text, line, column, null, null, null); }

    public Token(Kind kind, String text, int line, int column, Double doubleValue, Long longValue, String stringValue) {
        this.kind = kind; this.text = text; this.line = line; this.column = column;
        this.doubleValue = doubleValue; this.longValue = longValue; this.stringValue = stringValue;
    }

    public boolean isKeyword(String keyword) { return kind == Kind.IDENTIFIER && text.equalsIgnoreCase(keyword); }

    public boolean isSymbol(String symbol) { return kind == Kind.SYMBOL && text.equals(symbol); }

    @Override public String toString() { return kind == Kind.END_OF_INPUT ? "<eof>" : text; }
}
