package io.streamsforge.engine.sql;

import io.streamsforge.engine.SqlDiagnostic;
import java.util.ArrayList;
import java.util.List;

/** Hand-rolled tokenizer: whitespace/{@code -- comment} skipping, identifiers, numbers, 'strings', and symbols.
 *  Records accurate 1-based line/column for every token. Never throws; unrecognized characters become
 *  diagnostics and are skipped so scanning can continue. */
public final class Tokenizer {
    // "->>" must precede "->" — matched in array order (first match wins), not by length.
    private static final String[] MULTI_CHAR_SYMBOLS = {"->>", "<>", "<=", ">=", "!=", "->"};

    public record Result(List<Token> tokens, List<SqlDiagnostic> diagnostics) {}

    private final String src;
    private int pos;
    private int line = 1;
    private int col = 1;
    private final List<SqlDiagnostic> diagnostics = new ArrayList<>();

    public Tokenizer(String sql) { this.src = sql; }

    public Result tokenize() {
        var tokens = new ArrayList<Token>();
        while (true) {
            skipTrivia();
            int ln = line, cl = col;
            if (isAtEnd()) { tokens.add(new Token(Token.Kind.END_OF_INPUT, "", ln, cl)); break; }
            char c = current();
            if (Character.isDigit(c)) { tokens.add(readNumber(ln, cl)); continue; }
            if (c == '\'') { tokens.add(readString(ln, cl)); continue; }
            if (Character.isLetter(c) || c == '_') { tokens.add(readIdentifier(ln, cl)); continue; }
            String multi = null;
            for (String s : MULTI_CHAR_SYMBOLS) if (matches(s)) { multi = s; break; }
            if (multi != null) { advance(multi.length()); tokens.add(new Token(Token.Kind.SYMBOL, multi, ln, cl)); continue; }
            if ("=<>+-*/%,.()".indexOf(c) >= 0) { advance(1); tokens.add(new Token(Token.Kind.SYMBOL, String.valueOf(c), ln, cl)); continue; }
            diagnostics.add(new SqlDiagnostic("Unexpected character '" + c + "'", ln, cl));
            advance(1);
        }
        return new Result(tokens, diagnostics);
    }

    private boolean isAtEnd() { return pos >= src.length(); }
    private char current() { return src.charAt(pos); }
    private char peek(int ahead) { return pos + ahead < src.length() ? src.charAt(pos + ahead) : '\0'; }

    private boolean matches(String s) {
        if (pos + s.length() > src.length()) return false;
        for (int i = 0; i < s.length(); i++) if (src.charAt(pos + i) != s.charAt(i)) return false;
        return true;
    }

    private void advance(int count) {
        for (int i = 0; i < count; i++) {
            if (isAtEnd()) return;
            if (src.charAt(pos) == '\n') { line++; col = 1; } else { col++; }
            pos++;
        }
    }

    private void skipTrivia() {
        while (!isAtEnd()) {
            char c = current();
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') { advance(1); continue; }
            if (c == '-' && peek(1) == '-') { while (!isAtEnd() && current() != '\n') advance(1); continue; }
            break;
        }
    }

    private Token readNumber(int ln, int cl) {
        int start = pos;
        boolean isDouble = false;
        while (!isAtEnd() && Character.isDigit(current())) advance(1);
        if (!isAtEnd() && current() == '.' && Character.isDigit(peek(1))) {
            isDouble = true;
            advance(1);
            while (!isAtEnd() && Character.isDigit(current())) advance(1);
        }
        String text = src.substring(start, pos);
        if (isDouble) return new Token(Token.Kind.NUMBER, text, ln, cl, Double.parseDouble(text), null, null);
        try {
            return new Token(Token.Kind.NUMBER, text, ln, cl, null, Long.parseLong(text), null);
        } catch (NumberFormatException e) {
            // overflow: fall back to double
            return new Token(Token.Kind.NUMBER, text, ln, cl, Double.parseDouble(text), null, null);
        }
    }

    private Token readString(int ln, int cl) {
        advance(1); // opening quote
        var sb = new StringBuilder();
        while (true) {
            if (isAtEnd()) { diagnostics.add(new SqlDiagnostic("Unterminated string literal", ln, cl)); break; }
            if (current() == '\'') {
                if (peek(1) == '\'') { sb.append('\''); advance(2); continue; }
                advance(1);
                break;
            }
            sb.append(current());
            advance(1);
        }
        return new Token(Token.Kind.STRING, sb.toString(), ln, cl, null, null, sb.toString());
    }

    private Token readIdentifier(int ln, int cl) {
        int start = pos;
        while (!isAtEnd() && (Character.isLetterOrDigit(current()) || current() == '_')) advance(1);
        return new Token(Token.Kind.IDENTIFIER, src.substring(start, pos), ln, cl);
    }
}
