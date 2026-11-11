package io.streamsforge.engine.sql;

import static io.streamsforge.engine.sql.Ast.*;

import io.streamsforge.engine.SqlDiagnostic;
import io.streamsforge.engine.runtime.StatAggregatorNames;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Recursive-descent parser with a Pratt-style precedence chain for expressions.
 *  Case-insensitive keywords; identifiers preserve source casing. Never throws outward —
 *  {@link #parseStatement} catches {@link ParseException} and turns it into a diagnostic. */
public final class Parser {
    public static final class ParseException extends RuntimeException {
        public final SqlDiagnostic diagnostic;
        public ParseException(SqlDiagnostic d) { super(d.message()); this.diagnostic = d; }
    }

    /** Result of {@link #parse}: legacy (query, diagnostics) shape. */
    public record ParseResult(SelectQuery query, List<SqlDiagnostic> diagnostics) {}

    /** Result of {@link #parseStatement}: exactly one of query/setOp is non-null on success. */
    public record StatementResult(SelectQuery query, SetOperationQuery setOp, List<SqlDiagnostic> diagnostics) {}

    // Identifiers that must not be swallowed as an implicit (AS-less) alias (case-insensitive).
    private static final Set<String> CLAUSE_KEYWORDS = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    static {
        CLAUSE_KEYWORDS.addAll(List.of("JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "WINDOW", "EMIT", "ON", "WITHIN", "FROM",
            // Plan 004 N2: reserved so `expr IN (...)`/`EXISTS (...)` never get mistaken for an AS-less alias.
            "IN", "EXISTS",
            // Plan 002 L2/L3
            "UNNEST", "LATEST",
            // Plan 008 W3: so `SELECT a FROM t UNION ...` never swallows UNION as an AS-less alias.
            "UNION"));
    }

    private final List<Token> tokens;
    private int pos;

    private Parser(List<Token> tokens) { this.tokens = tokens; }

    /** Pre-008 entry point: thin wrapper over {@link #parseStatement}; a top-level set operation yields a null query. */
    public static ParseResult parse(List<Token> tokens) {
        var r = parseStatement(tokens);
        return r.setOp() != null ? new ParseResult(null, r.diagnostics()) : new ParseResult(r.query(), r.diagnostics());
    }

    public static StatementResult parseStatement(List<Token> tokens) {
        var parser = new Parser(tokens);
        try {
            var top = parser.parseTopLevel();
            if (parser.current().kind != Token.Kind.END_OF_INPUT) {
                throw parser.error(parser.current(), "Unexpected token '" + parser.current().text + "'");
            }
            return new StatementResult(top.query, top.setOp, new ArrayList<>());
        } catch (ParseException ex) {
            var d = new ArrayList<SqlDiagnostic>();
            d.add(ex.diagnostic);
            return new StatementResult(null, null, d);
        }
    }

    // ------------------------------------------------------------------
    // WITH (CTEs) — desugar to derived tables at parse time.
    // ------------------------------------------------------------------

    private record Top(SelectQuery query, SetOperationQuery setOp) {}

    private Top parseTopLevel() {
        if (!current().isKeyword("WITH")) return parseSelectOrSetOperation();

        advance(); // WITH
        var cteNames = new ArrayList<String>();
        var cteBodies = new ArrayList<SelectQuery>();

        while (true) {
            var nameTok = expectIdentifierToken();
            for (String n : cteNames) {
                if (n.equalsIgnoreCase(nameTok.text)) throw error(nameTok, "Duplicate CTE name '" + nameTok.text + "' in WITH list");
            }
            expectKeyword("AS");
            expectSymbol("(");
            var body = parseSelectQuery();
            expectSymbol(")");
            cteNames.add(nameTok.text);
            cteBodies.add(body);
            if (current().isSymbol(",")) { advance(); continue; }
            break;
        }

        // The main query following WITH may itself be a UNION chain — CTE substitution must recurse into EVERY branch.
        var main = parseSelectOrSetOperation();

        // Desugar in declaration order: CTE i's body may only reference CTE 0..i-1.
        Set<String> allCteNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        allCteNames.addAll(cteNames);
        Map<String, SelectQuery> resolved = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < cteNames.size(); i++) {
            resolved.put(cteNames.get(i), substituteCtes(cteBodies.get(i), resolved, allCteNames));
        }

        if (main.setOp != null) {
            var branches = new ArrayList<SelectQuery>();
            for (var b : main.setOp.branches) branches.add(substituteCtes(b, resolved, allCteNames));
            return new Top(null, new SetOperationQuery(main.setOp.all, branches, main.setOp.line, main.setOp.column));
        }
        return new Top(substituteCtes(main.query, resolved, allCteNames), null);
    }

    /** {@code SELECT ... [UNION [ALL|DISTINCT] SELECT ...]*}. Every UNION in the chain must share the same ALL-ness. */
    private Top parseSelectOrSetOperation() {
        var first = parseSelectQuery();
        if (!current().isKeyword("UNION")) return new Top(first, null);

        var chainTok = current();
        var branches = new ArrayList<SelectQuery>();
        branches.add(first);
        Boolean all = null;

        while (current().isKeyword("UNION")) {
            var opTok = advance(); // 'UNION'
            boolean thisAll = matchKeyword("ALL");
            if (!thisAll) matchKeyword("DISTINCT"); // 'UNION DISTINCT' is sugar for plain 'UNION'

            if (all == null) {
                all = thisAll;
            } else if (all != thisAll) {
                throw error(opTok, "Mixing UNION and UNION ALL in the same statement is not supported — use the same set-operation kind throughout the chain");
            }
            branches.add(parseSelectQuery());
        }
        return new Top(null, new SetOperationQuery(all, branches, chainTok.line, chainTok.column));
    }

    private SelectQuery substituteCtes(SelectQuery query, Map<String, SelectQuery> resolved, Set<String> allCteNames) {
        var joins = new ArrayList<JoinClause>();
        for (var j : query.from.joins) {
            joins.add(new JoinClause(j.kind, substituteFromItem(j.source, resolved, allCteNames), j.within,
                j.on == null ? null : substituteCtesInExpr(j.on, resolved, allCteNames), j.line, j.column));
        }
        var newFrom = new FromClause(substituteFromItem(query.from.source, resolved, allCteNames), joins);

        // A CTE reference can ALSO appear inside a WHERE-position IN/EXISTS predicate's or a scalar subquery's
        // own nested SelectQuery — those need the same substitution pass.
        Expr newWhere = query.where == null ? null : substituteCtesInExpr(query.where, resolved, allCteNames);
        var items = new ArrayList<SelectItem>();
        for (var i : query.select.items) items.add(new SelectItem(substituteCtesInExpr(i.expression, resolved, allCteNames), i.alias));
        var newSelect = new SelectClause(query.select.isStar, items);
        List<Expr> newGroupBy = null;
        if (query.groupBy != null) {
            newGroupBy = new ArrayList<>();
            for (var g : query.groupBy) newGroupBy.add(substituteCtesInExpr(g, resolved, allCteNames));
        }
        return new SelectQuery(newSelect, newFrom, newWhere, newGroupBy, query.window, query.emit,
            query.emitLine, query.emitColumn, query.groupByLine, query.groupByColumn, query.windowLine, query.windowColumn,
            query.latestBy, query.latestByLine, query.latestByColumn);
    }

    private Expr substituteCtesInExpr(Expr e, Map<String, SelectQuery> resolved, Set<String> allCteNames) {
        if (e instanceof InSubqueryExpr ins) {
            return new InSubqueryExpr(substituteCtesInExpr(ins.left, resolved, allCteNames), substituteCtes(ins.subquery, resolved, allCteNames), ins.negated, ins.line, ins.column);
        } else if (e instanceof ExistsExpr ex) {
            return new ExistsExpr(substituteCtes(ex.subquery, resolved, allCteNames), ex.negated, ex.line, ex.column);
        } else if (e instanceof ScalarSubqueryExpr sse) {
            return new ScalarSubqueryExpr(substituteCtes(sse.query, resolved, allCteNames), sse.line, sse.column);
        } else if (e instanceof UnaryExpr u) {
            return new UnaryExpr(u.op, substituteCtesInExpr(u.operand, resolved, allCteNames), u.line, u.column);
        } else if (e instanceof BinaryExpr b) {
            return new BinaryExpr(b.op, substituteCtesInExpr(b.left, resolved, allCteNames), substituteCtesInExpr(b.right, resolved, allCteNames), b.line, b.column);
        } else if (e instanceof FunctionCallExpr f) {
            var args = new ArrayList<Expr>();
            for (var a : f.args) args.add(substituteCtesInExpr(a, resolved, allCteNames));
            return new FunctionCallExpr(f.name, args, f.line, f.column);
        } else if (e instanceof AggregateCallExpr agg) {
            return agg.arg == null ? agg : new AggregateCallExpr(agg.name, substituteCtesInExpr(agg.arg, resolved, allCteNames),
                agg.isStar, agg.line, agg.column, agg.isDistinct, agg.parameter, agg.argCount);
        } else if (e instanceof JsonAccessExpr j) {
            return new JsonAccessExpr(substituteCtesInExpr(j.left, resolved, allCteNames), j.returnText, j.key, j.line, j.column);
        }
        return e;
    }

    private FromItem substituteFromItem(FromItem item, Map<String, SelectQuery> resolved, Set<String> allCteNames) {
        if (item instanceof NamedSource ns) {
            var body = resolved.get(ns.name);
            if (body != null) return new DerivedSource(body, ns.alias, ns.line, ns.column);
            if (allCteNames.contains(ns.name)) {
                throw error(new Token(Token.Kind.IDENTIFIER, ns.name, ns.line, ns.column),
                    "Recursive or forward CTE reference '" + ns.name + "' is not supported — a CTE may only reference CTEs declared earlier in the same WITH list");
            }
            return ns;
        } else if (item instanceof DerivedSource ds) {
            return new DerivedSource(substituteCtes(ds.query, resolved, allCteNames), ds.alias, ds.line, ds.column);
        } else if (item instanceof DerivedSetOperationSource dus) {
            var branches = new ArrayList<SelectQuery>();
            for (var b : dus.setOp.branches) branches.add(substituteCtes(b, resolved, allCteNames));
            return new DerivedSetOperationSource(new SetOperationQuery(dus.setOp.all, branches, dus.setOp.line, dus.setOp.column), dus.alias, dus.line, dus.column);
        }
        return item;
    }

    private Token current() { return tokens.get(pos); }

    private Token advance() {
        var t = tokens.get(pos);
        if (pos < tokens.size() - 1) pos++;
        return t;
    }

    private ParseException error(Token at, String message) { return new ParseException(new SqlDiagnostic(message, at.line, at.column)); }

    /** Set operations inside IN/EXISTS/scalar subqueries are out of v1 scope — positioned diagnostic. */
    private void rejectUnionInSubqueryPosition(String context) {
        if (current().isKeyword("UNION")) {
            throw error(current(), "UNION is not supported inside " + context + " in this dialect — restructure the query without a set operation there");
        }
    }

    private void expectKeyword(String keyword) {
        if (!current().isKeyword(keyword)) throw error(current(), "Expected '" + keyword + "', got '" + current().text + "'");
        advance();
    }

    private boolean matchKeyword(String keyword) {
        if (current().isKeyword(keyword)) { advance(); return true; }
        return false;
    }

    private void expectSymbol(String symbol) {
        if (!current().isSymbol(symbol)) throw error(current(), "Expected '" + symbol + "', got '" + current().text + "'");
        advance();
    }

    private Token expectIdentifierToken() {
        if (current().kind != Token.Kind.IDENTIFIER) throw error(current(), "Expected an identifier, got '" + current().text + "'");
        return advance();
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    private SelectQuery parseSelectQuery() {
        expectKeyword("SELECT");
        var select = parseSelectClause();
        expectKeyword("FROM");
        var from = parseFromItem();
        var joins = new ArrayList<JoinClause>();
        // Plan 002 L2: comma form `FROM src alias, UNNEST(expr) AS l[, ...]` desugars to the JOIN form right here.
        while (current().isSymbol(",")) {
            var commaTok = advance();
            if (!current().isKeyword("UNNEST")) {
                throw error(current(), "Expected UNNEST after ',' in FROM — this dialect's only comma-FROM sugar is 'FROM src, UNNEST(expr) AS alias' (plan 002 L2)");
            }
            var unnest = parseUnnestSource();
            joins.add(new JoinClause(JoinKind.UNNEST, unnest, null, null, commaTok.line, commaTok.column));
        }
        while (isJoinStart()) joins.add(parseJoinClause());
        var fromClause = new FromClause(from, joins);

        Expr where = null;
        if (matchKeyword("WHERE")) where = parseOr();

        List<Expr> latestBy = null;
        Integer latestByLine = null, latestByCol = null;
        if (current().isKeyword("LATEST")) {
            var tok = current();
            latestByLine = tok.line; latestByCol = tok.column;
            advance();
            expectKeyword("BY");
            expectSymbol("(");
            latestBy = new ArrayList<>();
            latestBy.add(parseOr());
            while (current().isSymbol(",")) { advance(); latestBy.add(parseOr()); }
            expectSymbol(")");
        }

        List<Expr> groupBy = null;
        Integer gbLine = null, gbCol = null;
        if (current().isKeyword("GROUP")) {
            var tok = current();
            gbLine = tok.line; gbCol = tok.column;
            advance();
            expectKeyword("BY");
            // DuckDB/Snowflake sugar: `GROUP BY ALL`. "ALL" is contextual, only checked right after GROUP BY.
            if (current().isKeyword("ALL")) {
                var allTok = current();
                advance();
                if (select.isStar) {
                    throw error(allTok, "GROUP BY ALL cannot be used with 'SELECT *' — star expansion happens after grouping is resolved; list the grouping columns explicitly instead of '*'");
                }
                // Reuse the SAME Expr instances as the select items (not clones).
                groupBy = new ArrayList<>();
                for (var i : select.items) if (!Validator.containsAggregate(i.expression)) groupBy.add(i.expression);
                // Empty expansion means every select item is an aggregate — fall back to null (single global group).
                if (groupBy.isEmpty()) groupBy = null;
            } else {
                groupBy = new ArrayList<>();
                groupBy.add(parseOr());
                while (current().isSymbol(",")) { advance(); groupBy.add(parseOr()); }
            }
        }

        WindowSpec window = null;
        Integer windowLine = null, windowCol = null;
        if (current().isKeyword("WINDOW")) {
            var tok = current();
            windowLine = tok.line; windowCol = tok.column;
            advance();
            window = parseWindowSpec();
        }

        EmitMode emit = null;
        Integer emitLine = null, emitCol = null;
        if (current().isKeyword("EMIT")) {
            var tok = current();
            emitLine = tok.line; emitCol = tok.column;
            advance();
            if (matchKeyword("CHANGES")) emit = EmitMode.CHANGES;
            else if (matchKeyword("FINAL")) emit = EmitMode.FINAL;
            else throw error(current(), "Expected CHANGES or FINAL after EMIT, got '" + current().text + "'");
        }

        return new SelectQuery(select, fromClause, where, groupBy, window, emit, emitLine, emitCol, gbLine, gbCol, windowLine, windowCol, latestBy, latestByLine, latestByCol);
    }

    private SelectClause parseSelectClause() {
        if (current().isSymbol("*")) {
            advance();
            return new SelectClause(true, new ArrayList<>());
        }
        var items = new ArrayList<SelectItem>();
        items.add(parseSelectItem());
        while (current().isSymbol(",")) {
            advance();
            items.add(parseSelectItem());
        }
        return new SelectClause(false, items);
    }

    private SelectItem parseSelectItem() {
        // Qualified star `alias.*` — only recognized here (top-level select-item position).
        if (current().kind == Token.Kind.IDENTIFIER && peekIsSymbol(1, ".") && peekIsSymbol(2, "*")) {
            var aliasTok = advance();
            advance(); // '.'
            advance(); // '*'
            var qualifiedStar = new QualifiedStarExpr(aliasTok.text, aliasTok.line, aliasTok.column);
            if (current().isKeyword("AS") || (current().kind == Token.Kind.IDENTIFIER && !CLAUSE_KEYWORDS.contains(current().text))) {
                throw error(current(), "'alias.*' cannot be given an alias");
            }
            return new SelectItem(qualifiedStar, null);
        }

        var expr = parseOr();
        String alias = null;
        if (matchKeyword("AS")) {
            alias = expectIdentifierToken().text;
        } else if (current().kind == Token.Kind.IDENTIFIER && !CLAUSE_KEYWORDS.contains(current().text)) {
            alias = advance().text;
        }
        return new SelectItem(expr, alias);
    }

    private Token peekAt(int offset) {
        int idx = pos + offset;
        return tokens.get(idx < tokens.size() ? idx : tokens.size() - 1);
    }

    private boolean peekIsSymbol(int offset, String symbol) { return peekAt(offset).isSymbol(symbol); }

    private boolean peekIsKeyword(int offset, String keyword) { return peekAt(offset).isKeyword(keyword); }

    private NamedSource parseSourceRef() {
        var nameTok = expectIdentifierToken();
        String alias = nameTok.text;
        if (matchKeyword("AS")) {
            alias = expectIdentifierToken().text;
        } else if (current().kind == Token.Kind.IDENTIFIER && !CLAUSE_KEYWORDS.contains(current().text)) {
            alias = advance().text;
        }
        return new NamedSource(nameTok.text, alias, nameTok.line, nameTok.column);
    }

    /** FROM/JOIN item: a plain named source, or a derived table {@code ( SELECT ... ) alias} (alias mandatory). */
    private FromItem parseFromItem() {
        if (current().isKeyword("UNNEST")) {
            throw error(current(), "UNNEST cannot be the first FROM item — it must follow a real source, e.g. 'FROM src, UNNEST(expr) AS alias' or '... JOIN UNNEST(expr) AS alias'");
        }
        if (current().isSymbol("(")) {
            var openTok = advance();
            var inner = parseSelectOrSetOperation();
            expectSymbol(")");
            String alias = parseMandatoryDerivedAlias();
            return inner.setOp != null
                ? new DerivedSetOperationSource(inner.setOp, alias, openTok.line, openTok.column)
                : new DerivedSource(inner.query, alias, openTok.line, openTok.column);
        }
        return parseSourceRef();
    }

    private String parseMandatoryDerivedAlias() {
        if (matchKeyword("AS")) return expectIdentifierToken().text;
        if (current().kind == Token.Kind.IDENTIFIER && !CLAUSE_KEYWORDS.contains(current().text)) return advance().text;
        throw error(current(), "A derived table (subquery in FROM/JOIN) requires an alias");
    }

    private boolean isJoinStart() {
        var c = current();
        return c.isKeyword("JOIN") || c.isKeyword("INNER") || c.isKeyword("LEFT") || c.isKeyword("RIGHT") || c.isKeyword("FULL") || c.isKeyword("CROSS");
    }

    private UnnestSource parseUnnestSource() {
        var startTok = current();
        expectKeyword("UNNEST");
        expectSymbol("(");
        var expr = parseOr();
        expectSymbol(")");

        String alias;
        if (matchKeyword("AS")) {
            alias = expectIdentifierToken().text;
        } else if (current().kind == Token.Kind.IDENTIFIER && !CLAUSE_KEYWORDS.contains(current().text)) {
            alias = advance().text;
        } else {
            throw error(current(), "UNNEST requires an alias, e.g. UNNEST(expr) AS alias");
        }
        return new UnnestSource(expr, alias, startTok.line, startTok.column);
    }

    private JoinClause parseJoinClause() {
        var startTok = current();
        JoinKind kind;
        boolean isOuterKind = false;
        if (matchKeyword("CROSS")) { expectKeyword("JOIN"); kind = JoinKind.CROSS; }
        else if (matchKeyword("INNER")) { expectKeyword("JOIN"); kind = JoinKind.INNER; }
        else if (matchKeyword("LEFT")) { matchKeyword("OUTER"); expectKeyword("JOIN"); kind = JoinKind.LEFT; isOuterKind = true; }
        else if (matchKeyword("RIGHT")) { matchKeyword("OUTER"); expectKeyword("JOIN"); kind = JoinKind.RIGHT; isOuterKind = true; }
        else if (matchKeyword("FULL")) { matchKeyword("OUTER"); expectKeyword("JOIN"); kind = JoinKind.FULL; isOuterKind = true; }
        else { expectKeyword("JOIN"); kind = JoinKind.INNER; }

        if (current().isKeyword("UNNEST")) {
            if (isOuterKind) {
                throw error(startTok, "UNNEST may only be used with JOIN or CROSS JOIN, not LEFT/RIGHT/FULL — there is no LEFT UNNEST NULL-padding in this dialect");
            }
            var unnestSource = parseUnnestSource();
            return new JoinClause(JoinKind.UNNEST, unnestSource, null, null, startTok.line, startTok.column);
        }

        var source = parseFromItem();

        Duration within = null;
        if (matchKeyword("WITHIN")) within = parseDuration();

        Expr on = null;
        if (kind == JoinKind.CROSS) {
            if (current().isKeyword("ON")) throw error(current(), "CROSS JOIN may not have an ON clause");
        } else {
            expectKeyword("ON");
            on = parseOr();
        }
        return new JoinClause(kind, source, within, on, startTok.line, startTok.column);
    }

    /** TimeSpan.FromX(double) analogue: sub-millisecond precision is kept in nanoseconds. */
    private static Duration ofUnit(double n, double nanosPerUnit) { return Duration.ofNanos(Math.round(n * nanosPerUnit)); }

    private Duration parseDuration() {
        if (current().kind != Token.Kind.NUMBER) throw error(current(), "Expected a duration amount, got '" + current().text + "'");
        var numTok = advance();
        double n = numTok.doubleValue != null ? numTok.doubleValue : (double) numTok.longValue;
        var unitTok = expectIdentifierToken();
        switch (unitTok.text.toUpperCase(Locale.ROOT)) {
            case "MILLISECOND": case "MILLISECONDS": return ofUnit(n, 1e6);
            case "SECOND": case "SECONDS": return ofUnit(n, 1e9);
            case "MINUTE": case "MINUTES": return ofUnit(n, 60e9);
            case "HOUR": case "HOURS": return ofUnit(n, 3600e9);
            default: throw error(unitTok, "Expected a duration unit (MILLISECONDS|SECONDS|MINUTES|HOURS), got '" + unitTok.text + "'");
        }
    }

    private WindowSpec parseWindowSpec() {
        if (matchKeyword("TUMBLING")) {
            expectSymbol("(");
            expectKeyword("SIZE");
            var size = parseDuration();
            expectSymbol(")");
            return new TumblingWindowSpec(size);
        }
        if (matchKeyword("HOPPING")) {
            expectSymbol("(");
            expectKeyword("SIZE");
            var size = parseDuration();
            expectSymbol(",");
            expectKeyword("ADVANCE");
            expectKeyword("BY");
            var advance = parseDuration();
            expectSymbol(")");
            return new HoppingWindowSpec(size, advance);
        }
        if (matchKeyword("SESSION")) {
            expectSymbol("(");
            expectKeyword("GAP");
            var gap = parseDuration();
            expectSymbol(")");
            return new SessionWindowSpec(gap);
        }
        throw error(current(), "Expected TUMBLING, HOPPING, or SESSION, got '" + current().text + "'");
    }

    // ------------------------------------------------------------------
    // Expressions: OR < AND < NOT < comparisons < + - < * / % < unary minus < '->'/'->>' (postfix) < primary
    // ------------------------------------------------------------------

    private Expr parseOr() {
        var left = parseAnd();
        while (current().isKeyword("OR")) {
            var tok = advance();
            var right = parseAnd();
            left = new BinaryExpr("OR", left, right, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseAnd() {
        var left = parseNot();
        while (current().isKeyword("AND")) {
            var tok = advance();
            var right = parseNot();
            left = new BinaryExpr("AND", left, right, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseNot() {
        if (current().isKeyword("NOT")) {
            var tok = advance();
            // 'NOT EXISTS (...)' handled here so ExistsExpr carries its own Negated flag.
            if (current().isKeyword("EXISTS")) {
                advance();
                return parseExistsBody(true, tok.line, tok.column);
            }
            var operand = parseNot();
            return new UnaryExpr("NOT", operand, tok.line, tok.column);
        }
        return parseComparison();
    }

    private ExistsExpr parseExistsBody(boolean negated, int line, int col) {
        expectSymbol("(");
        var subquery = parseSelectQuery();
        rejectUnionInSubqueryPosition("EXISTS (...)");
        expectSymbol(")");
        return new ExistsExpr(subquery, negated, line, col);
    }

    private static final List<String> COMPARISON_OPS = List.of("=", "!=", "<>", "<", "<=", ">", ">=");

    private Expr parseComparison() {
        var left = parseAdditive();

        boolean inNegated = false;
        if (current().isKeyword("NOT") && peekIsKeyword(1, "IN")) {
            inNegated = true;
            advance(); // 'NOT'
        }
        if (current().isKeyword("IN")) {
            var tok = advance();
            expectSymbol("(");
            var subquery = parseSelectQuery();
            rejectUnionInSubqueryPosition("IN (...)");
            expectSymbol(")");
            return new InSubqueryExpr(left, subquery, inNegated, tok.line, tok.column);
        }

        if (current().kind == Token.Kind.SYMBOL && COMPARISON_OPS.contains(current().text)) {
            var tok = advance();
            var right = parseAdditive();
            left = new BinaryExpr(tok.text, left, right, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseAdditive() {
        var left = parseMultiplicative();
        while (current().isSymbol("+") || current().isSymbol("-")) {
            var tok = advance();
            var right = parseMultiplicative();
            left = new BinaryExpr(tok.text, left, right, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseMultiplicative() {
        var left = parseUnary();
        while (current().isSymbol("*") || current().isSymbol("/") || current().isSymbol("%")) {
            var tok = advance();
            var right = parseUnary();
            left = new BinaryExpr(tok.text, left, right, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseUnary() {
        if (current().isSymbol("-")) {
            var tok = advance();
            var operand = parseUnary();
            return new UnaryExpr("-", operand, tok.line, tok.column);
        }
        return parsePostfix();
    }

    // Postgres JSON access — left-associative, chainable, binds tighter than unary minus.
    private Expr parsePostfix() {
        var left = parsePrimary();
        while (current().isSymbol("->") || current().isSymbol("->>")) {
            var tok = advance();
            boolean returnText = tok.text.equals("->>");
            var key = parseJsonKey();
            left = new JsonAccessExpr(left, returnText, key, tok.line, tok.column);
        }
        return left;
    }

    private Expr parseJsonKey() {
        var tok = current();
        if (tok.kind == Token.Kind.STRING) {
            advance();
            return new StringLiteral(tok.stringValue, tok.line, tok.column);
        }
        if (tok.kind == Token.Kind.NUMBER && tok.longValue != null) {
            advance();
            return new NumberLiteral(null, tok.longValue, tok.line, tok.column);
        }
        throw error(tok, "'->'/'->>' right operand must be a string literal (object key) or integer literal (array index), got '" + tok.text + "'");
    }

    private Expr parsePrimary() {
        var tok = current();
        switch (tok.kind) {
            case NUMBER:
                advance();
                return new NumberLiteral(tok.doubleValue, tok.longValue, tok.line, tok.column);
            case STRING:
                advance();
                return new StringLiteral(tok.stringValue, tok.line, tok.column);
            case SYMBOL:
                if (tok.isSymbol("(")) {
                    advance();
                    // Plan 004 N3/N4: '( SELECT ... )' as a value expression — a scalar subquery.
                    if (current().isKeyword("SELECT")) {
                        var subquery = parseSelectQuery();
                        rejectUnionInSubqueryPosition("a scalar subquery");
                        expectSymbol(")");
                        return new ScalarSubqueryExpr(subquery, tok.line, tok.column);
                    }
                    var inner = parseOr();
                    expectSymbol(")");
                    return inner;
                }
                throw error(tok, "Expected an expression, got '" + tok.text + "'");
            case IDENTIFIER:
                return parseIdentifierPrimary();
            default:
                throw error(tok, "Expected an expression, got '" + tok.text + "'");
        }
    }

    private Expr parseIdentifierPrimary() {
        var tok = advance();
        String name = tok.text;

        if (name.equalsIgnoreCase("TRUE")) return new BoolLiteral(true, tok.line, tok.column);
        if (name.equalsIgnoreCase("FALSE")) return new BoolLiteral(false, tok.line, tok.column);
        if (name.equalsIgnoreCase("NULL")) return new NullLiteral(tok.line, tok.column);
        if (name.equalsIgnoreCase("EXISTS")) return parseExistsBody(false, tok.line, tok.column);
        // CAST/CASE only intercepted when the next token proves it.
        if (name.equalsIgnoreCase("CAST") && current().isSymbol("(")) return parseCastBody(tok.line, tok.column);
        if (name.equalsIgnoreCase("CASE") && current().isKeyword("WHEN")) return parseCaseBody(tok.line, tok.column);

        if (current().isSymbol("(")) return parseCall(name, tok.line, tok.column);

        if (current().isSymbol(".")) {
            advance();
            var field = expectIdentifierToken();
            return new QualifiedIdentifier(name, field.text, tok.line, tok.column);
        }
        return new Identifier(name, tok.line, tok.column);
    }

    private Expr parseCall(String name, int line, int col) {
        expectSymbol("(");

        if (name.equalsIgnoreCase("COUNT") && current().isSymbol("*")) {
            advance();
            expectSymbol(")");
            return new AggregateCallExpr(name, null, true, line, col);
        }

        // `agg(DISTINCT x)`. Contextual, checked only right after an aggregate's open paren.
        boolean isDistinct = false;
        if (AggregateNames.isAggregate(name) && current().isKeyword("DISTINCT")) {
            advance();
            isDistinct = true;
        }

        var args = new ArrayList<Expr>();
        if (!current().isSymbol(")")) {
            args.add(parseOr());
            while (current().isSymbol(",")) {
                advance();
                args.add(parseOr());
            }
        }
        expectSymbol(")");

        if (AggregateNames.isAggregate(name)) {
            // A parameterised aggregate is written parameter-first; the VALUE argument still lands in Arg.
            if (StatAggregatorNames.takesParameter(name) && args.size() == 2) {
                return new AggregateCallExpr(name, args.get(1), false, line, col, isDistinct, args.get(0), args.size());
            }
            Expr arg = !args.isEmpty() ? args.get(0) : null;
            return new AggregateCallExpr(name, arg, false, line, col, isDistinct, null, args.size());
        }
        return new FunctionCallExpr(name, args, line, col);
    }

    // ------------------------------------------------------------------
    // Searched CASE sugar -> nested three-argument IF
    // ------------------------------------------------------------------

    private Expr parseCaseBody(int line, int col) {
        var conds = new ArrayList<Expr>();
        var thens = new ArrayList<Expr>();
        while (matchKeyword("WHEN")) {
            var cond = parseOr();
            expectKeyword("THEN");
            conds.add(cond);
            thens.add(parseOr());
        }
        Expr result = matchKeyword("ELSE") ? parseOr() : new NullLiteral(line, col);
        expectKeyword("END");
        for (int i = conds.size() - 1; i >= 0; i--) {
            var cond = conds.get(i);
            var args = new ArrayList<Expr>();
            args.add(cond); args.add(thens.get(i)); args.add(result);
            result = new FunctionCallExpr("IF", args, cond.line, cond.column);
        }
        return result;
    }

    // ------------------------------------------------------------------
    // CAST(expr AS type) sugar
    // ------------------------------------------------------------------

    private Expr parseCastBody(int line, int col) {
        expectSymbol("(");
        var expr = parseOr();
        expectKeyword("AS");
        String fnName = parseCastTargetFunction();
        expectSymbol(")");
        var args = new ArrayList<Expr>();
        args.add(expr);
        return new FunctionCallExpr(fnName, args, line, col);
    }

    private String parseCastTargetFunction() {
        var tok = expectIdentifierToken();
        String typeName = tok.text.toUpperCase(Locale.ROOT);
        if (typeName.equals("DOUBLE") && current().isKeyword("PRECISION")) advance();
        switch (typeName) {
            case "STRING": case "TEXT": return "TO_STRING";
            case "DOUBLE": return "TO_DOUBLE";
            case "LONG": case "BIGINT": case "INT": return "TO_LONG";
            case "BOOL": case "BOOLEAN": return "TO_BOOL";
            case "TIMESTAMP": return "TO_TIMESTAMP";
            default:
                throw error(tok, "Unknown CAST target type '" + tok.text + "' — expected STRING, DOUBLE, LONG, BOOL, or TIMESTAMP (TEXT, BIGINT, INT, BOOLEAN, DOUBLE PRECISION also accepted)");
        }
    }
}
