package io.streamsforge.engine.sql;

import io.streamsforge.engine.runtime.StatAggregatorNames;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * AST — produced by {@link Parser}, consumed by Validator/Planner. The C# file declared ~30 top-level
 * {@code internal} types; here they are nested static classes of this holder (import
 * {@code io.streamsforge.engine.sql.Ast.*}). Like their C# originals they are plain classes with
 * <b>reference identity</b> (Validator's structural-equality and GROUP BY ALL rely on shared
 * instances), exposing immutable public fields instead of properties.
 */
public final class Ast {
    private Ast() {}

    public abstract static class Expr {
        public final int line;
        public final int column;
        protected Expr(int line, int column) { this.line = line; this.column = column; }
    }

    public static final class NumberLiteral extends Expr {
        public final Double doubleValue;
        public final Long longValue;
        public NumberLiteral(Double doubleValue, Long longValue, int line, int col) {
            super(line, col); this.doubleValue = doubleValue; this.longValue = longValue;
        }
        public boolean isDouble() { return doubleValue != null; }
        /** A {@link Double} or a {@link Long} (never promoted — see the C# note on ternary numeric promotion). */
        public Object value() { return isDouble() ? (Object) doubleValue : (Object) longValue; }
    }

    public static final class StringLiteral extends Expr {
        public final String value;
        public StringLiteral(String value, int line, int col) { super(line, col); this.value = value; }
    }

    public static final class BoolLiteral extends Expr {
        public final boolean value;
        public BoolLiteral(boolean value, int line, int col) { super(line, col); this.value = value; }
    }

    public static final class NullLiteral extends Expr {
        public NullLiteral(int line, int col) { super(line, col); }
    }

    public static final class Identifier extends Expr {
        public final String name;
        public Identifier(String name, int line, int col) { super(line, col); this.name = name; }
    }

    public static final class QualifiedIdentifier extends Expr {
        public final String qualifier;
        public final String name;
        public QualifiedIdentifier(String qualifier, String name, int line, int col) {
            super(line, col); this.qualifier = qualifier; this.name = name;
        }
    }

    public static final class StarExpr extends Expr {
        public StarExpr(int line, int col) { super(line, col); }
    }

    /** Postgres-style qualified star in a SELECT list: {@code alias.*} — expands to every column of that
     *  alias's FROM/JOIN input. Only meaningful as a top-level select-item expression. */
    public static final class QualifiedStarExpr extends Expr {
        public final String alias;
        public QualifiedStarExpr(String alias, int line, int col) { super(line, col); this.alias = alias; }
    }

    public static final class UnaryExpr extends Expr {
        public final String op;
        public final Expr operand;
        public UnaryExpr(String op, Expr operand, int line, int col) { super(line, col); this.op = op; this.operand = operand; }
    }

    public static final class BinaryExpr extends Expr {
        public final String op;
        public final Expr left;
        public final Expr right;
        public BinaryExpr(String op, Expr left, Expr right, int line, int col) {
            super(line, col); this.op = op; this.left = left; this.right = right;
        }
    }

    /** Postgres-style JSON access: {@code left -> key} (returns the JSON value) or {@code left ->> key}
     *  (same access, returns TEXT). {@code key} is always a literal (StringLiteral or NumberLiteral with a long). */
    public static final class JsonAccessExpr extends Expr {
        public final Expr left;
        /** false = {@code ->} (dict/list/primitive or NULL), true = {@code ->>} (TEXT or NULL). */
        public final boolean returnText;
        public final Expr key;
        public JsonAccessExpr(Expr left, boolean returnText, Expr key, int line, int col) {
            super(line, col); this.left = left; this.returnText = returnText; this.key = key;
        }
    }

    public static final class FunctionCallExpr extends Expr {
        public final String name;
        public final List<Expr> args;
        public FunctionCallExpr(String name, List<Expr> args, int line, int col) {
            super(line, col); this.name = name; this.args = args;
        }
    }

    public static final class AggregateNames {
        private AggregateNames() {}
        public static final String[] ALL;
        static {
            var all = new ArrayList<String>(List.of("COUNT", "SUM", "AVG", "MIN", "MAX"));
            all.addAll(StatAggregatorNames.all());
            ALL = all.toArray(new String[0]);
        }

        /** Built-ins first, then SqlFunctions' registered aggregates — this is what makes the parser build an
         *  AggregateCallExpr (rather than a plain function call) for a name the Engine has never heard of. */
        public static boolean isAggregate(String name) {
            for (String a : ALL) if (a.equalsIgnoreCase(name)) return true;
            return SqlFunctions.findAggregate(name) != null;
        }
    }

    public static final class AggregateCallExpr extends Expr {
        public final String name;
        /** The expression being aggregated. For a parameterised aggregate this is still the VALUE argument. */
        public final Expr arg;
        public final boolean isStar;
        /** {@code COUNT(DISTINCT x)}. Only legal where the Validator says so. */
        public final boolean isDistinct;
        /** The literal constant configuring a parameterised aggregate (the p of PERCENTILE_CONT(p, x)). */
        public final Expr parameter;
        /** How many arguments were actually written (the node only keeps the ones it has a slot for). */
        public final int argCount;

        public AggregateCallExpr(String name, Expr arg, boolean isStar, int line, int col) {
            this(name, arg, isStar, line, col, false, null, 1);
        }
        public AggregateCallExpr(String name, Expr arg, boolean isStar, int line, int col, boolean isDistinct, Expr parameter, int argCount) {
            super(line, col);
            this.name = name.toUpperCase(java.util.Locale.ROOT);
            this.arg = arg; this.isStar = isStar; this.isDistinct = isDistinct; this.parameter = parameter; this.argCount = argCount;
        }
    }

    /** Plan 004 N2: {@code expr [NOT] IN ( SELECT ... )}. WHERE top-level AND-conjunct only; uncorrelated. */
    public static final class InSubqueryExpr extends Expr {
        public final Expr left;
        public final SelectQuery subquery;
        public final boolean negated;
        public InSubqueryExpr(Expr left, SelectQuery subquery, boolean negated, int line, int col) {
            super(line, col); this.left = left; this.subquery = subquery; this.negated = negated;
        }
    }

    /** Plan 004 N2: {@code [NOT] EXISTS ( SELECT ... )}. */
    public static final class ExistsExpr extends Expr {
        public final SelectQuery subquery;
        public final boolean negated;
        public ExistsExpr(SelectQuery subquery, boolean negated, int line, int col) {
            super(line, col); this.subquery = subquery; this.negated = negated;
        }
    }

    /** Plan 004 N3/N4: {@code ( SELECT agg(...) FROM ... )} used as a value expression. */
    public static final class ScalarSubqueryExpr extends Expr {
        public final SelectQuery query;
        public ScalarSubqueryExpr(SelectQuery query, int line, int col) { super(line, col); this.query = query; }
    }

    public static final class SelectItem {
        public final Expr expression;
        public final String alias;
        public SelectItem(Expr expression, String alias) { this.expression = expression; this.alias = alias; }
    }

    public static final class SelectClause {
        public final boolean isStar;
        public final List<SelectItem> items;
        public SelectClause(boolean isStar, List<SelectItem> items) { this.isStar = isStar; this.items = items; }
    }

    /** One FROM/JOIN item — a named source, a derived table, an UNNEST, or a derived set operation. */
    public abstract static class FromItem {
        public final String alias;
        public final int line;
        public final int column;
        protected FromItem(String alias, int line, int col) { this.alias = alias; this.line = line; this.column = col; }
    }

    public static final class NamedSource extends FromItem {
        public final String name;
        public NamedSource(String name, String alias, int line, int col) { super(alias, line, col); this.name = name; }
    }

    /** A parenthesized derived table {@code ( SELECT ... ) alias}, or a WITH-list CTE reference already
     *  substituted in place by the parser. */
    public static final class DerivedSource extends FromItem {
        public final SelectQuery query;
        public DerivedSource(SelectQuery query, String alias, int line, int col) { super(alias, line, col); this.query = query; }
    }

    /** Plan 002 L2: {@code UNNEST(expr) AS alias}; wrapped by a JoinClause of JoinKind.UNNEST. */
    public static final class UnnestSource extends FromItem {
        public final Expr expr;
        public UnnestSource(Expr expr, String alias, int line, int col) { super(alias, line, col); this.expr = expr; }
    }

    /** SEMI/ANTI/SCALAR are never produced by the parser — Planner synthesizes them. */
    public enum JoinKind { INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI, SCALAR, UNNEST }

    public static final class JoinClause {
        public final JoinKind kind;
        public final FromItem source;
        public final Duration within;
        public final Expr on;
        public final int line;
        public final int column;
        public JoinClause(JoinKind kind, FromItem source, Duration within, Expr on, int line, int col) {
            this.kind = kind; this.source = source; this.within = within; this.on = on; this.line = line; this.column = col;
        }
    }

    public static final class FromClause {
        public final FromItem source;
        public final List<JoinClause> joins;
        public FromClause(FromItem source, List<JoinClause> joins) { this.source = source; this.joins = joins; }
    }

    public abstract static class WindowSpec {}

    public static final class TumblingWindowSpec extends WindowSpec {
        public final Duration size;
        public TumblingWindowSpec(Duration size) { this.size = size; }
    }

    public static final class HoppingWindowSpec extends WindowSpec {
        public final Duration size;
        public final Duration advance;
        public HoppingWindowSpec(Duration size, Duration advance) { this.size = size; this.advance = advance; }
    }

    public static final class SessionWindowSpec extends WindowSpec {
        public final Duration gap;
        public SessionWindowSpec(Duration gap) { this.gap = gap; }
    }

    public enum EmitMode { FINAL, CHANGES }

    /** Plan 008 W3: {@code SELECT ... UNION [ALL] SELECT ...} flattened into one node. */
    public static final class SetOperationQuery {
        public final boolean all;
        public final List<SelectQuery> branches;
        public final int line;
        public final int column;
        public SetOperationQuery(boolean all, List<SelectQuery> branches, int line, int col) {
            this.all = all; this.branches = branches; this.line = line; this.column = col;
        }
    }

    /** Plan 008 W3: derived-table-position set operation. */
    public static final class DerivedSetOperationSource extends FromItem {
        public final SetOperationQuery setOp;
        public DerivedSetOperationSource(SetOperationQuery setOp, String alias, int line, int col) {
            super(alias, line, col); this.setOp = setOp;
        }
    }

    public static final class SelectQuery {
        public final SelectClause select;
        public final FromClause from;
        public final Expr where;
        public final List<Expr> groupBy;
        public final WindowSpec window;
        public final EmitMode emit;
        public final Integer emitLine;
        public final Integer emitColumn;
        public final Integer groupByLine;
        public final Integer groupByColumn;
        public final Integer windowLine;
        public final Integer windowColumn;
        /** Plan 002 L3: {@code LATEST BY (col[, col...])} — table-mode-only. Null when absent. */
        public final List<Expr> latestBy;
        public final Integer latestByLine;
        public final Integer latestByColumn;

        public SelectQuery(SelectClause select, FromClause from, Expr where, List<Expr> groupBy, WindowSpec window,
                           EmitMode emit, Integer emitLine, Integer emitColumn, Integer groupByLine, Integer groupByColumn,
                           Integer windowLine, Integer windowColumn, List<Expr> latestBy, Integer latestByLine, Integer latestByColumn) {
            this.select = select; this.from = from; this.where = where; this.groupBy = groupBy; this.window = window;
            this.emit = emit; this.emitLine = emitLine; this.emitColumn = emitColumn;
            this.groupByLine = groupByLine; this.groupByColumn = groupByColumn;
            this.windowLine = windowLine; this.windowColumn = windowColumn;
            this.latestBy = latestBy; this.latestByLine = latestByLine; this.latestByColumn = latestByColumn;
        }
    }
}
