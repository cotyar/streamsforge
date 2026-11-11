package io.streamsforge.engine.runtime;

import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.sql.Ast.*;
import io.streamsforge.engine.sql.SqlFunctions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntPredicate;
import java.util.function.LongBinaryOperator;

/** Tree-walking interpreter for the expression AST. Implements SQL-style three-valued NULL logic:
 *  NULL propagates through arithmetic/comparisons; AND/OR use Kleene logic; WHERE requires a literal {@code true}. */
public final class ExpressionEvaluator {
    private ExpressionEvaluator() {}

    /** A bound column reference: the alias and field a resolved Identifier/QualifiedIdentifier reads. */
    public record Binding(String alias, String field) {}

    /** Resolves an already-bound AggregateCallExpr node to its aggregator's current result. */
    @FunctionalInterface
    public interface AggregateLookup { Object lookup(AggregateCallExpr node); }

    /** Evaluation context: row + resolved-column bindings (keyed by Expr reference identity) + aggregate lookup. */
    public record EvalContext(WorkingRow row, Map<Expr, Binding> bindings, AggregateLookup aggregates) {
        public EvalContext(WorkingRow row, Map<Expr, Binding> bindings) { this(row, bindings, null); }
    }

    public static Object eval(Expr expr, EvalContext ctx) {
        if (expr instanceof NumberLiteral n) return n.value();
        if (expr instanceof StringLiteral s) return s.value;
        if (expr instanceof BoolLiteral b) return b.value;
        if (expr instanceof NullLiteral) return null;
        if (expr instanceof StarExpr) return null;
        if (expr instanceof Identifier || expr instanceof QualifiedIdentifier) return evalColumn(expr, ctx);
        if (expr instanceof UnaryExpr u) return evalUnary(u, ctx);
        if (expr instanceof BinaryExpr b) return evalBinary(b, ctx);
        if (expr instanceof FunctionCallExpr f) return evalFunction(f, ctx);
        if (expr instanceof AggregateCallExpr agg) return ctx.aggregates() == null ? null : ctx.aggregates().lookup(agg);
        if (expr instanceof JsonAccessExpr j) return evalJsonAccess(j, ctx);
        return null;
    }

    /** WHERE/ON residual semantics: only a literal {@code true} passes; NULL and {@code false} both filter the row out. */
    public static boolean isTrue(Object value) { return Boolean.TRUE.equals(value); }

    private static Object evalColumn(Expr id, EvalContext ctx) {
        var b = ctx.bindings().get(id);
        if (b == null) return null;
        String key = b.alias() + "_" + b.field();
        return ctx.row().fields().get(key);
    }

    private static Object evalUnary(UnaryExpr u, EvalContext ctx) {
        if (u.op.equals("NOT")) {
            Object v = eval(u.operand, ctx);
            return v instanceof Boolean bv ? (Object) !bv : null;
        }
        Object val = eval(u.operand, ctx);
        if (val instanceof Long l) return -l;
        if (val instanceof Double d) return -d;
        return null;
    }

    private static Object evalBinary(BinaryExpr b, EvalContext ctx) {
        if (b.op.equals("AND")) return evalAnd(b, ctx);
        if (b.op.equals("OR")) return evalOr(b, ctx);

        Object l = eval(b.left, ctx);
        Object r = eval(b.right, ctx);
        switch (b.op) {
            case "+": return arith(l, r, (x, y) -> x + y, (x, y) -> x + y);
            case "-": return arith(l, r, (x, y) -> x - y, (x, y) -> x - y);
            case "*": return arith(l, r, (x, y) -> x * y, (x, y) -> x * y);
            case "/": return divide(l, r);
            case "%": return mod(l, r);
            case "=": return eq(l, r);
            case "!=": case "<>": return negateBool(eq(l, r));
            case "<": return compareOp(l, r, c -> c < 0);
            case "<=": return compareOp(l, r, c -> c <= 0);
            case ">": return compareOp(l, r, c -> c > 0);
            case ">=": return compareOp(l, r, c -> c >= 0);
            default: return null;
        }
    }

    private static Object evalAnd(BinaryExpr b, EvalContext ctx) {
        Object lo = eval(b.left, ctx);
        Boolean l = lo instanceof Boolean x ? x : null;
        if (l != null && !l) return false;
        Object ro = eval(b.right, ctx);
        Boolean r = ro instanceof Boolean y ? y : null;
        if (r != null && !r) return false;
        return l == null || r == null ? null : (Object) true;
    }

    private static Object evalOr(BinaryExpr b, EvalContext ctx) {
        Object lo = eval(b.left, ctx);
        Boolean l = lo instanceof Boolean x ? x : null;
        if (l != null && l) return true;
        Object ro = eval(b.right, ctx);
        Boolean r = ro instanceof Boolean y ? y : null;
        if (r != null && r) return true;
        return l == null || r == null ? null : (Object) false;
    }

    private static Object arith(Object l, Object r, LongBinaryOperator longOp, DoubleBinaryOperator doubleOp) {
        if (l == null || r == null) return null;
        if (l instanceof Long ll && r instanceof Long rl) return longOp.applyAsLong(ll, rl);
        if (SqlValues.isNumber(l) && SqlValues.isNumber(r)) return doubleOp.applyAsDouble(SqlValues.toDouble(l), SqlValues.toDouble(r));
        return null;
    }

    // Division always promotes to double, even for long/long, to avoid silent integer truncation.
    private static Object divide(Object l, Object r) {
        if (l == null || r == null || !SqlValues.isNumber(l) || !SqlValues.isNumber(r)) return null;
        double rd = SqlValues.toDouble(r);
        return rd == 0 ? null : (Object) (SqlValues.toDouble(l) / rd);
    }

    private static Object mod(Object l, Object r) {
        if (l == null || r == null) return null;
        if (l instanceof Long ll && r instanceof Long rl) {
            if (rl == 0) return null;
            if (rl == -1) { if (ll == Long.MIN_VALUE) throw new ArithmeticException("Arithmetic operation resulted in an overflow."); return 0L; }
            return ll % rl;
        }
        if (SqlValues.isNumber(l) && SqlValues.isNumber(r)) {
            double rd = SqlValues.toDouble(r);
            return rd == 0 ? null : (Object) (SqlValues.toDouble(l) % rd);
        }
        return null;
    }

    private static Object eq(Object l, Object r) {
        if (l == null || r == null) return null;
        if (SqlValues.isNumber(l) && SqlValues.isNumber(r)) return SqlValues.toDouble(l) == SqlValues.toDouble(r);
        if (l instanceof String ls && r instanceof String rs) return ls.equals(rs);
        if (l instanceof Boolean lb && r instanceof Boolean rb) return lb.booleanValue() == rb.booleanValue();
        return false;
    }

    private static Object negateBool(Object v) { return v == null ? null : (Object) !(Boolean) v; }

    private static Object compareOp(Object l, Object r, IntPredicate pred) {
        if (l == null || r == null) return null;
        boolean sameKind = (SqlValues.isNumber(l) && SqlValues.isNumber(r))
            || (l instanceof String && r instanceof String)
            || (l instanceof Boolean && r instanceof Boolean);
        if (!sameKind) return null;
        return pred.test(SqlValues.compare(l, r));
    }

    // ------------------------------------------------------------------
    // Postgres JSON access: '->' (returns the JSON node) and '->>' (TEXT). NULL propagates.
    // ------------------------------------------------------------------

    private static Object evalJsonAccess(JsonAccessExpr j, EvalContext ctx) {
        Object left = eval(j.left, ctx);
        Object node = accessJsonNode(left, j.key);
        return j.returnText ? stringifyJson(node) : node;
    }

    /** {@code left -> key}: object field for a string key (NULL if not a dict or key missing), 0-based element for
     *  an integer key (NULL if not a list or out of range). */
    private static Object accessJsonNode(Object left, Expr key) {
        if (key instanceof StringLiteral sl) {
            return left instanceof Map<?, ?> dict ? dict.get(sl.value) : null;
        }
        if (key instanceof NumberLiteral nl && nl.longValue != null) {
            long idx = nl.longValue;
            return left instanceof List<?> list && idx >= 0 && idx < list.size() ? list.get((int) idx) : null;
        }
        return null;
    }

    private static Object stringifyJson(Object node) {
        if (node == null) return null;
        if (node instanceof String s) return s;
        if (node instanceof Boolean b) return b ? "true" : "false";
        if (node instanceof Long l) return Long.toString(l);
        if (node instanceof Double d) return Invariant.toString(d);
        if (node instanceof Map<?, ?> || node instanceof List<?>) return JsonText.serialize(node);
        return node.toString();
    }

    private static Object evalFunction(FunctionCallExpr f, EvalContext ctx) {
        // IF — and therefore every searched CASE — must not evaluate all of its arguments up front.
        if (f.args.size() == 3 && f.name.equalsIgnoreCase("IF")) {
            return eval(isTrue(eval(f.args.get(0), ctx)) ? f.args.get(1) : f.args.get(2), ctx);
        }

        var args = new ArrayList<Object>(f.args.size());
        for (var a : f.args) args.add(eval(a, ctx));
        switch (f.name.toUpperCase(Locale.ROOT)) {
            case "ABS": return !args.isEmpty() ? abs(args.get(0)) : null;
            case "ROUND": return round(args);
            case "UPPER": return !args.isEmpty() && args.get(0) instanceof String su ? mapCase(su, true) : null;
            case "LOWER": return !args.isEmpty() && args.get(0) instanceof String sl ? mapCase(sl, false) : null;
            case "COALESCE": {
                for (var v : args) if (v != null) return v;
                return null;
            }
            case "TO_LONG": return coerce(FieldKind.LONG, args);
            case "TO_DOUBLE": return coerce(FieldKind.DOUBLE, args);
            case "TO_BOOL": return coerce(FieldKind.BOOL, args);
            case "TO_TIMESTAMP": {
                if (!args.isEmpty() && args.get(0) != null) {
                    var c = FieldValueConversion.tryToTimestamp(args.get(0));
                    if (c.ok()) return c.value();
                }
                return null;
            }
            case "TO_STRING": return !args.isEmpty() ? evalToString(f, args.get(0)) : null;
            default: {
                // A registered scalar (SqlFunctions). Consulted last, and never for a built-in's name.
                var fn = SqlFunctions.findScalar(f.name);
                return fn == null ? null : fn.eval(args);
            }
        }
    }

    private static Object coerce(FieldKind kind, List<Object> args) {
        if (!args.isEmpty() && args.get(0) != null) {
            var c = FieldValueConversion.tryCoerce(kind, args.get(0));
            if (c.ok()) return c.value();
        }
        return null;
    }

    /** ToUpperInvariant/ToLowerInvariant: simple per-code-point case mapping (no ß→SS expansion). */
    private static String mapCase(String s, boolean upper) {
        var sb = new StringBuilder(s.length());
        s.codePoints().forEach(cp -> sb.appendCodePoint(upper ? Character.toUpperCase(cp) : Character.toLowerCase(cp)));
        return sb.toString();
    }

    /** TO_STRING: culture-invariant always. ISO-8601 rendering only fires when the argument is SYNTACTICALLY a
     *  TO_TIMESTAMP(...) call (a Timestamp column is a bare long at runtime) — a documented limitation. */
    private static Object evalToString(FunctionCallExpr f, Object value) {
        if (value == null) return null;
        if (value instanceof Map<?, ?> || value instanceof List<?>) return JsonText.serialize(value);

        if (value instanceof Long epochMs && !f.args.isEmpty()
            && f.args.get(0) instanceof FunctionCallExpr inner && inner.name.equalsIgnoreCase("TO_TIMESTAMP")) {
            return FieldValueConversion.formatEpochMsIso8601(epochMs);
        }
        var c = FieldValueConversion.tryCoerce(FieldKind.STRING, value);
        return c.ok() ? c.value() : null;
    }

    private static Object abs(Object v) {
        if (v instanceof Long l) {
            if (l == Long.MIN_VALUE) throw new ArithmeticException("Negating the minimum value of a twos complement number is invalid.");
            return Math.abs(l);
        }
        if (v instanceof Double d) return Math.abs(d);
        return null;
    }

    private static final double[] ROUND_POW10 = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15};

    private static Object round(List<Object> args) {
        if (args.isEmpty() || args.get(0) == null || !SqlValues.isNumber(args.get(0))) return null;
        int digits = 0;
        if (args.size() > 1) {
            Object a1 = args.get(1);
            digits = a1 instanceof Long dl ? (int) dl.longValue() : a1 instanceof Double dd ? (int) dd.doubleValue() : 0;
        }
        return roundAwayFromZero(SqlValues.toDouble(args.get(0)), Math.max(0, digits));
    }

    /** {@code Math.Round(value, digits, MidpointRounding.AwayFromZero)} as .NET implements it (scale, ModF,
     *  fix-up, unscale; values with |x| ≥ 1e16 are returned unchanged; digits must be 0..15). */
    public static double roundAwayFromZero(double value, int digits) {
        if (digits < 0 || digits > 15) throw new IllegalArgumentException("Specified argument was out of the range of valid values. (Parameter 'digits')");
        if (Math.abs(value) < 1e16) {
            double power10 = ROUND_POW10[digits];
            value *= power10;
            double intPart = value < 0 ? Math.ceil(value) : Math.floor(value); // ModF
            double fraction = value - intPart;
            value = intPart;
            if (Math.abs(fraction) >= 0.5) value += Math.signum(fraction);
            value /= power10;
        }
        return value;
    }
}
