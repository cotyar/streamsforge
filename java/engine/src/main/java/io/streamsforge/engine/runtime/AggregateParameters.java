package io.streamsforge.engine.runtime;

import io.streamsforge.engine.sql.Ast;

/** Reads a parameterised aggregate's constant out of the parsed call. The Validator has already rejected a
 *  missing or non-constant parameter by the time an executor is built, so the fallback is unreachable in a
 *  compiled plan — it exists so this is total rather than throwing in the middle of a row. */
public final class AggregateParameters {
    private AggregateParameters() {}

    public static double probability(Ast.AggregateCallExpr node) {
        return node.parameter instanceof Ast.NumberLiteral n && SqlValues.isNumber(n.value()) ? SqlValues.toDouble(n.value()) : 0.5;
    }

    /** The Validator's own check, kept next to the reader so the two cannot disagree. */
    public static boolean isUsableProbability(Ast.Expr parameter) {
        return parameter instanceof Ast.NumberLiteral n && SqlValues.isNumber(n.value())
            && SqlValues.toDouble(n.value()) >= 0 && SqlValues.toDouble(n.value()) <= 1;
    }
}
