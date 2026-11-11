package io.streamsforge.engine.planning;

import io.streamsforge.engine.sql.Ast.Expr;

/** One SELECT output column: either a direct substitution of a stored GROUP BY value (groupByIndex set, used
 *  for windowed/grouped queries) or a general expression to evaluate. */
public final class OutputItem {
    public String name;
    public Expr expression;
    public Integer groupByIndex;

    public OutputItem(String name, Expr expression) { this.name = name; this.expression = expression; }

    public OutputItem(String name, Expr expression, Integer groupByIndex) {
        this.name = name; this.expression = expression; this.groupByIndex = groupByIndex;
    }
}
