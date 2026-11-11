package io.streamsforge.engine.sql;

import io.streamsforge.engine.FieldKind;
import java.util.List;

/** One scalar SQL function contributed from outside the Engine. Implementations must be <b>total and pure</b>:
 *  never throw, never depend on anything but their arguments (every built-in scalar follows that rule — an
 *  unconvertible value is NULL, because a per-row exception kills the pipeline for every other row too). */
public interface IScalarFunction {
    /** Case-insensitive; may not collide with a built-in (see {@link SqlFunctions}). */
    String name();

    /** Called at validate time. False produces the usual "wrong number of arguments" diagnostic. */
    boolean isValidArity(int argCount);

    /** The result kind given each argument's inferred kind (an entry is null when the Engine could not infer
     *  it). Return null to leave the result kind unknown. */
    FieldKind resultKind(List<FieldKind> argKinds);

    /** Called per row with already-evaluated arguments (arity already checked). Must not throw. */
    Object eval(List<Object> args);
}
