package io.streamsforge.actors;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A handle to a virtual actor. Holding one does not keep anything alive; sending to it activates
 * the actor if it is not active (Orleans semantics: actors always exist, activations come and go).
 */
public interface ActorRef<S> {
    ActorKey key();

    /** Run an (async) turn and get its result. Failures (thrown or a failed stage) fail the returned stage
     *  and do NOT kill the activation — the next message runs normally. */
    <R> CompletionStage<R> ask(Turn<S, R> turn);

    /** Convenience: synchronous turn. */
    default <R> CompletionStage<R> call(Function<S, R> fn) {
        return ask(ctx -> Turns.completed(fn.apply(ctx.state())));
    }

    /** Fire-and-forget synchronous turn; ordering relative to other tell/ask from the SAME caller thread
     *  is preserved (FIFO per sender). Failures are reported to the runtime's error handler. */
    void tell(Consumer<S> fn);
}
