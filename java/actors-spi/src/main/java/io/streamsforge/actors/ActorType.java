package io.streamsforge.actors;

import java.time.Duration;
import java.util.function.Function;

/**
 * Registration of one actor type with a runtime: how to create a fresh state object for a key on
 * activation, and how long an activation may sit idle before the runtime deactivates it
 * (Orleans' activation collection). {@code idleTimeout == null} means "never collect".
 *
 * <p>If the state object implements {@link ActorLifecycle}, the runtime calls its hooks as
 * ordinary turns: {@code onActivate} is the first turn an activation ever runs, {@code onDeactivate}
 * the last.
 */
public record ActorType<S>(String name, Function<ActorKey, S> stateFactory, Duration idleTimeout) {
    public static <S> ActorType<S> of(String name, Function<ActorKey, S> stateFactory) {
        return new ActorType<>(name, stateFactory, null);
    }

    public ActorType<S> withIdleTimeout(Duration timeout) {
        return new ActorType<>(name, stateFactory, timeout);
    }
}
