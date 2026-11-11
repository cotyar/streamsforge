package io.streamsforge.actors;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * A virtual-actor runtime: the Orleans-silo shape, pluggable over CompletableFuture mailboxes,
 * JDK 21 virtual threads, or Apache Pekko. Everything above this interface (the SQL streaming host)
 * is written once and runs on any of the three.
 */
public interface ActorRuntime extends AutoCloseable {
    /** "futures", "loom" or "pekko" — for logs and parameterized tests. */
    String name();

    /** Register an actor type. Must be called before the first {@link #ref} for that type; re-registering
     *  a name throws. */
    <S> void register(ActorType<S> type);

    /** Get a reference by key. Never activates by itself; the first message does. Unknown type throws. */
    <S> ActorRef<S> ref(ActorKey key);

    default <S> ActorRef<S> ref(String type, String id) {
        return ref(new ActorKey(type, id));
    }

    /** Keys currently ACTIVE (in memory). A diagnostic snapshot — racy by nature. */
    Set<ActorKey> activeKeys();

    /** Deactivate one activation after its in-flight turn completes (runs {@code onDeactivate}). No-op if
     *  not active. Completes when the activation is gone. */
    CompletionStage<Void> deactivate(ActorKey key);

    /** Where failures of {@code tell} turns and timer ticks go. Default: print to stderr. */
    void onUnhandledError(Consumer<Throwable> handler);

    /** Deactivates every activation (running {@code onDeactivate}) and stops the runtime. Idempotent. */
    @Override
    void close();
}
