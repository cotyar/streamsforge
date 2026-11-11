package io.streamsforge.host;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A subscriber list that lives INSIDE an actor's state (only touched by that actor's turns). Delivery is a
 * synchronous call of each consumer on the publisher's turn; the consumers the host installs between actors
 * only enqueue a message to the subscriber's mailbox and return, so a publisher never waits for (and never
 * deadlocks with) a subscriber — the in-process analogue of an Orleans stream.
 */
final class Publisher<T> {
    private final HostEnv env;
    private final Map<Long, Consumer<T>> subs = new LinkedHashMap<>();
    private List<Consumer<T>> snapshot = List.of();
    private long next = 1;

    Publisher(HostEnv env) { this.env = env; }

    long subscribe(Consumer<T> c) {
        long id = next++;
        subs.put(id, c);
        snapshot = new ArrayList<>(subs.values());
        return id;
    }

    boolean unsubscribe(long id) {
        boolean removed = subs.remove(id) != null;
        if (removed) snapshot = new ArrayList<>(subs.values());
        return removed;
    }

    void clear() {
        subs.clear();
        snapshot = List.of();
    }

    void publish(T value) {
        for (var c : snapshot) {
            try {
                c.accept(value);
            } catch (Throwable t) {
                env.errors.add(t);
            }
        }
    }
}
