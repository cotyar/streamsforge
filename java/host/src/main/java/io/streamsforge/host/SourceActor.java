package io.streamsforge.host;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.SourceSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * One activation per source name (Orleans: the source's stream + connector/ingest driver). Owns the subscriber
 * list; {@link #publish} stamps {@code _source} and fans the batch out to every consumer in ONE turn, so two
 * concurrent publishers are serialized here and every subscriber sees the same order. No retention: a consumer
 * only sees what is published after it subscribed (the original's replay log is out of scope).
 */
final class SourceActor implements RowPublisher {
    private final String name;
    private final Publisher<List<EventRecord>> subs;
    private SourceSchema schema;
    private boolean accepting = true;

    SourceActor(HostEnv env, String name) {
        this.name = name;
        this.subs = new Publisher<>(env);
    }

    void init(SourceSchema s) {
        schema = s;
        accepting = true;
    }

    void reset() {
        schema = null;
        subs.clear();
    }

    void setAccepting(boolean accepting) { this.accepting = accepting; }

    void publish(List<EventRecord> rows) {
        if (schema == null) throw new CatalogException("unknown source '" + name + "'");
        if (!accepting) throw new IllegalStateException("source '" + name + "' is stopped");
        if (rows.isEmpty()) return;
        var out = new ArrayList<EventRecord>(rows.size());
        for (var r : rows) {
            if (name.equals(r.get(EventRecord.SOURCE_FIELD))) {
                out.add(r);
            } else {
                var c = new EventRecord(r);
                c.put(EventRecord.SOURCE_FIELD, name);
                out.add(c);
            }
        }
        subs.publish(Collections.unmodifiableList(out));
    }

    @Override
    public long subscribeRows(Consumer<List<EventRecord>> consumer) {
        if (schema == null) throw new CatalogException("unknown source '" + name + "'");
        return subs.subscribe(consumer);
    }

    @Override
    public boolean unsubscribeRows(long id) { return subs.unsubscribe(id); }
}
