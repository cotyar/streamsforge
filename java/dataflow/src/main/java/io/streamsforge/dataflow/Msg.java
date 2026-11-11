package io.streamsforge.dataflow;

import java.util.List;

/** An immutable batch of updates in flight on an edge. {@code seq > 0} marks an arrangement batch. */
@SuppressWarnings({"rawtypes", "unchecked"})
final class Msg {
    final List<Update> updates;
    final long seq;
    /** Minimal times present in {@link #updates}; this is the message's progress-tracking pointstamp. */
    final Antichain times = new Antichain();

    Msg(List<Update> updates, long seq) {
        this.updates = updates;
        this.seq = seq;
        Object last = null;
        for (Update u : updates) {
            Object t = u.time();
            if (t == last || (last != null && t.equals(last))) continue;
            times.insert((Time) t);
            last = t;
        }
    }
}
