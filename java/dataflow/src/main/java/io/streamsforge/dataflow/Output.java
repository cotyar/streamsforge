package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;

/** An operator output: fans a message out to the queues of every consumer. */
@SuppressWarnings({"rawtypes"})
final class Output {
    final List<InPort> consumers = new ArrayList<>();

    void connect(InPort p) { consumers.add(p); }

    void send(List<Update> updates) { send(updates, 0); }

    void send(List<Update> updates, long seq) {
        if (updates.isEmpty()) return;
        Msg m = new Msg(updates, seq);
        for (InPort c : consumers) c.queue.add(m);
    }
}
