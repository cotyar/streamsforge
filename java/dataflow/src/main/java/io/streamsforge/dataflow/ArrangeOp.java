package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds the shared {@link Trace} and forwards each consolidated batch, tagged with its sequence number. */
@SuppressWarnings({"rawtypes", "unchecked"})
final class ArrangeOp<K, V, T extends Time<T>> extends Op {
    final InPort in = addIn();
    final Output out = addOut();
    final Trace<K, V, T> trace = new Trace<>();

    ArrangeOp(Worker w) { super(w); }

    @Override boolean step() {
        boolean did = false;
        Msg m;
        while ((m = in.queue.poll()) != null) {
            did = true;
            Map<Update, long[]> acc = new HashMap<>();
            for (Update u : m.updates) acc.computeIfAbsent(new Update(u.data(), u.time(), 0), x -> new long[1])[0] += u.diff();
            List<Update> batch = new ArrayList<>(acc.size());
            for (var e : acc.entrySet()) {
                if (e.getValue()[0] != 0) batch.add(new Update(e.getKey().data(), e.getKey().time(), e.getValue()[0]));
            }
            if (batch.isEmpty()) continue;
            long seq = trace.add((List) batch);
            out.send(batch, seq);
        }
        return did;
    }

    @Override long stateSize() { return trace.size(); }
}
