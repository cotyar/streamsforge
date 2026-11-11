package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Records every update of a collection; mostly for tests and examples. */
public final class Capture<D, T extends Time<T>> {
    private final List<Update<D, T>> updates = new ArrayList<>();
    private final Probe<T> probe;

    @SuppressWarnings({"rawtypes", "unchecked"})
    Capture(Worker w, Output source) {
        SinkOp sink = new SinkOp(w, m -> { for (Update u : m.updates) updates.add(u); });
        source.connect(sink.in);
        this.probe = new Probe<>(w, sink.in);
    }

    public List<Update<D, T>> updates() { return updates; }
    public Probe<T> probe() { return probe; }
    public boolean done(T t) { return probe.done(t); }

    /** The collection's contents at time {@code t}: per record, the sum of diffs at times {@code <= t}; zeros omitted. */
    public Map<D, Long> accumulate(T t) {
        Map<D, Long> m = new HashMap<>();
        for (Update<D, T> u : updates) if (u.time().lessEqual(t)) m.merge(u.data(), u.diff(), Long::sum);
        m.values().removeIf(v -> v == 0);
        return m;
    }
}
