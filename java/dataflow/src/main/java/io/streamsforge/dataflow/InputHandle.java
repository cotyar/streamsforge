package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The external source of a collection. The handle owns a <em>capability</em>, an {@link Antichain} of times at or
 * beyond which it may still send updates. {@link #advanceTo} promises no more updates before the new frontier,
 * which is what lets downstream operators finish earlier times; {@link #close} gives up the capability entirely.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class InputHandle<D, T extends Time<T>> extends Op {
    private final Output out = addOut();
    private final Antichain<T> cap = new Antichain<>();
    private List<Update> buf = new ArrayList<>();
    private boolean closed;

    InputHandle(Worker w, T initial) {
        super(w);
        cap.insert(initial);
    }

    public Collection<D, T> collection() { return new Collection<>(worker, out); }

    public Antichain<T> capability() { return cap.copy(); }

    /** The single current time; fails if the capability is a multi-element antichain. */
    public T time() {
        if (cap.size() != 1) throw new IllegalStateException("capability is " + cap + ", not a single time");
        return cap.elements().get(0);
    }

    /** Sends an update at an explicit time, which must be at or beyond the capability. */
    public void update(D data, T time, long diff) {
        if (!cap.lessEqual(time)) throw new IllegalArgumentException("time " + time + " is behind capability " + cap);
        if (diff != 0) buf.add(new Update(data, time, diff));
    }

    public void update(D data, long diff) { update(data, time(), diff); }
    public void insert(D data) { update(data, time(), 1); }
    public void remove(D data) { update(data, time(), -1); }

    public void advanceTo(T t) { advanceTo(Antichain.of(t)); }

    public void advanceTo(Antichain<T> next) {
        for (T e : next.elements()) {
            if (!cap.lessEqual(e)) throw new IllegalArgumentException("cannot regress capability " + cap + " to " + next);
        }
        flush();
        cap.clear();
        for (T e : next.elements()) cap.insert(e);
    }

    public void close() {
        flush();
        cap.clear();
        closed = true;
    }

    public boolean isClosed() { return closed; }

    private boolean flush() {
        if (buf.isEmpty()) return false;
        List<Update> b = buf;
        buf = new ArrayList<>();
        out.send(b);
        return true;
    }

    @Override boolean step() { return flush(); }

    @Override void capabilities(Consumer<Time> sink) { for (T t : cap.elements()) sink.accept(t); }
}
