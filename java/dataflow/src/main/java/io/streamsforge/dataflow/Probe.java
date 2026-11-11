package io.streamsforge.dataflow;

/** Observes the frontier of a collection: which times may still receive updates. */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class Probe<T extends Time<T>> {
    private final Worker worker;
    private final InPort port;

    Probe(Worker worker, InPort port) {
        this.worker = worker;
        this.port = port;
    }

    /** The current frontier (recomputed on demand). */
    public Antichain<T> frontier() {
        worker.computeFrontiers();
        return port.frontier.copy();
    }

    /** True if some update at a time {@code <= t} may still arrive. */
    public boolean lessEqual(T t) { return frontier().lessEqual(t); }

    /** True if some update at a time strictly before {@code t} may still arrive. */
    public boolean lessThan(T t) { return frontier().lessThan(t); }

    /** True if everything at times {@code <= t} has been delivered, i.e. time {@code t} is final. */
    public boolean done(T t) { return !lessEqual(t); }
}
