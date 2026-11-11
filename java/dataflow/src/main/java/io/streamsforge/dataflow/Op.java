package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** A dataflow operator. Operators are stepped by the {@link Worker}; they hold no threads. */
@SuppressWarnings({"rawtypes"})
abstract class Op {
    final Worker worker;
    final List<InPort> ins = new ArrayList<>();
    final List<Output> outs = new ArrayList<>();

    Op(Worker w) {
        this.worker = w;
        w.register(this);
    }

    InPort addIn() { InPort p = new InPort(this); ins.add(p); return p; }
    Output addOut() { Output o = new Output(); outs.add(o); return o; }

    /** Consume available input, emit output. Returns true if any work was done. */
    abstract boolean step();

    /** How an input time maps to the earliest output time it can cause (identity except at scope boundaries). */
    Time summarize(Time t) { return t; }

    /** Times at which this operator may still produce output on its own (held capabilities). */
    void capabilities(Consumer<Time> sink) {}

    /** Number of retained state entries (for the compaction metric). */
    long stateSize() { return 0; }
}
