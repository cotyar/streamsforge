package io.streamsforge.dataflow;

import java.util.function.Consumer;

/** Terminal operator: hands each message to a callback; its input frontier is what probes read. */
@SuppressWarnings({"rawtypes"})
final class SinkOp extends Op {
    final InPort in = addIn();
    final Consumer<Msg> sink;

    SinkOp(Worker w, Consumer<Msg> sink) {
        super(w);
        this.sink = sink;
    }

    @Override boolean step() {
        boolean did = false;
        Msg m;
        while ((m = in.queue.poll()) != null) {
            did = true;
            if (sink != null) sink.accept(m);
        }
        return did;
    }
}
