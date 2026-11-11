package io.streamsforge.dataflow;

import java.util.List;
import java.util.function.Function;

/** map/filter/concat/negate/enter/leave/feedback/inspect: transforms each message as it arrives. */
@SuppressWarnings({"rawtypes"})
final class StatelessOp extends Op {
    final Function<List<Update>, List<Update>> fn;
    final Function<Time, Time> summary;
    final Output out = addOut();

    StatelessOp(Worker w, Function<List<Update>, List<Update>> fn, Function<Time, Time> summary) {
        super(w);
        this.fn = fn;
        this.summary = summary;
    }

    @Override boolean step() {
        boolean did = false;
        for (InPort p : ins) {
            Msg m;
            while ((m = p.queue.poll()) != null) {
                did = true;
                out.send(fn.apply(m.updates));
            }
        }
        return did;
    }

    @Override Time summarize(Time t) { return summary == null ? t : summary.apply(t); }
}
