package io.streamsforge.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Buffers updates until their time is complete, then emits them summed per record (zeros dropped). */
@SuppressWarnings({"rawtypes", "unchecked"})
final class ConsolidateOp extends Op {
    final InPort in = addIn();
    final Output out = addOut();
    private final TreeMap<Time, Map<Object, long[]>> buf = new TreeMap<>();

    ConsolidateOp(Worker w) { super(w); }

    @Override boolean step() {
        boolean did = false;
        Msg m;
        while ((m = in.queue.poll()) != null) {
            did = true;
            for (Update u : m.updates) {
                buf.computeIfAbsent((Time) u.time(), t -> new HashMap<>())
                        .computeIfAbsent(u.data(), d -> new long[1])[0] += u.diff();
            }
        }
        Antichain f = in.frontier;
        List<Update> res = new ArrayList<>();
        var it = buf.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            if (f.lessEqual(e.getKey())) continue;
            did = true;
            for (var d : e.getValue().entrySet()) if (d.getValue()[0] != 0) res.add(new Update(d.getKey(), e.getKey(), d.getValue()[0]));
            it.remove();
        }
        out.send(res);
        return did;
    }

    @Override void capabilities(Consumer<Time> sink) { for (Time t : buf.keySet()) sink.accept(t); }

    @Override long stateSize() {
        long n = 0;
        for (var v : buf.values()) n += v.size();
        return n;
    }
}
