package io.streamsforge.engine.dd;

import io.streamsforge.dataflow.Capture;
import io.streamsforge.dataflow.InputHandle;
import io.streamsforge.dataflow.LongTime;
import io.streamsforge.dataflow.Worker;
import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.TableDelta;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A running dataflow-compiled table query. Feed rows with {@link #update}, close an epoch with {@link #commit()};
 * after that {@link #snapshot()} is the accumulated output as of the closed epoch and {@link #deltasAt} the (netted)
 * output changes that epoch produced. Single-threaded.
 */
public final class DataflowTableQuery {
    private final Worker worker;
    private final Map<String, InputHandle<EventRecord, LongTime>> inputs;
    private final Capture<Out, LongTime> capture;

    private long openEpoch;
    private int consumed;
    private final Map<String, Long> weights = new LinkedHashMap<>();
    private final Map<String, EventRecord> rows = new LinkedHashMap<>();
    private final Map<Long, Map<String, Long>> epochWeights = new LinkedHashMap<>();
    private final Map<Long, Map<String, EventRecord>> epochRows = new LinkedHashMap<>();

    DataflowTableQuery(GraphBuilder g, Capture<Out, LongTime> capture) {
        this.worker = g.worker;
        this.inputs = g.inputs;
        this.capture = capture;
    }

    /** The leaf stream/table names this query reads. */
    public Set<String> inputNames() { return inputs.keySet(); }

    /** The epoch currently accepting input (0 for a fresh query). */
    public long openEpoch() { return openEpoch; }

    /** Adds {@code weight} copies (negative = retraction) of {@code row} to input {@code source} in the open epoch.
     *  A source this query does not read is ignored, like the engine does. */
    public void update(String source, EventRecord row, long weight) {
        var in = inputs.get(source);
        if (in == null) return;
        if (Boolean.TRUE.equals(row.get("_retract"))) {
            throw new IllegalArgumentException("client key-retraction rows (_retract) are not supported by the dataflow compiler");
        }
        in.update(row, weight);
    }

    /** Closes the open epoch: advances every input, runs the dataflow to quiescence and folds the new output in.
     *  Returns the epoch that was closed. */
    public long commit() {
        long closing = openEpoch;
        var next = LongTime.of(closing + 1);
        for (var in : inputs.values()) in.advanceTo(next);
        worker.step();
        var updates = capture.updates();
        for (int i = consumed; i < updates.size(); i++) {
            var u = updates.get(i);
            String key = u.data().key();
            weights.merge(key, u.diff(), Long::sum);
            rows.putIfAbsent(key, u.data().row());
            long t = u.time().value();
            epochWeights.computeIfAbsent(t, k -> new LinkedHashMap<>()).merge(key, u.diff(), Long::sum);
            epochRows.computeIfAbsent(t, k -> new LinkedHashMap<>()).putIfAbsent(key, u.data().row());
        }
        consumed = updates.size();
        openEpoch = closing + 1;
        return closing;
    }

    /** Accumulated output as of the last closed epoch, keyed by canonical row key, rows with weight &gt; 0 only
     *  (the same view {@code TableExecutor.snapshot()} gives). */
    public Map<String, RowWeight> snapshot() {
        var m = new LinkedHashMap<String, RowWeight>();
        for (var e : weights.entrySet()) if (e.getValue() > 0) m.put(e.getKey(), new RowWeight(rows.get(e.getKey()), e.getValue()));
        return m;
    }

    /** Raw accumulated weights including zero-free negative entries — for asserting the output is a valid Z-set. */
    public Map<String, Long> rawWeights() {
        var m = new LinkedHashMap<String, Long>();
        for (var e : weights.entrySet()) if (e.getValue() != 0) m.put(e.getKey(), e.getValue());
        return m;
    }

    /** The output changes produced at {@code epoch}, netted per row. */
    public List<TableDelta> deltasAt(long epoch) {
        var out = new ArrayList<TableDelta>();
        var w = epochWeights.get(epoch);
        if (w == null) return out;
        for (var e : w.entrySet()) if (e.getValue() != 0) out.add(new TableDelta(epochRows.get(epoch).get(e.getKey()), e.getValue()));
        return out;
    }

    /** Retained dataflow state entries (traces, reduce state) — the memory metric of {@link Worker#stateSize()}. */
    public long stateSize() { return worker.stateSize(); }
}
