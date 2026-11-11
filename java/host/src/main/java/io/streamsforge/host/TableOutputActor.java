package io.streamsforge.host;

import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.TableExecutor;
import io.streamsforge.engine.dataflow.DeltaBatch;
import io.streamsforge.engine.dataflow.EdgeId;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.dataflow.EpochBuffer;
import io.streamsforge.engine.dataflow.FrontierTracker;
import io.streamsforge.engine.dataflow.UpstreamId;
import io.streamsforge.engine.runtime.ConsolidationLedger;
import io.streamsforge.engine.runtime.JsonText;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Parallelism &gt;= 2 only. One activation per table (Orleans {@code TableOutputGrain} fused with the coordinator-mode
 * half of {@code TableGrain}: {@code OnOutputBatchAsync}, the coordinator ledger, {@code AttachSnapshotAsync}). It is
 * the table's {@link TableView}.
 *
 * <p>Each terminal-stage partition calls {@link #publish}(partition, frontier, deltas) as its own epoch advances. This
 * actor runs the same machinery as every other hop — a {@link FrontierTracker} over the terminal partitions and an
 * {@link EpochBuffer} — and only when the MIN frontier over ALL partitions reaches epoch F does it release the ready
 * batches: net them by canonical row key (a row one partition retracted and another re-asserted in the same logical
 * epoch never reaches a subscriber as two messages), fold them into the ledger, and publish one
 * {@link TableDeltaBatch} stamped F. {@code lastEpoch} = F also when the net is empty, so (ledger, lastEpoch) is a
 * consistent pair for a late-attaching downstream table, exactly as in the single-partition case.
 */
final class TableOutputActor implements TableView {
    private final HostEnv env;
    private final Publisher<TableDeltaBatch> out;
    private final ConsolidationLedger ledger = new ConsolidationLedger();
    private String table = "";
    private boolean running;
    private boolean exists;
    private FrontierTracker frontier;
    private EpochBuffer buffer;
    private EdgeId terminalEdge;
    private long lastEpoch = -1;

    TableOutputActor(HostEnv env, String table) {
        this.env = env;
        this.table = table;
        this.out = new Publisher<>(env);
    }

    void init() { exists = true; }

    void start(Specs.TableSpec spec) {
        var dataflow = spec.plan().createDataflow(spec.parallelism());
        var terminal = dataflow.terminalEdge();
        int terminalPartitions = dataflow.partitionCountOf(terminal.fromStageId());
        terminalEdge = terminal.edgeId();
        var ups = new ArrayList<UpstreamId>();
        for (int p = 0; p < terminalPartitions; p++) ups.add(new UpstreamId(terminalEdge, p));
        frontier = new FrontierTracker(ups);
        buffer = new EpochBuffer();
        ledger.clear();
        lastEpoch = -1;
        exists = true;
        running = true;
    }

    void stop() {
        running = false;
        frontier = null;
        buffer = null;
    }

    void reset() {
        stop();
        exists = false;
        ledger.clear();
        lastEpoch = -1;
        out.clear();
    }

    CompletionStage<Void> publish(int fromPartition, long epochValue, List<TableDelta> deltas) {
        if (!running) return Async.DONE;
        var epoch = new Epoch(epochValue);
        buffer.add(new DeltaBatch(terminalEdge, fromPartition, epoch, deltas));
        var observation = frontier.observe(new UpstreamId(terminalEdge, fromPartition), epoch);
        if (!observation.advanced()) return Async.DONE;

        var ready = buffer.onFrontier(observation.frontier());
        var merged = new ArrayList<TableDelta>();
        for (var b : ready) merged.addAll(b.deltas());
        var net = consolidate(merged);
        for (var d : net) ledger.apply(TableExecutor.canonicalRowKeyOf(d.row()), d.row(), d.weight());
        lastEpoch = observation.frontier().value();
        if (!net.isEmpty()) out.publish(new TableDeltaBatch(lastEpoch, Collections.unmodifiableList(net)));
        return Async.DONE;
    }

    /** Z-set consolidation of one released round: net weight per canonical row key, zero nets dropped, first-seen order. */
    static List<TableDelta> consolidate(List<TableDelta> raw) {
        if (raw.size() <= 1) return raw;
        var net = new LinkedHashMap<String, long[]>();
        var first = new HashMap<String, TableDelta>();
        for (var d : raw) {
            String key = JsonText.serializeCanonicalRow(d.row());
            net.computeIfAbsent(key, k -> new long[1])[0] += d.weight();
            first.putIfAbsent(key, d);
        }
        var result = new ArrayList<TableDelta>(net.size());
        for (var e : net.entrySet()) {
            long w = e.getValue()[0];
            if (w != 0) result.add(new TableDelta(first.get(e.getKey()).row(), w));
        }
        return result;
    }

    // ------------------------------------------------------------------ view

    @Override
    public long subscribe(Consumer<TableDeltaBatch> consumer) {
        if (!exists) throw new CatalogException("unknown table '" + table + "'");
        return out.subscribe(consumer);
    }

    @Override
    public boolean unsubscribe(long id) { return out.unsubscribe(id); }

    @Override
    public SnapshotPair snapshotPair() {
        var rows = new ArrayList<TableDelta>(ledger.visible().size());
        for (RowWeight rw : ledger.visible().values()) rows.add(new TableDelta(rw.row(), rw.weight()));
        return new SnapshotPair(rows, lastEpoch);
    }

    @Override
    public Map<String, RowWeight> snapshotMap() { return new LinkedHashMap<>(ledger.visible()); }
}
