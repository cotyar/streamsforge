package io.streamsforge.host;

import io.streamsforge.engine.RowWeight;
import io.streamsforge.engine.TableDelta;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * What a table looks like from outside: implemented by {@code TableActor} for parallelism 1 and by
 * {@code TableOutputActor} for parallelism >= 2 (whichever actor owns the consolidated output ledger).
 * All methods are called inside the owning actor's turn.
 */
interface TableView {
    long subscribe(Consumer<TableDeltaBatch> consumer);

    boolean unsubscribe(long id);

    /** The consolidated output and the epoch it reflects, read together in one turn (an atomic pair). */
    SnapshotPair snapshotPair();

    Map<String, RowWeight> snapshotMap();

    record SnapshotPair(List<TableDelta> rows, long lastEpoch) {}
}
