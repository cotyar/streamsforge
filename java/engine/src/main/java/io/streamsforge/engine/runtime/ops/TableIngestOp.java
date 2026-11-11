package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import io.streamsforge.engine.runtime.WorkingRow;
import java.util.ArrayList;
import java.util.List;

/**
 * The admission op for one input role (one per FROM/JOIN alias): turns a public {@link TableDelta} into the alias-
 * qualified {@link TableRowDelta} the rest of the chain consumes. A row carrying {@code _retract = true} (a client
 * key-retraction) is admitted with weight -1 regardless of its delta weight. STATE: none.
 */
public final class TableIngestOp implements ITableOp {
    public static final String RETRACT_FIELD = "_retract";

    private final String alias;

    public TableIngestOp(String alias) { this.alias = alias; }

    public String alias() { return alias; }

    public List<TableRowDelta> onBatch(Epoch epoch, List<TableDelta> input) {
        var results = new ArrayList<TableRowDelta>(input.size());
        for (var d : input) {
            long weight = isKeyRetraction(d.row()) ? -1L : d.weight();
            results.add(new TableRowDelta(WorkingRow.fromEvent(alias, d.row()), weight));
        }
        return results;
    }

    private static boolean isKeyRetraction(EventRecord row) { return Boolean.TRUE.equals(row.get(RETRACT_FIELD)); }

    @Override public List<TableDelta> onFrontier(Epoch epoch) { return new ArrayList<>(); }
}
