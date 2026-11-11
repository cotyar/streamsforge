package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.dataflow.Epoch;
import java.util.List;

/**
 * The two-input-edge contract every table-mode JOIN-CHAIN stage implements — ordinary equi-joins
 * ({@link TableJoinOp}), outer joins, and plan 004 N2's semi/anti presence joins ({@link TableSemiAntiOp}) alike.
 * Scalar-kind stages reuse TableJoinOp directly.
 */
public interface ITableJoinStage extends ITableOp {
    List<TableRowDelta> onLeftBatch(Epoch epoch, List<TableRowDelta> input);

    List<TableRowDelta> onRightBatch(Epoch epoch, List<TableRowDelta> input);
}
