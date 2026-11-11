package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.WorkingRow;
import java.util.List;

/** Additional contract for plan 004 N2/N3/N4's rolling-snapshot join stages — batch-shaped B-side delivery
 *  instead of the ordinary per-row {@link IPipelineJoinStage#onRight}. */
public interface IPipelineSnapshotJoinStage extends IPipelineJoinStage {
    void onRightBatch(List<WorkingRow> rows);
}
