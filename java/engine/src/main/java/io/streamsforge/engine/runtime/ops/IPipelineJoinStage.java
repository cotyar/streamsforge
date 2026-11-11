package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.WorkingRow;
import java.util.List;

/**
 * The two-input-edge contract every pipeline-mode join-chain stage implements — ordinary WITHIN-buffered interval
 * joins ({@link PipelineJoinOp}) and plan 004 N2/N3/N4's rolling-snapshot subquery stages
 * ({@link PipelineSubqueryOp}) alike. OnRight/Evict are still called for a snapshot stage but always return an
 * empty list there: a snapshot stage's B-side state only ever changes via
 * {@link IPipelineSnapshotJoinStage#onRightBatch}.
 */
public interface IPipelineJoinStage {
    List<WorkingRow> onLeft(WorkingRow row);

    List<WorkingRow> onRight(WorkingRow row);

    List<WorkingRow> evict(long watermark);
}
