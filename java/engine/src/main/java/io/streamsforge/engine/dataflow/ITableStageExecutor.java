package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.TableDelta;
import java.util.List;

/**
 * Per-stage, per-partition operator instance. A fresh instance (with its own private op state) is created per
 * (stage, partition) via {@link TableDataflowPlan#createStageExecutor}; nothing is shared across partitions. Table-mode
 * ops are order-insensitive per-delta; a host buffers inbound batches per epoch (EpochBuffer), feeds them to
 * {@link #onBatch} in the deterministic order EpochBuffer.onFrontier returns, then calls {@link #onFrontier}.
 */
public interface ITableStageExecutor {
    int stageId();

    int partition();

    /** Feed one inbound batch (already known to belong to edge {@code inEdge}) at {@code epoch}. {@code originName} is
     *  the real external input name the batch ultimately came from — only meaningful for a Broadcast-mode edge feeding
     *  a scalar-subquery/semi-anti join's nested residual execution; ignored otherwise. */
    List<TableStageOutput> onBatch(EdgeId inEdge, String originName, Epoch epoch, List<TableDelta> deltas);

    /** Frontier-advance hook — every table-mode op's onFrontier is a pass-through today. */
    List<TableStageOutput> onFrontier(Epoch epoch);
}
