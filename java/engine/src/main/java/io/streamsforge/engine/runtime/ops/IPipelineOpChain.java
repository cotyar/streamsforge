package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.EventRecord;
import java.util.List;

/**
 * The composability contract for a pipeline-mode op-chain (plan 003 M1 Part B): an entire op-chain can itself be
 * wrapped as a node inside another chain. {@code PipelineExecutor} implements this; the shape
 * (onEvent/advanceWatermark both returning the emitted rows) is exactly what plan 004 N1's derived tables need —
 * child chain's emissions become parent's input events. A documentation/design anchor; adds no behavior.
 */
public interface IPipelineOpChain {
    List<EventRecord> onEvent(String sourceName, EventRecord evt);

    List<EventRecord> advanceWatermark(long nowMs);
}
