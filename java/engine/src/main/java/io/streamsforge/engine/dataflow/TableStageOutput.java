package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.TableDelta;
import java.util.List;

/** A batch of deltas a stage executor emitted on one outbound edge, still needing the Host to route it. */
public record TableStageOutput(TableEdgeDescriptor outEdge, List<TableDelta> deltas) {}
