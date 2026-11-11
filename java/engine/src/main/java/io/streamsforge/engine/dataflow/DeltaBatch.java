package io.streamsforge.engine.dataflow;

import io.streamsforge.engine.TableDelta;
import java.util.List;

/**
 * ALL cross-grain movement is batched per (edge, epoch). A batch may carry zero deltas: an upstream that produced
 * no output for an epoch still sends the marker so downstream frontier tracking can advance.
 */
public record DeltaBatch(EdgeId edgeId, int fromPartition, Epoch epoch, List<TableDelta> deltas) {
    /** The (edge, partition) this batch came from — the key FrontierTracker and EpochBuffer both key off. */
    public UpstreamId upstream() { return new UpstreamId(edgeId, fromPartition); }

    /** True for an empty epoch marker: no rows changed upstream this epoch. */
    public boolean isMarker() { return deltas.isEmpty(); }
}
