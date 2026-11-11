package io.streamsforge.engine.dataflow;

/** Identifies one upstream sender: a specific partition of a specific edge. The unit {@link FrontierTracker}
 *  tracks high-water marks for, and the unit {@link EpochBuffer} orders flushed batches by. */
public record UpstreamId(EdgeId edgeId, int fromPartition) implements Comparable<UpstreamId> {
    @Override public int compareTo(UpstreamId other) {
        int byEdge = edgeId.compareTo(other.edgeId);
        return byEdge != 0 ? byEdge : Integer.compare(fromPartition, other.fromPartition);
    }

    @Override public String toString() { return edgeId + "/p" + fromPartition; }
}
