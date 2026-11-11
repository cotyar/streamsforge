package io.streamsforge.engine.dataflow;

import java.util.List;

/**
 * One directed edge in a table's partitioned dataflow graph. {@code fromStageId == -1} means the edge originates
 * OUTSIDE the graph — a real external input named in {@code externalInputNames}. {@code toStageId == -1} means the
 * edge is the table's terminal output (see {@link TableEdgeMode#GATHER}).
 *
 * @param arrangeKeyFields Plan 003 M3 — non-null ONLY for a HashPartition-mode edge out of an Ingest stage whose key
 *        is a bare reference to the raw input's OWN field(s) with no pre-join transform: the raw field name(s), in
 *        order, used to attach a shared arrangement. Null for every other edge.
 */
public record TableEdgeDescriptor(
        EdgeId edgeId,
        int fromStageId,
        int toStageId,
        String role,
        TableEdgeMode mode,
        List<String> externalInputNames,
        List<String> arrangeKeyFields) {

    public TableEdgeDescriptor(EdgeId edgeId, int fromStageId, int toStageId, String role, TableEdgeMode mode,
                               List<String> externalInputNames) {
        this(edgeId, fromStageId, toStageId, role, mode, externalInputNames, null);
    }
}
