package io.streamsforge.engine.dataflow;

import java.util.List;

/** One node in a table's partitioned dataflow graph. {@code alias} is the join/ingest alias this stage plays (empty for
 *  the single-per-plan FilterProject/Reduce/LatestBy stages). {@code inEdges} lists every inbound edge (1 for
 *  Ingest/FilterProject/Unnest/Reduce/LatestBy, 2 — "Left"+"Right" — for Join/SemiAnti). An Ingest stage always has
 *  partition count 1; every other stage runs at the plan's full partition count. */
public record TableStageDescriptor(int stageId, TableStageKind kind, String alias, List<TableEdgeDescriptor> inEdges) {}
