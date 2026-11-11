package io.streamsforge.engine.dataflow;

/** One kind of node in a table plan's partitioned dataflow graph. Mirrors the M1 op inventory 1:1: Ingest wraps
 *  TableIngestOp, Join wraps TableJoinOp (plain INNER/CROSS equi-joins AND scalar-subquery joins) OR, for JoinKind
 *  Left/Right/Full, TableOuterJoinOp (plan 008 — see JoinChainStageExecutor's op selection), SemiAnti wraps
 *  TableSemiAntiOp, Unnest wraps TableUnnestOp, FilterProject wraps TableFilterProjectOp, Reduce wraps
 *  TableReduceOp, LatestBy wraps TableLatestByOp. No dedicated kind for outer joins — deliberately: they share
 *  every structural property a plain Join stage has; only the op instantiated differs by JoinKind. */
public enum TableStageKind { INGEST, JOIN, SEMI_ANTI, UNNEST, FILTER_PROJECT, REDUCE, LATEST_BY }
