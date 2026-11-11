package io.streamsforge.engine.dataflow;

/** How a {@link TableEdgeDescriptor}'s deltas are routed from producer to consumer partitions. */
public enum TableEdgeMode {
    /** Partition-preserving: producer partition p feeds consumer partition p directly, no exchange. */
    LOCAL,
    /** Hash-partitioned by the consuming stage's key expression(s) via TableDataflowPlan.partitionOf — the DBSP
     *  "exchange" edge; both sides of a join/semi-anti feed this mode so matching keys co-partition. */
    HASH_PARTITION,
    /** Replicated to every partition of the consuming stage, unpartitioned (scalar-subquery / semi-anti
     *  singleton sides: every partition runs its own private nested single-partition execution of the subquery). */
    BROADCAST,
    /** The terminal edge: a stage's final output (toStageId == -1) is gathered to the table's one output publisher. */
    GATHER
}
