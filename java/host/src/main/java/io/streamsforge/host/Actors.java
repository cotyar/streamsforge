package io.streamsforge.host;

/** Actor type names and key composition. Entity names are SQL identifiers, so ':' is a safe key separator. */
final class Actors {
    private Actors() {}

    static final String REGISTRY = "registry";
    static final String SOURCE = "source";
    static final String PIPELINE = "pipeline";
    /** A table: the executor + view for parallelism 1, only the lifecycle coordinator for parallelism >= 2. */
    static final String TABLE = "table";
    /** key "{table}:{input}" — parallelism >= 2 only (Orleans TableIngestGrain). */
    static final String INGEST = "table-ingest";
    /** key "{table}:{stageId}:{partition}" — parallelism >= 2 only (Orleans TableStageGrain). */
    static final String STAGE = "table-stage";
    /** key "{table}" — parallelism >= 2 only; also the table's VIEW (ledger, subscribers, snapshot). */
    static final String OUTPUT = "table-output";

    static final String REGISTRY_ID = "default";

    static String ingestId(String table, String input) { return table + ":" + input; }

    static String stageId(String table, int stageId, int partition) { return table + ":" + stageId + ":" + partition; }
}
