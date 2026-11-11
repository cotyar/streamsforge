package io.streamsforge.engine.dataflow;

/**
 * Plan 003 M4 — a pure, exhaustive {@link TableStageKind} to human-facing operator name mapping. The switch is
 * deliberately exhaustive (no default fallback) so a future enum addition that forgets to extend this mapping fails
 * LOUDLY. The labels are the C# enum member names ("Ingest", "SemiAnti", ...), kept byte-identical.
 */
public final class TableStageKindLabel {
    private TableStageKindLabel() {}

    public static String of(TableStageKind kind) {
        return switch (kind) {
            case INGEST -> "Ingest";
            case JOIN -> "Join";
            case SEMI_ANTI -> "SemiAnti";
            case UNNEST -> "Unnest";
            case FILTER_PROJECT -> "FilterProject";
            case REDUCE -> "Reduce";
            case LATEST_BY -> "LatestBy";
        };
    }
}
