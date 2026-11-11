package io.streamsforge.engine.dataflow;

/**
 * Where a stage's output is exchanged to. Key expressions are a plan-time concern; by the time a delta reaches
 * {@link ExchangeRouter} it has already been reduced to canonical key bytes via the engine's existing key encoding
 * (TableKeyEncoding), so this only carries the routing target.
 */
public record ExchangeSpec(EdgeId edgeId, int partitionCount) {
    public ExchangeSpec {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1. (Parameter 'partitionCount', actual value was " + partitionCount + ")");
        }
    }
}
