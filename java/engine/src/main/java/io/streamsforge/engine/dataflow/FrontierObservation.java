package io.streamsforge.engine.dataflow;

/** The result of a single {@link FrontierTracker#observe} call. */
public record FrontierObservation(FrontierObserveResult result, Epoch frontier) {
    public boolean advanced() { return result == FrontierObserveResult.ADVANCED; }

    public boolean regressed() { return result == FrontierObserveResult.REGRESSION; }
}
