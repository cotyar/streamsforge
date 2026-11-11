package io.streamsforge.actors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Small helpers for writing turns. */
public final class Turns {
    private Turns() {}

    private static final CompletionStage<Void> DONE = CompletableFuture.completedFuture(null);

    public static CompletionStage<Void> done() {
        return DONE;
    }

    public static <R> CompletionStage<R> completed(R value) {
        return CompletableFuture.completedFuture(value);
    }

    public static <R> CompletionStage<R> failed(Throwable t) {
        return CompletableFuture.failedFuture(t);
    }
}
