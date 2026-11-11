package io.streamsforge.host;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Tiny stage combinators. */
final class Async {
    private Async() {}

    static final CompletionStage<Void> DONE = CompletableFuture.completedFuture(null);

    /** Completes when all complete; fails with the first failure. */
    static CompletionStage<Void> allOf(Collection<? extends CompletionStage<?>> stages) {
        if (stages.isEmpty()) return DONE;
        var arr = new CompletableFuture<?>[stages.size()];
        int i = 0;
        for (var s : stages) arr[i++] = s.toCompletableFuture();
        return CompletableFuture.allOf(arr);
    }

    /** Runs the steps one after another (each starts after the previous completed). */
    static CompletionStage<Void> sequence(List<Supplier<CompletionStage<?>>> steps) {
        CompletionStage<Void> f = DONE;
        for (var step : steps) f = f.thenCompose(v -> step.get().thenApply(x -> (Void) null));
        return f;
    }

    /** Like allOf but swallows failures (best-effort cleanup). */
    static CompletionStage<Void> allOfQuiet(Collection<? extends CompletionStage<?>> stages) {
        var out = new ArrayList<CompletionStage<?>>();
        for (var s : stages) out.add(s.handle((v, e) -> null));
        return allOf(out);
    }

    static Throwable unwrap(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) t = t.getCause();
        return t;
    }
}
