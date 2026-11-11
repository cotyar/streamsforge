package io.streamsforge.host;

import io.streamsforge.actors.ActorRef;
import io.streamsforge.engine.EventRecord;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** How a consuming actor (table, or table ingest) attaches to its inputs. Shared by both. */
final class InputWiring {
    private InputWiring() {}

    /**
     * Subscribe to a source / pipeline row stream. Every delivery becomes a message in {@code self}'s mailbox
     * (sent by the PUBLISHER's turn, never awaited). The cleanup that unsubscribes is added to {@code cleanups}.
     */
    static <S> CompletionStage<Void> subscribeRows(HostEnv env, ActorRef<S> self, Specs.InputInfo in,
                                                   BiFunction<S, List<EventRecord>, CompletionStage<Void>> onRows,
                                                   List<Supplier<CompletionStage<?>>> cleanups) {
        ActorRef<RowPublisher> pub = env.ref(in.kind() == Specs.InputKind.SOURCE ? Actors.SOURCE : Actors.PIPELINE, in.name());
        return pub.call(p -> p.subscribeRows(rows -> env.send(self, ctx -> onRows.apply(ctx.state(), rows)))).thenAccept(id ->
            cleanups.add(() -> pub.call(p -> p.unsubscribeRows(id))));
    }

    /**
     * The table-over-table attach protocol: SUBSCRIBE FIRST (deliveries queue in {@code self}'s mailbox behind the
     * turn that is calling this), THEN read the upstream's atomic (snapshot, lastEpoch) pair. The caller applies the
     * snapshot and records {@code lastEpoch} as the cutoff; every queued/later batch with epoch &lt;= cutoff is
     * already contained in the snapshot and is skipped. No gap (subscribed before the pair was read), no double
     * count (cutoff).
     */
    static <S> CompletionStage<TableView.SnapshotPair> attachTable(HostEnv env, ActorRef<S> self, Specs.InputInfo up,
                                                                   BiFunction<S, TableDeltaBatch, CompletionStage<Void>> onBatch,
                                                                   List<Supplier<CompletionStage<?>>> cleanups) {
        ActorRef<TableView> view = env.viewRef(up.name(), up.tableParallelism());
        return view.call(v -> v.subscribe(batch -> env.send(self, ctx -> onBatch.apply(ctx.state(), batch)))).thenCompose(id -> {
            cleanups.add(() -> view.call(v -> v.unsubscribe(id)));
            return view.call(TableView::snapshotPair);
        });
    }
}
