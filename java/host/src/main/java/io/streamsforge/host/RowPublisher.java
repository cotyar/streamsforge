package io.streamsforge.host;

import io.streamsforge.engine.EventRecord;
import java.util.List;
import java.util.function.Consumer;

/** A stream of event rows: implemented by {@code SourceActor} and {@code PipelineActor}. */
interface RowPublisher {
    long subscribeRows(Consumer<List<EventRecord>> consumer);

    boolean unsubscribeRows(long id);
}
