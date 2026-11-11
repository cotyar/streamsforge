package io.streamsforge.host;

import java.util.concurrent.CompletionStage;

/** Handle of a pipeline/table subscription. Cancelling is asynchronous (it is a message to the publisher). */
public interface Subscription {
    /** Stop delivery. Batches already handed to the consumer are not recalled; idempotent. */
    CompletionStage<Void> cancel();
}
