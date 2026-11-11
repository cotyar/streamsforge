package io.streamsforge.host;

import io.streamsforge.engine.RowWeight;
import java.util.Map;

/**
 * Result of {@link StreamsHost#attachTable}: the table's snapshot and the epoch it reflects, taken atomically
 * after the subscription was installed. Every batch the consumer sees has an epoch greater than {@code lastEpoch}
 * OR is already contained in the snapshot (consumers apply only {@code epoch > lastEpoch}).
 */
public record TableAttach(Map<String, RowWeight> snapshot, long lastEpoch, Subscription subscription) {}
