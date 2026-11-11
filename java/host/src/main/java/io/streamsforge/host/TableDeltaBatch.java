package io.streamsforge.host;

import io.streamsforge.engine.TableDelta;
import java.util.List;

/**
 * One published batch of a table's output: the deltas it emitted for {@code epoch}. Epochs of one table's
 * output strictly increase; two batches never share one. A downstream table that attached late uses the
 * epoch as the cutoff of its snapshot ({@code epoch <= snapshot.lastEpoch} is already in the snapshot).
 */
public record TableDeltaBatch(long epoch, List<TableDelta> deltas) {}
