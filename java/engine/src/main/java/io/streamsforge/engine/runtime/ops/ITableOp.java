package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.TableDelta;
import io.streamsforge.engine.dataflow.Epoch;
import java.util.List;

/**
 * Frontier hook every table-mode op implements. Table mode has no epoch/watermark-driven eviction today (joins keep
 * unbounded consolidated state; GROUP BY groups live forever), so every op returns an empty list — a real, tested,
 * documented pass-through — from onFrontier. The hook exists for partition frontier propagation and EMIT FINAL
 * close-on-frontier table variants.
 */
public interface ITableOp {
    List<TableDelta> onFrontier(Epoch epoch);
}
