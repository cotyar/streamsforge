package io.streamsforge.engine.sql;

import io.streamsforge.engine.FieldKind;
import io.streamsforge.engine.runtime.Aggregator;
import io.streamsforge.engine.runtime.IZAggregator;

/** One aggregate contributed from outside the Engine. Both accumulators are required because this dialect runs
 *  the same SQL two ways: pipelines fold forward over a stream ({@link Aggregator}), tables maintain a Z-set
 *  where a superseded row arrives again with weight −1 ({@link IZAggregator}). */
public interface IAggregateFunction {
    String name();

    /** Result kind given the argument's inferred kind (null when not inferred). Return null to leave it unknown. */
    FieldKind resultKind(FieldKind argKind);

    Aggregator createStream();

    IZAggregator createZ();
}
