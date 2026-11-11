package io.streamsforge.host;

import io.streamsforge.engine.PipelinePlan;
import io.streamsforge.engine.SourceSchema;
import io.streamsforge.engine.TablePlan;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Immutable start-up descriptions the registry hands to entity actors (so they never call back into it). */
final class Specs {
    private Specs() {}

    enum InputKind { SOURCE, PIPELINE, TABLE }

    /** {@code tableParallelism} is meaningful for TABLE inputs only: it selects the upstream's view actor. */
    record InputInfo(String name, InputKind kind, int tableParallelism) {}

    record PipelineSpec(String name, PipelinePlan plan, List<String> sources) {}

    record TableSpec(String name, int parallelism, TablePlan plan, List<String> streamInputs, List<String> tableInputs,
                     Map<String, InputInfo> inputs, SourceSchema outputSchema) {
        List<String> allInputs() {
            var all = new ArrayList<String>(streamInputs);
            for (var t : tableInputs) if (!all.contains(t)) all.add(t);
            return all;
        }
    }
}
