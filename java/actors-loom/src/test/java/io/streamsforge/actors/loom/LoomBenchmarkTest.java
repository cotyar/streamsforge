package io.streamsforge.actors.loom;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeBenchmark;

class LoomBenchmarkTest extends ActorRuntimeBenchmark {
    @Override
    protected ActorRuntime newRuntime() {
        return new LoomActorRuntime();
    }
}
