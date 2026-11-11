package io.streamsforge.actors.loom;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeConformance;

class LoomConformanceTest extends ActorRuntimeConformance {
    @Override
    protected ActorRuntime newRuntime() {
        return new LoomActorRuntime();
    }
}
