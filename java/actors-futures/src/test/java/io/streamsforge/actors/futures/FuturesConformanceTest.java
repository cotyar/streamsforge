package io.streamsforge.actors.futures;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeConformance;

class FuturesConformanceTest extends ActorRuntimeConformance {
    @Override
    protected ActorRuntime newRuntime() {
        return new FuturesActorRuntime();
    }
}
