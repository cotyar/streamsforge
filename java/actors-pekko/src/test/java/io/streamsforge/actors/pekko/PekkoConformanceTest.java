package io.streamsforge.actors.pekko;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeConformance;

class PekkoConformanceTest extends ActorRuntimeConformance {
    @Override
    protected ActorRuntime newRuntime() {
        return new PekkoActorRuntime();
    }
}
