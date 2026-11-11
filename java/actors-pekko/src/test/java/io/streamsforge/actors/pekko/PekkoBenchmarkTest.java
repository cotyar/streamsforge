package io.streamsforge.actors.pekko;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeBenchmark;

class PekkoBenchmarkTest extends ActorRuntimeBenchmark {
    @Override
    protected ActorRuntime newRuntime() {
        return new PekkoActorRuntime();
    }
}
