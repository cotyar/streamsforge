package io.streamsforge.actors.futures;

import io.streamsforge.actors.ActorRuntime;
import io.streamsforge.actors.testkit.ActorRuntimeBenchmark;

class FuturesBenchmarkTest extends ActorRuntimeBenchmark {
    @Override
    protected ActorRuntime newRuntime() {
        return new FuturesActorRuntime();
    }
}
