package io.streamsforge.actors;

import java.util.concurrent.CompletionStage;

/**
 * One unit of work on an actor. The runtime guarantees TURN-BASED, NON-REENTRANT execution:
 * a turn starts only after the previous turn's returned stage has COMPLETED, so a turn that
 * awaits another actor (by returning a stage composed from that actor's {@code ask}) keeps its
 * own mailbox closed until the whole chain finishes — exactly an Orleans non-reentrant grain.
 * Consequence (also exactly Orleans): an A→B→A call cycle deadlocks; the SPI conformance suite
 * does not test for it and runtimes are not required to detect it.
 */
@FunctionalInterface
public interface Turn<S, R> {
    CompletionStage<R> run(ActorContext<S> ctx) throws Exception;
}
