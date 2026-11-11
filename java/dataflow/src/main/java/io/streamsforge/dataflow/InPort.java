package io.streamsforge.dataflow;

import java.util.ArrayDeque;

/** An operator input: a queue of messages plus the frontier computed by progress tracking. */
@SuppressWarnings({"rawtypes"})
final class InPort {
    final Op owner;
    final ArrayDeque<Msg> queue = new ArrayDeque<>();
    Antichain frontier = new Antichain();

    InPort(Op owner) { this.owner = owner; }
}
