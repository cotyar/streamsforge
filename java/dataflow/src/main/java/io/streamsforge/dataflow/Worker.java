package io.streamsforge.dataflow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Owns a dataflow graph and runs it on the calling thread.
 *
 * <p>Progress tracking: before every pass over the operators the worker recomputes, for every operator input, the
 * <em>frontier</em> - the minimal times at which updates may still arrive. Sources of "may still arrive" are
 * (a) messages queued on any input, and (b) capabilities held by operators (an input's open frontier, an operator's
 * pending output times). Each such pointstamp is propagated along every path to every downstream input, mapped by the
 * operators it crosses ({@code enter} adds an iteration coordinate, the feedback edge increments it, {@code leave}
 * drops it). Because every cycle passes through the incrementing feedback edge, the propagation reaches a fixpoint.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class Worker {
    private final List<Op> ops = new ArrayList<>();
    private long maxPasses = Long.MAX_VALUE;
    private long passes;

    void register(Op op) { ops.add(op); }

    /** Creates an input whose capability starts at {@code initial}. */
    public <D, T extends Time<T>> InputHandle<D, T> newInput(T initial) {
        return new InputHandle<>(this, initial);
    }

    /** Creates an input over {@link LongTime} epochs starting at 0. */
    public <D> InputHandle<D, LongTime> newInput() {
        return newInput(LongTime.ZERO);
    }

    /** Safety valve against non-converging iterations: {@link #step} throws after this many passes in one call. */
    public void setMaxPassesPerStep(long n) { this.maxPasses = n; }

    /** Total operator passes executed so far (a rough work counter). */
    public long passes() { return passes; }

    /**
     * Runs operators until no operator can make progress. Returns true if any work was done.
     */
    public boolean step() {
        boolean any = false;
        long n = 0;
        while (true) {
            computeFrontiers();
            boolean did = false;
            for (int i = 0; i < ops.size(); i++) did |= ops.get(i).step();
            passes++;
            if (!did) break;
            any = true;
            if (++n > maxPasses) throw new IllegalStateException("dataflow did not quiesce within " + maxPasses + " passes");
        }
        return any;
    }

    /** Total retained state entries across all operators (traces, per-key reduce state, buffers). */
    public long stateSize() {
        long n = 0;
        for (Op op : ops) n += op.stateSize();
        return n;
    }

    void computeFrontiers() {
        for (Op op : ops) for (InPort p : op.ins) p.frontier = new Antichain();
        ArrayDeque<Object[]> work = new ArrayDeque<>();
        for (Op op : ops) {
            for (InPort p : op.ins) {
                for (Msg m : p.queue) {
                    for (Object t : m.times.elements()) offer(p, (Time) t, work);
                }
            }
            op.capabilities(c -> {
                for (Output o : op.outs) for (InPort q : o.consumers) offer(q, c, work);
            });
        }
        while (!work.isEmpty()) {
            Object[] w = work.poll();
            InPort p = (InPort) w[0];
            Time s = p.owner.summarize((Time) w[1]);
            for (Output o : p.owner.outs) for (InPort q : o.consumers) offer(q, s, work);
        }
    }

    private static void offer(InPort p, Time t, ArrayDeque<Object[]> work) {
        if (p.frontier.insert(t)) work.add(new Object[] {p, t});
    }
}
