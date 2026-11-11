# dataflow - differential dataflow for Java 21

A small, dependency-free implementation of differential dataflow (McSherry et al., CIDR 2013; semantics follow the
Rust `differential-dataflow` crate): incremental computation over collections of `(data, time, diff)` updates, with
partially ordered times, frontier-based progress tracking, shared compacting arrangements, and nested incremental
fixpoint iteration. Package `io.streamsforge.dataflow`.

```
cd java && ./gradlew :dataflow:test
```

## Example: word count

```java
Worker worker = new Worker();
InputHandle<String, LongTime> words = worker.newInput();
Capture<Pair<String, Long>, LongTime> counts = words.collection().count().capture();

words.insert("a"); words.insert("b"); words.insert("a");
words.advanceTo(LongTime.of(1));          // epoch 0 is now closed to new input
worker.step();                            // run until nothing can progress
counts.accumulate(LongTime.of(0));        // {(a,2)=1, (b,1)=1}

words.remove("a");                        // epoch 1: a retraction
words.advanceTo(LongTime.of(2));
worker.step();
counts.accumulate(LongTime.of(1));        // {(a,1)=1, (b,1)=1}
```

## Example: incremental reachability

```java
Collection<Integer, LongTime> reach(Collection<Integer, LongTime> roots,
                                    Collection<Pair<Integer, Integer>, LongTime> edges) {
    return roots.iterate((scope, r) -> {
        var e = scope.enter(edges).keyed(Pair::first, Pair::second);     // (src, dst)
        return r.keyed(n -> n, n -> Unit.U)
                .join(e, (src, u, dst) -> dst)                           // follow one edge
                .concat(scope.enter(roots))
                .distinct();
    });
}
```
Edges inserted or retracted at later epochs update the result incrementally, including disconnecting nodes.

## Semantics, in short

* A **collection** is a stream of `Update(data, time, diff)` with signed `long` `diff` (a Z-set). Its contents at
  time `t` are the sum of diffs of updates with `time <= t` in the (partial) order. `Capture.accumulate(t)` computes this.
* **Times** are lattices (`Time`: `lessEqual`, `join`, `meet`, plus a `compareTo` that is a linear extension of the
  order). Provided: `LongTime` (total order) and `Product<O, I>` (product order, used for iteration scopes; nests).
* **Progress.** `Antichain<T>` is a frontier: the minimal times at which updates may still arrive. An
  `InputHandle` owns a capability (an antichain); `advanceTo` promises there will be no updates before it, `close`
  gives it up. The `Worker` recomputes the frontier at every operator input before each pass, by propagating
  *pointstamps* (queued messages and held capabilities) along all paths, mapped through scope boundaries (`enter`
  adds an iteration coordinate, the feedback edge increments it, `leave` drops it). Because every cycle goes through
  the incrementing feedback edge the propagation terminates. Time `t` is **complete** at an input when no frontier
  element is `<= t` (`Probe.done(t)`); operators only finalize output at complete times.
* **Arrangements / traces.** `Keyed.arrange()` indexes `(key, val)` updates into a `Trace` (`key -> [(val, time,
  diff)]`) held in sequence-numbered batches that are merged geometrically. One arrangement can feed any number of
  joins and reduces; each reader has a cursor and only sees batches it has consumed (this is what stops a bilinear
  join from double counting). **Logical compaction:** when batches merge, every time is advanced to the lower
  envelope of what readers still need (`Time.advanceBy(frontier)`), and entries that collapse are summed and dropped
  when they cancel. A join reading trace A uses B's input frontier as A's compaction frontier (and vice versa); a
  reduce uses its own input frontier. `Trace.size()` / `Arranged.traceSize()` / `Worker.stateSize()` expose the
  size metric; a test pushes 30k updates with retractions through a 40-key count and asserts the trace stays below
  `6 * keys`.
* **join** is bilinear, `d(A join B) = dA join B + A' join dB`, over arrangements: each batch is joined against the
  other side's trace as consumed so far, then marked consumed, so every pair of updates is produced exactly once; the
  output time of a pair is the lattice **join** of the two times. `semijoin`, `antijoin` (= self - semijoin with the
  `distinct` keys) are built on it.
* **reduce** (per key, partially-ordered-time correct): the operator tracks the join-closed set of times at which a
  key's input accumulation can change. A time becomes *pending* when an update at or below it arrives, and is
  evaluated once complete: the input accumulated at that time goes through the user logic, and the operator emits
  `desired - (what it already emitted at times <= t)` at time `t`. So the output is always the diff against what
  was previously emitted. Pending times are held as capabilities, so downstream cannot finish them early.
  `count`, `distinct`, `threshold` are one-line reduces.
* **iterate** creates a scope with `Product<outer, LongTime>` times. The loop variable is
  `seed + feedback(body(variable) - seed)`; the feedback edge increments the iteration counter, so the variable at
  iteration `i+1` is `body(variable at i)`. `leave` drops the counter; summing over all iterations of one outer time
  gives the fixpoint. A change at a later outer time re-uses the trace of earlier outer times (their updates sit at
  `(t_old, i)`, which are *not* `<=` the new `(t_new, 0)`, so iteration restarts from the seed and only the
  difference to the old iterations is recomputed).

## What is implemented

`map filter flatMap concat negate consolidate inspect probe capture`, `keyed arrange join semijoin antijoin reduce
count distinct threshold`, `iterate` (one loop variable, `IterScope.enter`), `InputHandle` with antichain capabilities,
`Probe`, `Antichain`, `LongTime`, `Product`, shared `Trace`s with compaction, single-threaded `Worker`.

## Not implemented / known limitations

* **No multi-worker mode.** Everything runs on one thread in one `Worker`. Data exchange by key hash and a
  cross-worker progress protocol were not attempted. (`Worker` is not thread-safe.)
* One loop variable per `iterate` (mutual recursion needs a tuple-typed variable). No iteration cap: a body that
  does not converge never quiesces (`Worker.setMaxPassesPerStep` is a guard). Bodies should end in `distinct()` /
  a reduce so the variable is a set.
* Operators are stepped in creation order with a global frontier recomputation per pass (O(queued messages +
  operators) each) - simple and correct, not tuned for very large graphs.
* Trace maintenance is batch-merge based: a merge costs O(merged batches). Geometric merging amortizes this but
  a trace is only compacted when batches merge, so an idle trace keeps its stale (uncompacted) older batches.
* Reduce state per key (`known` times, emitted history) is compacted when the key is touched, not by a sweep.
* Reduce logic sees input values in unspecified order and must be a pure function. Antijoin/distinct assume the
  *key* collection has non-negative multiplicities (negative counts are treated as absent).
* No persistence, no spill to disk, no `Arranged.enter` (arrangements are not imported across scopes: enter the
  collection and re-arrange).
* Data records need correct `equals/hashCode` (records recommended); no serialization is involved.

## Tests

`RandomizedTest` (8 tests; 300 seeded cases each by default, also run at 4000 cases x 10 epochs during development):
join, self-join on a shared arrangement, count/min/sum/distinct on one shared arrangement, semijoin/antijoin,
stateless chain + consolidate, iterate reachability and connected components (retractions that disconnect), and
`Product`-time join/count/distinct with incomparable times; every completed epoch / lattice time is compared with a
naive from-scratch recomputation. `ClassicExamplesTest`: word count, reachability, connected components against
BFS / union-find. `LatticeAndFrontierTest`: lattice laws, antichain dominance, frontier monotonicity, "no emission
at an incomplete time". `CompactionAndPerfTest`: trace-size bounds, rough throughput.

## Performance notes (rough, one laptop, JDK 23, single thread, printed by `CompactionAndPerfTest`)

* count over a 1000-word vocabulary: roughly 1M input updates/s.
* join of two 5000-key arrangements: roughly 400k input updates/s.
* reachability on a 2000-node / 6000-edge random graph: initial computation about 50 ms; a single-edge
  insert+retract epoch about 0.5 ms.

These are order-of-magnitude figures from a short, unwarmed test, not benchmarks.
