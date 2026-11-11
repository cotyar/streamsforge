# actors-spi — a turn-based virtual-actor SPI, three ways

`actors-spi` is the Orleans-grain shape in about 150 lines of Java 21: keyed actors that always
"exist", are activated by the first message, run **one turn at a time**, and are collected when idle.
Everything above it (the SQL streaming host) is written once against `ActorRuntime`; three
interchangeable runtimes implement it:

| module | mailbox / threading | class |
|---|---|---|
| `actors-futures` | queue + `scheduled` flag per activation, turns hop onto an `Executor`, next turn chained on the previous turn's stage | `FuturesActorRuntime` |
| `actors-loom` | one JDK 21 virtual thread per activation, blocking queue, a turn's stage is *joined* | `LoomActorRuntime` |
| `actors-pekko` | Pekko typed actors: shard actors (placement/passivation) + one entity actor per activation, stash while busy | `PekkoActorRuntime` |

All three pass the same abstract conformance suite
(`actors-spi/src/testFixtures/.../ActorRuntimeConformance.java`) **unmodified**.

```
cd java && ./gradlew :actors-spi:build :actors-futures:test :actors-loom:test :actors-pekko:test
```

## The contract in prose

The Javadoc on the SPI types is normative; this is the same thing in one place, plus the decisions the
Javadoc leaves open (marked **decision**) which the conformance suite pins down.

* **Identity.** `ActorKey(type, id)` names an actor forever. `ref(key)` is just a handle: it never
  activates, holding one keeps nothing alive. The first message activates. Unknown type -> `ref` throws;
  registering a type name twice throws.
* **Activation state** comes from `ActorType.stateFactory(key)`, one object per activation, only ever
  touched by one turn at a time (so plain fields are fine; the suite checks visibility too).
* **Turns.** A turn is `Turn<S,R>.run(ctx)` returning a `CompletionStage`. The next turn of the same
  activation starts only after that stage has **completed** — a turn that awaits another actor keeps
  its own mailbox closed (non-reentrant, exactly an Orleans grain). Consequence, also Orleans': A -> B -> A
  deadlocks; nothing detects it.
* **Ordering.** `tell` and `ask` from one sender thread are FIFO. This holds across deactivation: messages
  queued behind a deactivation are handed to the next activation in order.
* **Failures.** A turn that throws (checked or not) or returns a failed stage fails *that ask*, with the
  original exception object as the cause (no wrapping). The activation and its state live on. A failing
  `tell` or timer tick goes to `onUnhandledError`.
* **Lifecycle.** If the state implements `ActorLifecycle`, `onActivate` is the first turn of an
  activation (before any message) and `onDeactivate` the last (idle, explicit, `deactivateOnIdle`,
  `close()`). Both may be async. **decision:** if `onActivate` throws/fails, the triggering call fails
  with that error, *no turn runs*, the half-built state is dropped, `onDeactivate` is **not** called (it
  never activated), and the next message tries again from a fresh state. Failures of `onDeactivate` are
  reported to `onUnhandledError` and deactivation proceeds.
* **Timers.** `startTimer(name, initialDelay, period, tick)` delivers each tick as an ordinary turn.
  A tick that would fire while the previous tick is still queued/outstanding is coalesced (dropped).
  Starting a timer under an existing name replaces it; **decision:** once `cancelTimer`/replace has
  returned, no tick of the old timer runs, even one already queued. Timers die with the activation and do
  **not** count as activity for idle collection, nor resurrect a dead activation.
* **Idle collection.** After `idleTimeout` without a *message* (timer ticks do not count) the activation
  is deactivated; the next message activates a fresh one with fresh state. `null` timeout = never.
* **`deactivate(key)`** is ordered after what is already queued and the in-flight turn; the returned stage
  completes when the activation is gone (`onDeactivate` finished). No-op if not active.
  **`ctx.deactivateOnIdle()`** requests the same once the current turn completes.
* **No message loss across deactivation** (the hard part): a message sent while an activation is
  deactivating or just deactivated runs on a *fresh* activation — never dropped, never run on the dead one,
  never concurrently with the old one's `onDeactivate`. Hence "exactly once per message" even with
  continuous random deactivation (see the stress tests).
* **`close()`** deactivates everything (running `onDeactivate`, waiting for async ones), then stops the
  runtime; idempotent. **decision:** after `close()`, `ask` returns a failed stage / `tell` reports to the
  error handler; nothing hangs and nothing runs. Do not call `close()` from inside a turn.

### One semantic difference between runtimes

`ctx.deactivateOnIdle()`: *futures* and *loom* deactivate immediately after the current turn and forward
everything queued behind it to a fresh activation. *Pekko* passivates through its shard: messages the shard
already delivered to the entity (ahead of `Stop` in its mailbox) are still processed by the **old**
activation before `onDeactivate`; only later ones go to the fresh one. Both are loss-free and both satisfy the
SPI text ("once the current turn completes"); the conformance test therefore asserts no loss + eventual
deactivation, not where each message runs. The same holds for `deactivate(key)`: it is ordered after
everything that was delivered before it.

## How each runtime maps it

### futures — `FuturesActorRuntime`

* Per activation: an `ArrayDeque` + `scheduled`/`dead` flags behind one tiny monitor (held for a few field
  writes, never across user code). First message into an idle mailbox flips `scheduled` and submits
  `drain()`. `drain()` runs turns in a loop (budget of 64, then yields the pool thread).
* Async turn: `drain()` returns *leaving `scheduled = true`*; the stage's `whenComplete` callback resubmits
  `drain()`. No thread ever blocks. A stage that is already complete is consumed inline (fast path).
* **Deactivation race.** `deactivate` and idle are *control tasks in the same queue*. When the control task
  runs, `onDeactivate` runs (async ok); then the activation is retired **inside
  `ConcurrentHashMap.compute`**: under the bin lock it is marked `dead`, its remaining queue is drained and
  installed — in order — as the initial queue of a successor activation in the same slot. A sender that raced
  gets `offer == false` from the dead activation, parks on a no-op `compute` of the same key (which waits for
  the retire to finish), and retries. Messages that arrived during an async `onDeactivate` simply sit behind
  it. Lock order is always map-bin -> activation-monitor, never the reverse.
* Idle: one pending scheduled check per activation (single scheduler thread); it re-arms for the remaining
  time while the lease has not expired, and only queues a control task if the mailbox is empty and no message
  arrived. An idle control task that finds the activation busy is discarded.
* Default executor: **async-mode `ForkJoinPool`**, CPU-count parallelism, daemon threads — short continuations,
  work-stealing, cheap submit-from-worker for actor -> actor calls. A turn that blocks a pool thread starves
  the pool (no compensation); pass your own `Executor` to change that.

### loom — `LoomActorRuntime`

* One virtual thread per activation, started on first message; it loops on a `LinkedBlockingQueue`. A turn's
  stage is joined — straight-line code, so non-reentrancy needs no machinery at all.
* Idle collection is `poll(remainingLease)`. Timer ticks wake the poll but don't renew the lease. When the
  lease expires and the queue is empty the thread retires the activation.
* Deactivation race: same retire/successor protocol as futures, with a `ReentrantLock` per activation guarding
  `dead` + enqueue. The thread is *busy* in `onDeactivate`, so messages arriving then queue up and are forwarded.
* No `synchronized` anywhere (`ReentrantLock` only, no blocking inside it). Ran the whole suite plus stress
  tests with `-Djdk.tracePinnedThreads=short` on JDK 23: **no pinned-thread reports**.

### pekko — `PekkoActorRuntime`

* Caller -> **shard actor** (`2 x cores`, chosen by key hash) -> **entity actor** (one per activation, a child of
  its shard). Entities are `ready` / `busy` / `deactivating` behaviors. An async turn does `pipeToSelf` and
  switches to `busy`, which `stash`es everything else; `TurnDone` -> `stash.unstashAll(ready)`.
* **Deactivation race = Pekko Cluster Sharding's passivation protocol.** An idle entity (receive timeout) or
  `deactivateOnIdle` sends `Passivate` to its shard; `deactivate(key)` is a `DeactivateKey` message. The shard
  marks the slot *stopping*, sends `Stop`, and from then on **buffers** work for that key. Everything it had
  delivered earlier is ahead of `Stop` in the entity's mailbox. The entity runs `onDeactivate` and stops; the
  shard sees `Terminated` (`watchWith`), removes the slot, and if anything was buffered, spawns a fresh entity
  and replays the buffer in order. The shard is single-threaded, so none of this needs a lock, and the old
  entity is provably dead before the new one sees a message.
* Idle = `setReceiveTimeout`; on timeout the entity re-checks its own `lastActivity` before passivating.
* **Finding:** Pekko's `TimerScheduler` envelope resets the receive timeout, so it *does* keep an activation
  alive — wrong for our timer semantics. Ticks are therefore scheduled with `ctx.scheduleOnce` (re-armed as
  each tick starts, so at most one outstanding tick = coalescing) and the tick message implements
  `NotInfluenceReceiveTimeout`. A generation counter makes stale ticks no-ops after cancel/replace.
* Stash capacity is `Integer.MAX_VALUE`: a bounded stash overflows with an exception, i.e. would lose messages.

## Trade-offs

| | futures | loom | pekko |
|---|---|---|---|
| Programming model for runtime author | callbacks, explicit state machine; most subtle code | straight-line; least code | actor protocol; passivation is a recognized pattern |
| Threads | pool of N platform threads | N carriers + a virtual thread per **active-or-parked** activation | dispatcher threads |
| Memory per idle activation | queue + monitor (smallest) | virtual thread stack (grows with call depth) + queue | actor cell + stash + 3 behaviors (largest) |
| Blocking user code in a turn | starves the pool | fine (parks) unless `synchronized`+blocking pins | starves the dispatcher |
| Per-message cost | 1 queue op + 1 pool hop, inline fast path | 1 queue op + virtual-thread wakeup | 2 mailbox hops (shard, entity) |
| Deactivation correctness argument | `compute` handover + `dead` flag (two-lock discipline) | same, plus thread-owned `onDeactivate` | single-threaded shard, `Terminated` barrier |
| Ecosystem | none | none | supervision, remoting, cluster sharding are one step away |
| Dependencies | none | none | Pekko + Scala 2.13 runtime library |

Rule of thumb: **futures** if throughput and footprint matter and the turns are non-blocking; **loom** if
turns want to read like blocking code (SQL operators calling out) and you accept a thread per live activation;
**pekko** if this is the first step toward distribution, because the shard tier is the cluster-sharding
shape already.

## Benchmark

`./gradlew :actors-<x>:test -Dbench=true --tests '*Benchmark*' -i` (off by default). Crude single-JVM
wall-clock numbers, one warm-up pass then one measured pass, one run each — compare *between rows*, not
against anything else. **Machine:** Apple M1 Max (10 cores), 32 GB, macOS, JDK 23.0.2 (bytecode level 21),
**2026-11-11**.

| runtime | ping-pong ask, 8 clients (ops/s) | pipelined ask, 1M over 1k keys (ops/s) | activate 100k (ms) | 100k second pass (ms) | A->B->C async chain (us/chain) |
|---|---:|---:|---:|---:|---:|
| futures | 239,273 | 3,122,772 | 122 | 46 | 23.5 |
| loom | 229,611 | 3,105,233 | 141 | 126 | 30.5 |
| pekko | 177,238 | 4,359,090 | 133 | 30 | 36.8 |

Reading (explanations are hypotheses, not profiled): sequential ask latency is probably dominated by the cross-thread hand-back (futures/loom close; pekko pays
two mailbox hops); when the producer pipelines, pekko's batched mailbox processing wins; loom's second 100k
pass is slower, probably because every message wakes a parked virtual thread.

## What the conformance suite covers

No two turns overlap (many threads, sync + async turns, checks visibility of plain fields); per-sender FIFO
for `tell`/`ask` including across continuous deactivation; non-reentrancy of an async turn awaiting another
actor; A -> B -> C chains; failure isolation (thrown, checked, failed-stage, async-failed; state survives,
exception identity preserved); `tell`/timer failures reach `onUnhandledError`; lifecycle ordering (async
hooks, failed `onActivate`); idle collection and fresh-state reactivation; keep-alive under activity;
timers (turns, coalescing, cancel/replace, not keeping alive, dying with the activation, killed by explicit
deactivation); `close()` idempotent and running `onDeactivate` everywhere (waits for async ones; use after
close fails cleanly); **deactivation-race stress** — 16 threads x tell/ask/async-ask against 4 and 64 keys
with chaos threads deactivating continuously, a lockstep variant with short queues (~4k re-activations per
run), and an idle-collection variant (2 ms timeout, ~3.7k re-activations) — each asserting an external
count of executed turns equals messages sent (exactly once), no overlap of any turn *or lifecycle hook* of one
key across activations, at most one live activation per key, and activations == deactivations after
`close()`; 100k activations fan-out. Mutation check: dropping the leftover queue on retire in the futures
runtime fails six of these tests.

## What's missing vs Orleans

* **Distribution / clustering**: single process. No silo membership, directory, placement, gateways, remote
  calls, rebalancing. (Pekko makes this the easiest to grow: the shard tier maps onto cluster sharding.)
* **Placement strategies** and **locality** (random/hash/prefer-local/activation-count): fixed hash (pekko) or
  none (futures/loom).
* **Persistence**: no `IPersistentState`, no storage providers, no write-behind; state is plain in-memory
  objects. (The C# host's grain storage has no analogue here yet.)
* **Streams / pub-sub**, reminders (durable timers that survive deactivation), and **stateless workers**.
* **Reentrancy options**: only non-reentrant. No `[Reentrant]`, `[AlwaysInterleave]`, `[MayInterleave]`
  allowlists (CLAUDE.md hard rule 3), no `RequestContext`.
* **Call-chain deadlock detection**: A -> B -> A just hangs. Orleans has call-chain reentrancy and a
  per-request timeout; neither exists here — no `ask` timeout at all.
* **Backpressure / overload shedding**: mailboxes are unbounded; no per-grain queue limits or load shedding.
* **Grain observers**, interceptors/filters, grain extensions, versioning, `GrainReference` serialization.
* **Supervision policy**: a failed turn never kills an activation (by design); there is no
  restart/backoff vocabulary (Pekko's supervision is deliberately not exposed).
* **Observability**: no metrics/tracing hooks beyond `onUnhandledError` and the diagnostic `activeKeys()`.
* Ordering guarantee is per sender *thread*; there is no cross-actor causal ordering and no exactly-once
  across crashes (there are no crashes; it is one JVM).
