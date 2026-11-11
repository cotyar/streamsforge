# java/host — a miniature StreamsForge host, written once on a virtual-actor SPI

`StreamsHost` is the sources → pipelines → tables core of the Orleans host (`orleans/src/StreamsForge.Host/Grains/`),
including **coordinator mode** (tables with parallelism ≥ 2 over the engine's partitioned dataflow), written **once**
against `actors-spi` and run unmodified on all three runtimes (`FuturesActorRuntime`, `LoomActorRuntime`,
`PekkoActorRuntime`). SQL is `java/engine`. No Orleans, no REST, no auth, no persistence.

```
cd java && ./gradlew :host:test                                   # 66 tests (22 scenarios x 3 runtimes), ~4 s
./gradlew :host:test -Dbench=true --tests '*HostBenchmarkTest*' -i  # throughput table
```

```java
var host = new StreamsHost(new LoomActorRuntime());                 // or Futures / Pekko
host.createSource("trades", schema).toCompletableFuture().join();
host.createTable("totals", "SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol", /*parallelism*/ 4);
host.start("totals");
host.publish("trades", rows);
host.subscribeTable("totals", batch -> ...);                        // TableDeltaBatch(epoch, deltas)
host.snapshot("totals");                                            // canonical row key -> RowWeight
host.quiesce(Duration.ofSeconds(30));                               // test/bench hook: everything processed
```

## Actor catalog

Every type is registered with `idleTimeout == null` (see *Idle collection*).

| type | key | state | Orleans analogue |
|---|---|---|---|
| `registry` | `default` | name → source/pipeline/table record, compiled plans, running flags | `RegistryGrain` |
| `source` | `{name}` | schema, `accepting`, subscriber list | the source's stream + driver |
| `pipeline` | `{name}` | `PipelineExecutor`, result subscribers, watermark timer | `PipelineGrain` |
| `table` | `{name}` | **P=1:** `TableExecutor`, subscribers, per-upstream epoch cutoffs (the table's *view*). **P≥2:** lifecycle coordinator only | `TableGrain` |
| `table-ingest` | `{table}:{input}` | pending deltas, epoch counter, flush timer (P≥2 only) | `TableIngestGrain` |
| `table-stage` | `{table}:{stageId}:{partition}` | `ITableStageExecutor`, `FrontierTracker`, `EpochBuffer` (P≥2 only) | `TableStageGrain` |
| `table-output` | `{table}` | terminal `FrontierTracker` + `EpochBuffer`, consolidation ledger, subscribers — the P≥2 table's *view* | `TableOutputGrain` + the coordinator half of `TableGrain` (`OnOutputBatchAsync`, ledger, `AttachSnapshotAsync`) |

Entity names are SQL identifiers, so `:` is a safe key separator. Sources, pipelines and tables share one namespace; a
pipeline reads sources only (hard rule 6), a table reads sources, pipelines (as a stream relation whose schema is the
pipeline's output schema) and other tables.

## Message flow

```
publish(rows) ──ask──▶ source ──(consumer closure, tell-style)──▶ pipeline ──▶ pipeline subscribers
                         │                                              └──▶ table / ingest (table-over-pipeline)
                         ├────────▶ table (P=1)  ── exec.onTableDeltaBatch ──▶ subscribers: user, downstream tables
                         └────────▶ table-ingest ─┐  (one per table input; epoch every flushPeriod / flushThreshold)
                                                  ▼  batch per (edge, epoch), one to EVERY partition, empty = marker
                                      table-stage[s,p] ──▶ table-stage[s',q] ──▶ … ──▶ table-output ──▶ subscribers
```

Two kinds of hop, and the difference is the whole deadlock story (rule 3):

* **awaited `ask`** — only *down* the dataflow and only to actors that never call back up: ingest → stage → … →
  output, and the registry → entity start/stop calls. A flush turn of an ingest awaits the whole chain, so when it
  completes, epoch *e* has reached the output actor, and *e+1* cannot start before it (the ingest is its own backpressure).
* **accounted fire-and-forget** (`HostEnv.send`: `ref.ask(turn)` whose stage is *not* awaited, counted in an
  `inflight` counter) — every *publisher → subscriber* hop (source → pipeline/table/ingest, pipeline → table, table →
  downstream table/ingest, timer tick → own flush). A publisher therefore never waits for a subscriber, which is what
  makes the late-attach protocol below deadlock-free (a subscriber that is mid-`start`, awaiting the publisher, would
  otherwise sit in a cycle with the publisher awaiting it). It is the in-process analogue of an Orleans stream.

**Rule 3 by construction.** `RegistryGrain` is non-reentrant with a `[MayInterleave]` allowlist because entities call
back into it. Here the registry hands every entity a complete immutable spec (`Specs`) and *no entity ever calls the
registry*, so there is no cycle to allowlist. For the same reason the P≥2 `table` actor is **not on the data path**:
in Orleans `TableGrain.OnOutputBatchAsync` needs `[MayInterleave]` because a `StopAsync` turn awaiting an ingest that
awaits a flush chain that ends in the same grain deadlocks. Here the chain ends in `table-output`, which calls nobody.

## Epoch / frontier protocol (P ≥ 2)

1. **Ingest** buffers deltas (stream rows `+1`, upstream-table deltas with their own weight) and cuts epoch `e` (a
   per-ingest counter) on a timer or at `flushThreshold`. For each edge `plan.edgesForExternalInput(input)`: a
   *Broadcast* edge goes to every partition of the consuming stage (`fromPartition` = the input's index among the
   edge's external inputs); any other edge targets an Ingest-kind engine stage, and the real routing decision is that
   stage's one forward edge (`outEdgeOf`), hash-partitioned with `plan.partitionOf(edge, row)` (FNV-1a over the
   canonical join/group key = `ExchangeRouter`). **Every** downstream partition receives a `DeltaBatch` for `(edge, e)`
   — an empty marker if it got no rows — so a quiet ingest cannot stall a downstream frontier.
2. **Stage `(s, p)`** registers one `UpstreamId(edge, fromPartition)` per producer of each inbound edge, buffers each
   arriving batch (`EpochBuffer`) and observes it in its `FrontierTracker`. The *combined* frontier is the **min over
   all registered upstreams** (an upstream that never reported holds it at −∞). When it advances to `F`, the buffer
   releases every batch with `epoch ≤ F` sorted by `(epoch, edgeId, fromPartition)` — a function of the batches, not of
   arrival order — and each is fed to the executor. Output deltas are routed by the stage's single outbound edge
   (`LOCAL` = same partition, `HASH_PARTITION`, `GATHER`) and every downstream partition gets a batch **stamped `F`**
   (marker if empty). The push awaits those pushes.
3. **Output** runs the same machinery over the terminal stage's partitions. When the min frontier reaches `F` it nets
   the released batches by canonical row key (zero nets dropped, so a row one partition retracts and another
   re-asserts in the same logical epoch is never published as two messages), folds the net into the ledger, sets
   `lastEpoch = F` (also when the net is empty) and publishes one `TableDeltaBatch(F, net)`.

Epochs of one output strictly increase. A P=1 table stamps its output with `exec.lastEpoch()`, which also increases
per admitted batch; both flavours therefore expose the same `(snapshot, lastEpoch)` contract documented on
`TableExecutor.lastEpoch()`.

### Table over table (late attach)

A downstream table `D` attaching to a running upstream `U` (`InputWiring.attachTable`, used by `TableActor` for P=1
and by `TableIngestActor` for P≥2):

1. **subscribe** to `U`'s view first — `U`'s publish turn enqueues each batch into `D`'s mailbox (queued *behind* the
   start turn that is attaching);
2. then read `U`'s **atomic `(rows, lastEpoch)`** pair (one turn of `U`; `U`'s ledger only changes inside turns);
3. `D` applies the rows as one batch and records `cutoff[U] = lastEpoch`;
4. every later delivery with `epoch ≤ cutoff` is skipped (already inside the snapshot), the rest are applied.

No gap (subscribed before the pair was read) and no double count (cutoff). `StreamsHost.attachTable` exposes the same
protocol to a user consumer. A mutation check confirms the tests bite: deleting the cutoff line makes the late-attach
tests fail.

### Quiescence (`StreamsHost.quiesce`)

Fire-and-forget hops and timer-driven flushes make "is everything processed?" a real question, and tests must not
sleep. `HostEnv.inflight` counts every accounted message until its turn completes. `quiesce` loops: wait for
`inflight == 0`; for each running P≥2 table read every ingest's epoch counter, pick `E = max + 1` and `flush(E)` every
ingest of the table to that **one epoch number** (epochs must increase, not be dense) — afterwards every stage's
min-frontier is ≥ E, so the whole chain has drained to the output actor; repeat until a pass flushed nothing and
nothing is in flight (a table's output feeds downstream tables' ingests). It is a hook for tests/benchmarks and must
not be called inside a turn.

## Lifecycle semantics

* `start`/`stop` on a **source** toggles whether `publish` is accepted; on a **pipeline/table** it activates/drops the
  executor state. `stop` discards state (nothing is persisted) and a restarted table is empty — there is no replay of
  rows published while it was stopped. Stage and ingest activations are released with `runtime.deactivate(key)`.
* `delete` refuses an entity something else reads; a running entity is stopped first; the name is then free.
* A table cannot **start** before the tables it reads are running, and cannot **stop/delete** while a running table
  reads it (a restarted upstream would re-emit its rows into a downstream that still holds the old ones; the original
  handles this with a `rebuilding` flag, this host just refuses).
* Invalid SQL, collisions, unknown names, `parallelism < 1` and a plan the partitioned dataflow cannot run at P≥2
  (derived table in FROM, UNION, CROSS JOIN) are refused with a `CatalogException` that carries the engine's
  `SqlDiagnostic`s.
* Pipeline watermarks advance on an actor timer (`Options.watermarkPeriod`, clock `Options.clock`, both injectable) or by
  `advanceWatermark(pipeline, nowMs)`, the manual hook the tests use.

### Idle collection

**Every actor type is registered with `idleTimeout == null`.** An entity's state *is* its activation — a table's
executor, a stage's frontier and `EpochBuffer`, an output's ledger — and nothing is persisted, so collecting any of
them would silently lose rows (the original relies on `DelayDeactivation(365d)` + persisted state). What *is* lost, and
only on purpose: `stop`/`delete` drop the state; closing the runtime runs `onDeactivate` and drops everything. A test
(`runningEntitiesAreNeverIdleCollectedAndStopReleasesTheirActivations`) asserts a quiet spell collects nothing and that
`stop` releases the per-(stage, partition) and ingest activations. Pekko's receive-timeout/passivation, loom's
retire-on-lease-expiry and futures' scheduler never fire because no timeout is configured.

## Simplifications vs the Orleans original

* **No persistence** (no `IPersistentState`, no snapshots, no `Replay` log): state dies with the activation.
* **Single process**: no silo, directory, placement, gateways. (Pekko would grow into cluster sharding; the others would
  not.)
* **No environments, auth, entitlements, approvals, audit, REST, SignalR, gRPC, config import/export.**
* **No source replay / late-consumer buffer** (plan 023/026): a consumer sees only what is published after it subscribed.
  Table-over-table *does* backfill (snapshot + epoch cutoff); table-over-source and table-over-pipeline do not.
* **No shared arrangements** (plan 003 M3): an "arrangeable" edge is an ordinary hash-partitioned fan-in; two tables over the same
  source each keep their own join state.
* **No retention policies, sharded tables, `replayFrom`, CRDT, connectors, generators, ingest buffer/dedup.**
* The P≥2 **ledger/subscribers live in `table-output`**, not in `table` (see rule 3 above), so a P≥2 table's coordinator
  is not queried for data; the host resolves the view actor from the table's parallelism.
* `stop` of a table with a running dependent is refused instead of handled with a rebuilding handshake.
* `publish` does not validate row values against the source schema (the engine coerces / tolerates); the schema feeds the
  SQL compiler only.
* Events are shared **by reference** across all consumers of a source (Orleans serializes them). The engine never mutates
  an input row; the equivalence tests would catch it if it did.
* The flush cadence (`flushPeriod`, default 250 ms like `TABLES__FLUSHMS`) is the end-to-end latency floor of a P≥2 table;
  tests use 10 ms plus `quiesce`.

## Per-runtime observations

* All three pass every scenario unmodified, including 8 concurrent publishers, publish-while-attach with the cutoff
  mutation check, and a stop that deactivates dozens of stage/ingest activations. No flaky run in 6 consecutive full
  runs, 3 runs with `-XX:ActiveProcessorCount=2`, and 3 runs next to 12 busy-loop processes.
* **Throughput is engine-bound, not runtime-bound**: with batches of 1000 the hop count per event is tiny, and the
  per-row cost is `TableExecutor` / `PipelineExecutor` (and, for P=4, the extra `partitionOf` expression evaluations and
  the per-epoch marker fan-out). The three runtimes land within ~10 % of each other in every cell; the differences are
  inside run-to-run noise (not statistically measured). Per-message costs from the SPI README (pekko pays two mailbox
  hops) would show with small batches; this benchmark deliberately does not.
* **P=4 is slower than P=1** here: one JVM, one 100-key aggregate, a single ingest actor feeding four partitions through
  three stages — the exchange and the epoch coordination cost more than the parallelism gains. It is a protocol model, not
  a speed-up claim.
* **loom** runs a turn that awaits another actor as straight-line parked code; **futures** never blocks a pool thread in
  this host (all waits are stage compositions); **pekko** stashes while an async turn is in flight. None of this was
  visible to the host code.
* No SPI quirk bit us. The Pekko `deactivateOnIdle`/timer notes from `actors-spi/README.md` are moot: no type uses
  `deactivateOnIdle`, and timers (pipeline watermark, ingest flush) only ever enqueue a message — the tick itself does no
  work — so coalescing a tick is harmless.

## Benchmark

`./gradlew :host:test -Dbench=true --tests '*HostBenchmarkTest*' -i` (off by default; `-Dbench.events=N` to resize).
400 000 events, batches of 1000, 100 group keys, one publisher thread with ≤ 64 batches in flight, ingest flush 20 ms;
time = first publish until `quiesce()` returns (for the pipeline also the final watermark advance and delivery). One
warm-up pass on a fresh host, one measured pass per cell, one run — compare *between rows*, not against anything else.
**Machine:** Apple M1 Max (10 cores), 32 GB, macOS, JDK 23.0.2 (bytecode level 21), **2026-11-11**.

| scenario (events/s) | futures | loom | pekko |
|---|---:|---:|---:|
| source → GROUP BY table, P=1 | 623,650 | 690,095 | 658,622 |
| source → GROUP BY table, P=4 | 448,060 | 472,514 | 450,968 |
| source → tumbling-window pipeline | 2,107,548 | 2,232,145 | 2,259,113 |

(An earlier identical run: 630,340 / 692,613 / 670,951; 427,022 / 450,449 / 450,748; 2,187,324 / 2,207,601 / 2,296,265.)

## Layout

`StreamsHost` (public API, options, `quiesce`) · `RegistryActor` · `SourceActor` · `PipelineActor` · `TableActor` ·
`TableIngestActor` · `TableStageActor` · `TableOutputActor` · `InputWiring` (subscribe / late-attach protocol) · `Publisher`
(in-actor subscriber list) · `HostEnv` (`send`/`exec`, `inflight`, error queue) · `Specs`, `Actors`, `TableView`,
`RowPublisher`, `Async`, plus the public records `TableDeltaBatch`, `TableAttach`, `Subscription`, `CatalogException`.

Tests (`src/test`): `HostScenarioTest` (1, 2), `HostDataflowTest` (3, 4, 5), `HostLifecycleTest` (6 + idle collection),
`HostBenchmarkTest`; `Fx` is the fixture (runtime factories, blocking helpers, engine-only oracle) and `@OnAllRuntimes`
the meta-annotation that parameterizes a scenario over futures / loom / pekko.
