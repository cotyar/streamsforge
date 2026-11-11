# StreamsForge — Design Decisions (Orleans implementation)

ADR-style record of the choices that shaped the codebase, with the rejected alternatives and the
known ceilings. Structure lives in [ARCHITECTURE.md](ARCHITECTURE.md); phase-by-phase execution
history in [`../plans/`](../plans/README.md).

## D1 — Pure-C# engine, runtime at the edges
The SQL compiler, both executors, and the dataflow primitives have **zero Orleans dependencies**.
Orleans appears only in Abstractions (serialization attributes) and Host (grains/transport).
*Why*: testability (400+ engine tests run without a cluster), and runtime portability — a second
host (Dapr or anything else) reuses the entire semantic core. This boundary is the single most
load-bearing decision in the repo; do not let runtime types leak inward.

## D2 — Interpreted operators, no codegen
Plans compile to interpreted operator chains, not IL/expression-tree codegen. *Why*: demo-scale
throughput is generator-bound anyway; interpretation keeps diagnostics, determinism tests, and the
M1 op decomposition tractable. *Ceiling*: per-event dispatch overhead; revisit only with evidence.

## D3 — DBSP/epochal dataflow, not full differential (the "no recursion" pact)
Partitioned tables use unitemporal epochs + frontiers (DBSP/Feldera model, same lineage as
TanStack DB's d2ts), **not** Materialize-style multitemporal timestamps. *Why*: multidimensional
time exists to support *iterative/recursive* dataflows; our dialect deliberately has no recursive
CTEs, so epochs suffice and the progress protocol stays simple enough to property-test.
*Consequence*: recursion stays out permanently (bounded traversals unroll into joins; "latest per
chain" is `LATEST BY` + row history). Reversing this decision means replacing the protocol layer —
plan 003 documents the full argument.

## D4 — Z-set weights everywhere in table mode
Every table change is a weighted delta; correctness under *updates* (retraction cascades through
joins, subtractable aggregates, multiset MIN/MAX) is structural, not bolted on. Semi/anti joins are
presence-based (refcounted keys), so duplicates never fan out and the last retraction flips
dependents. This is also why history, SignalR reconciliation, and the partitioned path compose:
they all speak deltas.

## D5 — Frozen public contracts, additive-only evolution
`PublicApi.cs`, existing Abstractions members, and `web/src/api/types.ts` are frozen; changes are
additive (next `[Id]`, nullable/optional fields). *Why*: it let a dozen concurrent subagent tracks
build against stable seams without merge chaos, and it keeps generated clients honest. The same
discipline produced M1's headline property: all pre-existing tests pass **unmodified** through the
operator-decomposition refactor (façades preserve behavior byte-for-byte).

## D6 — Dynamic protobuf with a persisted field-number registry
Typed gRPC surface is generated *at runtime* from entity schemas (descriptor factory + hand-rolled
wire encoder — C# protobuf has no `DynamicMessage`). Field numbers live in the registry
(`Active` + `Reserved`; numbers are never reused, removed fields reserve theirs forever), shared by
reflection, proto downloads, and the streaming payloads — so a client generated today still decodes
streams after schema edits. The typed-client "magic" is deliberately **standard-toolchain**: serve
a self-contained `.proto`, let protoc/Grpc.Tools do the typing. Runtime-emitted assemblies were
rejected (you'd get `dynamic` access — pointless).

## D7 — The delta stream is the event log (history without JournaledGrain)
Row history is a plain state grain consuming the table-delta stream, not Orleans event sourcing.
*Why*: JournaledGrain needs a log-consistency provider our JSON storage doesn't implement, and the
delta stream already *is* an ordered event log. Retention modes (All/LastN/FirstN/MinBy/MaxBy +
window) are a pure, grain-free class — unit-tested exhaustively.

## D8 — Rebuild-from-live over full checkpointing
On restart, persisted snapshots serve reads immediately while operator state rebuilds from live
traffic (`Rebuilding` flag). *Why*: honest scope — exactly-once replay logs are a different
project. Arrangements (M3) add periodic checkpoints because shared state amortizes the cost.
*Ceiling*: a table's aggregates reflect only post-restart traffic until warmed; documented, not
hidden.

## D9 — Parallelism is opt-in, classic path untouched
`Parallelism == 1` runs the original single-grain executor — zero risk to existing tables;
`≥ 2` deploys the stage-grain grid. Joins co-partition on the join key; scalar sides broadcast;
tables whose plans can't partition honestly stay classic. Reads gain a real guarantee
(`frontierEpoch`: all deltas ≤ F, none beyond — enforced by atomic per-epoch application, which
required fixing a genuine pre-existing stop-flush bug rather than claiming atomicity by convention).

## D10 — Grain reentrancy is allowlisted, never default
`RegistryGrain` (and the M4 coordinator callback) use `[MayInterleave]` with an explicit method
allowlist. *Why*: the Registry→Pipeline→Registry start cycle deadlocks without it; but blanket
`[Reentrant]` would let mutations interleave. Hard-won rule: any grain that orchestrates grains
which call back into it needs this treatment — check the allowlist before adding cycles.

## D11 — Streaming honesty rules (the semantic fine print)
Pinned, tested, and documented rather than fudged:
- Pipeline-mode subqueries (membership/scalar) must be **windowed**; their value is a rolling
  snapshot replaced at inner window close.
- `NOT IN` follows PostgreSQL's three-valued logic: TRUE for an empty subquery (even for a NULL
  operand), FALSE on a match, otherwise NULL (filtered) when the operand is NULL or the subquery
  yields a NULL — one NULL in the subquery filters every unmatched row. (Until 2026-11-11 it ignored
  subquery NULLs, a plan 004 deviation.) `SUM` over no non-NULL value is NULL, as in PostgreSQL.
- Aggregate JSON legs with `->` (raw node); `->>` is text, which aggregates skip, so the sum is NULL.
- `LATEST BY` keeps one row per key (older-timestamp arrivals ignored; upstream retraction drops
  the key — no multiset fallback history; that's what row history is for).
- Windows-in-windows: inner emissions carry the inner window's **end** timestamp.
- Late events: pipelines drop past watermark (bounded lateness); tables never drop — epoch-stamped
  at arrival.
- Table-mode `LEFT`/`RIGHT`/`FULL OUTER` joins are incremental, not window-deferred: an unmatched
  row is NULL-padded the instant it arrives, and that pad is *retracted* the instant a match shows
  up later (re-asserted if the last match disappears). A consumer watching the raw delta stream
  during cold start sees pad → retract → product chatter; only the consolidated state is meaningful.
  Pipeline mode dodges this by deferring pads to window eviction (D11, late-events rule above);
  table mode has no eviction to defer to, so it can't.
- A non-equi residual on a table-mode outer join (an `ON` conjunct that isn't itself an equality
  between the two sides) still works, at a cost: presence flips from a key-level O(1) check to a
  per-row rescan of the arriving side's own bucket against every already-indexed row on the other
  side — O(|left bucket| × |right bucket|) predicate evaluations per delta. Worth knowing before
  putting a residual on a hot join.
- Known hazard, documented not fixed: a table-mode outer join's null-padded rows all carry the same
  (NULL) value in the padded side's columns, so they all hash to the same partition on any
  downstream edge keyed on one of those columns — correct, but potentially skewed.
- `UNION ALL` runs in both pipelines and tables (plain concatenation — a row asserted by two
  branches shows up twice, weights summed in tables); plain `UNION`/`UNION ALL DISTINCT` is
  **tables-only** — pipelines have no Z-set weight to dedup with, so an unbounded distinct over an
  unbounded stream would be unbounded state; a pipeline-mode `UNION` gets a diagnostic naming
  `UNION ALL` as the fix. Accepted at the top level (optionally under `WITH`, recursing into every
  branch) and in derived-table position (`FROM (… UNION …) alias`); rejected inside
  `IN`/`EXISTS`/scalar subqueries — those synthesize their own joins and are the highest-risk
  surface for no benefit. Branches must have equal arity; kinds unify positionally (`Long`+`Double`
  → `Double`, everything else exact, `JSON`/timestamp only with themselves); output column names
  come from branch 0. A set-operation table is pinned to `Parallelism = 1` (a real N-ary merge
  stage is out of scope for this wave).
- **Type-conversion functions are total, never throwing** (plan 009 Round C wave C1): `TO_LONG`/
  `TO_DOUBLE`/`TO_BOOL`/`TO_TIMESTAMP`/`TO_STRING`, plus `CAST(expr AS type)` sugar desugaring to the
  same nodes. An unconvertible value yields NULL, not an error — a streaming operator can't throw per
  row without killing the pipeline for every other row, and `COALESCE` already covers "supply a
  default". This is a real tradeoff, not a footnote: a typo'd or malformed field silently becomes
  NULL and keeps flowing (visible only as a missing/zero contribution downstream, e.g. `SUM` skipping
  it) rather than surfacing anywhere. `TO_TIMESTAMP` accepts epoch-ms (numeric or numeric string) or
  ISO-8601 text; `TO_STRING` is culture-invariant everywhere (never the ambient culture). Three
  specific spots are worth knowing rather than assuming:
  - `TO_BOOL`'s string rule is **permissive, not a fixed spelling list**: `"true"`/`"false"` parse
    case-insensitively, and every OTHER non-empty, non-`"0"` string (garbage included, e.g. `"abc"`)
    coerces to `true`. This isn't a design choice made for the SQL function in isolation — it's
    `FieldValueCoercion.TryToBool`'s existing inbound-ingest rule, and the SQL function shares the
    exact same canonical implementation (`StreamsForge.Engine.Runtime.FieldValueConversion`) so the two
    can never drift apart. A stricter "true/false/0/1 else NULL" rule was the original intent but was
    dropped once it became clear it would mean two different bool-coercion rules for the same field
    kind depending on which code path hit it.
  - Double-to-`long` narrowing (`TO_LONG` on a double, or the `Long`/`Timestamp` coercion generally)
    is an **unchecked** cast, matching the same shared implementation's existing behavior: a double
    outside the `long` range does not come back NULL, it comes back as whatever the CLR's unchecked
    conversion produces. A numeric *string* that overflows `long` DOES come back NULL (`long.TryParse`
    fails outright) — only the direct-double path has this gap.
  - `TO_STRING` renders ISO-8601 text only when its argument is syntactically a `TO_TIMESTAMP(...)`
    call (which `CAST(x AS TIMESTAMP)` also produces, being the same desugared node) — a bare column
    declared `Timestamp`-kind does NOT get ISO-8601 rendering from a plain `TO_STRING(col)`, because at
    runtime it is represented identically to a `Long`-kind value (a bare CLR `long`, with no per-value
    type tag). Threading compile-time `FieldKind` through the runtime evaluator to fix this was out of
    scope for the three seams this wave touched (`Validator`/`ExpressionEvaluator`/`Parser`).
  - A JSON leaf reached via `->` is in scope and is the wave's whole reason to exist: `payload -> 'qty'`
    on a producer that quoted its numbers (`"qty": "10.5"`) is otherwise permanently stuck as a JSON
    string leaf — `SUM(TO_DOUBLE(payload -> 'qty'))` is what makes it summable. A composite JSON node
    (dict/list, from a non-terminal `->`) fed to `TO_STRING` renders as compact JSON text, same as
    `->>` would for that node.

## D12 — One process, JSON files, zero infrastructure
Localhost clustering, in-memory streams, one-file-per-grain JSON storage. *Why*: the demo must run
with `dotnet run` and nothing else. The Orleans programming model is production-shaped; a real
deployment swaps clustering/stream/storage providers without touching the engine. RBAC is built-in
JWT (no IdP) for the same reason.

## D13 — Stream delivery is a transport choice, and the default is honest about it
The 005 benchmark's "Dapr 17× faster" result was never the actor model — Orleans grain calls are
microseconds — it was memory streams' pull-based delivery (100ms polling agents, paid once per
stream hop; two hops on the table path). The fix is a config-switchable transport under the same
provider name: `--Streams:Transport push` swaps in an in-process push bus (bounded channel per
subscription + one pump task delivering into the consumer grain's turn via a grain extension) and
takes `tableDelta` from p50 115ms to **p50 1ms**, ahead of Dapr's sidecar path (7ms). *Why pull
stays the default*: it is byte-identical stock Orleans — zero new failure modes in the default
path — and the push bus is single-silo by construction, which is fine for this flavor's documented
localhost topology but must not silently become a portability assumption. *Why not a transport
seam interface*: `IStreamProvider`/`IAsyncStream<T>` already are the seam — implementing them
keeps every one of the ~14 producer/consumer call sites unmodified, so "both modes behave the
same" is true by construction instead of argued. Fine print: per-(key, subscriber) FIFO is kept
(one pump per subscription, delivery awaited before the next read); publish never awaits inside
the producer's turn (rule D10's call-cycle discipline extends to transports); backlog overflow
drops the incoming item with an exact logged counter rather than blocking a grain turn. The
`TABLES__FLUSHMS` epoch-flush knob is a separate, orthogonal cadence and only exists on the
partitioned path (D9) — P=1 tables never had a flush window, which the second round of measuring
proved the first round had misattributed.

## Known ceilings (quick list)
Table-mode CROSS JOIN needs Parallelism = 1 · single-node topology · generator ~1 000 ev/s/source (1 ms timer floor) ·
arrangement partitions each subscribe the full input stream (filter-at-consumer) · no exactly-once
replay · JSON path keys are literals · correlated subqueries beyond equality rejected ·
pipeline names not enforced unique (name-resolution falls back only on unambiguous match).

## Memory ceilings — what grows without bound, and with what (plan 011 wave C)

Written down because none of it was, and because a long run on the *stock seed* exhausts memory: the
seeded `order_states` table is `LATEST BY (order_id)` over a fresh-GUID key, seeded Running, so it gained
roughly one permanent row per second forever — until wave C2 gave it a retention bound (see the
"Row retention" section below and `SeedCatalog.Tables()`' own comment). Everything below is **by design** — table mode is a materialized view, and a
materialized view over an unbounded key space is unbounded. The defect was never that these grow; it was
that nobody could find out that they do.

Wave C fixed the **amplifier**, which is a different thing from the ceiling and worth keeping distinct:
`TableGrain`/`TableHistoryGrain` (and their Dapr mirrors) used to rebuild their whole persisted mirror —
a fresh dictionary per row — on the grain turn every `FlushMs` (default 2000 ms), and `JsonFileGrainStorage`
then serialized it with `WriteIndented = true`. That made allocation proportional to the *table* at a rate
set by the *clock*, so a table growing linearly produced quadratic total allocation and a GC/LOH stall long
before any real heap exhaustion. Both captures are now O(rows changed since the last flush) and the storage
format is compact.

**Measured**, `tools/soak/run-soak.sh`, 5-minute window, 100 ev/s, one `order_states`-shaped table
(`LATEST BY` an unbounded key, history `LastN(8)`), same machine, ~27 000 rows accumulated in each run:

| run | RSS slope | RSS peak | RSS added per row | state dir growth |
|---|---|---|---|---|
| before (master, `Batched`) | 287 MB/min | 1728 MB | 43.3 KB | 5.1 MB/min |
| after (`Batched`, the default) | 109 MB/min | 1009 MB | 28.8 KB | 3.6 MB/min |
| after (`Journaled`) | 90 MB/min | 814 MB | 23.9 KB | 3.4 MB/min |

Flatter, not flat — and the difference matters. What remains is (a) the row set itself, which grows by
~5 000 rows/min in this load and is the ceiling, not the amplifier; and (b) the **whole-state JSON
serialization** every `FlushMs`, which is `Batched`'s contract, not a defect: `JsonFileGrainStorage`
rewrites a whole state object per write. `Journaled` removes that for the table's own snapshot but not for
its history — `TableHistoryGrain`'s flush has no journal branch and falls through to a full awaited write,
and wave C2 made the Dapr flavor match (it previously had no `JournalWrite` case at all, so a Journaled
table's history was **never persisted there**; `TableHistoryApplication.DecideHistoryFlushAction` now maps
it to the same full awaited write Orleans does). Giving history its own journal, so it would be
O(changed) too, is still open — it is a durability-contract decision, not a memory fix.

What none of it does is bound anything below.

| Structure | Grows with | Evicts? |
|---|---|---|
| `TableExecutorImpl._ledger` (`Runtime/TableExecutorImpl.cs`) | distinct output rows | only under an opt-in row retention policy (wave C2) |
| `TableLatestByOp.Current` | distinct `LATEST BY` keys | only under an opt-in row retention policy (wave C2) |
| `TableReduceOp.Groups` | distinct GROUP BY keys ("groups live forever" — its own doc) | never |
| `TableJoinOp` / `TableOuterJoinOp` / `TableSemiAntiOp` ZSet indexes | distinct join keys on both sides | never — no `WITHIN` eviction in table mode; `OnFrontier` is a documented no-op |
| `TableDistinctOp._weights` | distinct rows | never |
| `TableGrainState.Snapshot` (the persisted mirror) | distinct output rows — a **second** full copy alongside the ledger for `Batched`/`FireAndForget` (`MemoryOnly`/`Journaled` read live and hold one) | with its ledger |
| `TableHistoryGrain._liveEntries` | distinct row identities. Per-key *version* counts ARE capped (`HistoryLimit`, `AllModeCap = 1000`); the **key count is not** | only when the owning table has a retention policy — an evicted row's whole version trail is reclaimed with it (wave C2) |
| `TableSearchIndex`'s five row-keyed maps (`AppCore/Search/TableSearchIndex.cs`) | distinct rows, ~4–5× multiplier on whatever the table holds | with the table's rows |
| `ArrangementGrain._index` | distinct arranged keys | never |
| `EpochBuffer._pending` + `TableStageGrain._originByBatch` | (stall duration × ingest rate) while any one upstream partition holds the frontier back | on frontier advance only — no cap, no backpressure, no status; see EpochBuffer's class doc |
| Table-path grains generally | — | nothing deactivates: every grain in the table path calls `DelayDeactivation(TimeSpan.FromDays(365))` |

Rules of thumb that follow: a table's resident cost is roughly *(distinct output rows) × (2 copies for the
default persistence mode + 4–5× again if `SearchEnabled` + up to `HistoryLimit` versions if
`HistoryEnabled`)*; `Persistence = MemoryOnly` or `Journaled` removes one of those copies; `SearchEnabled`
is the single most expensive flag on a wide table. Two mechanisms bound the row set itself rather than
shrink its constant factor: the per-table **row retention policy** below (wave C2), and **per-key sharding
with deactivatable shards** (wave D, still open).

### Row retention — the bound, and what it costs (plan 011 wave C2)

`TableDefinition.RetentionMaxRows` / `RetentionTtlMs`, **both 0 = off by default**. Off is not a
formality: a table with retention is **not the relation its SQL describes**, it is a *bounded view* of it.
Rows that belong in the table by the SQL's own semantics are dropped once a bound is exceeded. Enable it
where an unbounded key space (an order id, a session id, a request id) would otherwise grow the table
forever; do not enable it where a consumer assumes completeness.

What makes it a fix rather than a metric:

- **Eviction reclaims the operator's own per-key state**, not the persisted mirror. For a `LATEST BY` plan
  that means `TableLatestByOp.Current` — the map that actually holds the row and its field dictionary; for
  a plain projection it means the consolidation ledger. Trimming `TableGrainState.Snapshot` alone would
  have left every structure in the table above growing while the row count "plateaued".
- **Eviction emits a real retraction** (negative weight) through the same `OnStreamEvent`/`OnTableDelta`
  return value an ordinary delta travels through — so downstream tables, the delta stream, SignalR, sinks,
  the search index and the row history all follow along with no retention-specific code on any of those
  paths. A row that vanished without a retraction would corrupt every consumer downstream of it, which is
  strictly worse than the leak.
- **History follows the table**: a delta marked `TableDeltaDto.Evicted` makes the history grain/actor drop
  the key's whole version trail rather than bump its retraction counter. Otherwise the bound would bound
  the visible row count and none of the memory.
- **Order is deterministic**: oldest-first by the row's **event timestamp** (`_ts`), tie-broken by the
  entry's identity string — never wall clock, never hash order, so replaying the same input produces the
  same bounded table. The TTL cutoff is likewise event-time (`max admitted _ts − TtlMs`), with the honest
  consequence that a stalled input ages nothing out: a TTL keeps the last N ms *of data*, not of clock.
- **Refused where it could not be honest**: joins, set operations, derived sources, GROUP BY/aggregates and
  `Parallelism ≥ 2` are rejected at create/update (409). A join's ZSet indexes hold the *input* rows, so
  evicting an output row would bound nothing; an evicted aggregate group would restart its SUM/COUNT from
  zero and emit a *wrong* value. Both flavors enforce the same rule
  (`RegistryGrain.ValidateRetention` / `CatalogStore.ValidateRetention`).

The seeded `order_states` is its first customer, at `RetentionMaxRows = 2000` (~the last half hour of
orders at the seeded rate). Measured with the same harness and the same load as the table above
(`tools/soak/run-soak.sh --retention-max-rows 2000`, 5 min, 100 ev/s, `Batched`):

| run | RSS slope (whole window) | RSS slope (last third) | RSS at end | rows at end | state dir |
|---|---|---|---|---|---|
| C1 (`Batched`, unbounded) | 109 MB/min | **+162 MB/min** | 1009 MB | 27 616, still climbing | 3587 KB/min |
| C2 (`Batched`, `RetentionMaxRows = 2000`) | 44 MB/min | **−12 MB/min** | 507 MB | 2 000, flat | 122 KB/min |

Read the last-third column, not the first: both runs ingest the identical 5 200 deltas/min, and the
whole-window slope of the C2 run is dominated by the startup ramp (JIT, the seeded catalog spinning up, the
GC growing to its steady-state heap). What the two columns say together is the whole point — the unbounded
run was still *accelerating* at the end of its window and the bounded one had **plateaued**: RSS
oscillating in a 485–552 MB band with no trend, a row count pinned at the bound, and a state directory that
stopped growing (29× less write per minute). Rows/min fell from 5 204 to 3.2.

Operational guidance, unchanged for a table WITHOUT a policy: keep an eye on one whose key space is
unbounded, and measure with `tools/soak/run-soak.sh`.

### Sharded tables — per-key locality and swap-out, NOT a memory optimisation (plan 011 wave D)

`TableDefinition.ShardBy`, **empty = off by default**, and off means byte-for-byte today's behavior — the
same opt-in discipline `Parallelism` established (D9). Orleans-only: the Dapr flavor stores the field but
refuses to *start* a table carrying it (`TableActor.StartAsync`; see that guard and
`CatalogStore.ValidateParallelism`'s doc for why the refusal sits at start rather than at upsert on that
flavor).

**READ THIS FIRST — the memory claim was measured and it does not hold.** `ShardBy` was built to cut
resident memory. Across waves D1, D2 and D3 it never once reduced total process RSS beyond the noise
floor, and below roughly 100 versions per key it actively *costs* — up to +68% RSS. The feature is real
and useful, but it is **per-key query locality and swap-out of cold keys to disk**, and it must be
described that way. See "The measurement, and the verdict" at the end of this section for the curve and
the cost model before enabling it on anything.

**This is not retention, and confusing the two wastes the feature.** Retention DELETES rows to bound a
table. Sharding KEEPS everything and bounds what is *resident*. The case it was built for is a financial
instrument modelled as a state machine with legs: the frequent query is "give me everything for this
key", the full history must be kept, and keeping all of it resident is what costs — one instrument's
history, even a thousand versions, is small, so a grain per instrument is cheap to save and cheap to load.

**Where it sits.** The shard tier is a *consumer of the table-delta stream*, not a change to execution —
the same hook `TableHistoryGrain` already uses, which is D7's stated principle ("the delta stream is the
event log"). The SQL path, the planner, the partitioned dataflow and every downstream table-over-table
subscriber are untouched, and because `TableOutputGrain` republishes onto that same stream, a shard
consumer behaves identically at `Parallelism == 1` and `Parallelism >= 2`.

| grain | key | holds | deactivates? |
|---|---|---|---|
| `TableShardRouterGrain` | table name | nothing per-key: a config, a sequence counter, two counters | no — `DelayDeactivation`, like every other stream consumer (a local subscription callback does not survive deactivation) |
| `TableShardDirectoryGrain` | table name | the live shard-key set — **O(distinct keys) of strings, resident** | not pinned, but kept alive by traffic; see the ceiling below |
| `TableShardGrain` | `{table}\|{token}` | one key's rows + that key's version trails | **YES — and it is the entire point** |

**The one rule.** `TableShardGrain` never calls `DelayDeactivation`. Every other grain in the table path
does (`TableGrain`, `TableHistoryGrain`, `TableStageGrain`, `ArrangementGrain`, `TableOutputGrain`), which
is exactly why nothing in it has ever been swapped out — the last row of the ceilings table above. An idle
shard being collected, with its state on disk until the next lookup, IS the memory win; a shard that
pinned itself alive would deliver nothing while passing every functional test. How long "idle" is is
configurable — `Shards:IdleSeconds` (default 120s) sets the grain class's collection age, and
`Shards:QuantumSeconds` the silo-wide scan interval, which Orleans requires to be strictly smaller.

**Reads, and the trap.** A per-key read (`POST /api/tables/{id}/shard/lookup`) is *strictly consistent by
construction*: one grain, one ordered delta stream, Orleans serializing its turns — a read sees a whole
prefix of the stream, never a half-applied batch, with no fence and nothing to configure. A **keyless
`/rows` listing on a sharded table consults no shard at all**: it is served from the table's own
consolidated snapshot exactly as before, and the response says so (`shards.shardsConsulted: false`). That
is not laziness. The console polls `/rows` every two seconds; a listing that fanned out across the
directory would wake every shard on every poll, nothing would ever be swapped out, and the feature would
be self-defeating while looking correct. A genuine full scan is a separate endpoint,
`GET /{id}/shards/scan`, precisely so nothing reaches it by accident — and it is a set of per-shard
observations at different sequence numbers, not a consistent cut.

**Built now, used later:** the router stamps a monotonically increasing per-table sequence on every
forwarded batch and each shard records the highest it has applied. Wave D1 only reports it. It exists
because a *fenced* consistent whole-table scan (wave D2) needs an ordering primitive, retrofitting one
onto an accumulated tier means reprocessing its history, and the dataflow's own `Epoch` cannot serve:
`SnapshotFrontierEpoch` is null for every `Parallelism == 1` table, so an epoch-based fence would work for
half the tables and silently not for the other half.

**Refusals and interactions, all deliberate:**

- **`SearchEnabled` + `ShardBy` is refused** (409, `RegistryGrain.ValidateShardBy`). The reverse index is
  table-wide and row-keyed — five maps, a 4–5× multiplier on whatever the table holds — so keeping it
  would keep every row resident and defeat the point, while a per-shard index would answer a table-wide
  query by waking every shard, which is worse. Refusing loudly beats half-serving, matching C2's refusal
  of retention on plan shapes it could not honor.
- **On a sharded table the per-key history replaces the table-wide `TableHistoryGrain`**, which is
  disabled rather than left subscribed. Running both would hold every version trail twice and hold the
  second copy resident.
- **Retention and sharding COMPOSE**, and the rule is the one already written down above: history follows
  the table. A delta marked `Evicted` makes the shard reclaim that row's version trail, and a shard left
  with no rows and no history clears its own state and drops out of the directory. The two knobs answer
  different questions — retention bounds what the table *holds*, sharding bounds what is *resident* — and
  a user who wants to keep everything simply leaves retention at its default of off.
- **Shard columns are explicit and validated against the compiled output schema.**
  `TableGroupKeyExtractor.ExtractIdentityColumns` is best-effort *textual* matching that returns null on
  any ambiguity: acceptable for "which versions belong together", not acceptable for "which grain owns
  this row", and never used to pick a key silently.

**What this does NOT bound, stated plainly rather than discovered later:**

- **`TableGrain`'s own consolidated snapshot is untouched.** It still holds one entry per output row, still
  pinned alive. D1 moves the per-key *history* — the term that grows with versions × keys, and the one
  that dominates on the shape this was built for — out of resident memory; bounding the snapshot itself is
  what retention (C2) is for, and shedding it on a sharded table is a later question.
- **The shard directory is resident and O(distinct keys)**, one string per key. Kilobytes against the
  shards' megabytes on the shape this targets, but a table with tens of millions of distinct keys would
  feel it, and the honest answer for that shape is an external index, not a bigger grain.
- **`Persistence = MemoryOnly` + `ShardBy` throws the win away in the other direction**: a shard that
  never writes has nothing to reactivate from, so deactivating it *loses* its state rather than swapping
  it out. Honored literally, since that is already what the mode's contract says; not blocked.
- **Disk grows, on purpose.** "Keep everything, just not resident" means one state file per shard key,
  forever. That is the trade, not a leak.

**The measurement, and the verdict (plan 011 wave D3).** `tools/soak/run-soak.sh --shape trails` sweeps
trail depth at FIXED total event volume — only the distribution over keys changes, which is the variable
the answer depends on. The key is a retiring cohort (an instrument's state machine finishes and the key
goes quiet forever), which is both realistic and sharding's *best* case. 8 min @ 200 ev/s, `Batched`,
`history All`, `--shard-idle-s 15`. `meanRSS` is the mean over the second half; the least-squares slope is
GC-dominated at this size (several are negative). Control run-to-run spread on identical volume was
277/271/308 MB, so **the noise floor is ±37 MB** — anything smaller is not a result.

| versions/key | keys | control meanRSS | sharded meanRSS | Δ | resident/known | activations |
|---|---|---|---|---|---|---|
| 9.4 | 9 272 | 277 MB | 465 MB | **+188 MB** | 368/9 232 (4.0%) | 19.4/s |
| 89 | ~930 | 271 MB | 293 MB | +22 MB | 40/936 (4.3%) | 2.0/s |
| ~800 | 104 | 308 MB | 259 MB | −49 MB (noise) | 16/104 (15.4%) | 0.2/s |

**The penalty disappears between 10 and 100 versions per key. A benefit never appears anywhere in range.**
Wave D2's estimate of a crossover "in the hundreds" was right about where sharding stops costing and wrong
to imply it then starts paying.

**The cost model, which is the useful output.** The overhead tracks the shard ACTIVATION RATE, not the key
count and not the resident set — and activation rate = key-creation rate ≈ `events-per-sec ÷
versions-per-key`, so the cost curve is `1/x`:

| activations/s | RSS penalty |
|---|---|
| 128 (D2 `instruments`, uniform random keys — 6 reloads per key) | +69 MB/min slope |
| 19.4 | +188 MB |
| 2.0 | +22 MB |
| 0.2 | −49 MB (noise) |

At v10 only 4% of shards were resident and the arm still lost 188 MB: the cost is not residency, it is
9 232 activate/serialize/deactivate cycles in 8 minutes. In `trails` each key activates exactly once and
never reloads, so the sharded arm pays *zero* reload cost and still loses. This is the same finding as
D2's "per-key JSON rewrite is the wrong storage shape", now with the axis named.

**What the measurement cannot show, stated so nobody over-reads it.** At this dataset size there was
nothing to save: the full content is 14 MB on disk (~172 B/version, 85 k versions) against a ~270 MB
baseline RSS — about 5%. Perfect swap-out of *everything* would save less than the ±37 MB noise floor, so
the v100/v1000 points honestly read "no penalty", not "a win", and could not have read "a win" by
construction. A single `GeneratorGrain` emits one event per timer tick (1 ms floor ⇒ ~200 ev/s), so at a
fixed window trail depth and key count trade off directly and the top point is 104 keys × ~800 versions,
not 1000 × 1000. Extrapolating (NOT measured): 1000 instruments × 1000 changes ≈ 1 M versions ≈ 170 MB on
disk, plausibly 350–700 MB resident — the first scale at which a 15% resident set could matter. Testing it
needs hours of soak, or the REST ingest path to get past the generator's rate ceiling.

**So: enable `ShardBy` for keys with long trails that go quiet, and never for a fast-churning key space.**

**Measured**, `tools/soak/run-soak.sh`, 6-minute window, 100 ev/s, `Batched`, same machine and the same
`order_states`-shaped table as the C1/C2 rows above; the sharded arm adds `--shard-by orderId
--shard-idle-s 30 --shard-quantum-s 10` (results: `tools/soak/results/d1-{unsharded,sharded}-latest.json`).

| run | rows at end | shards known | shards RESIDENT | resident slope | shard-count slope | RSS slope | state dir |
|---|---|---|---|---|---|---|---|
| D1 unsharded (control) | 32 141 | — | — | — | — | 123 MB/min | 23 MB |
| D1 `ShardBy = orderId` | 34 489 | 34 546 | **4 034** (peak 4 234) | **+106 /min** | +5 552 /min | 186 MB/min | 154 MB |

**The claim this proves, and the one it does not.** The shard tier's resident set IS bounded by the ACTIVE
key set: shards known grew at 5 552/min while shards resident grew at 106/min — a **52× flatter slope**,
ending at 12% of the key space, with 34 546 activations against 30 512 deactivations. Keys are genuinely
being swapped out and faithfully reloaded, which is the mechanism the whole wave rests on.

**Total process RSS went UP, not down, on this workload — 186 MB/min against the control's 123.** Said
plainly rather than buried, because it is the honest shape of D1 and the reason the ceilings above still
matter:

- The tier is **additive**. `TableGrain`'s consolidated snapshot is untouched and still holds every one of
  those 34 000 rows, resident and pinned. D1 moves the per-key *history* out of memory; it does not move
  the table.
- This soak is the **worst possible shape for sharding**: `LATEST BY` a fresh GUID gives each key exactly
  one row and one version, so the per-shard overhead (an Orleans activation, a state file, a flush timer)
  is larger than the per-key content it holds. The shape D1 was built for is the inverse — few keys
  relative to the data, each with a long version trail — where the resident term the tier removes is
  `keys × versions` rather than `keys × 1`.
- Disk grew 6.6×, on purpose: one state file per key, forever, is what "keep everything, just not
  resident" costs.

Read `derived.residentFractionAtEnd` and `series.activations` first. Resident far below the directory's key
count, with activations climbing past both, is the feature working. Resident tracking the key count means
nothing is being reclaimed — and then the RSS slope is not evidence of anything at all.

### Sharded tables, part 2 — shedding the duplicate copy, and the fenced cut (plan 011 wave D2)

D1 built the tier and proved the mechanism: resident ended at 12% of the key space, 52× flatter than the
key count, and twenty keyless `/rows` polls left resident at zero. It also made total RSS **worse**, because
the tier was purely *additive* — `TableGrain` still held and still persisted a full consolidated snapshot of
exactly the rows the shards already held durably. D2 stops keeping it.

#### What is actually redundant, and what is not

There are up to three copies of a table's output rows in one process. **D2 removes exactly one of them**,
and being precise about which is the difference between a measurement and a slogan.

| copy | where | D2 |
|---|---|---|
| the **persisted mirror** — `TableGrainState.Snapshot`, rewritten every `FlushMs`, resident for the life of the activation | `TableGrain` | **REMOVED** on a sharded table |
| the **executor's own ledger** — `TableExecutorImpl`, and for the motivating shape `TableLatestByOp.Current` | `StreamsForge.Engine` | **kept**, and out of reach |
| the **coordinator ledger** — `_coordinatorLedger`, `Parallelism >= 2` only | `TableGrain` | **kept** |

The executor's ledger is what the SQL is *computed from*; shedding it means sharding **execution**, which
plan 003 superseded and which `TableGrain`'s own class doc (plan 009 A2) rejected on record. So a sharded
table's memory is **not** `O(active keys)` — it is `O(keys)` for the executor plus `O(active keys)` for the
shard tier. The coordinator ledger stays for a different reason: at `Parallelism >= 2` there is no local
executor and it is the table's only live copy of its own output, so serving `/rows` without it would mean
fanning out across the shard directory — waking every idle key on every two-second console poll, which is
the exact trap the tier is designed against.

#### Why the mirror could go at all

Because it was never the source of a restored row. A non-empty persisted snapshot is how
`StartClassicAsync`/`StartCoordinatorAsync` tell a **resume** from a first-ever start — and the very next
thing they do is throw those rows away and mark the table `Rebuilding`, because operator state (join
indexes, `LATEST BY`'s current row) cannot be reconstructed from output rows. The mirror was durably storing
every row of the table in order to answer one boolean. D2 stores the boolean (`TableGrainState.HadRows`,
`O(1)`) and lets the shards be the rows.

Reads follow: a sharded table serves `/rows` **live from its executor**, joining `MemoryOnly` and
`Journaled`, which already did. That is *fresher* than the up-to-one-flush-interval-stale mirror it
replaces, and still consults no shard. The one honest consequence: a **stopped** sharded table reports zero
rows where a stopped unsharded one still reports its last snapshot. The rows are not lost — they are in the
shards, and a per-key lookup returns them.

#### Persistence modes: none redefined

- **Batched / FireAndForget** keep their contracts exactly. What they write is now an `O(1)` marker instead
  of an `O(rows)` snapshot; neither ever restored rows.
- **Journaled**'s promise is *Batched's durability at an `O(changed)` write cost*, and sharding meets it
  **structurally** rather than bypassing it: with no table-level snapshot to rewrite there is nothing to
  journal, and each shard's own write is already `O(one key)`.
- **`MemoryOnly` + `ShardBy` is now REFUSED** (409), where D1 honored it and documented the consequence. D2
  is what made it indefensible: a shard's write on deactivation is not a durability nicety, it *is* the
  swap-out, so a mode that never writes turns an idle minute into data loss — and with the table's own
  mirror gone there is nothing behind the shards at all. `MemoryOnly` promises "a **restart** brings the
  table back empty". Refusing beats redefining that underneath the user.

#### The fenced scan — and why "each shard waits for S" is wrong twice

`GET /{id}/shards/scan?fenced=true`. Opt-in; unfenced stays the default.

The obvious design is that the reader picks a sequence `S` and every shard answers only once its own
`AppliedSeq` has reached it. That design **deadlocks on the common case**: the sequence is per *table* and
stamped per forwarded batch, so a shard whose key has seen no traffic since sequence 183 sits there while
the table runs to 5 217, will never reach it, and would hang the scan for good. Measured on the live check:
**993 of 994 shards were below the fence** at any instant — the wait-for-`S` fence hangs on essentially
every shard, essentially always. And its state at 183 *is* its state at 5 217, so there was never anything
to wait for. It does not even give a cut in the other direction: a shard that raced ahead to `S+5` before
being read would report deltas from *after* the fence.

What is true is stronger and needs no waiting anywhere. The router forwards **one batch at a time and awaits
every shard's apply before returning**, and the scan runs as a call **on the router**, which is
non-reentrant — so no batch can be forwarded while the scan is in flight, on either stream transport (memory
streams and the push bus both deliver as real Orleans messages, subject to the activation's normal
concurrency rules). Every shard therefore holds exactly the deltas of batches `<= S` at the moment it is
read, and nothing beyond. Nothing waits; the idle shard answers immediately with its honest, lagging
`AppliedSeq`.

**It is checkable, not just claimed.** Every forwarded delta lands in exactly one shard, so across a page
covering all of them the shards' `deltasApplied` must sum to the router's own `routedDeltasAtFence` — one
delta from at-or-before the fence missing, or one from after it counted, and the identity breaks. Live, at
200 ev/s over ~990 shards, three consecutive fenced scans came back exact (`7283/7283`, `8002/8002`,
`8720/8720`) in 38–41 ms each, with zero shards beyond the fence.

The costs, stated where they are paid: the shard tier's ingest is **paused for the duration of the scan**
(the table's own snapshot, its delta stream and every other consumer are unaffected — only this router's
subscription backs up, and it drains afterwards), and the cut is **per request**, so paging a large table
with several fenced calls gives several cuts rather than one. It retains no per-sequence versions, so it
costs no memory at all. Per-key reads need none of this and get none of it: one key is one grain, already
strictly consistent by construction.

#### Renaming a sharded table is refused

The whole tier is keyed by the table's **name** — the router grain, the directory grain, and every shard
grain (`TableShardKeys.GrainKey`). A rename therefore moves nothing: it points the table at a fresh, empty
tier while every existing shard keeps its rows and its full version trail on disk under a key nothing will
ever look up again. Nothing throws and nothing logs, which is the worst shape a data loss can take, so it is
refused (409) rather than documented.

**The better fix is to key the tier on the immutable `TableDefinition.Id`**, which costs nothing while the
feature is unreleased. It was implemented and backed out for one concrete, non-technical reason: wave D1's
own cluster tests address the tier by name in a dozen places, and D2's brief forbids modifying a
pre-existing test file. The change is mechanical — the grain keys plus those call sites — and should be made
the moment that call is made; the refusal is what closes the hole until then. The way around it today is to
clear `shardBy` first (which deletes the shards, explicitly and visibly), rename, then shard again.

The identical exposure exists, pre-existing and unchanged, for `TableHistoryGrain` and for `TableGrain`
itself, both also keyed by name. Neither is in this wave's scope.

#### Measured — and the answer is still "sharding costs more total RSS"

`tools/soak/run-soak.sh`, same machine, `Batched`, least-squares RSS slope over the sampled window.
Results in `tools/soak/results/d2-*-latest.json`.

**Shape A — `--shape orders`, 6 min @ 100 ev/s** (`LATEST BY` a fresh GUID: unbounded key space, one row and
one version per key). This is the *worst* shape for sharding and is kept as the harness default precisely
because it is unflattering and because every recorded run since C1 used it.

| run | RSS slope | rows at end | shards known | shards resident | state dir |
|---|---|---|---|---|---|
| D1 unsharded (control) | 123 MB/min | 32 141 | — | — | 23 MB |
| D1 `ShardBy = orderId` | 186 MB/min | 34 489 | 34 546 | 4 034 (11.7%) | 154 MB |
| **D2 unsharded (control)** | **125 MB/min** | 32 559 | — | — | 23 MB |
| **D2 `ShardBy = orderId`** | **151 MB/min** | 34 042 | 34 042 | 3 909 (11.5%) | 137 MB |

Shedding the mirror took the sharded arm from **186 → 151 MB/min, −19%**, against a control that did not
move (123 → 125, i.e. the machine and the harness are comparable). **It is still 21% worse than the
control**, where D1 was 51% worse. The direction is right and the destination is not reached.

**Shape B — `--shape instruments`, 8 min @ 200 ev/s** (bounded ~10k key space, `history All` so trails grow
without bound: the instrument-with-legs case this feature exists for, ~8.5 versions per key by the end).

| run | RSS slope | rows | deltas | shards known | resident | activations | state dir |
|---|---|---|---|---|---|---|---|
| unsharded (control) | **69 MB/min** | 9 996 | 84 674 | — | — | — | 16 MB |
| `ShardBy`, `--shard-idle-s 15` | 138 MB/min | 9 998 | 88 665 | 9 998 | 2 927 (29%) | 60 783 | 41 MB |
| `ShardBy`, `--shard-idle-s 60` | 142 MB/min | 9 998 | 85 435 | 9 998 | 7 577 (76%) | 23 225 | 41 MB |

**On the shape it targets, sharding is twice the control's slope.** Reported plainly because the flattering
alternative would have been to report only shape A's improvement.

The third row is there to kill the obvious excuse. Quadrupling the collection age moves residency from 29%
to 76% and cuts activations by 62% — the knob works exactly as designed — and **total RSS does not move**
(138 vs 142 MB/min). So this is not a mistuned idle age. The tier's cost is the tier: at ~8.5 versions per
key, an Orleans activation plus a JSON state file plus a serializer graph *per key* is about as large as the
per-key data it holds, and the swap itself (60 783 serialize/deserialize round trips in eight minutes) is
pure garbage on top. The resident-set claim is true and measurable; it is simply not the dominant term at
this ratio.

**What would have to change for the total to win**, stated so the next wave does not rediscover it:

- **The remaining copy is the executor's**, `O(keys)` and untouched (see the table above). At 10k keys and
  10k rows, shedding one of two copies of the rows cannot beat a control that keeps one.
- **The trail has to be much longer than the row.** The tier's fixed per-key overhead is amortised over
  `versions × key`, so the crossover is at *hundreds* of versions per key, not eight. The soak cannot reach
  that ratio and a low residency at the same time: at a fixed event rate, `versions/key ≈ x · T_run/T_idle`
  where `x = −ln(1 − resident_fraction)` — 20 versions/key at 10% residency needs a run roughly 190× the
  idle age.
- **Per-shard persistence is the wrong shape at this granularity.** One rewritten JSON file per key per
  flush is what puts the state directory at 2.5× the control's and drives the churn cost. A shard tier that
  paid off would need a storage provider that appends per key rather than rewriting per key.

**What D2 does deliver, unambiguously:** the duplicate copy is gone (measured directly on the live check —
`TableGrain`'s state file for a sharded table was **39 bytes** against the unsharded control's **308 325**
for the same 994 rows), the resident set genuinely tracks the active key set, per-key reads are exact and
cold-load from disk correctly, and the fenced scan is a real cut. What it does not deliver is a smaller
process.
