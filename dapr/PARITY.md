# Dapr parity: what is deliberately absent, what plan 025 closed, and what's still unverified

**Working rule, updated 2026-09-05 (plan 025).** The Orleans flavor is still the primary runtime and
new plans still land there first. What changed: as of plan 025 the Dapr flavor is no longer a black
box for live verification — an isolated Dapr test instance exists for the first time, and most of the
2026-08-20 debt list below (gRPC serving, TLS, late-consumer replay, boot order, the paced relay,
table-over-pipeline, an honest `crdt` status) is closed in code AND has been run against a real,
isolated, sidecar'd process. What is NOT closed is smaller than before but not zero — see section 3.

This file still has three sections, and the split is still the point:

1. **Permanent descopes** — never coming to Dapr, by decision. Unaffected by plan 025.
2. **Closed debt** — was owed, plan 025 (or an earlier plan, noted per item) closed it. Kept here
   rather than deleted so the history of "this used to be missing" survives, with a pointer to the
   code and to what verified it.
3. **Unverified** — code exists on both flavors, but nobody has run the Dapr side live, OR it depends
   on something outside this repo (Docker, `dapr init`) that a given environment may not have. Smaller
   than the 2026-08-20 version, still not empty.

### Methodology (read this before trusting any "verified live" line in this file)

A claim in this file is "verified live" only when it names either an automated test (a file and a
method the reader can re-run) or an explicit manual check with what was run and what came back —
never a bare adjective. Section 3's table is the enforcement mechanism: a plan that writes "verified
live on Dapr" with no flavor, no test name and no transcript is exactly the failure mode that produced
this document in 2026-08-20 (see the "window" story below, kept for the record). Two kinds of evidence
appear below and are labeled accordingly:

- **Automated, re-runnable**: an xunit `[Fact]` in `dapr/tests/StreamsForge.Dapr.Live.Tests/` (process-
  level, spawns a real `dapr run`-wrapped host) or `dapr/tests/StreamsForge.Dapr.Tests/` (unit-level,
  no sidecar). Named by class and method.
- **Manual, dated**: a curl/grpcurl/SDK transcript the orchestrator ran by hand against the isolated
  instance during plan 025's integration, dated, with what was asked and what came back. These are
  real but not re-runnable from CI the way a `[Fact]` is — a future regression here would not fail a
  build, only a future manual re-check. Where plan 025's own outcomes section (`plans/025-dapr-parity.md`)
  later records the same check under a wave, that is the authoritative transcript; this file summarizes
  it in-place rather than duplicating it verbatim.

---

## 1. Permanent descopes — not debt

`dapr/ARCHITECTURE.md`'s "What's NOT here yet (by design)" section is the authority; this is the index.
Unaffected by plan 025 — none of these were ever debt.

| Descoped | Where it is refused | Decision |
|---|---|---|
| Partitioned table execution (`Parallelism` 2–16, the ingest/stage/output grid) | `Catalog/CatalogStore.cs` `ValidateParallelism` → 409; defensively again in `Actors/TableActor.cs` | 005 D-F |
| Frontier-consistent reads (`frontierEpoch`) | `Facades/StubFacades.cs` — always `null` | 005 D-F |
| Shared arrangements, `GET /api/meta/arrangements` | `EmptyArrangementMetaFacade` — always `[]`, explicitly *not* a stub awaiting a wave | 005 D-F |
| Per-key table sharding (`ShardBy`) | refused at **start**, not at upsert, so the definition still round-trips and can be promoted back to an Orleans instance without loss | 011 D1 |
| Table text search (`SearchEnabled`) on a sharded table | inherited from sharding | 011 D1 |
| Cluster-aggregated ingress stats | `Ingest/DaprIngressFacade.cs` returns the local view with `Aggregated=false` | 009 D-F |
| `IngestStatus.DownstreamDropped` | always `0` — there is no Dapr equivalent of `PushStreamBus.TotalDropped` to observe | 009 |
| `--Streams:Transport push\|pull` | an Orleans stream-provider knob; Dapr's transport is Redis pub/sub and has nothing to select | — |

Plan 020's `crdt` source kind is declared Orleans-first (020 D9) and stays that way — plan 025 only
made the REFUSAL honest (see D5/D7 below), it did not build a Dapr document runtime. That is a
**descope with an escape hatch**, not a permanent one: the escape hatch is a `grpc` source subscribing
to a document an Orleans instance projects.

---

## 2. Closed debt

Everything in this section was owed as of the 2026-08-20 snapshot. Each entry names what closed it,
when, and what evidence exists — per the methodology above.

### D1 · gRPC serving on `:5499` — CLOSED (plan 025 G1/G2)

Was: `Program.cs` passed `GrpcStaticServices: []` and nothing listened on `:5499`; `GET
/api/meta/instance` reported a Dapr instance gRPC-incapable; it could never be a federation server; the
four generated clients had no gRPC transport against it.

Now: the seven gRPC services (`Source/Pipeline/Table/Stream/Ingest/DynamicStream/ServerReflection`)
moved out of the Orleans host into `shared/StreamsForge.Api/Grpc/`, retargeted from
`IClusterClient`/`IRegistryGrain` onto the environment-scoped `ICatalogFacadeFactory` both hosts
already registered, plus one new runtime primitive both flavors implement:
`IEntityStreamFacade` (`shared/StreamsForge.Contracts/EntityStreamFacade.cs`) — subscribe one entity's
live stream for the life of one server-streaming RPC. Orleans implements it over its stream provider
(`orleans/src/StreamsForge.Host/Facades/OrleansEntityStreamFacade.cs`); Dapr implements it as an
in-process per-key fan-out fed by the five fixed pub/sub topics
(`dapr/src/StreamsForge.Dapr.Host/Streaming/EntityStreamFanout.cs`, which also registers as one more
`ISourceEventsSink`/`ITableDeltaSink`/`IPipelineResultsSink` — see D6 below for why `sf-pipeline-out`
had to become a generic sink for this). `dapr/src/StreamsForge.Dapr.Host/Program.cs` now maps a
second Kestrel listener on `Grpc:Port` (default `5499`, HTTP/2-only) alongside the REST/SignalR/SPA
port, `GrpcStaticServices: StreamsForgeGrpc.StaticServiceNames` so `/api/meta/instance` advertises
`grpc` honestly (the list IS the mapping — one definition, `shared/StreamsForge.Api/Grpc/StreamsForgeGrpc.cs`).

**Evidence**: manual, dated 2026-09-05 (orchestrator, isolated instance) — gRPC ingest returns
`{"accepted":1}`; all four generated client SDKs (.NET, TypeScript, Python, Kotlin) subscribe a Dapr
entity's stream over gRPC successfully. Unit-level: `dapr/tests/StreamsForge.Dapr.Tests/
EntityStreamFanoutTests.cs` proves the per-key fan-out (subscribe/dispatch/unsubscribe, handler-failure
containment, pipeline-keyed-bare vs source/table-keyed-qualified) without a sidecar. A dedicated
process-level gRPC live test is one of the items plan 025's wave 2 (agent L, running alongside this
document's own wave) is adding — see section 3's "landing in the same plan" note; this file does not
claim that test's result.

### D2 · Table-over-table warm attach — CLOSED in code (pre-025); still not live-verified

Unchanged from the 2026-08-20 snapshot, restated here because plan 025's new isolated instance is the
first place it COULD be checked and has not been yet: `ITableActor.AttachSnapshotAsync` (a synchronous
read of `TableExecutor.Snapshot()` + `LastEpoch`) plus `TableActor.StartAsync`/`OnActivateAsync`
registering `TableEventRouter` from inside their own turn before attaching — both closed in code, both
covered by `TableAttachPolicyTests` (the epoch-cutoff filter), neither exercised against a live sidecar.
Distinct from D6's table-OVER-PIPELINE (closed below) — this is table-over-TABLE, the `hot_symbols FROM
positions` shape, and it is not what the new `Live.Tests` project's `RestartResumeTests` exercises
(`hot_symbols` seeds `Stopped` and stays that way in that test). A live check — start `hot_symbols`
after `positions` is already warm and confirm the rows appear immediately — remains open; see
`dapr/ARCHITECTURE.md`'s D2 section for the reentrancy residual (an actor-activation cycle would hang
until Dapr's call timeout, not silently answer wrong — documented, not guarded against).

### D3 · `instance.json` in `DataDir` — CLOSED by documentation (plan 025 D10)

Was debt: "either move the identity into the statestore or document the directory as load-bearing."
Plan 025 chose the second option deliberately: the directory stays load-bearing for exactly one file,
and `reset.sh` does not touch it, because an operator resetting the catalog wants the SAME instance
identity peers already know, not a reissued one. `dapr/ARCHITECTURE.md`'s "How to run" section and this
file's own history record the choice; no code changed.

### D4 · Stale comment — CLOSED (pre-025, unchanged)

`Facades/StubFacades.cs`'s `ShardBy`-refusal comment was already corrected before plan 025; the orphaned
"WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" explanation stacked on `ValidateParallelism` is left
exactly where the 2026-08-20 snapshot found it, for the same reason: it is the best explanation of the
asymmetry in the repo and moving it is a bigger edit than any plan touching it has needed.

### D5 / D7 · `crdt` source status — CLOSED (plan 025, PARITY wave C3)

Was: the kind was refused loudly (a log line) but had no status to refuse INTO — a sharded table gets a
`Failed` status because `TableActor` exists to hold one; a `crdt` source had no actor on this flavor at
all, so `GET /api/sources/{name}/status` had nothing honest to return.

Now: `Facades/DaprFacades.cs`'s `DaprConnectorStatusFacade.GetStatusAsync` recognizes an enabled `crdt`
source and returns a SYNTHESIZED status (`CrdtSourceStatus.Synthesize`) — `LastStatus = "error"`,
`LastError` carrying the plan 020 D9 message verbatim, every counter/schedule field zeroed/null — with
**no connector actor ever activated**. `POST /api/sources/{name}/crdt/replay` still answers 501 via
`ICrdtFacade.Enabled`, unchanged (020 D9 is Orleans-first; this closes the STATUS honesty gap, not the
document runtime itself — see section 1 above).

**Evidence**: automated, unit-level — `dapr/tests/StreamsForge.Dapr.Tests/CrdtSourceStatusTests.cs`
(`Synthesize_ReturnsErrorStatus_WithTheOrleansOnlyMessage`,
`Synthesize_ReportsNotRunning_ViaNullNextRunMs_AndZeroedCounters`, `MessageFor_NamesTheActualSourceAndKind`).
No sidecar needed for this one — it is a pure function of a `SourceDefinition`.

**Found, not yet fixed**: `Lifecycle/DaprLifecycleOrchestrator.cs`'s own `Crdt` case comment still says
"What is still missing... is a Failed status carrying this text" — stale as of this fix, since the
facade above now provides exactly that. `DaprLifecycleOrchestrator.cs` is not owned by this document's
wave; flagged here so whoever next touches that file corrects the comment.

### D6 · Source relay, boot order, late-consumer replay, table-over-pipeline — CLOSED (plan 025)

Four related items, all owed as one entry in the 2026-08-20 snapshot, closed together because the
underlying mechanism (a table/pipeline that starts after its inputs have already emitted must not lose
rows) is the same story told four ways:

- **Paced source relay.** `Streaming/SourceRateSampler.cs` is now a genuine pacer (mirrors Orleans'
  `StreamBridgeService` rule exactly): a too-early event WAITS OUT the remainder of its 50ms slot and
  is then sent, rather than being dropped, up to `MaxPacedStreak` (40) consecutive paced events per key
  before degrading to the old drop behavior — protecting the relay from an unbounded backlog behind a
  sustained firehose. `StreamingSourceRateSamplerTests` was rewritten from "exactly one relayed event"
  to "three for three, in order" (the behavior decision plan 025 D5 records).
- **Source lifecycle events.** `Lifecycle/DaprLifecycleOrchestrator.cs` now publishes `source-started`/
  `source-stopped`/`source-deleted` on `sf-lifecycle`, `PipelineId` = the source's qualified name — the
  Dapr-shape mirror of `RegistryGrain`'s identical publish. `source-*` lifecycle events are ignored by
  the bridge on this flavor too (no hub message), matching Orleans.
- **Coordinated boot order.** `Services/BootResume.cs` (`BootResumePlan.Build` — pure, unit-tested in
  `BootResumePlanTests.cs` — plus `EntityResume` and a process-wide `BootGate.Shared`) runs ONE ordered
  pass per environment: Running pipelines → Running tables (topo-sorted by `TableInputs`, tolerating a
  cycle by falling back to catalog order rather than diagnosing one — boot is the wrong place to reject
  a catalog) → enabled sources, from `CatalogInitializationService`. The four periodic supervisors each
  await the gate (bounded 60s) before their first sweep via `BootGateWait.AwaitBootPassAsync`, then
  remain exactly what they were: 15s self-healing, not the resume mechanism. Log line (manual, dated
  2026-09-05): `boot resume for environment '' — 4 pipeline(s), 3 table(s), 6 source(s).`
- **Late-consumer replay.** `IConnectorActor.BeginAttachAsync`/`EndAttachAsync` (Dapr's counterpart of
  `IConnectorGrain.BeginAttachAsync`) plus the shared `SourceReplayBuffer` ring (10,000, in
  `StreamsForge.AppCore.Connectors`) — hold-and-flush with a 10s safety timer so a consumer that dies
  mid-attach cannot gate the source forever. Covers ALL source kinds, not just polled ones: the
  subscriber kinds (grpc/nats/fix/transports), which used to publish straight from a background thread,
  now marshal rows onto the actor's own turn via one more actor method, `RecordSubscriberRowsAsync`
  (one proxy call per batch — those callbacks already paid for the bookkeeping call it replaces).
  Consumer side: `TableActor.RegisterRouterAndAttachToTableInputsAsync` (holds taken BEFORE router
  registration) and `PipelineActor` (which now registers itself with `PipelineEventRouter` inside its
  own turn — the orchestrator no longer does it from outside). Residual, stated rather than hidden: a
  pre-hold in-flight batch already on the topic can duplicate a few replayed rows — the same gap
  Orleans documents on its own `BeginAttachAsync`; `LATEST BY` tables are unaffected (idempotent by
  construction). Also: the replay ring is per-ACTIVATION, and Dapr deactivates idle actors more eagerly
  than Orleans collects grains — a connector with a live timer/subscriber is never idle, but the window
  during which a freshly-reactivated connector's ring is empty is wider than on Orleans.
- **Table-over-pipeline.** `PipelineDefinition.OutputFields` is now populated by `CatalogStore`
  (`ApplyPipelineCompileResult`/`EnsureFieldNumbers`, with an init-time backfill for a catalog that
  predates this — the seeded VWAP pipeline picked this up automatically) exactly as Orleans populates
  it, so a table's SQL can name a pipeline as a relation; `TableDefinition.PipelineInputs` records which
  of a table's compiled relations are pipelines (bare pipeline id — GUIDs are already globally unique,
  no qualification needed, unlike stream/table inputs); `TableEventRouter.RegisterPipelineInputs` keys
  the routing table by that bare id and `ITableActor.ProcessPipelineResultsAsync` is the new ingress
  method `sf-pipeline-out` routes to (which is why `sf-pipeline-out` had to become the generic
  `IPipelineResultsSink` fan-out plan 025 D2 describes — the bridge, the NATS publisher, this router
  AND the gRPC fan-out (D1) are all consumers of the same topic now). The three relation-name-collision
  refusals now match Orleans exactly (`CatalogStore.cs`'s `"Name '{name}' is already used by a
  pipeline/table/stream source — a table's SQL resolves a relation name to exactly one entity"`); the
  old doc comment that called `trades FROM trades` legal was rewritten with the reason. **The one
  asymmetry this closes**: `POST /api/tables/validate` is no longer optimistic on Dapr — it used to
  offer pipelines as relations (shared endpoint code) while `CatalogStore.CompileTableSql`/
  `TableActor.TryCompile` refused the same SQL at create time; both now agree.

**Evidence**: manual, dated 2026-09-05 (orchestrator, isolated instance) — a table with
`pipelineInputs:["p"]` receives rows from the named pipeline; each of the three relation-name refusals
returns 409/400 with the Orleans-identical message. Automated: `PipelineResultMappingTests.cs`,
`TableEventRouterTests.cs`, `BootResumePlanTests.cs`, `ConnectorAttachStateTests.cs`,
`StreamingDaprStreamBridgeTests.cs`/`StreamingSourceRateSamplerTests.cs` (all in
`dapr/tests/StreamsForge.Dapr.Tests/`, no sidecar needed for any of them — the actor-level parts of
late-consumer replay and table-over-pipeline routing rest on the same "no live actor-level harness
exists" limitation D2 already states). A dedicated process-level live test for late-consumer replay and
table-over-pipeline is one of the items plan 025's wave 2 (agent L) is adding on the new isolated
instance — see section 3; this file does not claim that test's result.

### D8 · Isolated Dapr test instance — CLOSED (plan 025 D1/D8)

Was: every Dapr live check had to run against the shared, fixed-port `streamsforge-dapr` dev instance
(reset via `tools/reset.sh` first), because an arbitrary `--app-id` had no statestore component in
scope and panicked the 1.18 actor runtime — a much heavier gate than Orleans' arbitrary-port isolated
instances, and a large part of why Dapr live checks kept being skipped for 164+ commits (see section 3's
history below).

Now: `dapr/components-test/{statestore,pubsub,config}.yaml` scope a second app-id,
`streamsforge-dapr-test`, to its own statestore/pubsub, both pinned to Redis **logical database 1**
(`redisDB: "1"`) — the dev instance and the polyglot processors stay on database 0, untouched. Fixed
ports (an app-id is baked into actor state at the component level, not a per-instance parameter): app
`5799`, gRPC `5899` (this harness spawns the SAME `StreamsForge.Dapr.Host` binary the dev instance runs,
so once D1 landed this port is served here too, not merely reserved — `DaprHostProcess`'s own doc
comment predates that merge and still calls it reserved; flagged as stale below, not owned by this
wave), sidecar HTTP `3799` / gRPC `4799`. A THIRD thing had to be
isolated, found empirically rather than assumed: **actor placement is placement-global by actor TYPE
NAME, not scoped to app-id** — two app-ids hosting the identically-named actor types (`RegistryActor`,
`TableActor`, ...) against the SAME placement service produced outright cross-app breakage (measured:
placement disseminated one shared rebalance round covering both apps' types, an actor call from one app
timed out trying to reach the other app's host address, and one app's own read-after-write on its own
catalog came back empty mid-sequence) — not a hypothetical, reproduced live. So this test instance uses
a DEDICATED `dapr_placement_test` container on host port `6150`, never the shared `dapr_placement`
`dapr init` created. `docker exec dapr_redis redis-cli -n 1 FLUSHDB` is this instance's `reset.sh`
equivalent (`DaprHostProcess.ResetAsync`). The harness itself:
`dapr/tests/StreamsForge.Dapr.Live.Tests/DaprHostProcess.cs` (process-level, spawns a real `dapr
run`-wrapped host, ported from `orleans/tests/StreamsForge.Chain.Tests/HostProcess.cs`'s pattern), one
xunit collection (`DaprLiveTestCollection`) so no two instances of this fixed-identity harness ever run
concurrently.

**Evidence**: this item's own evidence IS the seven tests section 3 lists as passing against it — the
harness existing and being provably safe to reuse across a test run (isolated Redis DB, isolated
placement, no interference with the dev instance's 17 `streamsforge-dapr||...` keys) is what the rest
of section 3 depends on.

### D9 · TLS on the Dapr host — CLOSED (plan 025 G2)

Was: `dapr/PARITY.md`/AGENTS.md both said "Orleans only — the Dapr host is untouched and still
loopback-only."

Now: mirrors plan 024's Orleans shape exactly — `Tls:Enabled`, the standard
`Kestrel:Certificates:Default` section, both listeners (REST + gRPC), fail-fast startup with no
certificate, HSTS (not emitted for loopback), `OutboundTls.Configure` wired for the first time on this
flavor (previously `Tls:TrustedCaPath`/`Tls:AcceptAnyCertificate` were silently ignored here — a
federated `grpc` source pointed at a privately-signed peer simply failed with no way to fix it). The
Dapr-only fact, with no Orleans counterpart: the SIDECAR calls the app port for every actor
invocation and topic delivery, and daprd speaks plain `http://` unless told otherwise — a TLS app port
therefore also needs `dapr run … --app-protocol https` (`dapr/tools/run.sh`'s `DAPR_RUN_EXTRA_ARGS`
carries it, e.g. `DAPR_RUN_EXTRA_ARGS="--app-protocol https" ./tools/run.sh --Tls:Enabled true …`).
daprd does not verify the app's certificate on that channel, so a self-signed dev pair
(`tools/tls/dev-cert.sh`) needs nothing further.

**Evidence**: manual, dated 2026-09-05 (orchestrator, isolated instance) — https healthz 200, plain
http gets an empty reply (not a redirect — Kestrel simply has no cleartext listener), `https://`
endpoints reported in `/api/meta/instance`, `grpcurl -cacert` streaming over the TLS gRPC port, seeded
tables continuing to fill over https. A dedicated automated TLS live test on the new isolated instance
(`DaprHostProcess.TlsCertPath` exists in the harness today but its own doc comment calls it "inert —
shaped for a later wave", written before this TLS work landed) is one of the items plan 025's wave 2
(agent L) is adding — see section 3; this file does not claim that test's result, and the harness'
own doc comment is now stale (flagged, not owned by this wave).

### D10 · RID default — CLOSED (plan 025, found during TLS work)

Not in the original 2026-08-20 list — found while wiring TLS's client fixtures. `dapr/src/
StreamsForge.Dapr.Host/Publish.props`'s bare `dotnet publish` (no `-r`) fallback defaulted to
**linux-x64**, exactly the bug plan 024 had already fixed on the Orleans side — a Mac publishing this
host with no explicit RID got an unrunnable binary. Now defaults to `$(NETCoreSdkRuntimeIdentifier)`
like the Orleans host; `tools/publish.sh` and `deploy/dapr/Dockerfile.app` are unaffected because they
already pass `-r` explicitly.

### Explicitly NOT debt — checked and present on Dapr

Plan 014 database connectors, 015 entitlements/approvals/audit (`AccessPolicyActor`, `ApprovalActor`,
`AuditLogActor`, and the shared sweeper), 016 peer discovery + `@name` endpoints, 017 CDC, 018 FIX,
019 `fix-duplex`, 021 environments (`EnvironmentRegistryActor`, `CatalogFacadeFactory`). All wired in
`dapr/src/StreamsForge.Dapr.Host/Program.cs`. Present is not the same as verified — see section 3.

## 2b. Open debt — plan 026 (stream replay), Orleans-first by design

### D11 · Replay from a position — OPEN (plan 026 wave 1, 2026-09-14)

Orleans wave 1 gave every source driver (connector, generator, ingest) a producer-owned,
sequence-numbered replay log behind the plan 023 attach gate (`SourceReplayGate` over the shared
`ReplayLog<T>` in `shared/StreamsForge.AppCore/Streaming/`), and `IEntityStreamFacade` a replaying
overload (`SubscribeSourceAsync(env, name, ReplayFrom? from, onEvent(row, ts, position))`) that gRPC
`SubscribeSource{from_seq|from_timestamp_ms}` and the hub's `SubscribeSourceFrom` use. On Dapr,
`EntityStreamFanout` implements the overload by ACCEPTING AND IGNORING `from`: positions are counted
per subscription from 1, and the handle reports `Truncated` whenever a replay was actually asked for,
so a client is told rather than silently given live-only. `ConnectorActor` still uses plan 023's
`SourceReplayBuffer` (count-only ring, no positions). Closing this = switch the three Dapr drivers to
`ReplayLog`, add `ReplayFrom` to `IConnectorActor.BeginAttachAsync`, and route the fan-out through
the gate; the log and the wire contract are already shared, so it is wiring, not design.

**Wave 2 (2026-09-14) widened the same debt, same shape:** Orleans' `PipelineGrain` (result batches)
and classic-mode `TableGrain` (delta batches) now publish through a `ReplayGate<T>` too, gRPC
`SubscribePipeline`/`SubscribeTable` take `from_seq`/`from_timestamp_ms` and write `position`, and
`TableDefinition.ReplayFrom`/`PipelineDefinition.ReplayFrom` (keyed by input name) tell a table or
pipeline where each source/pipeline input starts when it (re)starts. On Dapr: the two facade
overloads accept and ignore `from` exactly like the source one; a definition with a non-empty
`replayFrom` STORES fine (config promotion round-trips) but `TableActor.StartAsync` refuses to run it
with a named reason, the same rule `shardBy` already follows, and `PipelineActor.StartAsync` refuses
identically. Wave 3 (persisted log) will add its line here.

---

---

## 3. Unverified — the honest state of every "verified live on Dapr" claim

### The window: `db01ab1` → `32ea87e` (history, unchanged by plan 025 — kept for the record)

Plan 009's `db01ab1` added two C# **optional parameters** to
`IConnectorActor.RecordSubscriberBatchAsync`. Dapr's actor-interface validation forbids optional
parameters outright and threw inside `MapActorsHandlers` during startup, so the host died before
serving its first request. It was found and fixed in plan 021 wave 1 (`32ea87e`) by making both
parameters required. **164 commits sit between those two** — the entire span during which the Dapr
host could not boot, so any live claim from that window is false regardless of what it says. This
plan (025) is what finally closes the resulting verification gap; the table below is the up-to-date
answer, not this history.

### What plan 025 actually ran live, by test name

| Area | Automated test | What it proves |
|---|---|---|
| Boot | `BootSmokeTests.Boot_smoke` | A fresh isolated instance boots, seeds, and answers healthy |
| Source exact counts | `SourceExactCountTests.File_source_500_rows_land_exactly_then_an_appended_200` | A `file` source delivers exactly the rows it should, twice in a row, no loss/dup |
| Source exact counts | `SourceExactCountTests.Folder_files_slipping_in_while_the_source_polls_all_land_exactly` | A `folder` source catches files added mid-poll, exactly |
| Source exact counts | `SourceExactCountTests.Url_json_array_rows_land_exactly_then_a_grown_dataset` | A `url` JSON-array source delivers exactly the rows it should as the dataset grows |
| Environments | `EnvironmentIsolationTests.Environments_are_separate_redis_keys_and_a_force_deleted_environment_does_not_reseed` | Plan 021's cross-flavor requirement, finally run on Dapr: two same-named tables in two environments are two Redis keys, not one filtered read; force-delete does not silently re-seed |
| Access/audit | `AccessAuditTests.Access_deny_grant_is_enforced_and_audited` | Plan 015's grant/deny/audit round trip, on Dapr, for the first time |
| Restart resume | `RestartResumeTests.Restart_resumes_running_tables_and_they_keep_filling` | A killed-and-restarted instance resumes each table at its PRE-restart status (not just "Running") and keeps consuming live deltas (`deltasIn` growth, not `rowCount` — checked, not assumed) |

Plus the manual, dated (2026-09-05) checks recorded per-item in section 2 above for D1 (gRPC), D6
(table-over-pipeline) and D9 (TLS) — real but not re-runnable from CI the way the table above is.

**Landing in the same plan, not yet claimed here**: plan 025's wave 2 (agent L) is adding process-level
live tests for late-consumer replay, table-over-pipeline, a boot-order-under-restart scenario, TLS, gRPC,
and two-direction Orleans↔Dapr federation (an Orleans peer reserved on `4999`/`5099`, silo
`14999`/`34999` — see AGENTS.md's port table) on this same isolated instance, concurrently with this
document's own wave. This file names them so a reader knows they are coming, without asserting they
passed — check `dapr/tests/StreamsForge.Dapr.Live.Tests/` directly, or plan 025's own outcomes section,
for their actual result.

### Still unverified after plan 025

| Plan | What | Why it's still open |
|---|---|---|
| 014/017 DB connectors + CDC | Live sweep on the Dapr flavor | Docker-gated on BOTH flavors (`DockerGate` skips without a daemon + local images) — plan 025 did not touch this, and it was never Dapr-specific debt |
| 018/019 FIX / `fix-duplex` | A real FIX session against a Dapr-hosted `ConnectorActor` | Never driven on this flavor; the Orleans live check used a real acceptor, Dapr's twin is unit-tested only |
| 009 NATS (incl. its `tls` config group) | A real NATS broker against a Dapr-hosted source/sink | Never driven on this flavor |
| 016 peer-name discovery FROM a Dapr instance | A Dapr instance resolving a peer by NAME (not just subscribing a known peer's gRPC entity, which IS proven — see plan 006 W6/D1 above) | Plan 025's federation addition (agent L, see above) targets exactly this; not yet confirmed |
| SignalR pacing, live | `SourceRateSampler`'s pacer against a real SignalR client under load | `StreamingSourceRateSamplerTests` proves the pure decision; nobody has watched a browser tape stay in order under a real firehose on Dapr |
| Pipeline watermark under transport lag | `Pipelines:TransportLagAllowanceMs` (default 4000) keeping a live batch admissible under CPU pressure | `TableOverPipelineTests.A_single_large_live_batch_into_a_pipeline_lands_in_full` pins the fixed shape at test-time load; the 15–25 % late rate seen on the seeded pipelines during whole-solution builds was measured once by hand, not by a test — `lateEvents` on the pipeline metrics is the number to watch (`dapr/ARCHITECTURE.md` § "The pipeline watermark versus the sidecar hop") |
| 015 D9 environment-scoped grants | A grant scoped to one environment | Not built on EITHER flavor yet — not Dapr-specific debt, listed here only because it is the other half of "environments are a namespace, not a security boundary" that plan 025's environment work (D8's `EnvironmentIsolationTests`) does not change |
| D2 table-over-table warm attach, live | Starting `hot_symbols` after `positions` is already warm | Closed in code (section 2), not yet run against the new isolated instance — distinct from D6's table-over-pipeline, which IS manually verified |

### Why this took until plan 025

Two obstacles, both now resolved for good (kept here so the reasoning survives, not as current
blockers): this machine had never run `dapr init` (now has — runtime 1.18.3, containers
`dapr_redis`/`dapr_placement`/`dapr_scheduler`), and the flavor had no isolated-instance mode (D8
above). Running the shared dev instance's own checks — `dapr/tools/reset.sh && dapr/tools/run.sh`,
:5399 — is still how to verify the SEEDED dev catalog end-to-end; the isolated instance is for tests
that must not disturb it.

Record what actually ran in each plan's own outcomes section, per flavor and by test name. "Verified
live" without a flavor and a name is the sentence that produced this document.
